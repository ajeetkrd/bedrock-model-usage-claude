"""Stack: Bedrock invocation logs in S3 -> batch token tracking + alerts + dashboard.

Option B architecture:

    Bedrock model invocation logging (S3 destination)
        └─► S3 bucket: <log_bucket_name>/<prefix>/AWSLogs/.../BedrockModelInvocationLogs/...
                └─► EventBridge schedule (every 5 minutes)
                        └─► Lambda: UsageBatchProcessor
                                ├─► DynamoDB usage table (per-user/model/month counters)
                                ├─► DynamoDB state table (checkpoint + per-object dedup)
                                ├─► CloudWatch EMF metrics (per-model + total only)
                                └─► SNS topic ─► email alerts

There is no per-invocation Lambda and no high-cardinality (per-user) custom
metrics, so cost stays flat as the number of developers grows.

The budget is enforced in DOLLARS: a per-user monthly cap (default $20) that
spans every model the user touched. Token counts are stored per (user, model,
month); dollar cost is derived from a configurable per-model price table.
"""
import json
from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_dynamodb as ddb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from constructs import Construct


LAMBDA_DIR = Path(__file__).resolve().parents[2] / "lambdas"


class BedrockModelUsageTrackerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        log_bucket_name: str,
        log_key_prefix: str,
        log_account_id: str,
        log_regions: str,
        alert_email: str,
        threshold_usd: float,
        warn_ratio: float,
        model_pricing: dict[str, list[float]],
        schedule_minutes: int = 5,
        enforce_enabled: bool = False,
        enforce_dry_run: bool = True,
        identity_store_id: str = "",
        identity_store_region: str = "",
        enforce_group_ids: str = "",
        enforce_group_names: str = "",
        enforce_user_attribute: str = "userName",
        enforce_allowlist: str = "",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        pricing_json = json.dumps(model_pricing)

        # ---------------- DynamoDB: usage counters ----------------
        # PK = USER#<identityArn>   SK = MODEL#<modelId>#MONTH#<YYYY-MM>
        # GSI swaps PK/SK so we can query "top users for a model in a month".
        usage_table = ddb.Table(
            self,
            "UsageTable",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )
        usage_table.add_global_secondary_index(
            index_name="byModelMonth",
            partition_key=ddb.Attribute(name="modelMonth", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="totalTokens", type=ddb.AttributeType.NUMBER),
            projection_type=ddb.ProjectionType.ALL,
        )

        # ---------------- DynamoDB: batch state (checkpoint + dedup markers) ----------------
        # PK = STATE | OBJ#<s3key>   SK = CHECKPOINT | MARKER
        # Per-object markers carry a TTL so the table self-cleans.
        state_table = ddb.Table(
            self,
            "BatchStateTable",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="expireAt",
        )

        # ---------------- SNS topic for email alerts ----------------
        alert_topic = sns.Topic(
            self,
            "UsageAlertTopic",
            display_name="Bedrock Usage Threshold Alerts",
        )
        if alert_email:
            alert_topic.add_subscription(sns_subs.EmailSubscription(alert_email))

        # ---------------- Existing S3 log bucket (Bedrock logging destination) ----------------
        # The bucket is managed outside this stack (you already pointed Bedrock
        # model-invocation logging at it), so we import it by name.
        log_bucket = s3.Bucket.from_bucket_name(
            self, "BedrockLogBucket", log_bucket_name
        )

        # ---------------- Batch processor Lambda ----------------
        batch_fn = _lambda.Function(
            self,
            "UsageBatchProcessorFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="usage_batch_processor.handler",
            code=_lambda.Code.from_asset(str(LAMBDA_DIR / "usage_batch_processor")),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "USAGE_TABLE": usage_table.table_name,
                "STATE_TABLE": state_table.table_name,
                "LOG_BUCKET": log_bucket_name,
                "LOG_KEY_PREFIX": log_key_prefix,
                "LOG_ACCOUNT_ID": log_account_id,
                "LOG_REGIONS": log_regions,
                "ALERT_TOPIC_ARN": alert_topic.topic_arn,
                "THRESHOLD_USD": str(threshold_usd),
                "WARN_RATIO": str(warn_ratio),
                "MODEL_PRICING_JSON": pricing_json,
                "METRICS_NAMESPACE": "BedrockUsage",
                # Re-scan window each run; overlap is deduped via object markers.
                "LOOKBACK_MINUTES": str(max(schedule_minutes * 3, 15)),
                # ---- Optional Identity Center enforcement (off + dry-run by default) ----
                "ENFORCE_ENABLED": str(enforce_enabled).lower(),
                "ENFORCE_DRY_RUN": str(enforce_dry_run).lower(),
                "IDENTITY_STORE_ID": identity_store_id,
                "IDENTITY_STORE_REGION": identity_store_region,
                "ENFORCE_GROUP_IDS": enforce_group_ids,
                "ENFORCE_GROUP_NAMES": enforce_group_names,
                "ENFORCE_USER_ATTRIBUTE": enforce_user_attribute,
                "ENFORCE_ALLOWLIST": enforce_allowlist,
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        usage_table.grant_read_write_data(batch_fn)
        state_table.grant_read_write_data(batch_fn)
        log_bucket.grant_read(batch_fn)
        alert_topic.grant_publish(batch_fn)

        # ---------------- Identity Center enforcement IAM (only if enabled) ----------------
        # Identity Store read/membership actions don't support resource-level
        # scoping, so they're granted on "*". DeleteGroupMembership is the only
        # mutating action and is gated at runtime by ENFORCE_ENABLED + dry-run +
        # allowlist + idempotency markers. We attach the policy only when the
        # feature is enabled so a default deploy carries no IAM power to mutate
        # Identity Center.
        if enforce_enabled:
            batch_fn.add_to_role_policy(
                iam.PolicyStatement(
                    sid="IdentityCenterReadResolve",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "identitystore:GetUserId",
                        "identitystore:GetGroupId",
                        "identitystore:GetGroupMembershipId",
                        "identitystore:DescribeUser",
                        "identitystore:IsMemberInGroups",
                    ],
                    resources=["*"],
                )
            )
            batch_fn.add_to_role_policy(
                iam.PolicyStatement(
                    sid="IdentityCenterRemoveMembership",
                    effect=iam.Effect.ALLOW,
                    actions=["identitystore:DeleteGroupMembership"],
                    resources=["*"],
                )
            )

        # ---------------- EventBridge schedule: every N minutes ----------------
        schedule_rule = events.Rule(
            self,
            "UsageBatchSchedule",
            schedule=events.Schedule.rate(Duration.minutes(schedule_minutes)),
            description=f"Roll up Bedrock S3 invocation logs every {schedule_minutes} min",
        )
        schedule_rule.add_target(targets.LambdaFunction(batch_fn))

        # ---------------- Dashboard reader Lambda (leaderboard widget) ----------------
        reader_fn = _lambda.Function(
            self,
            "UsageReaderFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="usage_reader.handler",
            code=_lambda.Code.from_asset(str(LAMBDA_DIR / "usage_reader")),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "USAGE_TABLE": usage_table.table_name,
                "THRESHOLD_USD": str(threshold_usd),
                "WARN_RATIO": str(warn_ratio),
                "MODEL_PRICING_JSON": pricing_json,
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        usage_table.grant_read_data(reader_fn)

        # ---------------- CloudWatch Dashboard ----------------
        ns = "BedrockUsage"

        total_tokens_metric = cw.Metric(
            namespace=ns,
            metric_name="TotalTokens",
            statistic="Sum",
            period=Duration.minutes(5),
        )
        input_tokens_metric = cw.Metric(
            namespace=ns,
            metric_name="InputTokens",
            statistic="Sum",
            period=Duration.minutes(5),
        )
        output_tokens_metric = cw.Metric(
            namespace=ns,
            metric_name="OutputTokens",
            statistic="Sum",
            period=Duration.minutes(5),
        )

        # Per model only (per-user metrics intentionally dropped to keep custom
        # metric cardinality — and cost — flat at any number of developers).
        per_model_search = cw.MathExpression(
            expression='SEARCH(\'{BedrockUsage,ModelId} MetricName="TotalTokens"\', \'Sum\', 300)',
            label="Tokens per model",
            period=Duration.minutes(5),
        )
        invocations_per_model = cw.MathExpression(
            expression='SEARCH(\'{BedrockUsage,ModelId} MetricName="Invocations"\', \'Sum\', 300)',
            label="Invocations per model",
            period=Duration.minutes(5),
        )
        cost_total_metric = cw.Metric(
            namespace=ns,
            metric_name="CostUSD",
            statistic="Sum",
            period=Duration.minutes(5),
            label="Total cost (USD)",
        )
        cost_per_model_search = cw.MathExpression(
            expression='SEARCH(\'{BedrockUsage,ModelId} MetricName="CostUSD"\', \'Sum\', 300)',
            label="Cost per model (USD)",
            period=Duration.minutes(5),
        )

        dashboard = cw.Dashboard(
            self,
            "BedrockUsageDashboard",
            dashboard_name="BedrockUsageDashboard",
            period_override=cw.PeriodOverride.AUTO,
            default_interval=Duration.days(1),
        )

        dashboard.add_widgets(
            cw.TextWidget(
                markdown=(
                    "# Bedrock Usage Dashboard\n"
                    f"Per-user monthly budget: **${threshold_usd:,.2f}** across all models. "
                    f"Email alert fires at **{int(warn_ratio*100)}%** of budget.\n\n"
                    f"Batch rollup from S3 logs in `s3://{log_bucket_name}` "
                    f"every **{schedule_minutes} minutes**. Per-user spend is exact "
                    "in DynamoDB (table below); metric graphs are per-model only."
                ),
                width=24,
                height=3,
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Total tokens (sum, 5m)",
                left=[input_tokens_metric, output_tokens_metric, total_tokens_metric],
                width=12,
                height=6,
                stacked=False,
            ),
            cw.GraphWidget(
                title="Tokens per model",
                left=[per_model_search],
                width=12,
                height=6,
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Cost (USD, sum, 5m)",
                left=[cost_total_metric],
                width=12,
                height=6,
            ),
            cw.GraphWidget(
                title="Cost per model (USD)",
                left=[cost_per_model_search],
                width=12,
                height=6,
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Invocations per model",
                left=[invocations_per_model],
                width=24,
                height=6,
            ),
        )

        # Custom widget: top users per model for the current month, read from DynamoDB
        dashboard.add_widgets(
            cw.CustomWidget(
                title="Top users per model — current month (DynamoDB)",
                function_arn=reader_fn.function_arn,
                width=24,
                height=10,
                update_on_refresh=True,
                update_on_resize=True,
                update_on_time_range_change=False,
            ),
        )

        # ---------------- Stack outputs ----------------
        CfnOutput(self, "DashboardName", value=dashboard.dashboard_name)
        CfnOutput(self, "UsageTableName", value=usage_table.table_name)
        CfnOutput(self, "StateTableName", value=state_table.table_name)
        CfnOutput(self, "AlertTopicArn", value=alert_topic.topic_arn)
        CfnOutput(self, "BatchProcessorLambdaName", value=batch_fn.function_name)
        CfnOutput(self, "LogBucketName", value=log_bucket_name)
        # Surfaced so the Streamlit app can auto-load the same budget + pricing
        # the stack was deployed with (instead of its own hardcoded defaults).
        CfnOutput(self, "ThresholdUsd", value=str(threshold_usd))
        CfnOutput(self, "WarnRatio", value=str(warn_ratio))
        CfnOutput(self, "ModelPricingJson", value=pricing_json)
        if enforce_enabled:
            CfnOutput(
                self,
                "EnforcementStatus",
                value=(
                    f"ENABLED, dry_run={str(enforce_dry_run).lower()}, "
                    f"groups={enforce_group_names or enforce_group_ids or '<unset>'}"
                ),
            )
