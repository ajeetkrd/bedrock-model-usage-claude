#!/usr/bin/env python3
"""CDK entrypoint for the Bedrock usage tracking solution (Option B: S3 batch)."""
import os
import sys
from pathlib import Path

# Make 'stacks' importable when CDK runs `python3 infra/app.py` from the
# project root.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import aws_cdk as cdk

from stacks.usage_tracker_stack import BedrockModelUsageTrackerStack


app = cdk.App()

# Configuration (override via -c flags or environment variables)
account = os.environ.get("CDK_DEFAULT_ACCOUNT", "708895069383")
region = os.environ.get("CDK_DEFAULT_REGION", "ap-south-1")

# S3 bucket that Bedrock model-invocation logging delivers to. You already
# configured this in the Bedrock settings.
log_bucket_name = (
    app.node.try_get_context("logBucketName")
    or os.environ.get("LOG_BUCKET", "708895069383-berock-claude")
)

# Optional key prefix configured in the Bedrock logging settings (the part
# before "AWSLogs/"). Leave blank if you did not set one.
log_key_prefix = app.node.try_get_context("logKeyPrefix") or os.environ.get(
    "LOG_KEY_PREFIX", ""
)

# Account/region that own the logs (the prefix Bedrock writes encodes both).
log_account_id = (
    app.node.try_get_context("logAccountId")
    or os.environ.get("LOG_ACCOUNT_ID", account)
)
# Region partition(s) under BedrockModelInvocationLogs/ to scan. Claude Code
# uses cross-region inference profiles, so usage is logged under several region
# prefixes. Default "auto" discovers every region partition in the bucket; set
# a comma-separated list (e.g. "ap-south-1,eu-south-2,us-east-1") to pin them.
log_regions = (
    app.node.try_get_context("logRegions")
    or os.environ.get("LOG_REGIONS")
    or app.node.try_get_context("logRegion")  # backwards-compat single region
    or os.environ.get("LOG_REGION")
    or "auto"
)

# How often the batch rollup runs (minutes).
schedule_minutes = int(
    app.node.try_get_context("scheduleMinutes")
    or os.environ.get("SCHEDULE_MINUTES", "5")
)

# Email to receive threshold alerts
alert_email = app.node.try_get_context("alertEmail") or os.environ.get("ALERT_EMAIL", "")

# Per-user monthly budget in USD, summed across all models the user touched.
threshold_usd = float(
    app.node.try_get_context("thresholdUsd")
    or os.environ.get("THRESHOLD_USD", "20")
)

# Trigger the alert when spend crosses this fraction of the budget (0.0 - 1.0)
warn_ratio = float(
    app.node.try_get_context("warnRatio") or os.environ.get("WARN_RATIO", "0.8")
)

# Per-model price table: USD per 1,000,000 tokens [input, output], keyed by a
# lowercase substring of the modelId (checked in order). Mirrors the Bedrock
# Anthropic pricing table. Override at deploy time with -c modelPricing='{...}'
# or the MODEL_PRICING_JSON env var.
_pricing_override = app.node.try_get_context("modelPricing") or os.environ.get(
    "MODEL_PRICING_JSON"
)
if _pricing_override:
    model_pricing = (
        _pricing_override
        if isinstance(_pricing_override, dict)
        else __import__("json").loads(_pricing_override)
    )
else:
    model_pricing = {
        "opus": [5.0, 25.0],     # Opus 4.5 / 4.6 / 4.7 / 4.8
        "sonnet": [3.0, 15.0],   # Sonnet 4 / 4.5 / 4.6
        "haiku": [1.0, 5.0],     # Haiku 4.5
    }


# ---- Optional Identity Center enforcement (OFF + dry-run by default) ----
# When enabled, a user who reaches their monthly budget is removed from the
# Identity Center group that grants Bedrock access. This is reversible
# (re-add the membership) but touches authentication, so it is inert unless you
# explicitly enable it AND turn off dry-run.
def _ctx_bool(key: str, env: str, default: bool) -> bool:
    raw = app.node.try_get_context(key)
    if raw is None:
        raw = os.environ.get(env)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


enforce_enabled = _ctx_bool("enforceEnabled", "ENFORCE_ENABLED", False)
# Dry-run defaults to TRUE so the first enabled deploy only logs "would remove".
enforce_dry_run = _ctx_bool("enforceDryRun", "ENFORCE_DRY_RUN", True)
identity_store_id = (
    app.node.try_get_context("identityStoreId")
    or os.environ.get("IDENTITY_STORE_ID", "d-9066174cc7")
)
# Identity Center / Identity Store region (the SSO instance's region). This can
# differ from the stack region — here the store is in us-east-1 while the stack
# runs in ap-south-1.
identity_store_region = (
    app.node.try_get_context("identityStoreRegion")
    or os.environ.get("IDENTITY_STORE_REGION", "us-east-1")
)
# One or more Identity Center groups that grant Bedrock access. An over-budget
# user is removed from every group listed. Accept comma-separated names and/or
# ids; the singular legacy keys still work.
enforce_group_ids = (
    app.node.try_get_context("enforceGroupIds")
    or app.node.try_get_context("enforceGroupId")
    or os.environ.get("ENFORCE_GROUP_IDS")
    or os.environ.get("ENFORCE_GROUP_ID", "")
)
enforce_group_names = (
    app.node.try_get_context("enforceGroupNames")
    or app.node.try_get_context("enforceGroupName")
    or os.environ.get("ENFORCE_GROUP_NAMES")
    or os.environ.get("ENFORCE_GROUP_NAME", "claude")
)
enforce_user_attribute = (
    app.node.try_get_context("enforceUserAttribute")
    or os.environ.get("ENFORCE_USER_ATTRIBUTE", "userName")
)
# Comma-separated user labels that are never auto-removed (leads, svc accounts).
enforce_allowlist = (
    app.node.try_get_context("enforceAllowlist")
    or os.environ.get("ENFORCE_ALLOWLIST", "")
)

BedrockModelUsageTrackerStack(
    app,
    "BedrockModelUsageTrackerStack",
    env=cdk.Environment(account=account, region=region),
    log_bucket_name=log_bucket_name,
    log_key_prefix=log_key_prefix,
    log_account_id=log_account_id,
    log_regions=log_regions,
    schedule_minutes=schedule_minutes,
    alert_email=alert_email,
    threshold_usd=threshold_usd,
    warn_ratio=warn_ratio,
    model_pricing=model_pricing,
    enforce_enabled=enforce_enabled,
    enforce_dry_run=enforce_dry_run,
    identity_store_id=identity_store_id,
    identity_store_region=identity_store_region,
    enforce_group_ids=enforce_group_ids,
    enforce_group_names=enforce_group_names,
    enforce_user_attribute=enforce_user_attribute,
    enforce_allowlist=enforce_allowlist,
)

app.synth()
