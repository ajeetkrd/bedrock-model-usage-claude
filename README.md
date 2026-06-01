# Bedrock Usage Tracker

Bedrock model usage tracking of Bedrock model invocations from the S3 logs that Amazon
Bedrock model-invocation logging delivers, with per-user-per-model token
counters in DynamoDB, **dollar-based budget alerts**, and a CloudWatch
dashboard.

Design: it scales flat with the number of
developers because there is no per-invocation Lambda and no high-cardinality
(per-user) custom metrics. At 500 developers the monitoring layer costs
**~$50/month** (see [Cost at 500 developers](#cost-at-500-developers)).

## Architecture

```
Bedrock Model Invocation Logging (S3 destination)
    └─► S3: <log_bucket>/<prefix>/AWSLogs/<acct>/BedrockModelInvocationLogs/<region>/YYYY/MM/DD/HH/*.json.gz
            │   (one partition per region — Claude Code cross-region inference
            │    profiles span several regions; the processor scans them all)
            └─► EventBridge schedule (every 5 minutes)
                    └─► Lambda: UsageBatchProcessor
                            ├─► DynamoDB UsageTable  : per-user/model/month token counters
                            ├─► DynamoDB StateTable  : checkpoint + per-object dedup markers
                            ├─► CloudWatch EMF metrics (per-model + total: tokens, invocations, CostUSD)
                            ├─► SNS topic ─► Email subscription (dollar-budget alerts)
                            └─► (optional) Identity Center: remove over-budget user from group

CloudWatch Dashboard "BedrockUsageDashboard"
    ├─► Metric widgets (tokens, cost USD, invocations — per model + total)
    └─► Custom widget: top users by spend vs budget (Lambda reads DynamoDB)

Streamlit app (streamlit_app/)
    └─► Reads DynamoDB directly: per-user spend vs $ budget, charts, bulk emails
```

Every 5 minutes the batch processor lists the S3 objects written since its last
checkpoint, parses the gzipped JSON record batches, and rolls them up into the
DynamoDB schema the dashboard and Streamlit app share.

### Budget model: dollars, not tokens

The cap is a **per-user monthly budget in USD (default `$20`), summed across
every model the user touched.** A developer who splits work across Opus,
Sonnet, and Haiku has all of their spend totaled against the single budget.

Token counts are stored per `(user, model, month)`; dollar cost is **derived**
from a configurable per-model price table, so pricing has a single source of
truth and there is no floating-point accumulation in DynamoDB.

Default price table (USD per 1,000,000 tokens, base input/output rates),
matched by lowercase substring of the `modelId`:

| Match (substring) | Input / 1M | Output / 1M | Covers |
| ----------------- | ---------: | ----------: | ------ |
| `opus`            | $5.00      | $25.00      | Opus 4.5 / 4.6 / 4.7 / 4.8 |
| `sonnet`          | $3.00      | $15.00      | Sonnet 4 / 4.5 / 4.6 |
| `haiku`           | $1.00      | $5.00       | Haiku 4.5 |

Unrecognized models fall back to Sonnet-level rates ($3/$15). Override the whole
table at deploy time with `-c modelPricing='{"opus":[5,25],...}'` or the
`MODEL_PRICING_JSON` environment variable. The same table is used by the batch
processor, the dashboard reader Lambda, and the Streamlit app.

### Prompt-cache tokens

Bedrock logs report prompt-caching tokens separately from raw input/output, and
they are billed:

- **cache-write** (cache creation): `input.cacheWriteInputTokenCount`
- **cache-read** (cache hit): `input.cacheReadInputTokenCount`

These matter a lot for cache-heavy clients like Claude Code, where cache-write
tokens routinely **exceed** the raw `inputTokenCount` — ignoring them badly
undercounts both tokens and cost. The processor reads, stores, and prices all
four token types.

Cache rates default to multiples of each model's **base input** rate:

| Token type  | Rate (relative to input) | Default multiplier |
| ----------- | ------------------------ | -----------------: |
| cache-write | input × 1.25             | `CACHE_WRITE_MULTIPLIER` |
| cache-read  | input × 0.10             | `CACHE_READ_MULTIPLIER`  |

Override the multipliers with the `CACHE_WRITE_MULTIPLIER` / `CACHE_READ_MULTIPLIER`
environment variables, or set explicit per-model rates by giving a **4-element**
pricing entry `[input, output, cache_write, cache_read]` instead of the 2-element
`[input, output]` form, e.g. `-c modelPricing='{"opus":[5,25,6.25,0.5]}'`.

> The default multipliers approximate Anthropic's published cache pricing.
> Confirm the exact Bedrock cache rates for your models and pin them with
> 4-element entries if you need billing-grade accuracy.


## Exactly-once accounting

Schedule windows intentionally overlap (`LOOKBACK_MINUTES` defaults to 3× the
schedule), and Lambdas can retry, so the processor dedupes work using the
`StateTable`:

- a **checkpoint** item (`pk=STATE, sk=CHECKPOINT`) bounds how far back each run
  lists objects;
- one **marker** item per processed S3 object key (`pk=OBJ#<key>, sk=MARKER`)
  with a TTL (`expireAt`, default 45 days).

An object is counted only if it has no marker, and its marker is written only
after all its increments land. A crash midway through one object can re-count
that single object on the next run — a bounded, rare error that is acceptable
for a usage tracker.

## What gets tracked

Each Bedrock invocation log record provides:

- `input.inputTokenCount`, `output.outputTokenCount`
- `input.cacheWriteInputTokenCount`, `input.cacheReadInputTokenCount` (prompt cache)
- `identity.arn` (the IAM principal that called Bedrock)
- `modelId`
- `timestamp` (used to derive the `YYYY-MM` month bucket, UTC)

The processor derives a friendly user label from the ARN, then per S3 object:

1. Aggregates tokens/invocations per `(user, model, month)` in memory.
2. Atomically `ADD`s those deltas to the DynamoDB row keyed by `(user, model,
   month)`.
3. Emits per-model + total CloudWatch EMF metrics for the batch, including a
   `CostUSD` metric derived from the price table.
4. Computes each touched user's **total monthly spend across all models**
   (including prompt-cache tokens) and, if it crosses 80% of the budget
   (warning) or the budget itself (breach), publishes an email via SNS. The
   warn/breach spend email fires once per `(user, month)` boundary crossing.
   While a user is **at/over budget**, if enforcement is enabled (off by
   default) it runs every rollup — but only acts when the user is still a group
   member, and sends a `REMOVED` / `DRYRUN` / `ERROR` enforcement email — see
   [Enforcement](#enforcement-remove-over-budget-users-from-a-group-optional).

## DynamoDB schema

### UsageTable

| Attribute      | Type   | Notes                                                      |
| -------------- | ------ | ---------------------------------------------------------- |
| `pk`           | String | `USER#<user>`                                              |
| `sk`           | String | `MODEL#<modelId>#MONTH#<YYYY-MM>`                          |
| `user`         | String | Friendly user label (last segment of identity ARN)         |
| `modelId`      | String | Bedrock model id                                           |
| `month`        | String | `YYYY-MM` UTC                                              |
| `modelMonth`   | String | `MODEL#<modelId>#MONTH#<YYYY-MM>` (GSI partition key)      |
| `totalTokens`  | Number | input + output + cache-write + cache-read                  |
| `inputTokens`  | Number |                                                            |
| `outputTokens` | Number |                                                            |
| `cacheWriteTokens` | Number | Prompt-cache creation tokens                           |
| `cacheReadTokens`  | Number | Prompt-cache read (hit) tokens                          |
| `invocations`  | Number |                                                            |
| `lastUpdated`  | Number | epoch seconds                                              |

GSI `byModelMonth`: PK `modelMonth`, SK `totalTokens` — supports "top N users
for model X this month" queries. Dollar cost is computed at read time from
`inputTokens` / `outputTokens` / `cacheWriteTokens` / `cacheReadTokens`, not
stored.

### StateTable

| Attribute         | Type   | Notes                                              |
| ----------------- | ------ | -------------------------------------------------- |
| `pk`              | String | `STATE` / `OBJ#<s3key>` / `ENFORCE#<user>`         |
| `sk`              | String | `CHECKPOINT` / `MARKER` / `MONTH#<YYYY-MM>`        |
| `lastProcessedMs` | Number | checkpoint high-water mark (epoch ms)              |
| `processedAt`     | Number | marker write time (epoch seconds)                  |
| `mode`            | String | enforce audit: `REMOVED`/`DRYRUN`/`SKIPPED`/`ERROR`|
| `expireAt`        | Number | marker TTL (epoch seconds)                         |

## Prerequisites

1. Bedrock model-invocation logging enabled with an **S3 destination**. This
   project assumes bucket `<awsaccountID>-berock-claude` in `ap-south-1`.
2. The bucket policy allows `bedrock.amazonaws.com` to `s3:PutObject` under
   `.../AWSLogs/<accountId>/BedrockModelInvocationLogs/*` (AWS attaches this for
   you when you configure logging via the console).
3. Python 3.11+, Node.js, AWS CDK v2 (`npm i -g aws-cdk`), credentials for
   account `<awsaccountID>` with deploy permission in `ap-south-1`.

## Deploy

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk bootstrap aws://<awsaccountID>/ap-south-1   # one-time per account/region

cdk deploy \
  -c alertEmail=you@example.com \
  -c thresholdUsd=20 \
  -c warnRatio=0.8 \
  -c logBucketName=<awsaccountID>-berock-claude \
  -c logKeyPrefix= \
  -c scheduleMinutes=5
```
##Example
```bash
cdk deploy BedrockModelUsageTrackerStack -c logBucketName=<accountID>-berock-claude -c logKeyPrefix= -c logAccountId=<accountID> -c logRegions=auto -c scheduleMinutes=2 -c alertEmail=ajeetkrd@amazon.com -c thresholdUsd=0.01 -c warnRatio=0.8 -c modelPricing='{"opus":[5,25],"sonnet":[3,15],"haiku":[1,5]}' -c enforceEnabled=true -c enforceDryRun=false -c identityStoreId=d-9066174cc7 -c identityStoreRegion=us-east-1 -c enforceGroupNames=claude -c enforceGroupIds= -c enforceUserAttribute=userName -c enforceAllowlist=
```

After the first deploy, AWS sends an SNS confirmation email — click the link to
start receiving alerts.

> Note: if your environment hits a jsii cache permission error during synth,
> export `JSII_RUNTIME_PACKAGE_CACHE_ROOT="$(mktemp -d)"` first. If `cdk.out/`
> is locked, synth to a fresh dir with `-o ./cdk.out.new`.

## Configuration

| Context key       | Env var             | Default                      | Purpose                                              |
| ----------------- | ------------------- | ---------------------------- | ---------------------------------------------------- |
| `logBucketName`   | `LOG_BUCKET`        | `<awsaccountID>-berock-claude` | S3 bucket Bedrock logs are delivered to.             |
| `logKeyPrefix`    | `LOG_KEY_PREFIX`    | (empty)                      | Prefix before `AWSLogs/` if you set one in Bedrock.  |
| `logAccountId`    | `LOG_ACCOUNT_ID`    | deploy account               | Account id embedded in the S3 key path.              |
| `logRegions`      | `LOG_REGIONS`       | `auto`                       | Region partitions to scan. `auto` discovers all; or comma-separated list. |
| `scheduleMinutes` | `SCHEDULE_MINUTES`  | `5`                          | How often the batch rollup runs.                     |
| `alertEmail`      | `ALERT_EMAIL`       | (none)                       | Email subscribed to the SNS alert topic.             |
| `thresholdUsd`    | `THRESHOLD_USD`     | `20`                         | Per-user monthly budget in USD (all models).         |
| `warnRatio`       | `WARN_RATIO`        | `0.8`                        | Fraction of the budget that triggers a warn.         |
| `modelPricing`    | `MODEL_PRICING_JSON`| (built-in table)             | Override per-model pricing: `[input,output]` or `[input,output,cacheWrite,cacheRead]` USD per 1M tokens.|
| `cacheWriteMultiplier` | `CACHE_WRITE_MULTIPLIER` | `1.25`              | Cache-write rate as a multiple of base input (when not set explicitly). |
| `cacheReadMultiplier`  | `CACHE_READ_MULTIPLIER`  | `0.10`              | Cache-read rate as a multiple of base input (when not set explicitly). |
| `enforceEnabled`  | `ENFORCE_ENABLED`   | `false`                      | Master switch for Identity Center group removal on breach. |
| `enforceDryRun`   | `ENFORCE_DRY_RUN`   | `true`                       | When enabled, only logs "would remove" — makes no change. |
| `identityStoreId` | `IDENTITY_STORE_ID` | `d-9066174cc7`               | Identity Store id (`d-xxxx`) for enforcement.        |
| `identityStoreRegion` | `IDENTITY_STORE_REGION` | `us-east-1`            | Region of the SSO instance (may differ from stack region). |
| `enforceGroupNames`| `ENFORCE_GROUP_NAMES`| `claude`                    | Comma-separated group display names to remove from.  |
| `enforceGroupIds` | `ENFORCE_GROUP_IDS` | (none)                       | Comma-separated group ids (skip name lookup).        |
| `enforceUserAttribute` | `ENFORCE_USER_ATTRIBUTE` | `userName`         | Identity Store attribute the `user` label matches.   |
| `enforceAllowlist`| `ENFORCE_ALLOWLIST` | (empty)                      | Comma-separated users never auto-removed.            |
| `enforceRenotifyHours` | `ENFORCE_RENOTIFY_HOURS` | `24`               | Suppress repeat dry-run/error enforcement emails within this window. |

Override at deploy time with `-c key=value`.

The batch Lambda also reads `LOOKBACK_MINUTES` (defaults to `max(3×schedule,
15)`), `MAX_LOOKBACK_MINUTES` (7 days), and `MARKER_TTL_DAYS` (45) from its
environment; all are set by the CDK stack and rarely need changing.

## Enforcement: remove over-budget users from a group (optional)

When a user's monthly spend **reaches the budget (BREACH)**, the processor can
remove them from the IAM Identity Center group that grants Bedrock access. This
is **reversible** (re-add the membership) and scoped to one group — it does not
delete the user or revoke their other SSO access.

> Identity Center has **no API to "disable" a user**; the only programmatic
> options are deleting the user (nuclear, and blocked when SCIM-synced) or
> removing a group membership. This feature uses the latter
> (`identitystore:DeleteGroupMembership`).

**Safety model — the feature is inert unless you opt in:**

1. **`enforceEnabled=false` by default.** A normal deploy attaches no IAM power
   to mutate Identity Center; the enforcement IAM policy is only created when
   enabled.
2. **`enforceDryRun=true` by default.** Even when enabled, the first rollout
   only logs `enforce[DRY_RUN]: WOULD remove ...` and writes a dry-run audit
   record. No membership is changed until you set `enforceDryRun=false`.
3. **Allowlist.** `enforceAllowlist` users (leads, service accounts, yourself)
   are never removed.
4. **Reconciling, not one-shot.** Enforcement re-checks an over-budget user
   every rollup, but `DeleteGroupMembership` only fires when they are still a
   member. A removed user makes no new Bedrock calls, so they generate no new
   logs and are not re-evaluated; a user who is **manually re-added** and keeps
   spending is removed again. The `ENFORCE#<user>` / `MONTH#<month>` marker is
   used to throttle repeat dry-run/error emails (see `enforceRenotifyHours`),
   not to permanently block re-enforcement.
5. **Audit trail.** Every decision (`REMOVED` / `DRYRUN` / `SKIPPED` / `ERROR`)
   is written to the state table and logged, and the actionable outcomes email
   an operator.
6. **Never breaks the rollup.** Enforcement runs in a guarded try/except; any
   failure is logged and token accounting continues.

**Recommended rollout:**

```bash
# 1) Enable in dry-run and watch the logs for a cycle (no changes made).
cdk deploy \
  -c enforceEnabled=true \
  -c identityStoreId=d-xxxxxxxxxx \
  -c enforceGroupName=BedrockAccess \
  -c enforceAllowlist=alice,svc-ci

# 2) Once the "WOULD remove" logs look correct, flip dry-run off.
cdk deploy ... -c enforceDryRun=false
```

**Caveats:**

- The `user` label (last segment of the assumed-role ARN) must map **exactly**
  to the Identity Store attribute named by `enforceUserAttribute` (default
  `userName`). A wrong mapping risks removing the wrong person — validate in
  dry-run first.
- If Identity Center is **SCIM-synced** from an external IdP, a group change may
  be reverted on the next sync; manage the group in the IdP instead, or revoke
  the account assignment.
- Re-arming: enforcement reconciles continuously. If you re-add a user to the
  group while they are still over budget for the month, they will be removed
  again on the next rollup. Access is only restored for real once their spend
  resets next month (or you raise the budget / add them to `enforceAllowlist`).

## Dashboard

Find it in the CloudWatch console under **Dashboards → BedrockUsageDashboard**
(region `ap-south-1`). It contains:

- Total tokens (input/output/cache-write/cache-read/total) — 5 minute resolution
- Cost (USD) — total and per model (includes prompt-cache token cost)
- Tokens per model and invocations per model
- Custom widget: top users by **monthly spend vs the $ budget**, plus a
  per-model cost breakdown, sourced from DynamoDB so figures are exact.

For rich per-user analytics (search, heatmap, bulk usage emails), use the
Streamlit app in `streamlit_app/`. It shows per-user spend vs budget, cost
charts, a user×model cost heatmap, and can publish per-user spend notices to
the same SNS topic.

## Cost at 500 developers

This is the cost of the **tracking solution** (AWS plumbing), not the Bedrock
token spend it measures. With a `$20` per-user budget the usage being tracked is
~`500 × $20 = $10,000/month`; the monitoring layer is a small fraction of that.

**Workload assumptions**

| Assumption                       | Value                              |
| -------------------------------- | ---------------------------------- |
| Developers                       | 500                                |
| Invocations / dev / workday      | ~300 (Claude Code is chatty)       |
| Invocations / month              | ~3.3M (500 × 300 × 22)             |
| Avg gzipped log record           | ~20 KB (large context)             |
| Models tracked                   | ~3–5                               |
| Schedule                         | every 5 min ≈ 8,640 runs/month     |

**Estimated monthly cost (solution only)**

| Component                  | Driver                                                 | Est. cost     |
| -------------------------- | ------------------------------------------------------ | ------------- |
| CloudWatch custom metrics  | ~30 metrics (per-model + total, low cardinality)       | ~$9           |
| S3 storage                 | ~66 GB/mo ingested; ~200 GB steady-state w/ lifecycle  | ~$5           |
| S3 requests                | Bedrock PUTs + Lambda GET/LIST                         | $5–15         |
| DynamoDB (on-demand)       | coalesced writes + per-user alert queries + markers    | $10–20        |
| Lambda                     | 8,640 runs, 512 MB, mostly free tier                   | $0–5          |
| CloudWatch Logs            | only the batch Lambda's own logs (Bedrock logs → S3)   | $2–5          |
| CloudWatch dashboard       | 1 dashboard (first 3 free)                             | $0            |
| SNS email                  | a few hundred alerts (first 1,000 free)                | ~$0           |
| EventBridge schedule       | scheduled rules are free                               | $0            |
| **Total (solution)**       |                                                        | **~$35–65/mo**|



**What moves the monitoring cost of this solution**

- **Volume is the lever.** At ~1,000 invocations/dev/day the S3 + DynamoDB lines
  roughly triple, pushing the total toward ~$120–150/month — still trivial
  against the ~$10k/month of Bedrock model usage being tracked.
- **S3 storage is cumulative.** Add an S3 lifecycle rule to expire raw logs
  after 60–90 days or storage creeps up over the year. The bucket is managed
  outside this stack, so apply the rule on the bucket directly.
- **Throughput, not cost, is the scaling risk.** If a single 5-min run has too
  many objects to read within the Lambda's 5-minute timeout, shorten the window
  or shard by prefix. This affects latency/throughput, not the bill.

## Operational notes

- **Latency**: usage and alerts lag by up to one schedule interval (5 min).
- **Multi-region logs**: Claude Code uses cross-region inference profiles, so a
  user's calls are logged under several region partitions
  (`.../BedrockModelInvocationLogs/<region>/...`), e.g. `eu-south-2`,
  `us-east-1`, `ap-south-1`. The processor scans **all** region partitions by
  default (`LOG_REGIONS=auto`); pin them with a comma-separated `logRegions`
  list if you prefer. A single-region scan silently misses users whose calls
  landed elsewhere.
- **Backfill / catch-up**: after an outage the processor scans back from its
  checkpoint, capped at `MAX_LOOKBACK_MINUTES` (default 7 days) so a single run
  can't try to list months of objects at once.
- **Re-arming alerts**: budget/warn alerts fire once per `(user, month)`. Delete
  the user's DynamoDB rows for that month to re-arm.
- **Large bodies**: request/response bodies > 100 KB are stored by Bedrock as
  separate S3 objects under the data prefix; the processor ignores those and
  reads token counts from the log records, so they don't affect counters.
- **User labels**: the processor parses `identity.arn`; for SSO sessions the
  last path segment of the assumed-role ARN is typically the username.

## Teardown

```bash
cdk destroy BedrockModelUsageTrackerStack
```

What this removes vs. keeps:

| Resource                                | On `cdk destroy`                                   |
| --------------------------------------- | -------------------------------------------------- |
| Batch processor + reader Lambdas        | Deleted                                            |
| EventBridge schedule                    | Deleted                                            |
| CloudWatch dashboard                    | Deleted                                            |
| SNS topic + email subscription          | Deleted                                            |
| `BatchStateTable` (checkpoint + markers)| Deleted (`RemovalPolicy.DESTROY`) — safe to lose   |
| **`UsageTable` (usage/cost history)**   | **Retained** (`RemovalPolicy.RETAIN` + PITR) — orphaned, not deleted |
| **S3 log bucket**                       | **Untouched** — imported, managed outside the stack |
| **Bedrock model-invocation logging**   | **Untouched** — configured in Bedrock settings; keeps writing to S3 |

Follow-ups if you want a full cleanup:

- Delete the retained `UsageTable` manually (DynamoDB console or
  `aws dynamodb delete-table`) once you no longer need the history.
- To stop new logs (and the underlying Bedrock spend), disable model-invocation
  logging in the Bedrock console — the stack does not control it.
- Remove local build artifacts: `rm -rf cdk.out cdk.out.new`.

## Local layout

```
.
├── cdk.json
├── infra/
│   ├── app.py
│   └── stacks/
│       └── usage_tracker_stack.py
├── lambdas/
│   ├── usage_batch_processor/
│   │   ├── usage_batch_processor.py   # EventBridge-scheduled S3 rollup + $ alerts
│   │   └── identity_enforcement.py    # optional: remove over-budget user from IC group
│   ├── usage_reader/
│   │   └── usage_reader.py            # CloudWatch custom-widget data source (spend)
│   └── usage_processor/               # legacy streaming processor (unused, kept for reference)
│       └── usage_processor.py
├── streamlit_app/
│   └── app.py
├── requirements.txt
└── README.md
```
