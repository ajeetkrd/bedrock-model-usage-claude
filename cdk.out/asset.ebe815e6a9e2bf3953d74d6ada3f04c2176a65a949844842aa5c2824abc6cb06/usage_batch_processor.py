"""Batch-process Bedrock invocation logs delivered to S3 (Option B).

Triggered by an EventBridge schedule (every 5 minutes). Instead of reacting to
every single CloudWatch Logs event, we read the gzipped JSON batches that Amazon
Bedrock model-invocation logging writes to S3 and roll them up into the same
per-user/per-model/per-month DynamoDB counters used by the rest of the stack.

S3 layout (Bedrock model invocation logging, S3 destination):

    <keyPrefix>/AWSLogs/<accountId>/BedrockModelInvocationLogs/<region>/
        <YYYY>/<MM>/<DD>/<HH>/<...>.json.gz

Cost model
----------
The budget is enforced in DOLLARS, not tokens, and is a per-USER monthly cap
that spans every model the user touched (a user might split work across Opus,
Sonnet, Haiku, ... but their combined spend must stay under THRESHOLD_USD).

Token counts are still stored per (user, model, month); cost is derived at
read/alert time from a configurable per-model price table (MODEL_PRICING_JSON),
so there is a single source of truth for pricing and no float accumulation in
DynamoDB.

Exactly-once accounting
-----------------------
Schedule windows overlap and Lambdas can retry, so we keep a small "state"
DynamoDB table:

  * a checkpoint item (high-water mark) that bounds how far back we list, and
  * one marker item per processed S3 object key (with a TTL).

We only count an object that has no marker, and we write its marker only after
its increments land. Worst case a crash re-counts a single object once.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

import identity_enforcement

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USAGE_TABLE = os.environ["USAGE_TABLE"]
STATE_TABLE = os.environ["STATE_TABLE"]
BUCKET = os.environ["LOG_BUCKET"]
KEY_PREFIX = os.environ.get("LOG_KEY_PREFIX", "").strip("/")
ACCOUNT_ID = os.environ["LOG_ACCOUNT_ID"]
# Region partition(s) under BedrockModelInvocationLogs/. Bedrock writes one
# partition per region the request is logged in, and Claude Code uses
# cross-region inference profiles, so usage spans several regions. Set
# LOG_REGIONS to a comma-separated list to pin them, or leave it as "auto"
# (default) to discover every region partition present in the bucket.
LOG_REGIONS_RAW = (
    os.environ.get("LOG_REGIONS")
    or os.environ.get("LOG_REGION")  # backwards-compat single region
    or "auto"
).strip()

ALERT_TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]
# Dollar budget per user per month, across all models.
THRESHOLD_USD = Decimal(os.environ.get("THRESHOLD_USD", "20"))
WARN_RATIO = Decimal(os.environ.get("WARN_RATIO", "0.8"))
WARN_USD = THRESHOLD_USD * WARN_RATIO
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "BedrockUsage")

# How far back to (re)scan each run. Overlap is deduped by object markers.
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "15"))
MAX_LOOKBACK_MINUTES = int(os.environ.get("MAX_LOOKBACK_MINUTES", str(7 * 24 * 60)))
MARKER_TTL_DAYS = int(os.environ.get("MARKER_TTL_DAYS", "45"))

# ---------------------------------------------------------------------------
# Pricing: USD per 1,000,000 tokens (base input / output rates), resolved by
# substring match on modelId. Patterns are checked in order, so put more
# specific ones first. Values follow the Bedrock Anthropic price table:
#
#   Opus  4.5 / 4.6 / 4.7 / 4.8 : $5  in  / $25 out
#   Sonnet 4 / 4.5 / 4.6        : $3  in  / $15 out
#   Haiku 4.5                   : $1  in  / $5  out
#
# Override the whole table by setting MODEL_PRICING_JSON (a JSON object mapping
# a lowercase modelId substring -> [input_per_million, output_per_million]).
# ---------------------------------------------------------------------------
_DEFAULT_PRICING: dict[str, list[float]] = {
    # pattern (lowercased substring): [input_per_million, output_per_million]
    "opus": [5.0, 25.0],
    "sonnet": [3.0, 15.0],
    "haiku": [1.0, 5.0],
}
_DEFAULT_RATE = [3.0, 15.0]  # fallback for unrecognised models (Sonnet-like)

# Anthropic prompt-cache multipliers relative to the base input rate:
#   cache WRITE (cache creation, 5-min TTL) ≈ 1.25x input
#   cache READ  (cache hit)                 ≈ 0.10x input
# Used to derive cache rates when a pricing entry only specifies [in, out].
# Override per-model by giving a 4-element entry [in, out, cache_write, cache_read].
CACHE_WRITE_MULTIPLIER = float(os.environ.get("CACHE_WRITE_MULTIPLIER", "1.25"))
CACHE_READ_MULTIPLIER = float(os.environ.get("CACHE_READ_MULTIPLIER", "0.10"))


def _load_pricing() -> dict[str, list[float]]:
    raw = os.environ.get("MODEL_PRICING_JSON")
    if not raw:
        return _DEFAULT_PRICING
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed:
            return {k.lower(): v for k, v in parsed.items()}
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid MODEL_PRICING_JSON; using defaults")
    return _DEFAULT_PRICING


PRICING = _load_pricing()


def _normalize_rates(rates: list[float]) -> list[float]:
    """Expand a pricing entry to [in, out, cache_write, cache_read].

    Accepts 2-element [in, out] (cache rates derived from the input rate via the
    standard Anthropic multipliers) or an explicit 4-element entry.
    """
    rate_in = float(rates[0])
    rate_out = float(rates[1])
    if len(rates) >= 4:
        return [rate_in, rate_out, float(rates[2]), float(rates[3])]
    return [
        rate_in,
        rate_out,
        rate_in * CACHE_WRITE_MULTIPLIER,
        rate_in * CACHE_READ_MULTIPLIER,
    ]


def _rates_for(model_id: str) -> list[float]:
    mid = (model_id or "").lower()
    for pattern, rates in PRICING.items():
        if pattern in mid:
            return _normalize_rates(rates)
    return _normalize_rates(_DEFAULT_RATE)


def cost_usd(
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    cache_write_tokens: int = 0,
    cache_read_tokens: int = 0,
) -> Decimal:
    rate_in, rate_out, rate_cache_write, rate_cache_read = _rates_for(model_id)
    million = Decimal(1_000_000)
    return (
        Decimal(int(input_tokens)) * Decimal(str(rate_in))
        + Decimal(int(output_tokens)) * Decimal(str(rate_out))
        + Decimal(int(cache_write_tokens)) * Decimal(str(rate_cache_write))
        + Decimal(int(cache_read_tokens)) * Decimal(str(rate_cache_read))
    ) / million


ddb = boto3.resource("dynamodb")
table = ddb.Table(USAGE_TABLE)
state_table = ddb.Table(STATE_TABLE)
s3 = boto3.client("s3")
sns = boto3.client("sns")

# Prefix up to (and including) BedrockModelInvocationLogs/. The region partition
# is appended per-region by the listing code below.
_ROOT_PREFIX = (
    (f"{KEY_PREFIX}/" if KEY_PREFIX else "")
    + f"AWSLogs/{ACCOUNT_ID}/BedrockModelInvocationLogs/"
)

CHECKPOINT_KEY = {"pk": "STATE", "sk": "CHECKPOINT"}


def _configured_regions() -> list[str] | None:
    """Explicit region list from config, or None when set to auto-discover."""
    if LOG_REGIONS_RAW.lower() == "auto":
        return None
    return [r.strip() for r in LOG_REGIONS_RAW.split(",") if r.strip()]


def _discover_regions() -> list[str]:
    """List the region partitions present under BedrockModelInvocationLogs/.

    Uses a delimited list so we only fetch the immediate sub-prefixes (one S3
    call per page), not every object.
    """
    regions: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=_ROOT_PREFIX, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] looks like ".../BedrockModelInvocationLogs/<region>/"
            region = cp["Prefix"][len(_ROOT_PREFIX):].strip("/")
            if region:
                regions.append(region)
    return regions


def _regions_to_scan() -> list[str]:
    configured = _configured_regions()
    if configured:
        return configured
    discovered = _discover_regions()
    if not discovered:
        logger.warning("No region partitions found under %s", _ROOT_PREFIX)
    return discovered


# ---------------------------------------------------------------------------
# Identity / token extraction
# ---------------------------------------------------------------------------
def _identity_label(identity: dict | None) -> str:
    if not identity:
        return "unknown"
    arn = identity.get("arn") or identity.get("callerArn") or ""
    if not arn:
        return "unknown"
    if ":assumed-role/" in arn:
        return arn.rsplit("/", 1)[-1] or arn
    if ":user/" in arn or ":federated-user/" in arn:
        return arn.rsplit("/", 1)[-1] or arn
    return arn


def _usage_from_output(output_block: dict) -> dict:
    """Best-effort extraction of the Anthropic 'usage' object from the logged
    output body, tolerating both the non-streaming (dict) and streaming (list of
    events) shapes. For streaming, later events (message_delta / message_stop)
    carry the final cumulative totals, so the last usage seen wins."""
    body = output_block.get("outputBodyJson")
    if isinstance(body, dict):
        return body.get("usage") or {}
    if isinstance(body, list):
        usage: dict = {}
        for event in body:
            if not isinstance(event, dict):
                continue
            u = event.get("usage")
            if isinstance(u, dict):
                usage = u
            msg = event.get("message")
            if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
                usage = msg["usage"]
        return usage
    return {}


def _extract_tokens(message: dict) -> tuple[int, int, int, int]:
    """Return (input, output, cache_write, cache_read) token counts.

    Bedrock's top-level input/output blocks are authoritative (they already
    aggregate streaming responses); the Anthropic 'usage' block only backfills
    fields the top level omits. Cache tokens matter a lot for Claude Code, which
    is heavily prompt-cached: cache-creation (write) tokens routinely dwarf the
    raw input_tokens and are billed, so ignoring them badly undercounts usage.
    """
    input_block = message.get("input") or {}
    output_block = message.get("output") or {}

    input_tokens = (
        input_block.get("inputTokenCount") or input_block.get("inputTokens") or 0
    )
    output_tokens = (
        output_block.get("outputTokenCount") or output_block.get("outputTokens") or 0
    )
    cache_write = (
        input_block.get("cacheWriteInputTokenCount")
        or input_block.get("cacheCreationInputTokenCount")
        or 0
    )
    cache_read = (
        input_block.get("cacheReadInputTokenCount")
        or input_block.get("cacheReadInputTokens")
        or 0
    )

    # Backfill any missing field from the Anthropic usage block.
    if not (input_tokens and output_tokens and cache_write):
        usage = _usage_from_output(output_block)
        input_tokens = input_tokens or usage.get("input_tokens") or 0
        output_tokens = output_tokens or usage.get("output_tokens") or 0
        cache_write = cache_write or usage.get("cache_creation_input_tokens") or 0
        cache_read = cache_read or usage.get("cache_read_input_tokens") or 0

    try:
        return (
            int(input_tokens or 0),
            int(output_tokens or 0),
            int(cache_write or 0),
            int(cache_read or 0),
        )
    except (TypeError, ValueError):
        return 0, 0, 0, 0


def _month_for(message: dict) -> str:
    ts_str = message.get("timestamp")
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return ts.strftime("%Y-%m")
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc).strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Checkpoint + S3 listing
# ---------------------------------------------------------------------------
def _read_checkpoint() -> datetime | None:
    try:
        resp = state_table.get_item(Key=CHECKPOINT_KEY)
    except ClientError:
        logger.exception("Failed to read checkpoint; falling back to default lookback")
        return None
    item = resp.get("Item")
    if not item or "lastProcessedMs" not in item:
        return None
    return datetime.fromtimestamp(int(item["lastProcessedMs"]) / 1000, tz=timezone.utc)


def _write_checkpoint(when: datetime) -> None:
    try:
        state_table.put_item(
            Item={
                **CHECKPOINT_KEY,
                "lastProcessedMs": int(when.timestamp() * 1000),
                "updatedAt": int(time.time()),
            }
        )
    except ClientError:
        logger.exception("Failed to persist checkpoint")


def _hour_prefixes(cutoff: datetime, now: datetime) -> list[str]:
    prefixes: list[str] = []
    t = cutoff.replace(minute=0, second=0, microsecond=0)
    end = now.replace(minute=0, second=0, microsecond=0)
    while t <= end:
        prefixes.append(t.strftime("%Y/%m/%d/%H"))
        t += timedelta(hours=1)
    return prefixes


def _list_candidate_objects(cutoff: datetime, now: datetime, regions: list[str]) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    hours = _hour_prefixes(cutoff, now)
    for region in regions:
        region_base = f"{_ROOT_PREFIX}{region}/"
        for hour in hours:
            prefix = f"{region_base}{hour}/"
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith("/"):
                        continue
                    last_modified = obj["LastModified"]
                    if last_modified.tzinfo is None:
                        last_modified = last_modified.replace(tzinfo=timezone.utc)
                    if last_modified >= cutoff:
                        keys.append(obj["Key"])
    return keys


def _filter_unprocessed(keys: list[str]) -> list[str]:
    if not keys:
        return []
    seen: set[str] = set()
    unique = list(dict.fromkeys(keys))
    for i in range(0, len(unique), 100):
        chunk = unique[i : i + 100]
        request = {
            STATE_TABLE: {
                "Keys": [{"pk": f"OBJ#{k}", "sk": "MARKER"} for k in chunk],
                "ProjectionExpression": "pk",
            }
        }
        while request:
            resp = ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(STATE_TABLE, []):
                seen.add(item["pk"][len("OBJ#") :])
            request = resp.get("UnprocessedKeys") or None
    return [k for k in unique if k not in seen]


def _mark_processed(key: str) -> None:
    try:
        state_table.put_item(
            Item={
                "pk": f"OBJ#{key}",
                "sk": "MARKER",
                "processedAt": int(time.time()),
                "expireAt": int(time.time()) + MARKER_TTL_DAYS * 86400,
            }
        )
    except ClientError:
        logger.exception("Failed to write dedup marker for %s", key)


# ---------------------------------------------------------------------------
# Object parsing
# ---------------------------------------------------------------------------
def _iter_records(raw: bytes) -> Iterable[dict]:
    try:
        text = gzip.decompress(raw).decode("utf-8")
    except OSError:
        text = raw.decode("utf-8", errors="replace")

    text = text.strip()
    if not text:
        return

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
        return

    if isinstance(parsed, list):
        yield from (r for r in parsed if isinstance(r, dict))
    elif isinstance(parsed, dict):
        if isinstance(parsed.get("records"), list):
            yield from (r for r in parsed["records"] if isinstance(r, dict))
        else:
            yield parsed


def _process_object(key: str) -> dict[tuple[str, str, str], dict]:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    raw = obj["Body"].read()

    agg: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {"input": 0, "output": 0, "cacheWrite": 0, "cacheRead": 0, "count": 0}
    )
    for message in _iter_records(raw):
        if message.get("schemaType") and message.get("schemaType") != "ModelInvocationLog":
            continue
        model_id = message.get("modelId") or "unknown-model"
        user = _identity_label(message.get("identity"))
        in_tok, out_tok, cache_write, cache_read = _extract_tokens(message)
        month = _month_for(message)

        cell = agg[(user, model_id, month)]
        cell["input"] += in_tok
        cell["output"] += out_tok
        cell["cacheWrite"] += cache_write
        cell["cacheRead"] += cache_read
        cell["count"] += 1
    return agg


# ---------------------------------------------------------------------------
# DynamoDB write + cost rollup + alerts + metrics
# ---------------------------------------------------------------------------
def _apply_delta(
    user: str,
    model_id: str,
    month: str,
    in_tok: int,
    out_tok: int,
    count: int,
    cache_write: int = 0,
    cache_read: int = 0,
) -> None:
    pk = f"USER#{user}"
    sk = f"MODEL#{model_id}#MONTH#{month}"
    model_month = f"MODEL#{model_id}#MONTH#{month}"
    # Total tokens include cache tokens so the stored total reflects real usage.
    delta = in_tok + out_tok + cache_write + cache_read

    table.update_item(
        Key={"pk": pk, "sk": sk},
        UpdateExpression=(
            "ADD totalTokens :d, inputTokens :i, outputTokens :o, "
            "cacheWriteTokens :cw, cacheReadTokens :cr, invocations :c "
            "SET #u = :user, #m = :model, #mo = :month, modelMonth = :modelMonth, "
            "lastUpdated = :ts"
        ),
        ExpressionAttributeNames={"#u": "user", "#m": "modelId", "#mo": "month"},
        ExpressionAttributeValues={
            ":d": delta,
            ":i": in_tok,
            ":o": out_tok,
            ":cw": cache_write,
            ":cr": cache_read,
            ":c": count,
            ":user": user,
            ":model": model_id,
            ":month": month,
            ":modelMonth": model_month,
            ":ts": int(time.time()),
        },
    )


def _user_month_cost(user: str, month: str) -> Decimal:
    """Sum the user's cost across every model for the month (absolute, post-update)."""
    total = Decimal(0)
    kwargs = {
        "KeyConditionExpression": Key("pk").eq(f"USER#{user}")
        & Key("sk").begins_with("MODEL#"),
        "FilterExpression": Attr("month").eq(month),
    }
    while True:
        resp = table.query(**kwargs)
        for it in resp.get("Items", []):
            total += cost_usd(
                it.get("modelId", ""),
                int(it.get("inputTokens", 0) or 0),
                int(it.get("outputTokens", 0) or 0),
                int(it.get("cacheWriteTokens", 0) or 0),
                int(it.get("cacheReadTokens", 0) or 0),
            )
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    return total


def _maybe_alert_user(user: str, month: str, prev_cost: Decimal, new_cost: Decimal) -> bool:
    """Send an SNS alert the first time a user's monthly spend crosses a boundary.

    Returns True if a BREACH boundary was crossed on this run (so the caller can
    trigger enforcement).
    """
    crossings = []
    if prev_cost < WARN_USD <= new_cost:
        crossings.append(("WARN", WARN_USD))
    if prev_cost < THRESHOLD_USD <= new_cost:
        crossings.append(("BREACH", THRESHOLD_USD))

    breached = False
    for level, boundary in crossings:
        if level == "BREACH":
            breached = True
        pct = (new_cost / THRESHOLD_USD * 100) if THRESHOLD_USD else Decimal(0)
        subject = (
            f"[Bedrock {level}] {user} crossed ${boundary:.2f} Bedrock spend ({month})"
        )
        body = (
            f"Bedrock monthly spend crossed a configured boundary.\n\n"
            f"  Level         : {level}\n"
            f"  User          : {user}\n"
            f"  Month         : {month}\n"
            f"  Previous spend: ${prev_cost:.2f}\n"
            f"  New spend     : ${new_cost:.2f} ({pct:.1f}% of budget)\n"
            f"  Boundary      : ${boundary:.2f}\n"
            f"  Budget        : ${THRESHOLD_USD:.2f} per user / month "
            f"(warn at {int(WARN_RATIO*100)}%), summed across all models\n"
        )
        try:
            sns.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Subject=subject[:100],
                Message=body,
                MessageAttributes={
                    "user": {"DataType": "String", "StringValue": user},
                    "level": {"DataType": "String", "StringValue": level},
                },
            )
            logger.info("Published %s alert for user=%s month=%s", level, user, month)
        except ClientError:
            logger.exception("Failed to publish SNS alert")

    return breached


def _emit_emf(model_totals: dict[str, dict]) -> None:
    """Per-model + grand-total EMF: tokens, invocations, and USD cost."""
    if not model_totals:
        return
    timestamp_ms = int(time.time() * 1000)
    for model_id, t in model_totals.items():
        cost = cost_usd(
            model_id, t["input"], t["output"], t["cacheWrite"], t["cacheRead"]
        )
        doc = {
            "_aws": {
                "Timestamp": timestamp_ms,
                "CloudWatchMetrics": [
                    {
                        "Namespace": METRICS_NAMESPACE,
                        "Dimensions": [["ModelId"], []],
                        "Metrics": [
                            {"Name": "InputTokens", "Unit": "Count"},
                            {"Name": "OutputTokens", "Unit": "Count"},
                            {"Name": "CacheWriteTokens", "Unit": "Count"},
                            {"Name": "CacheReadTokens", "Unit": "Count"},
                            {"Name": "TotalTokens", "Unit": "Count"},
                            {"Name": "Invocations", "Unit": "Count"},
                            {"Name": "CostUSD", "Unit": "None"},
                        ],
                    }
                ],
            },
            "ModelId": model_id,
            "InputTokens": t["input"],
            "OutputTokens": t["output"],
            "CacheWriteTokens": t["cacheWrite"],
            "CacheReadTokens": t["cacheRead"],
            "TotalTokens": t["input"] + t["output"] + t["cacheWrite"] + t["cacheRead"],
            "Invocations": t["count"],
            "CostUSD": float(round(cost, 4)),
        }
        print(json.dumps(doc))


# ---------------------------------------------------------------------------
# Lambda entrypoint
# ---------------------------------------------------------------------------
def handler(event: dict, _context: Any) -> dict:
    now = datetime.now(tz=timezone.utc)

    checkpoint = _read_checkpoint()
    default_cutoff = now - timedelta(minutes=LOOKBACK_MINUTES)
    max_cutoff = now - timedelta(minutes=MAX_LOOKBACK_MINUTES)
    if checkpoint is None:
        cutoff = default_cutoff
    else:
        cutoff = min(checkpoint, default_cutoff)
        cutoff = max(cutoff, max_cutoff)

    regions = _regions_to_scan()
    candidate_keys = _list_candidate_objects(cutoff, now, regions)
    new_keys = _filter_unprocessed(candidate_keys)
    logger.info(
        "cutoff=%s regions=%s candidates=%d new=%d root=%s",
        cutoff.isoformat(),
        ",".join(regions) or "<none>",
        len(candidate_keys),
        len(new_keys),
        _ROOT_PREFIX,
    )

    model_totals: dict[str, dict] = defaultdict(
        lambda: {"input": 0, "output": 0, "cacheWrite": 0, "cacheRead": 0, "count": 0}
    )
    # Cost added this run per (user, month), so prev = new - delta for alerting.
    user_month_cost_delta: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal(0))
    objects_done = 0
    records_done = 0

    for key in new_keys:
        try:
            agg = _process_object(key)
        except ClientError:
            logger.exception("Failed to read/parse s3://%s/%s", BUCKET, key)
            continue

        ok = True
        for (user, model_id, month), cell in agg.items():
            try:
                _apply_delta(
                    user,
                    model_id,
                    month,
                    cell["input"],
                    cell["output"],
                    cell["count"],
                    cell["cacheWrite"],
                    cell["cacheRead"],
                )
            except ClientError:
                logger.exception("DynamoDB update failed for %s/%s", user, model_id)
                ok = False
                continue

            user_month_cost_delta[(user, month)] += cost_usd(
                model_id,
                cell["input"],
                cell["output"],
                cell["cacheWrite"],
                cell["cacheRead"],
            )

            mt = model_totals[model_id]
            mt["input"] += cell["input"]
            mt["output"] += cell["output"]
            mt["cacheWrite"] += cell["cacheWrite"]
            mt["cacheRead"] += cell["cacheRead"]
            mt["count"] += cell["count"]
            records_done += cell["count"]

        if ok:
            _mark_processed(key)
            objects_done += 1

    # User-level dollar alerting: compute each touched user's absolute monthly
    # spend across all models, derive the pre-run spend, and alert on crossings.
    for (user, month), delta in user_month_cost_delta.items():
        if delta <= 0:
            continue
        try:
            new_cost = _user_month_cost(user, month)
        except ClientError:
            logger.exception("Failed to compute monthly cost for %s/%s", user, month)
            continue
        prev_cost = new_cost - delta
        if prev_cost < 0:
            prev_cost = Decimal(0)
        breached = _maybe_alert_user(user, month, prev_cost, new_cost)
        # Enforce whenever the user is AT/OVER budget this run, not only on the
        # single edge-crossing run. enforce_user is idempotent (a user who is no
        # longer a group member is skipped silently), so a user who was removed
        # and stays removed makes no new calls and is never re-evaluated, while a
        # user who was manually re-added and keeps spending is removed again.
        if breached or new_cost >= THRESHOLD_USD:
            # Optional, guarded enforcement (disabled + dry-run by default).
            try:
                identity_enforcement.enforce_user(user, month)
            except Exception:  # never let enforcement break the rollup
                logger.exception("enforce_user failed for %s/%s", user, month)

    _emit_emf(model_totals)
    _write_checkpoint(now)

    summary = {
        "objectsProcessed": objects_done,
        "objectsSeen": len(candidate_keys),
        "recordsProcessed": records_done,
        "usersTouched": len({u for (u, _m) in user_month_cost_delta}),
        "regionsScanned": regions,
        "cutoff": cutoff.isoformat(),
    }
    logger.info("Done. %s", json.dumps(summary))
    return summary
