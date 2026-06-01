"""Process Bedrock invocation logs from CloudWatch Logs.

Triggered by a CloudWatch Logs subscription filter. The payload is base64-encoded
gzipped JSON containing one or more log events. Each log event message is a JSON
document emitted by Bedrock model-invocation logging, with this shape (relevant
fields only):

    {
      "schemaType": "ModelInvocationLog",
      "timestamp": "2024-...Z",
      "accountId": "...",
      "identity": {"arn": "arn:aws:sts::...:assumed-role/Foo/alice"},
      "region": "ap-south-1",
      "requestId": "...",
      "modelId": "anthropic.claude-3-5-sonnet-20240620-v1:0",
      "input":  {"inputTokenCount": 1234, "inputBodyJson": {...}},
      "output": {"outputTokenCount": 567, "outputBodyJson": {...}}
    }

Per event we:
1. Atomically increment per-user-per-model monthly counters in DynamoDB.
2. Emit CloudWatch EMF metrics (per model, per user, per user x model).
3. Publish an SNS alert when the monthly total first crosses warnRatio*threshold
   for that (user, model) pair.
"""
from __future__ import annotations

import base64
import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USAGE_TABLE = os.environ["USAGE_TABLE"]
ALERT_TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]
THRESHOLD_TOKENS = int(os.environ.get("THRESHOLD_TOKENS", "5000000"))
WARN_RATIO = float(os.environ.get("WARN_RATIO", "0.8"))
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "BedrockUsage")
WARN_THRESHOLD = int(THRESHOLD_TOKENS * WARN_RATIO)

ddb = boto3.resource("dynamodb")
table = ddb.Table(USAGE_TABLE)
sns = boto3.client("sns")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _decode_payload(event: dict) -> dict:
    """Decode the base64+gzip CloudWatch Logs subscription payload."""
    encoded = event["awslogs"]["data"]
    payload = gzip.decompress(base64.b64decode(encoded))
    return json.loads(payload)


def _identity_label(identity: dict | None) -> str:
    """Pick a human friendly user label from the identity block.

    Priority: identity.arn -> identity.callerArn -> 'unknown'. We strip the
    role/session boilerplate to keep the metric dimension reasonable.
    """
    if not identity:
        return "unknown"
    arn = identity.get("arn") or identity.get("callerArn") or ""
    if not arn:
        return "unknown"
    # Examples:
    #   arn:aws:sts::123:assumed-role/Foo/alice         -> alice
    #   arn:aws:iam::123:user/bob                       -> bob
    #   arn:aws:sts::123:federated-user/carol           -> carol
    if ":assumed-role/" in arn:
        return arn.rsplit("/", 1)[-1] or arn
    if ":user/" in arn or ":federated-user/" in arn:
        return arn.rsplit("/", 1)[-1] or arn
    return arn


def _extract_tokens(message: dict) -> tuple[int, int, int, int]:
    """Extract (input, output, cache_write, cache_read) tokens from a log.

    Cache (prompt-caching) tokens are billed and, for cache-heavy clients like
    Claude Code, often exceed raw input tokens — so they must be counted.
    """
    input_block = message.get("input") or {}
    output_block = message.get("output") or {}

    input_tokens = (
        input_block.get("inputTokenCount")
        or input_block.get("inputTokens")
        or 0
    )
    output_tokens = (
        output_block.get("outputTokenCount")
        or output_block.get("outputTokens")
        or 0
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

    # Fallback: Anthropic-style usage may live in the body json. Streaming
    # responses store a list of events; the last usage object wins.
    if not (input_tokens and output_tokens and cache_write):
        body = output_block.get("outputBodyJson")
        usage: dict = {}
        if isinstance(body, dict):
            usage = body.get("usage") or {}
        elif isinstance(body, list):
            for event in body:
                if not isinstance(event, dict):
                    continue
                u = event.get("usage")
                if isinstance(u, dict):
                    usage = u
                msg = event.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
                    usage = msg["usage"]
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


def _emit_emf(records: list[dict]) -> None:
    """Emit CloudWatch EMF metrics for every record in one batch.

    Embedded Metric Format: any structured JSON line written to stdout that
    contains the `_aws` block is converted into CloudWatch metrics by the
    Lambda runtime. We emit per-model, per-user, and per-user-x-model
    dimensioned metrics in a single document each.
    """
    timestamp_ms = int(time.time() * 1000)
    for r in records:
        doc = {
            "_aws": {
                "Timestamp": timestamp_ms,
                "CloudWatchMetrics": [
                    {
                        "Namespace": METRICS_NAMESPACE,
                        "Dimensions": [
                            ["ModelId"],
                            ["User"],
                            ["User", "ModelId"],
                            [],
                        ],
                        "Metrics": [
                            {"Name": "InputTokens", "Unit": "Count"},
                            {"Name": "OutputTokens", "Unit": "Count"},
                            {"Name": "CacheWriteTokens", "Unit": "Count"},
                            {"Name": "CacheReadTokens", "Unit": "Count"},
                            {"Name": "TotalTokens", "Unit": "Count"},
                            {"Name": "Invocations", "Unit": "Count"},
                        ],
                    }
                ],
            },
            "ModelId": r["model_id"],
            "User": r["user"],
            "InputTokens": r["input_tokens"],
            "OutputTokens": r["output_tokens"],
            "CacheWriteTokens": r.get("cache_write_tokens", 0),
            "CacheReadTokens": r.get("cache_read_tokens", 0),
            "TotalTokens": r["total_tokens"],
            "Invocations": 1,
        }
        # EMF needs each document on its own line in stdout/Lambda log stream.
        print(json.dumps(doc))


def _update_counter_full(
    user: str,
    model_id: str,
    month: str,
    input_tokens: int,
    output_tokens: int,
    cache_write: int = 0,
    cache_read: int = 0,
) -> tuple[int, int]:
    """Update the row with full token breakdown. Returns (prev_total, new_total)."""
    pk = f"USER#{user}"
    sk = f"MODEL#{model_id}#MONTH#{month}"
    model_month = f"MODEL#{model_id}#MONTH#{month}"
    delta = input_tokens + output_tokens + cache_write + cache_read

    resp = table.update_item(
        Key={"pk": pk, "sk": sk},
        UpdateExpression=(
            "ADD totalTokens :d, inputTokens :i, outputTokens :o, "
            "cacheWriteTokens :cw, cacheReadTokens :cr, invocations :one "
            "SET #u = :user, #m = :model, #mo = :month, modelMonth = :modelMonth, "
            "lastUpdated = :ts"
        ),
        ExpressionAttributeNames={
            "#u": "user",
            "#m": "modelId",
            "#mo": "month",
        },
        ExpressionAttributeValues={
            ":d": delta,
            ":i": input_tokens,
            ":o": output_tokens,
            ":cw": cache_write,
            ":cr": cache_read,
            ":one": 1,
            ":user": user,
            ":model": model_id,
            ":month": month,
            ":modelMonth": model_month,
            ":ts": int(time.time()),
        },
        ReturnValues="UPDATED_OLD",
    )
    previous_total = int((resp.get("Attributes") or {}).get("totalTokens", 0))
    return previous_total, previous_total + delta


def _maybe_alert(user: str, model_id: str, month: str, prev_total: int, new_total: int) -> None:
    """Send an SNS alert the first time we cross warn / threshold boundaries."""
    crossings = []
    if prev_total < WARN_THRESHOLD <= new_total:
        crossings.append(("WARN", WARN_THRESHOLD))
    if prev_total < THRESHOLD_TOKENS <= new_total:
        crossings.append(("BREACH", THRESHOLD_TOKENS))

    for level, boundary in crossings:
        subject = (
            f"[Bedrock {level}] {user} crossed {boundary:,} tokens "
            f"on {model_id} ({month})"
        )
        body = (
            f"Bedrock token usage crossed a configured boundary.\n\n"
            f"  Level         : {level}\n"
            f"  User          : {user}\n"
            f"  Model         : {model_id}\n"
            f"  Month         : {month}\n"
            f"  Previous total: {prev_total:,} tokens\n"
            f"  New total     : {new_total:,} tokens\n"
            f"  Boundary      : {boundary:,} tokens\n"
            f"  Threshold     : {THRESHOLD_TOKENS:,} tokens "
            f"(warn at {int(WARN_RATIO*100)}%)\n"
        )
        try:
            sns.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject[:100], Message=body)
            logger.info("Published %s alert for %s/%s month=%s", level, user, model_id, month)
        except ClientError:
            logger.exception("Failed to publish SNS alert")


# ---------------------------------------------------------------------------
# Lambda entrypoint
# ---------------------------------------------------------------------------
def handler(event: dict, _context: Any) -> dict:
    payload = _decode_payload(event)
    log_events = payload.get("logEvents", [])
    logger.info(
        "Received %d log events from %s/%s",
        len(log_events),
        payload.get("logGroup"),
        payload.get("logStream"),
    )

    metric_records: list[dict] = []
    processed = 0
    skipped = 0

    for ev in log_events:
        try:
            message = json.loads(ev["message"])
        except (KeyError, json.JSONDecodeError):
            skipped += 1
            continue

        if message.get("schemaType") and message.get("schemaType") != "ModelInvocationLog":
            skipped += 1
            continue

        model_id = message.get("modelId") or "unknown-model"
        user = _identity_label(message.get("identity"))
        input_tokens, output_tokens, cache_write, cache_read = _extract_tokens(message)
        total = input_tokens + output_tokens + cache_write + cache_read

        if total <= 0:
            # No tokens reported (e.g., guardrail block, error); still count an invocation.
            total = 0

        # Determine the month bucket from the event timestamp (UTC) when available,
        # otherwise the log event timestamp.
        ts_str = message.get("timestamp")
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc)
        else:
            ts = datetime.fromtimestamp(ev["timestamp"] / 1000, tz=timezone.utc)
        month = ts.strftime("%Y-%m")

        try:
            prev_total, new_total = _update_counter_full(
                user=user,
                model_id=model_id,
                month=month,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_write=cache_write,
                cache_read=cache_read,
            )
        except ClientError:
            logger.exception("DynamoDB update failed for %s/%s", user, model_id)
            skipped += 1
            continue

        _maybe_alert(user, model_id, month, prev_total, new_total)

        metric_records.append(
            {
                "user": user,
                "model_id": model_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_write_tokens": cache_write,
                "cache_read_tokens": cache_read,
                "total_tokens": total,
            }
        )
        processed += 1

    if metric_records:
        _emit_emf(metric_records)

    logger.info("Done. processed=%d skipped=%d", processed, skipped)
    return {"processed": processed, "skipped": skipped}
