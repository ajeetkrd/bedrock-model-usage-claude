"""Custom widget data source for the CloudWatch dashboard.

CloudWatch Custom Widgets invoke this lambda and render the returned markdown.
We scan the current UTC month's rows and present two leaderboards:

  1. Top users by total monthly SPEND (across all models) vs the $ budget.
  2. Per-model token/cost detail for context.

The budget is a per-user monthly dollar cap (default $20) summed across every
model. Dollar cost is derived from a configurable per-model price table
(MODEL_PRICING_JSON) so pricing has a single source of truth.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr

USAGE_TABLE = os.environ["USAGE_TABLE"]
THRESHOLD_USD = Decimal(os.environ.get("THRESHOLD_USD", "20"))
WARN_RATIO = Decimal(os.environ.get("WARN_RATIO", "0.8"))

_DEFAULT_PRICING = {
    "opus": [5.0, 25.0],
    "sonnet": [3.0, 15.0],
    "haiku": [1.0, 5.0],
}


def _load_pricing() -> dict[str, list[float]]:
    raw = os.environ.get("MODEL_PRICING_JSON")
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and parsed:
                return {k.lower(): v for k, v in parsed.items()}
        except (json.JSONDecodeError, TypeError):
            pass
    return _DEFAULT_PRICING


PRICING = _load_pricing()
_DEFAULT_RATE = [3.0, 15.0]

ddb = boto3.resource("dynamodb")
table = ddb.Table(USAGE_TABLE)

DOCS = """## Bedrock spend per user — current UTC month

Per-user monthly budget summed across all models. Spend is derived from token
counts using the configured per-model price table.
"""


def _rates_for(model_id: str) -> list[float]:
    mid = (model_id or "").lower()
    for pattern, rates in PRICING.items():
        if pattern in mid:
            return rates
    return _DEFAULT_RATE


def _cost_usd(model_id: str, input_tokens: int, output_tokens: int) -> Decimal:
    rin, rout = _rates_for(model_id)
    return (
        Decimal(int(input_tokens)) * Decimal(str(rin))
        + Decimal(int(output_tokens)) * Decimal(str(rout))
    ) / Decimal(1_000_000)


def _status(cost: Decimal) -> str:
    if cost >= THRESHOLD_USD:
        return "🔴 Breach"
    if cost >= THRESHOLD_USD * WARN_RATIO:
        return "🟡 Warning"
    return "🟢 Normal"


def _fetch_month(month: str) -> list[dict]:
    """Scan the usage table for the given month."""
    items: list[dict] = []
    kwargs = {"FilterExpression": Attr("month").eq(month) & Attr("sk").begins_with("MODEL#")}
    last_key = None
    while True:
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def _render(items: list[dict], month: str) -> str:
    if not items:
        return (
            f"{DOCS}\n\n_No usage recorded yet for {month}._\n"
            f"\nPer-user monthly budget: **${THRESHOLD_USD:,.2f}** across all models."
        )

    # Roll up per user (across models) and per (user, model).
    per_user_cost: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    per_user_tokens: dict[str, int] = defaultdict(int)
    per_user_inv: dict[str, int] = defaultdict(int)
    by_model: dict[str, list[dict]] = defaultdict(list)

    for it in items:
        user = it.get("user", "?")
        model_id = it.get("modelId", "?")
        i = int(it.get("inputTokens", 0) or 0)
        o = int(it.get("outputTokens", 0) or 0)
        t = int(it.get("totalTokens", 0) or 0)
        inv = int(it.get("invocations", 0) or 0)
        cost = _cost_usd(model_id, i, o)

        per_user_cost[user] += cost
        per_user_tokens[user] += t
        per_user_inv[user] += inv
        by_model[model_id].append({**it, "_cost": cost})

    parts = [
        DOCS,
        f"\n**Month:** `{month}`  •  **Budget:** `${THRESHOLD_USD:,.2f}` "
        f"per user (warn at {int(WARN_RATIO*100)}%), summed across all models.\n",
        "\n### Top users by spend (all models)\n",
        "| User | Spend | % of budget | Status | Tokens | Invocations |",
        "|------|------:|------------:|--------|-------:|------------:|",
    ]
    ranked = sorted(per_user_cost.items(), key=lambda kv: kv[1], reverse=True)
    for user, cost in ranked[:25]:
        pct = (cost / THRESHOLD_USD * 100) if THRESHOLD_USD else Decimal(0)
        parts.append(
            f"| {user} | **${cost:,.2f}** | {pct:.1f}% | {_status(cost)} "
            f"| {per_user_tokens[user]:,} | {per_user_inv[user]:,} |"
        )

    # Per-model detail
    for model_id, rows in sorted(by_model.items()):
        rows.sort(key=lambda r: r["_cost"], reverse=True)
        parts.append(f"\n### `{model_id}`\n")
        parts.append("| User | Input | Output | Cost | Invocations |")
        parts.append("|------|------:|-------:|-----:|------------:|")
        for r in rows[:10]:
            user = r.get("user", "?")
            i = int(r.get("inputTokens", 0) or 0)
            o = int(r.get("outputTokens", 0) or 0)
            inv = int(r.get("invocations", 0) or 0)
            parts.append(f"| {user} | {i:,} | {o:,} | ${r['_cost']:,.2f} | {inv:,} |")

    return "\n".join(parts)


def handler(event: dict, _context: Any) -> str:
    if isinstance(event, dict) and event.get("describe"):
        return DOCS

    month = datetime.now(tz=timezone.utc).strftime("%Y-%m")
    items = _fetch_month(month)
    return _render(items, month)
