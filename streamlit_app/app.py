"""Streamlit dashboard for Bedrock token usage.

Reads the DynamoDB table populated by the UsageProcessor lambda and renders a
CloudWatch-style dashboard built to scale to hundreds of users:

- top-N charts with an "Others" aggregation bucket
- a searchable, filterable, sortable leaderboard with multi-row selection
- bulk "Send usage email" action via SNS
- a users-vs-models heatmap for dense overviews

Tested with ~200 users × multiple models.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from boto3.dynamodb.conditions import Attr

# ---------------------------------------------------------------------------
# Pricing: USD per 1,000,000 tokens [input, output], matched by lowercase
# substring of the modelId. Mirrors the Bedrock Anthropic price table.
# Override via the MODEL_PRICING_JSON env var (JSON object).
# ---------------------------------------------------------------------------
import json as _json

_DEFAULT_PRICING = {
    "opus": [5.0, 25.0],     # Opus 4.5 / 4.6 / 4.7 / 4.8
    "sonnet": [3.0, 15.0],   # Sonnet 4 / 4.5 / 4.6
    "haiku": [1.0, 5.0],     # Haiku 4.5
}
_DEFAULT_RATE = [3.0, 15.0]


def _load_pricing() -> dict[str, list[float]]:
    raw = os.environ.get("MODEL_PRICING_JSON")
    if raw:
        try:
            parsed = _json.loads(raw)
            if isinstance(parsed, dict) and parsed:
                return {k.lower(): v for k, v in parsed.items()}
        except (ValueError, TypeError):
            pass
    return _DEFAULT_PRICING


PRICING = _load_pricing()
# Cache rate multipliers relative to base input (mirror the batch processor).
CACHE_WRITE_MULTIPLIER = float(os.environ.get("CACHE_WRITE_MULTIPLIER", "1.25"))
CACHE_READ_MULTIPLIER = float(os.environ.get("CACHE_READ_MULTIPLIER", "0.10"))


def rates_for(model_id: str) -> list[float]:
    mid = (model_id or "").lower()
    rates = _DEFAULT_RATE
    for pattern, r in PRICING.items():
        if pattern in mid:
            rates = r
            break
    rin = float(rates[0])
    rout = float(rates[1])
    if len(rates) >= 4:
        return [rin, rout, float(rates[2]), float(rates[3])]
    return [rin, rout, rin * CACHE_WRITE_MULTIPLIER, rin * CACHE_READ_MULTIPLIER]


def cost_usd(
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    cache_write_tokens: int = 0,
    cache_read_tokens: int = 0,
) -> float:
    rin, rout, rcw, rcr = rates_for(model_id)
    return (
        input_tokens * rin
        + output_tokens * rout
        + cache_write_tokens * rcw
        + cache_read_tokens * rcr
    ) / 1_000_000

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Bedrock Usage Dashboard",
    layout="wide",
    page_icon="📊",
)

DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-south-1")
DEFAULT_STACK_NAME = os.environ.get("STACK_NAME", "BedrockModelUsageTrackerStack")


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _session(region: str) -> boto3.Session:
    return boto3.Session(region_name=region)


@st.cache_data(ttl=300, show_spinner=False)
def discover_stack_outputs(stack_name: str, region: str) -> dict[str, str]:
    """Pull table name and topic ARN from CFN stack outputs so the app is portable."""
    cfn = _session(region).client("cloudformation")
    try:
        resp = cfn.describe_stacks(StackName=stack_name)
    except Exception as exc:
        st.warning(f"Could not read stack outputs ({exc}). Configure manually in the sidebar.")
        return {}
    outputs = resp["Stacks"][0].get("Outputs") or []
    return {o["OutputKey"]: o["OutputValue"] for o in outputs}


@st.cache_data(ttl=30, show_spinner=False)
def scan_usage_table(table_name: str, region: str, month: str | None) -> pd.DataFrame:
    """Scan the usage table once. Optionally filter to a specific YYYY-MM month."""
    table = _session(region).resource("dynamodb").Table(table_name)
    kwargs: dict[str, Any] = {}
    if month:
        kwargs["FilterExpression"] = Attr("month").eq(month)

    items: list[dict[str, Any]] = []
    last_key = None
    while True:
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if not items:
        return pd.DataFrame(
            columns=[
                "user", "modelId", "model", "month",
                "inputTokens", "outputTokens", "cacheWriteTokens", "cacheReadTokens",
                "totalTokens", "invocations", "costUsd", "lastUpdated", "lastUpdatedUtc",
            ]
        )

    df = pd.DataFrame(items)
    for col in [
        "inputTokens", "outputTokens", "cacheWriteTokens", "cacheReadTokens",
        "totalTokens", "invocations", "lastUpdated",
    ]:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].apply(lambda v: int(v) if isinstance(v, Decimal) else int(v or 0))
    df["model"] = df["modelId"].apply(_short_model)
    df["costUsd"] = df.apply(
        lambda r: cost_usd(
            r["modelId"],
            int(r["inputTokens"]),
            int(r["outputTokens"]),
            int(r["cacheWriteTokens"]),
            int(r["cacheReadTokens"]),
        ),
        axis=1,
    )
    df["lastUpdatedUtc"] = df["lastUpdated"].apply(
        lambda s: datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if s else "—"
    )
    return df


def _short_model(model_id: str) -> str:
    """Strip Bedrock inference-profile ARN noise to keep tables readable."""
    if not model_id:
        return "?"
    if "inference-profile/" in model_id:
        return model_id.split("inference-profile/", 1)[1]
    return model_id


def publish_usage_email(
    *,
    topic_arn: str,
    region: str,
    user: str,
    month: str,
    spend_usd: float,
    total_tokens: int,
    input_tokens: int,
    output_tokens: int,
    cache_write_tokens: int,
    cache_read_tokens: int,
    invocations: int,
    budget_usd: float,
    per_model: list[dict],
    actor: str,
) -> str:
    """Publish a per-user 'usage notice' (dollar spend) to SNS. Returns MessageId."""
    pct = (spend_usd / budget_usd * 100) if budget_usd else 0
    subject = f"[Bedrock USAGE] {user} • ${spend_usd:,.2f} spend ({month})"
    model_lines = "\n".join(
        f"    - {_short_model(m['modelId'])}: ${m['costUsd']:,.2f} "
        f"({m['totalTokens']:,} tok, {m['invocations']:,} inv)"
        for m in per_model
    )
    body = (
        "Manual usage notice triggered from the Bedrock Usage Dashboard.\n\n"
        f"  User            : {user}\n"
        f"  Month           : {month}\n"
        f"  Total spend     : ${spend_usd:,.2f}\n"
        f"  Budget          : ${budget_usd:,.2f} per user / month (all models)\n"
        f"  Consumption     : {pct:.1f}% of budget\n"
        f"  Total tokens    : {total_tokens:,} (in {input_tokens:,} / out {output_tokens:,} "
        f"/ cache-write {cache_write_tokens:,} / cache-read {cache_read_tokens:,})\n"
        f"  Invocations     : {invocations:,}\n"
        f"  Per-model spend :\n{model_lines}\n"
        f"  Triggered by    : {actor}\n"
        f"  Triggered at    : {datetime.now(tz=timezone.utc).isoformat(timespec='seconds')}\n"
    )
    sns = _session(region).client("sns")
    resp = sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
    return resp["MessageId"]


def status_label(pct: float) -> str:
    if pct >= 1.0:
        return "🔴 Breach"
    if pct >= 0.8:
        return "🟡 Warning"
    return "🟢 Normal"


# ---------------------------------------------------------------------------
# Sidebar: configuration + filters
# ---------------------------------------------------------------------------
st.sidebar.header("Configuration")
region = st.sidebar.text_input("AWS region", DEFAULT_REGION)
stack_name = st.sidebar.text_input("CloudFormation stack", DEFAULT_STACK_NAME)

outputs = discover_stack_outputs(stack_name, region) if stack_name else {}
default_table = outputs.get("UsageTableName", "")
default_topic = outputs.get("AlertTopicArn", "")

# Budget + pricing default to the values the STACK was deployed with (via CFN
# outputs), so `cdk deploy -c thresholdUsd=...` flows through to this UI. They
# remain editable in the sidebar for ad-hoc what-if analysis.
def _output_float(key: str, fallback: str) -> float:
    try:
        return float(outputs.get(key) or os.environ.get(key.upper(), fallback))
    except (TypeError, ValueError):
        return float(fallback)


# Override the module-level price table from the stack output when present.
_stack_pricing = outputs.get("ModelPricingJson")
if _stack_pricing:
    try:
        _parsed = _json.loads(_stack_pricing)
        if isinstance(_parsed, dict) and _parsed:
            PRICING = {k.lower(): v for k, v in _parsed.items()}
    except (ValueError, TypeError):
        pass

table_name = st.sidebar.text_input("DynamoDB table", default_table)
topic_arn = st.sidebar.text_input("SNS topic ARN", default_topic)

budget_usd = float(
    st.sidebar.number_input(
        "Budget (USD / user / month, all models)",
        min_value=0.0,
        value=_output_float("ThresholdUsd", os.environ.get("THRESHOLD_USD", "20")),
        step=0.01,
        format="%.2f",
        help="Defaults to the stack's deployed thresholdUsd; editable for what-if.",
    )
)

month_default = datetime.now(tz=timezone.utc).strftime("%Y-%m")
month_choice = st.sidebar.text_input("Month filter (YYYY-MM, blank for all)", month_default)

actor = st.sidebar.text_input(
    "Actor (recorded in alert email)",
    os.environ.get("USER", "dashboard-user"),
)
with st.sidebar.expander("Model pricing (USD / 1M tokens)"):
    st.table(
        pd.DataFrame(
            [{"match": k, "input": v[0], "output": v[1]} for k, v in PRICING.items()]
        )
    )
if st.sidebar.button("🔄 Refresh data", use_container_width=True):
    scan_usage_table.clear()


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
st.title("📊 Bedrock Usage Dashboard")
st.caption(
    f"Data source: DynamoDB `{table_name or '<not configured>'}` in `{region}`. "
    f"Budget: **${budget_usd:,.2f}** per user / month, summed across all models. "
    "Cost is derived from token counts using the model price table (sidebar)."
)

if not table_name:
    st.error("Set the DynamoDB table name in the sidebar.")
    st.stop()

df = scan_usage_table(table_name, region, month_choice or None)

if df.empty:
    st.info("No usage rows found for the selected month.")
    st.stop()

# Per-user monthly spend across ALL models = the budget basis (independent of
# the model filter, so the budget % always reflects true total spend).
df["userMonthSpend"] = df.groupby(["user", "month"])["costUsd"].transform("sum")
df["pct"] = df["userMonthSpend"] / max(budget_usd, 1e-9)
df["pctDisplay"] = (df["pct"] * 100).round(1)
df["status"] = df["pct"].apply(status_label)


# ---------------------------------------------------------------------------
# Sidebar filters (after data load so we can populate options)
# ---------------------------------------------------------------------------
st.sidebar.divider()
st.sidebar.header("Filters")

user_query = st.sidebar.text_input("Search users (substring)", "")
model_options = sorted(df["model"].unique().tolist())
selected_models = st.sidebar.multiselect("Models", model_options, default=model_options)

status_filter = st.sidebar.multiselect(
    "Status",
    ["🟢 Normal", "🟡 Warning", "🔴 Breach"],
    default=["🟢 Normal", "🟡 Warning", "🔴 Breach"],
)

min_spend = float(
    st.sidebar.number_input("Minimum user spend (USD)", min_value=0.0, value=0.0, step=1.0)
)

top_n = int(st.sidebar.slider("Top-N for charts", 5, 50, 15))


def apply_filters(d: pd.DataFrame) -> pd.DataFrame:
    out = d
    if user_query:
        out = out[out["user"].str.contains(user_query, case=False, na=False)]
    if selected_models:
        out = out[out["model"].isin(selected_models)]
    if status_filter:
        out = out[out["status"].isin(status_filter)]
    if min_spend > 0:
        out = out[out["userMonthSpend"] >= min_spend]
    return out


fdf = apply_filters(df)


# Per-user (per-month) spend rollup, used by KPIs, the user chart, and the
# primary leaderboard.
def user_spend_table(d: pd.DataFrame) -> pd.DataFrame:
    if d.empty:
        return pd.DataFrame(
            columns=["user", "month", "costUsd", "inputTokens", "outputTokens",
                     "cacheWriteTokens", "cacheReadTokens",
                     "totalTokens", "invocations", "pct", "pctDisplay", "status"]
        )
    g = (
        d.groupby(["user", "month"], as_index=False)
        .agg(
            costUsd=("costUsd", "sum"),
            inputTokens=("inputTokens", "sum"),
            outputTokens=("outputTokens", "sum"),
            cacheWriteTokens=("cacheWriteTokens", "sum"),
            cacheReadTokens=("cacheReadTokens", "sum"),
            totalTokens=("totalTokens", "sum"),
            invocations=("invocations", "sum"),
        )
    )
    g["pct"] = g["costUsd"] / max(budget_usd, 1e-9)
    g["pctDisplay"] = (g["pct"] * 100).round(1)
    g["status"] = g["pct"].apply(status_label)
    return g.sort_values("costUsd", ascending=False).reset_index(drop=True)


users_df = user_spend_table(fdf)


# ---------------------------------------------------------------------------
# KPI tiles
# ---------------------------------------------------------------------------
total_users = fdf["user"].nunique()
total_models = fdf["model"].nunique()
total_spend = float(fdf["costUsd"].sum())
breach_count = int((users_df["pct"] >= 1.0).sum())
warn_count = int(((users_df["pct"] >= 0.8) & (users_df["pct"] < 1.0)).sum())

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Users (filtered)", f"{total_users:,}")
c2.metric("Models", f"{total_models:,}")
c3.metric("Total spend", f"${total_spend:,.2f}")
c4.metric("Total tokens", f"{int(fdf['totalTokens'].sum()):,}")
c5.metric("🟡 Warnings", f"{warn_count:,}")
c6.metric("🔴 Breaches", f"{breach_count:,}")

st.divider()


# ---------------------------------------------------------------------------
# Charts (Top-N + Others, suited to many users)
# ---------------------------------------------------------------------------
def top_n_with_others(d: pd.DataFrame, group_col: str, n: int, value_cols: list[str]) -> pd.DataFrame:
    agg = (
        d.groupby(group_col, as_index=False)[value_cols]
        .sum()
        .sort_values(value_cols[0], ascending=False)
    )
    if len(agg) <= n:
        return agg
    head, tail = agg.head(n), agg.tail(len(agg) - n)
    others = {group_col: f"Others ({len(tail)})"}
    for c in value_cols:
        others[c] = tail[c].sum()
    return pd.concat([head, pd.DataFrame([others])], ignore_index=True)


tab_users, tab_models, tab_heat, tab_distribution = st.tabs(
    ["Top users (spend)", "Top models (spend)", "User × Model heatmap", "Distribution"]
)

with tab_users:
    by_user = top_n_with_others(fdf, "user", top_n, ["costUsd", "totalTokens"])
    fig_user = px.bar(
        by_user,
        x="costUsd",
        y="user",
        orientation="h",
        labels={"costUsd": "Spend (USD)", "user": "User"},
        height=max(380, 28 * len(by_user) + 80),
    )
    fig_user.add_vline(x=budget_usd, line_dash="dash", line_color="red",
                       annotation_text="budget", annotation_position="top")
    fig_user.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_user, use_container_width=True)

with tab_models:
    by_model = top_n_with_others(fdf, "model", top_n, ["costUsd", "inputTokens", "outputTokens"])
    fig_model = px.bar(
        by_model,
        x="costUsd",
        y="model",
        orientation="h",
        labels={"costUsd": "Spend (USD)", "model": "Model"},
        height=max(380, 36 * len(by_model) + 80),
    )
    fig_model.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_model, use_container_width=True)

with tab_heat:
    # Heatmap: top users × all selected models. Cell = spend (USD), log-color.
    top_users = (
        fdf.groupby("user")["costUsd"].sum().sort_values(ascending=False).head(top_n).index
    )
    pivot = (
        fdf[fdf["user"].isin(top_users)]
        .pivot_table(
            index="user",
            columns="model",
            values="costUsd",
            aggfunc="sum",
            fill_value=0,
        )
        .loc[top_users]
    )
    if pivot.empty:
        st.info("No data for current filters.")
    else:
        z = np.log1p(pivot.values)
        fig_heat = px.imshow(
            z,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            color_continuous_scale="Viridis",
            aspect="auto",
            labels=dict(x="Model", y="User", color="log(1+USD)"),
            height=max(380, 22 * len(pivot) + 120),
        )
        fig_heat.update_traces(
            customdata=pivot.values,
            hovertemplate="User: %{y}<br>Model: %{x}<br>Spend: $%{customdata:,.2f}<extra></extra>",
        )
        fig_heat.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_heat, use_container_width=True)

with tab_distribution:
    st.caption("Distribution of per-user monthly spend. Vertical lines show warn (80%) and budget.")
    fig_hist = px.histogram(
        users_df,
        x="costUsd",
        nbins=40,
        labels={"costUsd": "Spend per user (USD)"},
        height=380,
    )
    fig_hist.add_vline(x=0.8 * budget_usd, line_dash="dash", line_color="orange",
                       annotation_text="warn 80%", annotation_position="top right")
    fig_hist.add_vline(x=budget_usd, line_dash="dash", line_color="red",
                       annotation_text="budget", annotation_position="top right")
    fig_hist.update_layout(margin=dict(l=10, r=10, t=10, b=10), bargap=0.05)
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()


# ---------------------------------------------------------------------------
# Per-user leaderboard with selection + bulk send (budget basis)
# ---------------------------------------------------------------------------
st.subheader(f"User spend vs budget ({len(users_df):,} users)")
st.caption(
    "Spend is summed across all models per user. Select one or more users and "
    "click **Send usage email** to publish a per-user spend notice to SNS."
)

display_cols = [
    "user",
    "month",
    "costUsd",
    "pctDisplay",
    "status",
    "totalTokens",
    "inputTokens",
    "outputTokens",
    "invocations",
]
column_config = {
    "user": st.column_config.TextColumn("User", width="medium"),
    "month": st.column_config.TextColumn("Month", width="small"),
    "costUsd": st.column_config.NumberColumn("Spend (USD)", format="$%.2f"),
    "pctDisplay": st.column_config.ProgressColumn(
        "% of budget",
        min_value=0,
        max_value=200,
        format="%.1f%%",
        help="100% = at budget. Values can exceed 100% for breaches.",
    ),
    "status": st.column_config.TextColumn("Status", width="small"),
    "totalTokens": st.column_config.NumberColumn("Total tokens", format="%d"),
    "inputTokens": st.column_config.NumberColumn("Input", format="%d"),
    "outputTokens": st.column_config.NumberColumn("Output", format="%d"),
    "invocations": st.column_config.NumberColumn("Invocations", format="%d"),
}

action_col, info_col = st.columns([1, 3])

if "_last_publish" not in st.session_state:
    st.session_state["_last_publish"] = None

event = st.dataframe(
    users_df[display_cols],
    column_config=column_config,
    use_container_width=True,
    hide_index=True,
    selection_mode="multi-row",
    on_select="rerun",
    key="users_table",
    height=min(620, 38 * (len(users_df) + 1) + 4),
)

selected_indices = event.selection.rows if event and event.selection else []
n_selected = len(selected_indices)

with action_col:
    send_disabled = (n_selected == 0) or (not topic_arn)
    if st.button(
        f"📧 Send usage email ({n_selected})",
        type="primary",
        disabled=send_disabled,
        use_container_width=True,
    ):
        ok, fail = 0, 0
        errors: list[str] = []
        for idx in selected_indices:
            urow = users_df.iloc[idx]
            user = urow["user"]
            month = urow["month"]
            # Per-model breakdown for this user/month from the filtered detail.
            sub = fdf[(fdf["user"] == user) & (fdf["month"] == month)]
            per_model = [
                {
                    "modelId": r["modelId"],
                    "costUsd": float(r["costUsd"]),
                    "totalTokens": int(r["totalTokens"]),
                    "invocations": int(r["invocations"]),
                }
                for _, r in sub.iterrows()
            ]
            try:
                publish_usage_email(
                    topic_arn=topic_arn,
                    region=region,
                    user=user,
                    month=month,
                    spend_usd=float(urow["costUsd"]),
                    total_tokens=int(urow["totalTokens"]),
                    input_tokens=int(urow["inputTokens"]),
                    output_tokens=int(urow["outputTokens"]),
                    cache_write_tokens=int(urow["cacheWriteTokens"]),
                    cache_read_tokens=int(urow["cacheReadTokens"]),
                    invocations=int(urow["invocations"]),
                    budget_usd=budget_usd,
                    per_model=per_model,
                    actor=actor,
                )
                ok += 1
            except Exception as exc:
                fail += 1
                errors.append(f"{user}: {exc}")
        if fail == 0:
            st.session_state["_last_publish"] = (
                "success",
                f"Published {ok} usage notice(s) to SNS.",
            )
        else:
            st.session_state["_last_publish"] = (
                "error",
                f"Published {ok} OK, {fail} failed.\n" + "\n".join(errors[:5]),
            )
        st.rerun()

with info_col:
    if not topic_arn:
        st.warning("SNS topic ARN is not configured — email actions are disabled.")
    elif n_selected == 0:
        st.caption("Tick the checkboxes on the left of any user(s) to enable the action.")
    else:
        st.caption(f"{n_selected} user(s) selected.")

fb = st.session_state.get("_last_publish")
if fb:
    kind, msg = fb
    (st.success if kind == "success" else st.error)(msg)


# ---------------------------------------------------------------------------
# Per (user, model) detail rows
# ---------------------------------------------------------------------------
st.divider()
st.subheader(f"Detail rows — per user × model ({len(fdf):,})")

detail = fdf.sort_values("costUsd", ascending=False).reset_index(drop=True)
detail_cols = [
    "user", "model", "month",
    "inputTokens", "outputTokens", "cacheWriteTokens", "cacheReadTokens", "totalTokens",
    "costUsd", "invocations", "lastUpdatedUtc",
]
detail_config = {
    "user": st.column_config.TextColumn("User", width="medium"),
    "model": st.column_config.TextColumn("Model", width="large"),
    "month": st.column_config.TextColumn("Month", width="small"),
    "inputTokens": st.column_config.NumberColumn("Input", format="%d"),
    "outputTokens": st.column_config.NumberColumn("Output", format="%d"),
    "cacheWriteTokens": st.column_config.NumberColumn("Cache write", format="%d"),
    "cacheReadTokens": st.column_config.NumberColumn("Cache read", format="%d"),
    "totalTokens": st.column_config.NumberColumn("Total", format="%d"),
    "costUsd": st.column_config.NumberColumn("Cost (USD)", format="$%.2f"),
    "invocations": st.column_config.NumberColumn("Invocations", format="%d"),
    "lastUpdatedUtc": st.column_config.TextColumn("Last update (UTC)", width="medium"),
}
st.dataframe(
    detail[detail_cols],
    column_config=detail_config,
    use_container_width=True,
    hide_index=True,
    height=min(520, 38 * (len(detail) + 1) + 4),
)

# CSV export of the per-user view
csv = users_df[display_cols].to_csv(index=False).encode()
st.download_button(
    "⬇️ Download user spend (CSV)",
    data=csv,
    file_name=f"bedrock_spend_{month_choice or 'all'}.csv",
    mime="text/csv",
)
