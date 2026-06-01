# Streamlit Bedrock Usage Dashboard

A local-first dashboard that mirrors the CloudWatch view, backed by the same
DynamoDB table populated by the `UsageBatchProcessor` Lambda. It shows per-user
**dollar spend vs. a monthly budget** (summed across all models), with a
**Send email** action per user that publishes a spend notice to the SNS alert
topic.

## Install

```bash
python -m venv .venv-streamlit
source .venv-streamlit/bin/activate
pip install -r streamlit_app/requirements.txt
```

## Run

The app uses your default AWS credentials (e.g. SSO). The deployed stack name
defaults to `BedrockModelUsageTrackerStack` in `ap-south-1`. The table name, topic
ARN, **budget (`thresholdUsd`), warn ratio, and model pricing** are all read
from the stack outputs automatically — so whatever you pass to `cdk deploy`
(e.g. `-c thresholdUsd=0.01`) shows up here without editing anything. Override
in the sidebar for ad-hoc what-if analysis.

```bash
# from project root, with AWS creds active
streamlit run streamlit_app/app.py
```

Then open http://localhost:8501.

## What it shows

- **KPI tiles** — users, models, total spend, total tokens, and warning/breach
  counts for the selected month.
- **Top users by spend**, **Top models by spend**, a **user × model cost
  heatmap**, and a **per-user spend distribution** (with warn/budget lines).
- **User spend vs budget** leaderboard — per-user dollar spend across all
  models, a progress bar against the budget (🟢 < 80%, 🟡 80–100%, 🔴 ≥ 100%),
  multi-row selection, and a per-user **📧 Send usage email** action.
- **Detail rows** — the underlying per `(user, model, month)` rows with cost.

## Send email action

Selecting users and clicking **📧 Send usage email** publishes a structured
per-user notice to the SNS topic created by the CDK stack. The payload includes:

- user, month
- total spend (USD) and % of budget
- input / output / cache / total tokens, invocations
- a per-model spend breakdown
- the actor (sidebar field) and a UTC timestamp

If the email subscription on the topic shows `Deleted` or `PendingConfirmation`,
no email will arrive — re-subscribe and confirm:

```bash
aws sns subscribe \
  --topic-arn <AlertTopicArn> \
  --protocol email \
  --notification-endpoint you@example.com \
  --region ap-south-1
```

## IAM permissions needed by the runner

Whoever runs Streamlit needs:

- `cloudformation:DescribeStacks` (to auto-discover outputs; optional if you
  fill in the table/topic manually)
- `dynamodb:Scan` on the usage table
- `sns:Publish` on the alert topic
