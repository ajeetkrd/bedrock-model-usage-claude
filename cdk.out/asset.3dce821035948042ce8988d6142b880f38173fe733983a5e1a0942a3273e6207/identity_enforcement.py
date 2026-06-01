"""Optional enforcement: remove an over-budget user from an Identity Center group.

When a user's monthly Bedrock spend reaches the budget, the batch processor can
remove them from the IAM Identity Center group that grants Bedrock access. This
is REVERSIBLE (re-add the membership) and scoped to one group, unlike deleting
the user.

SAFETY MODEL — this touches authentication with a wide blast radius, so it is
designed to be inert until you deliberately enable it:

  * ENFORCE_ENABLED   (default "false") — master switch. Off => never touches IAM.
  * ENFORCE_DRY_RUN   (default "true")  — when on, logs "would remove" and writes
                                          a dry-run audit record, but makes NO
                                          change. Watch this for a cycle first.
  * ENFORCE_ALLOWLIST (comma-separated) — user labels that are NEVER removed
                                          (leads, service accounts, yourself).
  * Idempotency        — a marker in the state table guarantees we remove a user
                         at most once per (user, month). Stale data re-reads
                         cannot thrash a user's access.
  * Audit              — every decision (skip/dry-run/removed/error) is written
                         to the state table and logged.

Resolution path (Identity Store API), repeated per configured group:
  user label --GetUserId(userName)--> UserId
  group name --GetGroupId(displayName)--> GroupId   (or an explicit group id)
  (UserId, GroupId) --GetGroupMembershipId--> MembershipId
  MembershipId --DeleteGroupMembership--> removed

You may configure MULTIPLE groups (comma-separated names and/or ids); the user
is removed from every one they belong to.

NOTE: the `user` label must map EXACTLY to an Identity Center user. By default we
match it against the `userName` attribute; override the attribute path with
ENFORCE_USER_ATTRIBUTE if your session name maps to a different field.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()


def _as_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _as_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


ENFORCE_ENABLED = _as_bool(os.environ.get("ENFORCE_ENABLED"), False)
ENFORCE_DRY_RUN = _as_bool(os.environ.get("ENFORCE_DRY_RUN"), True)
IDENTITY_STORE_ID = os.environ.get("IDENTITY_STORE_ID", "")
# Identity Center / Identity Store is regional; this is the region the SSO
# instance lives in (may differ from the stack/Lambda region).
IDENTITY_STORE_REGION = os.environ.get("IDENTITY_STORE_REGION", "") or os.environ.get(
    "AWS_REGION", ""
)
# One or more groups to remove an over-budget user from. Accept both the legacy
# singular vars and the new plural (comma-separated) vars.
ENFORCE_GROUP_IDS = _as_list(
    os.environ.get("ENFORCE_GROUP_IDS") or os.environ.get("ENFORCE_GROUP_ID")
)
ENFORCE_GROUP_NAMES = _as_list(
    os.environ.get("ENFORCE_GROUP_NAMES") or os.environ.get("ENFORCE_GROUP_NAME")
)
ENFORCE_USER_ATTRIBUTE = os.environ.get("ENFORCE_USER_ATTRIBUTE", "userName")
ENFORCE_ALLOWLIST = {
    u.strip()
    for u in os.environ.get("ENFORCE_ALLOWLIST", "").split(",")
    if u.strip()
}
MARKER_TTL_DAYS = int(os.environ.get("ENFORCE_MARKER_TTL_DAYS", "60"))
STATE_TABLE = os.environ.get("STATE_TABLE", "")

_identitystore = None
_state_table = boto3.resource("dynamodb").Table(STATE_TABLE) if STATE_TABLE else None


def _client():
    """Lazily build the Identity Store client so import never fails if the
    enforcement feature is unused (or the SDK lacks the client). Pinned to the
    region the SSO instance lives in (IDENTITY_STORE_REGION)."""
    global _identitystore
    if _identitystore is None:
        kwargs = {"region_name": IDENTITY_STORE_REGION} if IDENTITY_STORE_REGION else {}
        _identitystore = boto3.client("identitystore", **kwargs)
    return _identitystore


def is_configured() -> bool:
    """True only if the feature can do anything meaningful."""
    if not ENFORCE_ENABLED:
        return False
    if not IDENTITY_STORE_ID:
        logger.warning("ENFORCE_ENABLED but IDENTITY_STORE_ID is empty; skipping enforcement")
        return False
    if not (ENFORCE_GROUP_IDS or ENFORCE_GROUP_NAMES):
        logger.warning("ENFORCE_ENABLED but no group id/name configured; skipping enforcement")
        return False
    return True


# ---------------------------------------------------------------------------
# Idempotency markers / audit (reuse the batch state table)
# ---------------------------------------------------------------------------
def _marker_key(user: str, month: str) -> dict:
    return {"pk": f"ENFORCE#{user}", "sk": f"MONTH#{month}"}


def _already_enforced(user: str, month: str) -> bool:
    if _state_table is None:
        return False
    try:
        resp = _state_table.get_item(Key=_marker_key(user, month))
    except ClientError:
        logger.exception("enforce: failed reading marker for %s/%s", user, month)
        return False
    item = resp.get("Item")
    # Only a real removal blocks future attempts; dry-run records do not.
    return bool(item and item.get("mode") == "REMOVED")


def _write_audit(user: str, month: str, mode: str, detail: dict) -> None:
    if _state_table is None:
        return
    try:
        _state_table.put_item(
            Item={
                **_marker_key(user, month),
                "mode": mode,  # REMOVED | DRYRUN | ERROR | SKIPPED
                "detail": detail,
                "at": int(time.time()),
                "expireAt": int(time.time()) + MARKER_TTL_DAYS * 86400,
            }
        )
    except ClientError:
        logger.exception("enforce: failed writing audit for %s/%s", user, month)


# ---------------------------------------------------------------------------
# Identity Store resolution
# ---------------------------------------------------------------------------
def _resolve_group_targets() -> list[tuple[str, str]]:
    """Return [(label, group_id)] for every configured group.

    Explicit ids are used as-is; names are resolved via GetGroupId. Groups that
    fail to resolve are skipped (logged), so one bad name doesn't block others.
    """
    targets: list[tuple[str, str]] = []
    seen: set[str] = set()

    for gid in ENFORCE_GROUP_IDS:
        if gid not in seen:
            seen.add(gid)
            targets.append((gid, gid))

    for name in ENFORCE_GROUP_NAMES:
        try:
            resp = _client().get_group_id(
                IdentityStoreId=IDENTITY_STORE_ID,
                AlternateIdentifier={
                    "UniqueAttribute": {
                        "AttributePath": "displayName",
                        "AttributeValue": name,
                    }
                },
            )
            gid = resp.get("GroupId")
        except ClientError:
            logger.exception("enforce: could not resolve group %r", name)
            continue
        if gid and gid not in seen:
            seen.add(gid)
            targets.append((name, gid))
    return targets


def _resolve_user_id(user: str) -> str | None:
    try:
        resp = _client().get_user_id(
            IdentityStoreId=IDENTITY_STORE_ID,
            AlternateIdentifier={
                "UniqueAttribute": {
                    "AttributePath": ENFORCE_USER_ATTRIBUTE,
                    "AttributeValue": user,
                }
            },
        )
        return resp.get("UserId")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            logger.warning("enforce: no Identity Center user matches %r on %s",
                           user, ENFORCE_USER_ATTRIBUTE)
        else:
            logger.exception("enforce: GetUserId failed for %r", user)
        return None


def _resolve_membership_id(group_id: str, user_id: str) -> str | None:
    try:
        resp = _client().get_group_membership_id(
            IdentityStoreId=IDENTITY_STORE_ID,
            GroupId=group_id,
            MemberId={"UserId": user_id},
        )
        return resp.get("MembershipId")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            # Not a member of the group — nothing to do.
            return None
        logger.exception("enforce: GetGroupMembershipId failed")
        return None


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
def enforce_user(user: str, month: str) -> None:
    """Remove an over-budget user from every configured Bedrock group.

    Idempotent and guarded (enabled, allowlist, dry-run). Call only when the
    user is at/over budget.
    """
    if not is_configured():
        return

    if user in ENFORCE_ALLOWLIST or user in ("unknown", "", None):
        logger.info("enforce: %s is allowlisted/unresolved; skipping", user)
        return

    if _already_enforced(user, month):
        return  # already actioned this month

    targets = _resolve_group_targets()
    if not targets:
        _write_audit(user, month, "ERROR", {"reason": "no_groups_resolved"})
        return

    user_id = _resolve_user_id(user)
    if not user_id:
        _write_audit(user, month, "ERROR", {"reason": "user_unresolved"})
        return

    removed: list[dict] = []
    dryrun: list[dict] = []
    skipped: list[dict] = []
    errored: list[dict] = []

    for label, group_id in targets:
        membership_id = _resolve_membership_id(group_id, user_id)
        entry = {"group": label, "groupId": group_id}
        if not membership_id:
            logger.info("enforce: %s not in group %s; nothing to remove", user, label)
            skipped.append({**entry, "reason": "not_a_member"})
            continue

        entry["membershipId"] = membership_id
        if ENFORCE_DRY_RUN:
            logger.warning(
                "enforce[DRY_RUN]: WOULD remove user=%s (userId=%s) from group=%s (membershipId=%s)",
                user, user_id, label, membership_id,
            )
            dryrun.append(entry)
            continue

        try:
            _client().delete_group_membership(
                IdentityStoreId=IDENTITY_STORE_ID,
                MembershipId=membership_id,
            )
        except ClientError:
            logger.exception("enforce: DeleteGroupMembership failed for user=%s group=%s", user, label)
            errored.append({**entry, "reason": "delete_failed"})
            continue

        logger.warning(
            "enforce: REMOVED user=%s (userId=%s) from group=%s (budget breach %s)",
            user, user_id, label, month,
        )
        removed.append(entry)

    detail = {
        "userId": user_id,
        "removed": removed,
        "dryrun": dryrun,
        "skipped": skipped,
        "errored": errored,
    }

    # Mode precedence for the audit/idempotency record:
    #   REMOVED  -> a real removal happened (blocks re-enforcement this month)
    #   DRYRUN   -> would have removed (does NOT block; re-evaluated next run)
    #   ERROR    -> something failed (does NOT block; retried next run)
    #   SKIPPED  -> user was in none of the groups
    if removed:
        mode = "REMOVED"
    elif errored:
        mode = "ERROR"
    elif dryrun:
        mode = "DRYRUN"
    else:
        mode = "SKIPPED"
    _write_audit(user, month, mode, detail)
