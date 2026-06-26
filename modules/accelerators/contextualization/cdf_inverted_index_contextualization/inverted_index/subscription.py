"""Instance subscription handler for target-driven contextualization."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from inverted_index.config import (
    SUBSCRIPTION_CONFIG,
    TARGET_DRIVEN_DEDUPE_CONFIG,
    VIEW_INSTANCE_TYPES,
)
from inverted_index.config_loader import build_runtime_config
from inverted_index.storage import get_storage_adapter
from inverted_index.target_driven import process_target_driven_contextualization

logger = logging.getLogger(__name__)

# CogniteDescribable.aliases — primary trigger signal from cdf_discovery_aliasing writeback.
ALIASES_PROPERTY = "aliases"


def _aliases_hash(aliases: list[str]) -> str:
    payload = json.dumps(sorted(aliases), separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _event_aliases_changed(event: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return whether aliases changed and the new alias list."""
    changed_props = event.get("changed_properties") or event.get("changedProperties") or []
    if ALIASES_PROPERTY not in changed_props and "aliases" not in changed_props:
        before = (event.get("before") or {}).get("properties") or {}
        after = (event.get("after") or {}).get("properties") or {}
        if before.get("aliases") == after.get("aliases"):
            return False, []
    after_props = (event.get("after") or {}).get("properties") or event.get("properties") or {}
    aliases = after_props.get("aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    return bool(aliases), [str(a) for a in aliases if a]


def _subscription_watch_views(cfg: dict) -> list[str]:
    views: list[str] = []
    for key in ("asset_views", "file_views", "equipment_views", "timeseries_views"):
        for view in cfg.get(key) or []:
            if view and view not in views:
                views.append(view)
    return views or ["CogniteAsset"]


def _instance_type_for_view(view: str | None, cfg: dict) -> str:
    if not view:
        return str(cfg.get("default_instance_type", "asset"))
    for key, instance_type in (
        ("file_views", "file"),
        ("asset_views", "asset"),
        ("equipment_views", "equipment"),
        ("timeseries_views", "timeseries"),
    ):
        if view in (cfg.get(key) or []):
            return instance_type
    return VIEW_INSTANCE_TYPES.get(view, str(cfg.get("default_instance_type", "asset")))


def _matches_subscription_filter(event: dict[str, Any], cfg: dict) -> bool:
    space = event.get("space") or event.get("instance_space")
    view = event.get("view_external_id") or event.get("viewExternalId") or event.get("view")
    allowed_spaces = cfg.get("instance_spaces") or ["cdf_cdm"]
    allowed_views = _subscription_watch_views(cfg)
    if space and space not in allowed_spaces:
        return False
    if view and view not in allowed_views:
        return False
    return True


def _dedupe_key(instance_space: str, instance_external_id: str, aliases: list[str], scope_key: str) -> str:
    return f"{instance_space}:{instance_external_id}:{_aliases_hash(aliases)}:{scope_key}"


def _should_skip_dedupe(
    client: Any,
    dedupe_key: str,
    *,
    cfg: dict,
) -> bool:
    """Check RAW dedupe state row; skip when same aliases_hash within cooldown."""
    if not client or not cfg.get("enabled", True):
        return False
    raw_db = cfg.get("raw_database", "db_contextualization_idx")
    table = cfg.get("state_table", "target_driven_state")
    cooldown = int(cfg.get("cooldown_seconds", 300))
    try:
        row = client.raw.rows.retrieve(raw_db, table, dedupe_key)
        cols = getattr(row, "columns", None) or {}
        last_run = cols.get("LAST_RUN_AT")
        if not last_run:
            return False
        last_dt = datetime.fromisoformat(str(last_run).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - last_dt).total_seconds()
        return age < cooldown
    except Exception:
        return False


def _record_dedupe_run(client: Any, dedupe_key: str, summary: dict, *, cfg: dict) -> None:
    if not client:
        return
    from inverted_index.raw_ops import create_table_if_not_exists

    raw_db = cfg.get("raw_database", "db_contextualization_idx")
    table = cfg.get("state_table", "target_driven_state")
    create_table_if_not_exists(client, raw_db, table)
    now = datetime.now(timezone.utc).isoformat()
    client.raw.rows.insert(
        db_name=raw_db,
        table_name=table,
        row={
            dedupe_key: {
                "RECORD_KIND": "target_driven_state",
                "LAST_RUN_AT": now,
                "ALIASES_HASH": dedupe_key.split(":")[2] if ":" in dedupe_key else "",
                "LINKS_CREATED": int(summary.get("links_created") or 0),
                "REFERENCES_FOUND": int(summary.get("references_found") or 0),
            }
        },
    )


def handle_aliases_subscription_event(
    client: Any,
    event: dict[str, Any],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
) -> dict:
    """
    Process a CDF instance subscription event when ``aliases`` changes.

    Expected event shape (illustrative)::

        {
          "space": "cdf_cdm",
          "externalId": "ASSET_P101",
          "view_external_id": "CogniteAsset",
          "changed_properties": ["aliases"],
          "after": {"properties": {"aliases": ["P-101A", ...]}}
        }
    """
    sub_cfg = SUBSCRIPTION_CONFIG
    if not sub_cfg.get("enabled", True):
        return {"status": "skipped", "reason": "subscription_disabled"}

    if not _matches_subscription_filter(event, sub_cfg):
        return {"status": "skipped", "reason": "filter_no_match"}

    changed, aliases = _event_aliases_changed(event)
    if not changed:
        return {"status": "skipped", "reason": "aliases_unchanged_or_empty"}

    runtime = runtime_config or build_runtime_config()
    instance_space = event.get("space") or event.get("instance_space") or "cdf_cdm"
    instance_external_id = event.get("externalId") or event.get("external_id") or ""
    view = event.get("view_external_id") or event.get("viewExternalId") or event.get("view")
    instance_type = _instance_type_for_view(view, sub_cfg)

    if _should_skip_dedupe(client, _dedupe_key(instance_space, instance_external_id, aliases, ""), cfg=TARGET_DRIVEN_DEDUPE_CONFIG):
        return {"status": "skipped", "reason": "dedupe_cooldown"}

    storage_adapter = get_storage_adapter(runtime["storage_config"], client)
    summary = process_target_driven_contextualization(
        client,
        instance_external_id=instance_external_id,
        instance_type=instance_type,
        instance_space=instance_space,
        scope_config=runtime["scope_config"],
        direct_relation_config=runtime["direct_relation_config"],
        dry_run=dry_run,
        storage_adapter=storage_adapter,
    )

    if not dry_run and client:
        _record_dedupe_run(
            client,
            _dedupe_key(instance_space, instance_external_id, aliases, summary.get("match_scope_key") or ""),
            summary,
            cfg=TARGET_DRIVEN_DEDUPE_CONFIG,
        )

    return {"status": "ok", "trigger": "instance_subscription", **summary}


def handle_subscription_batch(
    client: Any,
    events: list[dict[str, Any]],
    *,
    dry_run: bool = False,
) -> list[dict]:
    """Process a batch of subscription events (e.g. from a CDF Function handler)."""
    results = []
    for event in events:
        try:
            results.append(handle_aliases_subscription_event(client, event, dry_run=dry_run))
        except Exception as exc:
            logger.exception("Subscription event failed")
            results.append({"status": "error", "error": str(exc), "event": event})
    return results
