"""Instance subscription handler for target-driven contextualization."""

from __future__ import annotations

import logging
from typing import Any

from inverted_index.cdm_relations import resolve_view_key
from inverted_index.config import SUBSCRIPTION_CONFIG
from inverted_index.config_loader import build_runtime_config
from inverted_index.storage import get_storage_adapter
from inverted_index.target_driven import (
    process_target_driven_contextualization,
    resolve_query_property,
)
from inverted_index.target_driven_dedupe import (
    record_target_driven_run,
    should_skip_target_driven,
)
from inverted_index.workflow_items import (
    workflow_item_to_event,
    workflow_items_to_events,
)

logger = logging.getLogger(__name__)


def _subscription_watch_property(cfg: dict, runtime_config: dict | None = None) -> str:
    if cfg.get("watch_property"):
        return str(cfg["watch_property"])
    td_cfg = (runtime_config or {}).get("target_driven_config") or {}
    return resolve_query_property(target_driven_config=td_cfg)


def _event_property_changed(
    event: dict[str, Any],
    property_path: str,
) -> tuple[bool, list[str]]:
    """Return whether the watched property changed and the new term list."""
    top_level = property_path.split(".")[0]
    changed_props = event.get("changed_properties") or event.get("changedProperties") or []
    if top_level not in changed_props and property_path not in changed_props:
        before = (event.get("before") or {}).get("properties") or {}
        after = (event.get("after") or {}).get("properties") or {}
        before_val = before.get(top_level) if top_level in before else before.get(property_path)
        after_val = after.get(top_level) if top_level in after else after.get(property_path)
        if before_val == after_val:
            return False, []

    after_props = (event.get("after") or {}).get("properties") or event.get("properties") or {}
    value = after_props.get(top_level) if top_level in after_props else after_props.get(property_path)
    if value is None:
        return False, []
    if isinstance(value, str):
        stripped = value.strip()
        return bool(stripped), [stripped] if stripped else []
    if isinstance(value, (list, tuple, set)):
        terms = [str(item) for item in value if item]
        return bool(terms), terms
    text = str(value).strip()
    return bool(text), [text] if text else []


def _subscription_watch_view_keys(cfg: dict, views: dict) -> list[str]:
    keys = cfg.get("watch_view_keys")
    if isinstance(keys, list) and keys:
        return [str(k) for k in keys]
    return list(views.keys())


def _incoming_view_key_for_event(
    event: dict[str, Any],
    *,
    views: dict,
    cfg: dict,
) -> str | None:
    view_ext = (
        event.get("view_external_id")
        or event.get("viewExternalId")
        or event.get("view")
    )
    space = event.get("space") or event.get("instance_space")
    if view_ext:
        return resolve_view_key(views, space=str(space) if space else None, external_id=str(view_ext))
    watch_keys = _subscription_watch_view_keys(cfg, views)
    return watch_keys[0] if len(watch_keys) == 1 else None


def workflow_item_to_subscription_event(
    item: dict[str, Any],
    *,
    watch_property: str,
    views: dict,
    watch_view_keys: list[str],
) -> dict[str, Any] | None:
    """Map a dataModeling WorkflowTrigger item to a subscription event dict."""
    return workflow_item_to_event(
        item,
        watch_property=watch_property,
        views=views,
        watch_view_keys=watch_view_keys,
    )


def workflow_items_to_subscription_events(
    items: list[dict[str, Any]],
    *,
    watch_property: str,
    views: dict,
    watch_view_keys: list[str],
) -> list[dict[str, Any]]:
    return workflow_items_to_events(
        items,
        watch_property=watch_property,
        views=views,
        watch_view_keys=watch_view_keys,
    )


def _matches_subscription_filter(
    event: dict[str, Any],
    cfg: dict,
    *,
    views: dict,
) -> bool:
    space = event.get("space") or event.get("instance_space")
    view_ext = event.get("view_external_id") or event.get("viewExternalId") or event.get("view")
    allowed_spaces = cfg.get("instance_spaces") or []
    watch_keys = _subscription_watch_view_keys(cfg, views)
    allowed_view_ext_ids = {
        str((views.get(k) or {}).get("external_id", k)) for k in watch_keys
    }
    if space and allowed_spaces and space not in allowed_spaces:
        return False
    if view_ext and allowed_view_ext_ids and str(view_ext) not in allowed_view_ext_ids:
        return False
    return True


def handle_aliases_subscription_event(
    client: Any,
    event: dict[str, Any],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> dict:
    """
    Process a CDF instance subscription event when the configured watch property changes.

    Expected event shape (illustrative)::

        {
          "space": "cdf_cdm",
          "externalId": "ASSET_P101",
          "view_external_id": "CogniteAsset",
          "changed_properties": ["aliases"],
          "after": {"properties": {"aliases": ["P-101A", ...]}}
        }
    """
    runtime = runtime_config or build_runtime_config()
    sub_cfg = runtime.get("subscription_config") or SUBSCRIPTION_CONFIG
    dr_cfg = runtime.get("direct_relation_config") or {}
    views = dr_cfg.get("views") or {}

    if not sub_cfg.get("enabled", True):
        return {"status": "skipped", "reason": "subscription_disabled"}

    if not _matches_subscription_filter(event, sub_cfg, views=views):
        return {"status": "skipped", "reason": "filter_no_match"}

    watch_property = _subscription_watch_property(sub_cfg, runtime)
    changed, query_terms = _event_property_changed(event, watch_property)
    if not changed:
        return {"status": "skipped", "reason": "query_property_unchanged_or_empty"}

    instance_space = event.get("space") or event.get("instance_space") or "cdf_cdm"
    instance_external_id = event.get("externalId") or event.get("external_id") or ""
    incoming_view_key = _incoming_view_key_for_event(event, views=views, cfg=sub_cfg)
    if not incoming_view_key:
        return {"status": "skipped", "reason": "view_key_unresolved"}

    if should_skip_target_driven(
        client,
        instance_space,
        instance_external_id,
        query_terms,
        "",
        force=force,
    ):
        return {"status": "skipped", "reason": "dedupe_cooldown"}

    storage_adapter = get_storage_adapter(runtime["storage_config"], client)
    summary = process_target_driven_contextualization(
        client,
        instance_external_id=instance_external_id,
        incoming_view_key=incoming_view_key,
        instance_space=instance_space,
        scope_config=runtime["scope_config"],
        direct_relation_config=dr_cfg,
        target_driven_config=runtime.get("target_driven_config"),
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        query_property=watch_property,
    )

    if not dry_run and client and not summary.get("skipped"):
        record_target_driven_run(
            client,
            instance_space,
            instance_external_id,
            query_terms,
            summary.get("match_scope_key") or "",
            summary,
        )

    return {"status": "ok", "trigger": "instance_subscription", **summary}


def handle_subscription_batch(
    client: Any,
    events: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> list[dict]:
    """Process a batch of subscription events (e.g. from a CDF Function handler)."""
    results = []
    for event in events:
        try:
            results.append(
                handle_aliases_subscription_event(
                    client,
                    event,
                    dry_run=dry_run,
                    runtime_config=runtime_config,
                    force=force,
                )
            )
        except Exception as exc:
            logger.exception("Subscription event failed")
            results.append({"status": "error", "error": str(exc), "event": event})
    return results


def handle_aliases_subscription_payload(
    client: Any,
    payload: dict[str, Any],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Handle a function payload with either workflow ``items`` or a single subscription ``event``."""
    runtime = runtime_config or build_runtime_config()
    sub_cfg = runtime.get("subscription_config") or SUBSCRIPTION_CONFIG
    dr_cfg = runtime.get("direct_relation_config") or {}
    views = dr_cfg.get("views") or {}
    watch_property = _subscription_watch_property(sub_cfg, runtime)
    watch_view_keys = _subscription_watch_view_keys(sub_cfg, views)

    items = payload.get("items")
    if isinstance(items, list) and items:
        events = workflow_items_to_subscription_events(
            items,
            watch_property=watch_property,
            views=views,
            watch_view_keys=watch_view_keys,
        )
        if not events:
            return {"status": "skipped", "reason": "no_mappable_workflow_items"}
        results = handle_subscription_batch(
            client,
            events,
            dry_run=dry_run,
            runtime_config=runtime,
            force=force,
        )
        ok_count = sum(1 for row in results if row.get("status") == "ok")
        return {
            "status": "ok",
            "trigger": "workflow_data_modeling",
            "processed": len(results),
            "ok_count": ok_count,
            "results": results,
        }

    event = payload.get("event") or payload
    if not isinstance(event, dict):
        return {"error": "event dict or items list is required"}
    return handle_aliases_subscription_event(
        client,
        event,
        dry_run=dry_run,
        runtime_config=runtime,
        force=force,
    )
