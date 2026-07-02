"""Incremental metadata index maintenance for configured source views."""

from __future__ import annotations

import logging
from typing import Any

from inverted_index.cdm_relations import resolve_view_key
from inverted_index.config import INDEX_FIELD_CONFIG, SOURCE_INDEX_CONFIG
from inverted_index.config_loader import build_runtime_config
from inverted_index.dm_query import collect_view_property_paths
from inverted_index.incremental import build_metadata_index_for_instance
from inverted_index.source_index_dedupe import (
    record_source_index_run,
    should_skip_source_index,
    source_content_hash,
)
from inverted_index.storage import get_storage_adapter
from inverted_index.workflow_items import workflow_item_view_properties

logger = logging.getLogger(__name__)


def resolve_source_watch_view_keys(
    index_field_config: list[dict],
    direct_relation_config: dict | None,
) -> list[str]:
    """Derive watch_view_keys from index_field_config and direct_relation_config.views."""
    views = (direct_relation_config or {}).get("views") or {}
    configured_ext_ids = {
        str(v.get("view")) for v in index_field_config if v.get("view")
    }
    keys: list[str] = []
    for key, ref in views.items():
        if isinstance(ref, dict) and str(ref.get("external_id", "")) in configured_ext_ids:
            keys.append(str(key))
    if keys:
        return keys
    for view_cfg in index_field_config:
        ext = str(view_cfg.get("view", ""))
        if not ext:
            continue
        resolved = resolve_view_key(views, external_id=ext)
        if resolved and resolved not in keys:
            keys.append(resolved)
    return keys


def resolve_source_watch_properties(
    view_external_id: str,
    index_field_config: list[dict],
    scope_config: dict | None,
) -> list[str]:
    return collect_view_property_paths(
        view_external_id=view_external_id,
        index_field_config=index_field_config,
        scope_config=scope_config,
    )


def _indexed_view_external_ids(index_field_config: list[dict]) -> set[str]:
    return {str(v.get("view")) for v in index_field_config if v.get("view")}


def workflow_item_to_source_index_event(
    item: dict[str, Any],
    *,
    views: dict,
    watch_view_keys: list[str],
    index_field_config: list[dict],
    scope_config: dict | None,
) -> dict[str, Any] | None:
    """Map a dataModeling WorkflowTrigger item to a source index event dict."""
    if not isinstance(item, dict):
        return None
    instance_external_id = str(
        item.get("externalId") or item.get("external_id") or ""
    ).strip()
    if not instance_external_id:
        return None
    instance_space = str(item.get("space") or item.get("instance_space") or "cdf_cdm")
    view_ext, view_props = workflow_item_view_properties(
        item,
        views=views,
        watch_view_keys=watch_view_keys,
    )
    if not view_ext or view_ext not in _indexed_view_external_ids(index_field_config):
        return None
    watch_properties = resolve_source_watch_properties(
        view_ext,
        index_field_config,
        scope_config,
    )
    return {
        "space": instance_space,
        "externalId": instance_external_id,
        "view_external_id": view_ext,
        "properties": view_props,
        "watch_properties": watch_properties,
    }


def workflow_items_to_source_index_events(
    items: list[dict[str, Any]],
    *,
    views: dict,
    watch_view_keys: list[str],
    index_field_config: list[dict],
    scope_config: dict | None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for item in items:
        event = workflow_item_to_source_index_event(
            item,
            views=views,
            watch_view_keys=watch_view_keys,
            index_field_config=index_field_config,
            scope_config=scope_config,
        )
        if event:
            events.append(event)
    return events


def _has_indexable_content(view_props: dict[str, Any], watch_properties: list[str]) -> bool:
    for path in watch_properties:
        top = path.split(".")[0]
        value = view_props.get(top) if top in view_props else view_props.get(path)
        if value is None:
            continue
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, (list, tuple, set)) and any(value):
            return True
        if value not in ("", [], {}):
            return True
    return False


def _matches_source_index_filter(
    event: dict[str, Any],
    cfg: dict,
    *,
    indexed_view_ext_ids: set[str],
) -> bool:
    space = event.get("space") or event.get("instance_space")
    view_ext = event.get("view_external_id") or event.get("viewExternalId")
    allowed_spaces = cfg.get("instance_spaces") or []
    if space and allowed_spaces and space not in allowed_spaces:
        return False
    if view_ext and indexed_view_ext_ids and str(view_ext) not in indexed_view_ext_ids:
        return False
    return True


def handle_source_metadata_event(
    client: Any,
    event: dict[str, Any],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> dict:
    runtime = runtime_config or build_runtime_config()
    src_cfg = runtime.get("source_index_config") or SOURCE_INDEX_CONFIG
    if not src_cfg.get("enabled", True):
        return {"status": "skipped", "reason": "source_index_disabled"}

    index_field_config = runtime.get("index_field_config") or INDEX_FIELD_CONFIG
    indexed_view_ext_ids = _indexed_view_external_ids(index_field_config)
    if not _matches_source_index_filter(
        event, src_cfg, indexed_view_ext_ids=indexed_view_ext_ids
    ):
        return {"status": "skipped", "reason": "filter_mismatch"}

    view_ext = str(event.get("view_external_id") or "")
    view_props = event.get("properties") or {}
    watch_properties = event.get("watch_properties") or resolve_source_watch_properties(
        view_ext,
        index_field_config,
        runtime.get("scope_config"),
    )
    if not _has_indexable_content(view_props, watch_properties):
        return {"status": "skipped", "reason": "no_indexable_properties"}

    instance_external_id = str(event.get("externalId") or event.get("external_id") or "")
    instance_space = str(event.get("space") or event.get("instance_space") or "cdf_cdm")
    content_hash = source_content_hash(view_props, watch_properties)
    dedupe_cfg = src_cfg.get("dedupe") or {}
    if should_skip_source_index(
        client,
        instance_space,
        instance_external_id,
        content_hash,
        cfg=dedupe_cfg,
        force=force,
    ):
        return {
            "status": "skipped",
            "reason": "dedupe_cooldown",
            "instance_external_id": instance_external_id,
        }

    dr_cfg = runtime.get("direct_relation_config") or {}
    views = dr_cfg.get("views") or {}
    incoming_view_key = resolve_view_key(views, space=instance_space, external_id=view_ext)

    storage_cfg = runtime.get("storage_config") or {}
    storage_adapter = None
    if not dry_run and client is not None:
        storage_adapter = get_storage_adapter(storage_cfg, client)

    summary = build_metadata_index_for_instance(
        client,
        instance_external_id,
        view_external_id=view_ext,
        incoming_view_key=incoming_view_key,
        direct_relation_config=dr_cfg,
        instance_space=instance_space,
        write_mode="replace",
        index_field_config=index_field_config,
        scope_config=runtime.get("scope_config"),
        storage_config=storage_cfg,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        virtual_tag_creation_config=runtime.get("virtual_tag_creation_config"),
    )

    if summary.get("reason") == "scope_property_override_ambiguous":
        return {
            "status": "skipped",
            "reason": "scope_property_override_ambiguous",
            "matching_override_keys": summary.get("matching_override_keys", []),
            "instance_external_id": instance_external_id,
        }

    if not dry_run and client:
        record_source_index_run(
            client,
            instance_space,
            instance_external_id,
            content_hash,
            summary,
            cfg=dedupe_cfg,
        )

    return {"status": "ok", "trigger": "source_metadata", **summary}


def handle_source_metadata_batch(
    client: Any,
    events: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> list[dict]:
    results = []
    for event in events:
        try:
            results.append(
                handle_source_metadata_event(
                    client,
                    event,
                    dry_run=dry_run,
                    runtime_config=runtime_config,
                    force=force,
                )
            )
        except Exception as exc:
            logger.exception("Source metadata index event failed")
            results.append({"status": "error", "error": str(exc), "event": event})
    return results


def handle_source_metadata_payload(
    client: Any,
    payload: dict[str, Any],
    *,
    dry_run: bool = False,
    runtime_config: dict | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Handle a function payload with either workflow ``items`` or a single ``event``."""
    resolved_force = force or bool(payload.get("force", False))
    runtime = runtime_config or build_runtime_config()
    src_cfg = runtime.get("source_index_config") or SOURCE_INDEX_CONFIG
    dr_cfg = runtime.get("direct_relation_config") or {}
    views = dr_cfg.get("views") or {}
    index_field_config = runtime.get("index_field_config") or INDEX_FIELD_CONFIG
    watch_view_keys = src_cfg.get("watch_view_keys") or []
    if not watch_view_keys:
        watch_view_keys = resolve_source_watch_view_keys(index_field_config, dr_cfg)

    items = payload.get("items")
    if isinstance(items, list) and items:
        events = workflow_items_to_source_index_events(
            items,
            views=views,
            watch_view_keys=watch_view_keys,
            index_field_config=index_field_config,
            scope_config=runtime.get("scope_config"),
        )
        if not events:
            return {"status": "skipped", "reason": "no_mappable_workflow_items"}
        results = handle_source_metadata_batch(
            client,
            events,
            dry_run=dry_run,
            runtime_config=runtime,
            force=resolved_force,
        )
        ok_count = sum(1 for row in results if row.get("status") == "ok")
        skipped_count = sum(1 for row in results if row.get("status") == "skipped")
        return {
            "status": "ok",
            "trigger": "workflow_data_modeling",
            "processed": len(results),
            "ok_count": ok_count,
            "skipped_count": skipped_count,
            "results": results,
        }

    event = payload.get("event") or payload
    if not isinstance(event, dict):
        return {"error": "event dict or items list is required"}
    return handle_source_metadata_event(
        client,
        event,
        dry_run=dry_run,
        runtime_config=runtime,
        force=resolved_force,
    )
