"""Shared helpers for dataModeling WorkflowTrigger item payloads."""

from __future__ import annotations

from typing import Any

from inverted_index.cdm_relations import view_external_id


def view_external_id_from_property_key(view_key: str) -> str:
    text = str(view_key).strip()
    if "/" in text:
        return text.split("/", 1)[0]
    return text


def workflow_item_view_properties(
    item: dict[str, Any],
    *,
    views: dict,
    watch_view_keys: list[str],
) -> tuple[str | None, dict[str, Any]]:
    """Return (view_external_id, flat view properties) from a workflow trigger item."""
    allowed_ext_ids = {
        view_external_id(views, key) for key in watch_view_keys if key in views or key
    }
    props = item.get("properties") or {}
    if not isinstance(props, dict):
        return None, {}
    for _space_key, space_props in props.items():
        if not isinstance(space_props, dict):
            continue
        for view_key, view_props in space_props.items():
            if not isinstance(view_props, dict):
                continue
            ext = view_external_id_from_property_key(view_key)
            if ext in allowed_ext_ids:
                return ext, dict(view_props)
    return None, {}


def workflow_item_to_event(
    item: dict[str, Any],
    *,
    watch_property: str,
    views: dict,
    watch_view_keys: list[str],
) -> dict[str, Any] | None:
    """Map a dataModeling WorkflowTrigger item to a subscription-style event dict."""
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
    if not view_ext:
        return None
    top_level = watch_property.split(".")[0]
    return {
        "space": instance_space,
        "externalId": instance_external_id,
        "view_external_id": view_ext,
        "changed_properties": [top_level],
        "after": {"properties": view_props},
    }


def workflow_items_to_events(
    items: list[dict[str, Any]],
    *,
    watch_property: str,
    views: dict,
    watch_view_keys: list[str],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        event = workflow_item_to_event(
            item,
            watch_property=watch_property,
            views=views,
            watch_view_keys=watch_view_keys,
        )
        if event:
            events.append(event)
    return events
