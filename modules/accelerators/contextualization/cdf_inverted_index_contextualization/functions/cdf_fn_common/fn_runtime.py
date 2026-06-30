"""Shared CDF Function runtime helpers for inverted index handlers."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_MODULE_ROOT = Path(__file__).resolve().parents[2]
if str(_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT))


def require_client(client: Any) -> Any:
    if client is None:
        from cognite.client import CogniteClient

        return CogniteClient()
    return client


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def resolve_handler_payload(data: dict[str, Any] | None) -> dict[str, Any]:
    """Merge function payload with module default.config.yaml runtime settings."""
    from inverted_index.config_loader import build_runtime_config

    payload = dict(data or {})
    runtime = build_runtime_config(payload.get("config") if isinstance(payload.get("config"), dict) else None)

    overrides: dict[str, Any] = {
        "dry_run": bool(payload.get("dry_run", False)),
        "filter_updated_after": _parse_datetime(payload.get("filter_updated_after")),
        "instance_spaces": payload.get("instance_spaces") or runtime.get("instance_spaces"),
        "index_field_config": payload.get("index_field_config") or runtime["index_field_config"],
        "scope_config": payload.get("scope_config") or runtime["scope_config"],
        "storage_config": payload.get("storage_config") or runtime["storage_config"],
        "annotation_config": payload.get("annotation_config") or runtime["annotation_index_config"],
        "direct_relation_config": payload.get("direct_relation_config")
        or runtime["direct_relation_config"],
        "subscription_config": payload.get("subscription_config") or runtime["subscription_config"],
        "target_driven_config": payload.get("target_driven_config")
        or runtime.get("target_driven_config"),
        "virtual_tag_creation_config": payload.get("virtual_tag_creation_config")
        or runtime.get("virtual_tag_creation_config"),
    }
    return {"runtime": runtime, "payload": payload, "overrides": overrides}


def storage_adapter_for(client: Any, storage_config: dict, *, dry_run: bool = False) -> Any:
    from inverted_index.storage import get_storage_adapter

    if dry_run:
        return None
    return get_storage_adapter(storage_config, client)
