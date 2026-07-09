"""CDF handler: incremental metadata index write for one or more DM instances."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.fn_runtime import (  # noqa: E402
    require_client,
    resolve_handler_payload,
    storage_adapter_for,
)
from inverted_index.incremental import (  # noqa: E402
    build_metadata_index_for_instance,
    build_metadata_index_for_instance_ids,
)


def _resolve_instance_ids(payload: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    raw_ids = payload.get("instance_external_ids")
    if isinstance(raw_ids, list):
        for item in raw_ids:
            for part in str(item).split(","):
                key = part.strip()
                if key and key not in seen:
                    seen.add(key)
                    out.append(key)
    single = str(
        payload.get("instance_external_id")
        or payload.get("instance_id")
        or payload.get("external_id")
        or ""
    ).strip()
    if single and single not in seen:
        out.append(single)
    return out


def handle(data: dict[str, Any] | None = None, client: Any = None) -> dict[str, Any]:
    resolved = resolve_handler_payload(data)
    payload = resolved["payload"]
    overrides = resolved["overrides"]
    client = require_client(client)

    instance_ids = _resolve_instance_ids(payload)
    if not instance_ids:
        return {"error": "instance_external_id or instance_external_ids is required"}

    view_external_id = payload.get("view_external_id") or payload.get("view")
    incoming_view_key = payload.get("incoming_view_key")
    if not view_external_id and not incoming_view_key:
        return {"error": "view_external_id or incoming_view_key is required"}

    adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=overrides["dry_run"],
    )
    write_mode = str(payload.get("write_mode", "replace"))
    instance_space = str(payload.get("instance_space") or payload.get("space") or "cdf_cdm")
    common_kwargs = {
        "view_external_id": str(view_external_id) if view_external_id else None,
        "incoming_view_key": str(incoming_view_key) if incoming_view_key else None,
        "direct_relation_config": overrides.get("direct_relation_config"),
        "instance_space": instance_space,
        "write_mode": write_mode,  # type: ignore[arg-type]
        "index_field_config": overrides["index_field_config"],
        "scope_config": overrides["scope_config"],
        "storage_config": overrides["storage_config"],
        "dry_run": overrides["dry_run"],
        "storage_adapter": adapter,
        "virtual_tag_creation_config": overrides.get("virtual_tag_creation_config"),
    }
    if len(instance_ids) == 1:
        return build_metadata_index_for_instance(
            client,
            instance_ids[0],
            **common_kwargs,
        )
    return build_metadata_index_for_instance_ids(
        client,
        instance_ids,
        **common_kwargs,
    )
