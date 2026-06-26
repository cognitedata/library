"""CDF handler: target-driven contextualization."""

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
from inverted_index.target_driven import (  # noqa: E402
    process_target_driven_contextualization,
    run_target_driven_for_instance_ids,
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
        payload.get("instance_external_id") or payload.get("instance_id") or ""
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
    instance_type = str(payload.get("instance_type", "asset"))
    if instance_type not in ("asset", "file", "equipment", "timeseries"):
        instance_type = "asset"
    scope_keys = payload.get("match_scope_keys")
    if payload.get("match_scope_key"):
        scope_keys = [str(payload["match_scope_key"])]
    if isinstance(scope_keys, str):
        scope_keys = [scope_keys]
    instance_space = str(payload.get("instance_space", "cdf_cdm"))
    min_confidence = float(payload.get("min_confidence", 0.6))
    dry_run = overrides["dry_run"]
    storage_adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=dry_run,
    )
    scope_keys_list = list(scope_keys) if scope_keys else None
    scope_lookup_override = bool(scope_keys) and bool(
        payload.get("scope_lookup_override", True)
    )
    if len(instance_ids) == 1:
        return process_target_driven_contextualization(
            client,
            instance_external_id=instance_ids[0],
            instance_type=instance_type,  # type: ignore[arg-type]
            instance_space=instance_space,
            scope_config=overrides["scope_config"],
            direct_relation_config=overrides["direct_relation_config"],
            min_confidence=min_confidence,
            dry_run=dry_run,
            storage_adapter=storage_adapter,
            match_scope_keys=scope_keys_list,
            scope_lookup_override=scope_lookup_override,
        )
    return run_target_driven_for_instance_ids(
        client,
        instance_ids,
        instance_type=instance_type,  # type: ignore[arg-type]
        instance_space=instance_space,
        scope_config=overrides["scope_config"],
        direct_relation_config=overrides["direct_relation_config"],
        min_confidence=min_confidence,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        match_scope_keys=scope_keys_list,
        scope_lookup_override=scope_lookup_override,
    )
