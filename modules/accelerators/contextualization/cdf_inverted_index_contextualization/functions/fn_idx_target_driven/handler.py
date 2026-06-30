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
from inverted_index.cdm_relations import view_external_id
from inverted_index.target_driven import (  # noqa: E402
    effective_query_fallbacks,
    process_target_driven_contextualization,
    require_incoming_view_key,
    resolve_query_property,
    run_target_driven_for_instance_ids,
)
from inverted_index.target_driven_dedupe import (  # noqa: E402
    record_target_driven_run,
    should_skip_target_driven,
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

    dr_cfg = overrides.get("direct_relation_config") or {}
    instance_space = str(payload.get("instance_space", "cdf_cdm"))
    view_space = str(payload.get("view_space", instance_space))
    try:
        incoming_view_key = require_incoming_view_key(
            incoming_view_key=payload.get("incoming_view_key"),
            view_external_id_param=payload.get("view_external_id"),
            view_space=view_space,
            direct_relation_config=dr_cfg,
        )
    except ValueError as exc:
        return {"error": str(exc)}

    scope_keys = payload.get("match_scope_keys")
    if payload.get("match_scope_key"):
        scope_keys = [str(payload["match_scope_key"])]
    if isinstance(scope_keys, str):
        scope_keys = [scope_keys]
    min_confidence = float(payload.get("min_confidence", 0.6))
    dry_run = overrides["dry_run"]
    force = bool(payload.get("force", False))
    query_property = resolve_query_property(
        payload.get("query_property"),
        overrides.get("target_driven_config"),
    )
    storage_adapter = storage_adapter_for(
        client,
        overrides["storage_config"],
        dry_run=dry_run,
    )
    scope_keys_list = list(scope_keys) if scope_keys else None
    scope_lookup_override = bool(scope_keys) and bool(
        payload.get("scope_lookup_override", True)
    )
    views = dr_cfg.get("views") or {}
    scope_view_id = view_external_id(views, incoming_view_key)

    if len(instance_ids) == 1:
        only_id = instance_ids[0]
        if not dry_run and not force:
            nodes = client.data_modeling.instances.retrieve_nodes(
                [(instance_space, only_id)]
            )
            if nodes:
                instance = {
                    "externalId": nodes[0].external_id,
                    "space": instance_space,
                    "properties": dict(nodes[0].properties or {}),
                }
                from inverted_index.aliases import read_instance_query_terms
                from inverted_index.scope import resolve_match_scope

                td_cfg = overrides.get("target_driven_config") or {}
                terms = read_instance_query_terms(
                    instance,
                    query_property,
                    fallbacks=effective_query_fallbacks(td_cfg),
                )
                scope_key, _ = resolve_match_scope(
                    instance, scope_view_id, overrides["scope_config"]
                )
                if terms and should_skip_target_driven(
                    client,
                    instance_space,
                    only_id,
                    terms,
                    scope_key or "",
                    force=force,
                ):
                    return {
                        "status": "skipped",
                        "reason": "dedupe_cooldown",
                        "instance_external_id": only_id,
                    }
        result = process_target_driven_contextualization(
            client,
            instance_external_id=only_id,
            incoming_view_key=incoming_view_key,
            instance_space=instance_space,
            scope_config=overrides["scope_config"],
            direct_relation_config=dr_cfg,
            target_driven_config=overrides.get("target_driven_config"),
            min_confidence=min_confidence,
            dry_run=dry_run,
            storage_adapter=storage_adapter,
            match_scope_keys=scope_keys_list,
            scope_lookup_override=scope_lookup_override,
            query_property=query_property,
        )
        if (
            not dry_run
            and client
            and not result.get("skipped")
            and (result.get("query_terms") or [])
        ):
            record_target_driven_run(
                client,
                instance_space,
                only_id,
                result.get("query_terms") or [],
                result.get("match_scope_key") or "",
                result,
            )
        return result
    return run_target_driven_for_instance_ids(
        client,
        instance_ids,
        incoming_view_key=incoming_view_key,
        instance_space=instance_space,
        scope_config=overrides["scope_config"],
        direct_relation_config=dr_cfg,
        target_driven_config=overrides.get("target_driven_config"),
        min_confidence=min_confidence,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        match_scope_keys=scope_keys_list,
        scope_lookup_override=scope_lookup_override,
        query_property=query_property,
        force=force,
    )
