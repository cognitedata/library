"""Resolve diagram-detect ``entities`` for file annotation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.etl_aliases_to_pattern_entities import (
    build_debug_explicit_pattern_entities,
    build_pattern_entities_from_asset_aliases,
    collect_aliases_from_cohort_rows,
    use_debug_explicit_pattern_samples,
)
from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


def _agent_log(hypothesis_id: str, location: str, message: str, data: Mapping[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "e09635",
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": dict(data),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        with Path(
            "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-e09635.log"
        ).open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def collect_context_values_from_cohort_rows(
    rows: List[Mapping[str, Any]],
    *,
    property_name: str = "aliases",
) -> List[str]:
    return collect_aliases_from_cohort_rows(rows, property_name=property_name)


def build_pattern_entities_from_cohort_rows(
    rows: List[Mapping[str, Any]],
    params: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    return build_pattern_entities_from_asset_aliases(rows, params)


def is_prebuilt_detect_entities(val: Any) -> bool:
    if not isinstance(val, list) or not val:
        return False
    first = val[0]
    if not isinstance(first, dict):
        return False
    if "columns" in first or "properties" in first:
        return False
    return "sample" in first or "name" in first or "text" in first


def entities_from_prebuilt_payload(entities: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in entities:
        if isinstance(item, dict) and ("sample" in item or "name" in item or "text" in item):
            out.append(dict(item))
    return out


def _append_scalar_values(out: List[str], value: Any) -> None:
    if value is None:
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_scalar_values(out, item)
        return
    s = str(value).strip()
    if s:
        out.append(s)


def _collect_raw_entity_values(
    raw_entities: Any,
    *,
    value_key: str,
    fallback_key: str,
) -> List[str]:
    values: List[str] = []
    if raw_entities is None:
        return values
    if not isinstance(raw_entities, list):
        raw_entities = [raw_entities]
    for item in raw_entities:
        if isinstance(item, Mapping):
            props = item.get("properties") if isinstance(item.get("properties"), Mapping) else {}
            cols = item.get("columns") if isinstance(item.get("columns"), Mapping) else {}
            # Prebuilt entities may already include target detect keys.
            for key in (
                value_key,
                fallback_key,
                "sample",
                "text",
                "name",
                "externalId",
                "external_id",
                "aliases",
            ):
                if key in item:
                    _append_scalar_values(values, item.get(key))
                if isinstance(props, Mapping) and key in props:
                    _append_scalar_values(values, props.get(key))
                if isinstance(cols, Mapping) and key in cols:
                    _append_scalar_values(values, cols.get(key))
        else:
            _append_scalar_values(values, item)
    return sorted(set(values))


def entities_from_raw_payload(
    raw_entities: Any,
    cfg: Mapping[str, Any],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Normalize ad-hoc payload entities into detect ``entities``."""
    p = dict(params or cfg)
    pattern_mode = bool(cfg.get("pattern_mode", p.get("pattern_mode", False)))
    prop = str(cfg.get("patterns_entity_property") or p.get("patterns_entity_property") or "aliases")
    search_field = str(cfg.get("search_field") or p.get("search_field") or "aliases")
    resource_type = str(cfg.get("pattern_resource_type") or p.get("pattern_resource_type") or "equipment")

    if pattern_mode:
        samples = _collect_raw_entity_values(raw_entities, value_key="sample", fallback_key=prop)
        if not samples:
            return []
        return [{"sample": samples, "category": resource_type}]

    values = _collect_raw_entity_values(raw_entities, value_key=search_field, fallback_key=prop)
    if not values:
        return []
    return [{search_field: values, "category": resource_type}]


def entities_from_cohort_rows(
    rows: List[Mapping[str, Any]],
    cfg: Mapping[str, Any],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    p = dict(params or cfg)
    pattern_mode = cfg.get("pattern_mode")
    if pattern_mode is None:
        pattern_mode = False
    pattern_mode = bool(pattern_mode)

    if use_debug_explicit_pattern_samples(cfg):
        return build_debug_explicit_pattern_entities(p)

    if pattern_mode:
        return build_pattern_entities_from_cohort_rows(rows, p)

    prop = str(cfg.get("patterns_entity_property") or p.get("patterns_entity_property") or "aliases")
    search_field = str(cfg.get("search_field") or p.get("search_field") or "aliases")
    resource_type = str(cfg.get("pattern_resource_type") or p.get("pattern_resource_type") or "equipment")
    values = sorted(set(collect_context_values_from_cohort_rows(rows, property_name=prop)))
    if not values:
        return []
    return [{search_field: values, "category": resource_type}]


def resolve_file_annotation_entities(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *,
    client: Any = None,
    dep_task_id: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build diagram-detect entities from payload, config, or wired cohort."""
    p = dict(params or cfg)
    mode = str(cfg.get("entities_input_mode") or "auto").strip().lower()
    _agent_log(
        "H2",
        "entities.py:resolve_file_annotation_entities",
        "entity resolution start",
        {
            "mode": mode,
            "pattern_mode": bool(cfg.get("pattern_mode", p.get("pattern_mode", False))),
            "has_raw_entities": isinstance(data.get("entities"), list),
            "raw_entities_len": len(data.get("entities") or []) if isinstance(data.get("entities"), list) else 0,
        },
    )

    raw_entities = data.get("entities")
    if mode in {"payload", "auto"} and is_prebuilt_detect_entities(raw_entities):
        built = entities_from_prebuilt_payload(list(raw_entities))  # type: ignore[arg-type]
        if built:
            return built
    if mode in {"payload", "auto"}:
        built_from_raw = entities_from_raw_payload(raw_entities, cfg, params=p)
        if built_from_raw:
            return built_from_raw

    tids: List[str] = []
    seen_tids: set[str] = set()

    def _add_tid(raw: Any) -> None:
        s = str(raw or "").strip()
        if not s or s in seen_tids:
            return
        seen_tids.add(s)
        tids.append(s)

    def _add_tid_list(raw: Any) -> None:
        if raw is None:
            return
        if isinstance(raw, str):
            parts = [p.strip() for p in raw.replace(";", ",").split(",")]
            for p in parts:
                _add_tid(p)
            return
        if isinstance(raw, list):
            for item in raw:
                _add_tid(item)

    _add_tid(dep_task_id)
    _add_tid(task_id_from_data(data, "entities_input_task_id"))
    _add_tid(task_id_from_data(data, "input_a_task_id"))
    # Optional multi-source config override for entity inputs.
    _add_tid_list(data.get("entities_input_task_ids"))
    cfg_map = data.get("config") if isinstance(data.get("config"), dict) else {}
    if isinstance(cfg_map, Mapping):
        _add_tid_list(cfg_map.get("entities_input_task_ids"))
        _add_tid_list(cfg_map.get("input_a_task_ids"))
    payload_map = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    if isinstance(payload_map, Mapping):
        _add_tid_list(payload_map.get("entities_input_task_ids"))
        _add_tid_list(payload_map.get("input_a_task_ids"))

    rows: List[Mapping[str, Any]] = []
    for tid in tids:
        if client is not None:
            rows.extend(predecessor_cohort_rows(client, data, tid))
        else:
            rows.extend(predecessor_cohort_rows(None, data, tid))
    _agent_log(
        "H3",
        "entities.py:resolve_file_annotation_entities",
        "entity upstream rows collected",
        {
            "tids": tids,
            "rows_len": len(rows),
            "property": str(p.get("patterns_entity_property") or "aliases"),
        },
    )

    if not rows and mode == "payload" and isinstance(raw_entities, list):
        if is_prebuilt_detect_entities(raw_entities):
            return entities_from_prebuilt_payload(raw_entities)  # type: ignore[arg-type]

    entities = entities_from_cohort_rows(rows, cfg, params=p)
    _agent_log(
        "H4",
        "entities.py:resolve_file_annotation_entities",
        "entity resolution outcome",
        {"entities_len": len(entities), "keys": sorted(list(entities[0].keys())) if entities else []},
    )
    if not entities:
        prop = str(p.get("patterns_entity_property") or "aliases")
        n_vals = len(collect_context_values_from_cohort_rows(rows, property_name=prop))
        raise ValueError(
            "file_annotation: no pattern entities resolved "
            f"(cohort_rows={len(rows)}, property_values={n_vals}). "
            "Wire in__entities with a non-empty cohort, configure entities_input_task_ids/input_a_task_ids, "
            "or set entities_input_mode=payload."
        )
    return entities
