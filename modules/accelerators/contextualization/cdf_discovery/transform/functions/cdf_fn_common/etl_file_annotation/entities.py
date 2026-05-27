"""Resolve diagram-detect ``entities`` for file annotation."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.etl_aliases_to_pattern_entities import (
    build_debug_explicit_pattern_entities,
    build_pattern_entities_from_asset_aliases,
    collect_aliases_from_cohort_rows,
    use_debug_explicit_pattern_samples,
)
from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


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


def entities_from_cohort_rows(
    rows: List[Mapping[str, Any]],
    cfg: Mapping[str, Any],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    p = dict(params or cfg)
    pattern_mode = cfg.get("pattern_mode")
    if pattern_mode is None:
        pattern_mode = True
    pattern_mode = bool(pattern_mode)

    if use_debug_explicit_pattern_samples(cfg):
        return build_debug_explicit_pattern_entities(p)

    if pattern_mode:
        return build_pattern_entities_from_cohort_rows(rows, p)

    prop = str(cfg.get("patterns_entity_property") or p.get("patterns_entity_property") or "aliases")
    search_field = str(cfg.get("search_field") or p.get("search_field") or "text")
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

    raw_entities = data.get("entities")
    if mode in {"payload", "auto"} and is_prebuilt_detect_entities(raw_entities):
        built = entities_from_prebuilt_payload(list(raw_entities))  # type: ignore[arg-type]
        if built:
            return built

    tid = dep_task_id or task_id_from_data(data, "entities_input_task_id")
    if not tid:
        tid = task_id_from_data(data, "input_a_task_id")

    rows: List[Mapping[str, Any]] = []
    if tid and client is not None:
        rows = predecessor_cohort_rows(client, data, tid)
    elif tid:
        rows = predecessor_cohort_rows(None, data, tid)

    if not rows and mode == "payload" and isinstance(raw_entities, list):
        if is_prebuilt_detect_entities(raw_entities):
            return entities_from_prebuilt_payload(raw_entities)  # type: ignore[arg-type]

    entities = entities_from_cohort_rows(rows, cfg, params=p)
    if not entities:
        prop = str(p.get("patterns_entity_property") or "aliases")
        n_vals = len(collect_context_values_from_cohort_rows(rows, property_name=prop))
        raise ValueError(
            "file_annotation: no pattern entities resolved "
            f"(cohort_rows={len(rows)}, property_values={n_vals}). "
            "Wire in__entities with a non-empty cohort or set entities_input_mode=payload."
        )
    return entities
