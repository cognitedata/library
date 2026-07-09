"""Save-stage fan-in merge: validate config, score cohort rows, merge_list / tie_break."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from cdf_fn_common.etl_incremental_scope import (
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    _row_ts_ms_for_hash_tiebreak,
)
from cdf_fn_common.etl_property_merge import (
    STRATEGY_MERGE_LIST,
    STRATEGY_TIE_BREAK,
    FieldPolicy,
    MergeListOptions,
    build_merged_props_for_instance,
    merge_list_property_value,
    merge_list_property_values_list,
    parse_field_policies,
    parse_field_policies_from_list,
)

LEGACY_SAVE_CONFIG_KEYS = frozenset({"write_back_fields", "write_properties", "include_properties"})

SAVE_FAN_IN_NONE = "none"
SAVE_FAN_IN_MERGE = "merge_per_instance"
SAVE_FAN_IN_MODES = frozenset({SAVE_FAN_IN_NONE, SAVE_FAN_IN_MERGE})


def _first_nonempty_str(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return s


def validate_save_config(cfg: Mapping[str, Any], *, save_kind: str) -> None:
    """Reject legacy keys; validate save_fan_in_mode and save_field_policies; classic metadata rule."""
    for k in LEGACY_SAVE_CONFIG_KEYS:
        if k not in cfg:
            continue
        raw = cfg.get(k)
        if raw is None:
            continue
        if isinstance(raw, list) and len(raw) == 0:
            continue
        raise ValueError(
            f"save config no longer supports {k!r}; use save_fan_in_mode and save_field_policies instead"
        )

    mode = cfg.get("save_fan_in_mode")
    if mode not in SAVE_FAN_IN_MODES:
        raise ValueError(
            f"save_fan_in_mode must be one of {sorted(SAVE_FAN_IN_MODES)}; got {mode!r}"
        )

    policies_raw = cfg.get("save_field_policies")
    if policies_raw is None or policies_raw == []:
        return
    policy_map = parse_field_policies_from_list(policies_raw, list_key_name="save_field_policies")
    for prop, pol in policy_map.items():
        if save_kind == "classic" and prop == "metadata" and pol.strategy == STRATEGY_MERGE_LIST:
            raise ValueError("save_classic: metadata cannot use merge_list; use tie_break only")


def score_cohort_row(cols: Mapping[str, Any], pred_index: int) -> Tuple[float, str, int]:
    """Comparable tuple: higher is better (ts, run_id lex, pred_index)."""
    c = dict(cols) if isinstance(cols, Mapping) else {}
    ts = _row_ts_ms_for_hash_tiebreak(c)
    rid = _first_nonempty_str(c.get(RUN_ID_COLUMN))
    return (ts, rid, pred_index)


def filter_props_internal(props: Mapping[str, Any], internal_keys: frozenset[str]) -> Dict[str, Any]:
    return {k: v for k, v in props.items() if k not in internal_keys}


__all__ = [
    "SAVE_FAN_IN_MERGE",
    "SAVE_FAN_IN_MODES",
    "SAVE_FAN_IN_NONE",
    "STRATEGY_MERGE_LIST",
    "STRATEGY_TIE_BREAK",
    "FieldPolicy",
    "MergeListOptions",
    "build_merged_props_for_instance",
    "filter_props_internal",
    "merge_list_property_value",
    "merge_list_property_values_list",
    "parse_field_policies",
    "score_cohort_row",
    "validate_save_config",
]
