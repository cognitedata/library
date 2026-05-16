"""Save-stage fan-in merge: validate config, score cohort rows, merge_list / tie_break."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from .incremental_scope import (
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    _row_ts_ms_for_hash_tiebreak,
)

LEGACY_SAVE_CONFIG_KEYS = frozenset({"write_back_fields", "write_properties", "include_properties"})

SAVE_FAN_IN_NONE = "none"
SAVE_FAN_IN_MERGE = "merge_per_instance"
SAVE_FAN_IN_MODES = frozenset({SAVE_FAN_IN_NONE, SAVE_FAN_IN_MERGE})

STRATEGY_TIE_BREAK = "tie_break"
STRATEGY_MERGE_LIST = "merge_list"


@dataclass(frozen=True)
class MergeListOptions:
    unique: bool = False
    branch_order: str = "by_score"  # by_score | by_dependency


@dataclass(frozen=True)
class FieldPolicy:
    property_name: str
    strategy: str
    merge_list: MergeListOptions


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
    if not isinstance(policies_raw, list):
        raise ValueError("save_field_policies must be a list when provided")
    for i, entry in enumerate(policies_raw):
        if not isinstance(entry, dict):
            raise ValueError(f"save_field_policies[{i}] must be an object")
        prop = _first_nonempty_str(entry.get("property"))
        if not prop:
            raise ValueError(f"save_field_policies[{i}].property is required")
        strat = _first_nonempty_str(entry.get("strategy")) or STRATEGY_TIE_BREAK
        if strat not in (STRATEGY_TIE_BREAK, STRATEGY_MERGE_LIST):
            raise ValueError(
                f"save_field_policies[{i}].strategy must be tie_break or merge_list; got {strat!r}"
            )
        if save_kind == "classic" and prop == "metadata" and strat == STRATEGY_MERGE_LIST:
            raise ValueError("save_classic: metadata cannot use merge_list; use tie_break only")
        if strat == STRATEGY_MERGE_LIST:
            ml = entry.get("merge_list") if isinstance(entry.get("merge_list"), dict) else {}
            bo = _first_nonempty_str(ml.get("branch_order")) or "by_score"
            if bo not in ("by_score", "by_dependency"):
                raise ValueError(
                    f"save_field_policies[{i}].merge_list.branch_order must be by_score or by_dependency"
                )


def parse_field_policies(cfg: Mapping[str, Any]) -> Dict[str, FieldPolicy]:
    """property_name -> FieldPolicy. Empty dict when save_field_policies omitted."""
    policies_raw = cfg.get("save_field_policies")
    if policies_raw is None or policies_raw == []:
        return {}
    out: Dict[str, FieldPolicy] = {}
    for entry in policies_raw:
        if not isinstance(entry, dict):
            continue
        prop = _first_nonempty_str(entry.get("property"))
        if not prop:
            continue
        strat = _first_nonempty_str(entry.get("strategy")) or STRATEGY_TIE_BREAK
        ml_raw = entry.get("merge_list") if isinstance(entry.get("merge_list"), dict) else {}
        ml = MergeListOptions(
            unique=bool(ml_raw.get("unique")),
            branch_order=_first_nonempty_str(ml_raw.get("branch_order")) or "by_score",
        )
        out[prop] = FieldPolicy(property_name=prop, strategy=strat, merge_list=ml)
    return out


def score_cohort_row(cols: Mapping[str, Any], pred_index: int) -> Tuple[float, str, int]:
    """Comparable tuple: higher is better (ts, run_id lex, pred_index)."""
    c = dict(cols) if isinstance(cols, Mapping) else {}
    ts = _row_ts_ms_for_hash_tiebreak(c)
    rid = _first_nonempty_str(c.get(RUN_ID_COLUMN))
    return (ts, rid, pred_index)


def _segment_string_for_merge_list(v: Any) -> Optional[str]:
    """One row's contribution as segment string; None to skip."""
    if v is None:
        return None
    if isinstance(v, list):
        if not v:
            return None
        return ",".join(str(e) for e in v)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    return str(v)


def merge_list_property_value(
    rows_props_ordered: Sequence[Mapping[str, Any]],
    property_name: str,
    *,
    unique: bool,
) -> Optional[str]:
    """
    Build CSV-style string across rows (already in branch order).
    rows_props_ordered: property dicts per row, best row first or dependency order.
    """
    segments: List[str] = []
    for props in rows_props_ordered:
        if not isinstance(props, Mapping):
            continue
        seg = _segment_string_for_merge_list(props.get(property_name))
        if seg is None or seg == "":
            continue
        segments.append(seg)
    if unique and segments:
        seen: set[str] = set()
        deduped: List[str] = []
        for s in segments:
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        segments = deduped
    if not segments:
        return None
    return ",".join(segments)


def build_merged_props_for_instance(
    rows_scored: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
    policy_map: Dict[str, FieldPolicy],
) -> Dict[str, Any]:
    """
    rows_scored: (score_tuple, pred_index, props) for one instance (unordered).
    Winner for tie_break: best by score (ts desc, run_id desc, pred_index desc).
    merge_list: each property uses its policy merge_list.branch_order.
    """
    if not rows_scored:
        return {}

    def sort_by_score(
        rows: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
    ) -> List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]:
        return sorted(rows, key=lambda r: (r[0][0], r[0][1], r[0][2]), reverse=True)

    def sort_by_dependency(
        rows: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
    ) -> List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]:
        return sorted(rows, key=lambda r: (r[1], r[0][0], r[0][1], r[0][2]))

    winner_order = sort_by_score(list(rows_scored))
    winner_props = dict(winner_order[0][2])
    out: Dict[str, Any] = {}

    for k, v in winner_props.items():
        pol = policy_map.get(k)
        if pol is None or pol.strategy == STRATEGY_TIE_BREAK:
            out[k] = v

    for prop, pol in policy_map.items():
        if pol.strategy != STRATEGY_MERGE_LIST:
            continue
        ordered = (
            sort_by_dependency(rows_scored)
            if pol.merge_list.branch_order == "by_dependency"
            else sort_by_score(list(rows_scored))
        )
        order_props = [props for _, _, props in ordered]
        merged = merge_list_property_value(
            order_props,
            prop,
            unique=pol.merge_list.unique,
        )
        if merged is not None:
            out[prop] = merged
        elif prop in winner_props:
            out[prop] = winner_props[prop]

    return out


def filter_props_internal(props: Mapping[str, Any], internal_keys: frozenset[str]) -> Dict[str, Any]:
    return {k: v for k, v in props.items() if k not in internal_keys}
