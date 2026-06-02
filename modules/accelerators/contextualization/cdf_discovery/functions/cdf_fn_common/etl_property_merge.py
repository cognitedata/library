"""Shared property fan-in merge: field policies, merge_list, tie_break (save, transform, merge stage)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

STRATEGY_TIE_BREAK = "tie_break"
STRATEGY_MERGE_LIST = "merge_list"


@dataclass(frozen=True)
class MergeListOptions:
    unique: bool = True
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


def parse_field_policies_from_list(
    policies_raw: Any,
    *,
    list_key_name: str = "field_policies",
) -> Dict[str, FieldPolicy]:
    """Parse a list of policy dicts (save_field_policies or field_policies)."""
    if policies_raw is None or policies_raw == []:
        return {}
    if not isinstance(policies_raw, list):
        raise ValueError(f"{list_key_name} must be a list when provided")
    out: Dict[str, FieldPolicy] = {}
    for i, entry in enumerate(policies_raw):
        if not isinstance(entry, dict):
            raise ValueError(f"{list_key_name}[{i}] must be an object")
        prop = _first_nonempty_str(entry.get("property"))
        if not prop:
            raise ValueError(f"{list_key_name}[{i}].property is required")
        strat = _first_nonempty_str(entry.get("strategy")) or STRATEGY_TIE_BREAK
        if strat not in (STRATEGY_TIE_BREAK, STRATEGY_MERGE_LIST):
            raise ValueError(
                f"{list_key_name}[{i}].strategy must be tie_break or merge_list; got {strat!r}"
            )
        ml_raw = entry.get("merge_list") if isinstance(entry.get("merge_list"), dict) else {}
        ml = MergeListOptions(
            unique=bool(ml_raw["unique"]) if "unique" in ml_raw else True,
            branch_order=_first_nonempty_str(ml_raw.get("branch_order")) or "by_score",
        )
        if strat == STRATEGY_MERGE_LIST:
            bo = ml.branch_order
            if bo not in ("by_score", "by_dependency"):
                raise ValueError(
                    f"{list_key_name}[{i}].merge_list.branch_order must be by_score or by_dependency"
                )
        out[prop] = FieldPolicy(property_name=prop, strategy=strat, merge_list=ml)
    return out


def parse_field_policies(cfg: Mapping[str, Any]) -> Dict[str, FieldPolicy]:
    """property_name -> FieldPolicy from field_policies."""
    return parse_field_policies_from_list(cfg.get("field_policies"), list_key_name="field_policies")


def _segment_string_for_merge_list(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, list):
        if not v:
            return None
        return ",".join(str(e) for e in v)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    return str(v)


def _tokens_from_property_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            if isinstance(item, dict):
                raw = item.get("value")
                s = str(raw).strip() if raw is not None else ""
            else:
                s = str(item).strip()
            if s:
                out.append(s)
        return out
    if isinstance(value, dict) and "value" in value:
        s = str(value.get("value") or "").strip()
        return [s] if s else []
    s = str(value).strip()
    return [s] if s else []


def merge_list_property_values_list(
    rows_props_ordered: Sequence[Mapping[str, Any]],
    property_name: str,
    *,
    unique: bool,
) -> Optional[List[str]]:
    items: List[str] = []
    for props in rows_props_ordered:
        if not isinstance(props, Mapping):
            continue
        items.extend(_tokens_from_property_value(props.get(property_name)))
    if unique and items:
        seen: set[str] = set()
        deduped: List[str] = []
        for s in items:
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        items = deduped
    return items if items else None


def merge_list_property_value(
    rows_props_ordered: Sequence[Mapping[str, Any]],
    property_name: str,
    *,
    unique: bool,
) -> Optional[str]:
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


def _sort_by_score(
    rows: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
) -> List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]:
    return sorted(rows, key=lambda r: (r[0][0], r[0][1], r[0][2]), reverse=True)


def _sort_by_dependency(
    rows: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
) -> List[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]]:
    return sorted(rows, key=lambda r: (r[1], r[0][0], r[0][1], r[0][2]))


def build_merged_props_for_instance(
    rows_scored: Sequence[Tuple[Tuple[float, str, int], int, Mapping[str, Any]]],
    policy_map: Dict[str, FieldPolicy],
) -> Dict[str, Any]:
    """Merge property dicts from parallel branches using field policies."""
    if not rows_scored:
        return {}

    winner_order = _sort_by_score(list(rows_scored))
    winner_props = dict(winner_order[0][2])
    out: Dict[str, Any] = {}
    policies_only = bool(policy_map)

    if policies_only:
        for prop, pol in policy_map.items():
            if pol.strategy == STRATEGY_TIE_BREAK:
                if prop in winner_props:
                    out[prop] = winner_props[prop]
                continue
            if pol.strategy != STRATEGY_MERGE_LIST:
                continue
            ordered = (
                _sort_by_dependency(rows_scored)
                if pol.merge_list.branch_order == "by_dependency"
                else _sort_by_score(list(rows_scored))
            )
            order_props = [props for _, _, props in ordered]
            merged_list = merge_list_property_values_list(
                order_props,
                prop,
                unique=pol.merge_list.unique,
            )
            if merged_list is not None:
                out[prop] = merged_list
            elif prop in winner_props:
                out[prop] = winner_props[prop]
        return out

    for k, v in winner_props.items():
        pol = policy_map.get(k)
        if pol is None or pol.strategy == STRATEGY_TIE_BREAK:
            out[k] = v

    for prop, pol in policy_map.items():
        if pol.strategy != STRATEGY_MERGE_LIST:
            continue
        ordered = (
            _sort_by_dependency(rows_scored)
            if pol.merge_list.branch_order == "by_dependency"
            else _sort_by_score(list(rows_scored))
        )
        order_props = [props for _, _, props in ordered]
        merged_list = merge_list_property_values_list(
            order_props,
            prop,
            unique=pol.merge_list.unique,
        )
        if merged_list is not None:
            out[prop] = merged_list
        elif prop in winner_props:
            out[prop] = winner_props[prop]

    return out


def merge_property_dicts(
    branch_props: Sequence[Mapping[str, Any]],
    policy_map: Dict[str, FieldPolicy],
    *,
    default_score: Tuple[float, str, int] = (0.0, "", 0),
) -> Dict[str, Any]:
    """Merge parallel transform/merge branches without cohort row scoring."""
    if not branch_props:
        return {}
    if len(branch_props) == 1:
        return dict(branch_props[0])
    rows_scored = [
        (default_score, i, dict(p)) for i, p in enumerate(branch_props) if isinstance(p, Mapping)
    ]
    return build_merged_props_for_instance(rows_scored, policy_map)
