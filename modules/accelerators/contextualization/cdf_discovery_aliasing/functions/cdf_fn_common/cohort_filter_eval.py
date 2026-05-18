"""Evaluate query-style ``filters`` trees against discovery cohort entity ``props``.

Uses the same YAML shape as ``source_views[].filters`` (leaf operators plus ``and`` /
``or`` / ``not`` groups).
"""

from __future__ import annotations

from typing import Any, List, Mapping, Sequence, Tuple

from .discovery_query_shared import _first_nonempty
from .discovery_validate import _normalize_field_values
from .property_path import get_property_path
from .source_view_filter_build import (
    _coerce_values_list,
    _comparison_threshold,
    _filter_item_as_dict,
    normalize_filter_operator,
)


def parse_cohort_filters(cfg: Mapping[str, Any]) -> List[Any]:
    """Return top-level filter nodes from ``filters``."""
    raw = cfg.get("filters")
    if isinstance(raw, list) and raw:
        return list(raw)
    return []


def validate_cohort_filters_config(
    cfg: Mapping[str, Any],
    *,
    require_description: bool = False,
) -> None:
    if require_description and not _first_nonempty(cfg.get("description")):
        raise ValueError("filter config requires non-empty description")
    if not parse_cohort_filters(cfg):
        raise ValueError("config requires non-empty filters")


def _prop_value(props: Mapping[str, Any], target_property: str, property_scope: str) -> Any:
    scope = str(property_scope or "view").lower()
    path = str(target_property or "").strip()
    if not path:
        return None
    if scope == "node":
        node = props.get("node")
        if isinstance(node, dict):
            if "." in path:
                return get_property_path(node, path)
            return node.get(path)
        return None
    if "." in path:
        return get_property_path(props, path)
    return props.get(path)


def _scored_pairs(
    props: Mapping[str, Any],
    target_property: str,
    property_scope: str,
) -> List[Tuple[Any, float]]:
    raw = _prop_value(props, target_property, property_scope)
    return _normalize_field_values(
        raw,
        initial=1.0,
        field=target_property,
        parallel_source=props,
    )


def _values_for_compare(pairs: Sequence[Tuple[Any, float]]) -> List[Any]:
    return [v for v, _ in pairs]


def _value_eq(a: Any, b: Any) -> bool:
    if a == b:
        return True
    if a is None or b is None:
        return False
    try:
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return float(a) == float(b)
    except (TypeError, ValueError):
        pass
    return str(a).strip().casefold() == str(b).strip().casefold()


def _any_value_in(actual: Sequence[Any], expected: Sequence[Any]) -> bool:
    for a in actual:
        for e in expected:
            if _value_eq(a, e):
                return True
    return False


def _all_expected_in_actual(actual: Sequence[Any], expected: Sequence[Any]) -> bool:
    if not expected:
        return False
    for e in expected:
        if not any(_value_eq(a, e) for a in actual):
            return False
    return True


def _any_expected_in_actual(actual: Sequence[Any], expected: Sequence[Any]) -> bool:
    return _any_value_in(actual, expected)


def _search_match(actual: Sequence[Any], needle: Any) -> bool:
    if needle is None:
        return False
    s = str(needle).strip().casefold()
    if not s:
        return False
    for a in actual:
        if s in str(a).casefold():
            return True
    return False


def _prefix_match(actual: Sequence[Any], prefix: Any) -> bool:
    if prefix is None:
        return False
    p = str(prefix).casefold()
    if not p:
        return False
    for a in actual:
        if str(a).casefold().startswith(p):
            return True
    return False


def _comparison_match(actual: Sequence[Any], op: str, threshold: Any) -> bool:
    try:
        bound = float(threshold)
    except (TypeError, ValueError):
        return False
    for a in actual:
        try:
            n = float(a)
        except (TypeError, ValueError):
            continue
        if op == "GT" and n > bound:
            return True
        if op == "GTE" and n >= bound:
            return True
        if op == "LT" and n < bound:
            return True
        if op == "LTE" and n <= bound:
            return True
    return False


def _range_match(actual: Sequence[Any], d: Mapping[str, Any]) -> bool:
    gt = d.get("gt")
    gte = d.get("gte")
    lt = d.get("lt")
    lte = d.get("lte")

    def in_range(val: Any) -> bool:
        try:
            n = float(val)
        except (TypeError, ValueError):
            return False
        if gt is not None and not (n > float(gt)):
            return False
        if gte is not None and not (n >= float(gte)):
            return False
        if lt is not None and not (n < float(lt)):
            return False
        if lte is not None and not (n <= float(lte)):
            return False
        return True

    return any(in_range(a) for a in actual)


def _leaf_matches(props: Mapping[str, Any], d: Mapping[str, Any]) -> bool:
    op = normalize_filter_operator(d.get("operator"))
    target_property = str(d.get("target_property") or "").strip()
    if not target_property:
        return False
    property_scope = str(d.get("property_scope", "view"))
    pairs = _scored_pairs(props, target_property, property_scope)
    actual = _values_for_compare(pairs)
    values = _coerce_values_list(d.get("values"))
    negate = bool(d.get("negate", False))

    matched: bool
    if op == "EXISTS":
        matched = bool(actual)
    elif op == "IN":
        matched = _any_value_in(actual, values) if values else False
    elif op == "EQUALS":
        if len(values) > 1:
            matched = _any_value_in(actual, values)
        elif len(values) == 1:
            matched = _any_value_in(actual, [values[0]])
        else:
            matched = not actual if values == [] else False
    elif op in ("CONTAINSALL", "CONTAINS_ALL"):
        matched = _all_expected_in_actual(actual, values)
    elif op in ("CONTAINSANY", "CONTAINS_ANY"):
        matched = _any_expected_in_actual(actual, values)
    elif op == "SEARCH":
        needle = values[0] if values else d.get("values")
        matched = _search_match(actual, needle)
    elif op == "PREFIX":
        pv = values[0] if values else None
        matched = _prefix_match(actual, pv)
    elif op in ("GT", "GTE", "LT", "LTE"):
        try:
            threshold = _comparison_threshold(values)
        except ValueError:
            matched = False
        else:
            matched = _comparison_match(actual, op, threshold)
    elif op == "RANGE":
        matched = _range_match(actual, d)
    else:
        matched = False

    if negate:
        matched = not matched
    return matched


def filter_node_matches(props: Mapping[str, Any], node: Any) -> bool:
    """Evaluate one filter node (leaf or boolean group) against cohort props."""
    d = _filter_item_as_dict(node)
    if d is None:
        return True
    if "and" in d:
        children = d["and"]
        if not isinstance(children, list) or not children:
            return False
        return all(filter_node_matches(props, ch) for ch in children)
    if "or" in d:
        children = d["or"]
        if not isinstance(children, list) or not children:
            return False
        return any(filter_node_matches(props, ch) for ch in children)
    if "not" in d:
        inner = d["not"]
        if not isinstance(inner, dict):
            return False
        return not filter_node_matches(props, inner)
    return _leaf_matches(props, d)


def row_matches_filters(props: Mapping[str, Any], filters: Sequence[Any] | None) -> bool:
    """Top-level filters are AND-combined (same as view query user filters)."""
    if not filters:
        return True
    return all(filter_node_matches(props, f) for f in filters)
