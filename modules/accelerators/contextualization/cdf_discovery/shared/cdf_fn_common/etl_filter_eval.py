"""Evaluate filter trees against ETL row properties with ``{field}_score`` support."""

from __future__ import annotations

from typing import Any, List, Mapping, Sequence, Tuple

from .etl_common import _first_nonempty
from .etl_score_property import score_property_key
from .source_view_filter_build import (
    _coerce_values_list,
    _comparison_threshold,
    _filter_item_as_dict,
    normalize_filter_operator,
)


def _normalize_field_values(
    raw: Any,
    *,
    initial: float,
    field: str = "",
    parallel_source: Mapping[str, Any] | None = None,
) -> List[Tuple[Any, float]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [(s, initial)] if s else []
    if isinstance(raw, list):
        score_key = score_property_key(field) if field else ""
        if field and parallel_source is not None:
            par = parallel_source.get(score_key)
            if isinstance(par, list) and par and all(isinstance(x, str) for x in raw):
                strs = [str(x).strip() for x in raw if str(x).strip()]
                out: List[Tuple[Any, float]] = []
                for i, s in enumerate(strs):
                    try:
                        c = float(par[i]) if i < len(par) else initial
                    except (TypeError, ValueError):
                        c = initial
                    out.append((s, c))
                if out:
                    return out
        out2: List[Tuple[Any, float]] = []
        for item in raw:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    out2.append((s, initial))
            elif isinstance(item, dict):
                v = _first_nonempty(item.get("value"), item.get("key"))
                if not v:
                    continue
                try:
                    c = float(item.get("score", initial))
                except (TypeError, ValueError):
                    c = initial
                out2.append((v, c))
        return out2
    s = str(raw).strip()
    return [(s, initial)] if s else []


def parse_etl_filters(cfg: Mapping[str, Any]) -> List[Any]:
    raw = cfg.get("filters")
    if isinstance(raw, list) and raw:
        return list(raw)
    return []


def validate_filter_config(cfg: Mapping[str, Any]) -> None:
    if not _first_nonempty(cfg.get("description")):
        raise ValueError("filter config requires non-empty description")
    if not parse_etl_filters(cfg):
        raise ValueError("config requires non-empty filters")


def _prop_value(props: Mapping[str, Any], target_property: str, property_scope: str) -> Any:
    scope = str(property_scope or "view").lower()
    path = str(target_property or "").strip()
    if not path:
        return None
    if scope == "node":
        node = props.get("node")
        if isinstance(node, dict):
            return node.get(path)
        return None
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

    if op == "EXISTS":
        matched = bool(actual)
    elif op == "EQUALS":
        matched = _any_value_in(actual, values) if values else bool(actual)
    elif op in ("GT", "GTE", "LT", "LTE"):
        matched = _comparison_match(actual, op, _comparison_threshold(values))
    elif op == "IN" and isinstance(values, list):
        matched = _any_value_in(actual, values)
    else:
        matched = False

    return not matched if negate else matched


def _node_matches(props: Mapping[str, Any], node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    if "and" in node:
        children = node["and"]
        if not isinstance(children, list) or not children:
            return False
        return all(_node_matches(props, ch) for ch in children)
    if "or" in node:
        children = node["or"]
        if not isinstance(children, list) or not children:
            return False
        return any(_node_matches(props, ch) for ch in children)
    if "not" in node:
        inner = node["not"]
        if isinstance(inner, dict):
            return not _node_matches(props, inner)
        return False
    return _leaf_matches(props, node)


def row_passes_filter(props: Mapping[str, Any], filters: Sequence[Any] | None) -> bool:
    nodes = []
    for raw in filters or []:
        fd = _filter_item_as_dict(raw)
        if fd is not None:
            nodes.append(fd)
    if not nodes:
        return True
    return all(_node_matches(props, n) for n in nodes)
