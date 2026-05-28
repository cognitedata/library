"""Boolean ``join_on`` tree (``and`` / ``or`` / ``not`` + leaf operators) over left/right property paths."""

from __future__ import annotations

from typing import Any, Mapping

from cdf_fn_common.etl_property_path import get_property_path


def _scalar_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return ""
    return str(v)


def _normalize_join_operator(raw: str) -> str:
    op = str(raw or "EQUALS").strip().upper()
    if op == "EQUALS_IGNORE_CASE":
        return "IEQUALS"
    return op


def eval_join_on(
    left_props: Mapping[str, Any],
    right_props: Mapping[str, Any],
    node: Any,
) -> bool:
    if not isinstance(node, dict):
        raise TypeError("join_on node must be a mapping")

    if "and" in node:
        children = node["and"]
        if not isinstance(children, list):
            raise TypeError('"and" must be a list')
        if not children:
            raise ValueError('"and" list must not be empty')
        return all(eval_join_on(left_props, right_props, ch) for ch in children)

    if "or" in node:
        children = node["or"]
        if not isinstance(children, list):
            raise TypeError('"or" must be a list')
        if not children:
            raise ValueError('"or" list must not be empty')
        return any(eval_join_on(left_props, right_props, ch) for ch in children)

    if "not" in node:
        inner = node["not"]
        if not isinstance(inner, dict):
            raise TypeError('"not" value must be a mapping')
        return not eval_join_on(left_props, right_props, inner)

    op = _normalize_join_operator(str(node.get("operator") or "EQUALS"))
    lp = node.get("left_property")
    rp = node.get("right_property")
    if not isinstance(lp, str) or not lp.strip():
        raise ValueError("join_on leaf requires non-empty left_property")
    if not isinstance(rp, str) or not rp.strip():
        raise ValueError("join_on leaf requires non-empty right_property")

    lv = get_property_path(left_props, lp)
    rv = get_property_path(right_props, rp)
    ls = _scalar_str(lv)
    rs = _scalar_str(rv)

    if op == "EQUALS":
        return ls == rs
    if op == "IEQUALS":
        return ls.casefold() == rs.casefold()
    if op == "STARTS_WITH":
        return ls.startswith(rs)
    if op == "ENDS_WITH":
        return ls.endswith(rs)
    if op == "CONTAINS":
        return rs in ls
    raise ValueError(f"Unsupported join_on operator: {op!r}")
