"""Build Cognite DM filters from ``index_field_config`` view ``filters`` YAML.

Boolean composition follows the CDF filter grammar (``and``, ``or``, ``not``); see
https://docs.cognite.com/dev/guides/advanced-query/filtering

Leaf entries use: ``operator``, ``target_property``, ``property_scope`` (``view`` | ``node``),
``values`` (optional), optional ``negate``. ``RANGE`` also accepts ``gt``, ``gte``, ``lt``, ``lte``.
"""

from __future__ import annotations

from typing import Any, Sequence, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes.filters import Filter

PropertyRef = Union[tuple[str, str], list[str]]


def property_reference_for_filter(
    view_id: Any,
    target_property: str,
    property_scope: str = "view",
) -> PropertyRef:
    if str(property_scope or "view").lower() == "node":
        return ("node", target_property)
    return view_id.as_property_ref(target_property)


def _filter_item_as_dict(item: Any) -> dict[str, Any] | None:
    if isinstance(item, dict):
        return item
    model_dump = getattr(item, "model_dump", None)
    if callable(model_dump):
        try:
            d = model_dump(mode="python", exclude_none=False)
        except TypeError:
            d = model_dump()
        return d if isinstance(d, dict) else None
    return None


_OPERATOR_ALIASES: dict[str, str] = {
    ">": "GT",
    ">=": "GTE",
    "GE": "GTE",
    "<": "LT",
    "<=": "LTE",
    "LE": "LTE",
}


def normalize_filter_operator(raw: Any) -> str:
    if raw is None:
        return "EQUALS"
    val = getattr(raw, "value", raw)
    s = str(val or "EQUALS").strip()
    if s in _OPERATOR_ALIASES:
        return _OPERATOR_ALIASES[s]
    op = s.upper()
    if op == "CONTAINS_ALL":
        return "CONTAINSALL"
    if op == "CONTAINS_ANY":
        return "CONTAINSANY"
    return op


def _coerce_values_list(values: Any) -> list[Any]:
    if values is None:
        return []
    if isinstance(values, list):
        return list(values)
    return [values]


def _comparison_threshold(values: Any) -> Any:
    vals = _coerce_values_list(values)
    if not vals:
        raise ValueError("comparison operator requires a single value in values")
    return vals[0]


def _leaf_dict_to_dm(view_id: Any, d: dict[str, Any]) -> Filter:
    op = normalize_filter_operator(d.get("operator"))
    target_property = str(d.get("target_property") or "").strip()
    if not target_property:
        raise ValueError("Leaf filter requires non-empty target_property")
    property_scope = str(d.get("property_scope", "view")).lower()
    prop_ref = property_reference_for_filter(view_id, target_property, property_scope)
    values = d.get("values")
    negate = bool(d.get("negate", False))

    if op == "IN" and isinstance(values, list):
        filt: Filter = dm.filters.In(property=prop_ref, values=values)
    elif op == "EQUALS":
        if isinstance(values, list):
            if len(values) == 1:
                filt = dm.filters.Equals(property=prop_ref, value=values[0])
            elif len(values) > 1:
                filt = dm.filters.Or(
                    *[dm.filters.Equals(property=prop_ref, value=v) for v in values]
                )
            else:
                filt = dm.filters.Equals(property=prop_ref, value=None)
        else:
            filt = dm.filters.Equals(property=prop_ref, value=values)
    elif op in ("CONTAINSALL", "CONTAINS_ALL"):
        vals = _coerce_values_list(values)
        if not vals:
            raise ValueError(f"Operator {op} requires non-empty values")
        filt = dm.filters.ContainsAll(property=prop_ref, values=vals)
    elif op in ("CONTAINSANY", "CONTAINS_ANY"):
        vals = _coerce_values_list(values)
        if not vals:
            raise ValueError(f"Operator {op} requires non-empty values")
        filt = dm.filters.ContainsAny(property=prop_ref, values=vals)
    elif op == "SEARCH":
        if isinstance(values, list) and values:
            search_val = values[0]
        elif isinstance(values, str) and values:
            search_val = values
        else:
            search_val = ""
        filt = dm.filters.Search(property=prop_ref, value=search_val)
    elif op == "PREFIX":
        pv = values[0] if isinstance(values, list) and values else values
        if pv is None or (isinstance(pv, str) and not pv.strip()):
            raise ValueError("PREFIX requires a single non-empty value")
        filt = dm.filters.Prefix(property=prop_ref, value=pv)
    elif op == "GT":
        filt = dm.filters.Range(property=prop_ref, gt=_comparison_threshold(values))
    elif op == "GTE":
        filt = dm.filters.Range(property=prop_ref, gte=_comparison_threshold(values))
    elif op == "LT":
        filt = dm.filters.Range(property=prop_ref, lt=_comparison_threshold(values))
    elif op == "LTE":
        filt = dm.filters.Range(property=prop_ref, lte=_comparison_threshold(values))
    elif op == "RANGE":
        gt = d.get("gt")
        gte = d.get("gte")
        lt = d.get("lt")
        lte = d.get("lte")
        if all(x is None for x in (gt, gte, lt, lte)):
            raise ValueError(
                "RANGE requires at least one of gt, gte, lt, lte on the filter object"
            )
        filt = dm.filters.Range(property=prop_ref, gt=gt, gte=gte, lt=lt, lte=lte)
    elif op == "EXISTS":
        if property_scope == "node":
            filt = dm.filters.Exists(property=prop_ref)
        else:
            filt = dm.filters.HasData(views=[view_id], properties=[target_property])
    else:
        raise ValueError(f"Unsupported filter operator: {op!r}")

    if negate:
        return dm.filters.Not(filt)
    return filt


def filter_dict_to_dm(view_id: Any, node: Any) -> Filter:
    if not isinstance(node, dict):
        raise TypeError(f"Filter node must be a mapping, got {type(node).__name__}")

    if "and" in node:
        children = node["and"]
        if not isinstance(children, list):
            raise TypeError('"and" must be a list of filters')
        parts = [filter_dict_to_dm(view_id, ch) for ch in children]
        if not parts:
            raise ValueError('"and" list must not be empty')
        if len(parts) == 1:
            return parts[0]
        return dm.filters.And(*parts)

    if "or" in node:
        children = node["or"]
        if not isinstance(children, list):
            raise TypeError('"or" must be a list of filters')
        parts = [filter_dict_to_dm(view_id, ch) for ch in children]
        if not parts:
            raise ValueError('"or" list must not be empty')
        if len(parts) == 1:
            return parts[0]
        return dm.filters.Or(*parts)

    if "not" in node:
        inner = node["not"]
        if isinstance(inner, dict):
            return dm.filters.Not(filter_dict_to_dm(view_id, inner))
        raise TypeError('"not" value must be a mapping')

    return _leaf_dict_to_dm(view_id, node)


def compile_view_config_filters(view_id: Any, filters: Sequence[Any] | None) -> list[Filter]:
    """Compile YAML ``filters`` list entries for ``combine_node_filter`` user_filters."""
    compiled: list[Filter] = []
    for raw in filters or []:
        fd = _filter_item_as_dict(raw)
        if fd is not None:
            compiled.append(filter_dict_to_dm(view_id, fd))
    return compiled


def filter_target_property_paths(filters: Sequence[Any] | None) -> list[str]:
    """Collect view-scoped property names referenced in filter YAML for DM ``Select``."""
    paths: list[str] = []
    seen: set[str] = set()

    def add_path(name: str, scope: str) -> None:
        if str(scope or "view").lower() == "node":
            return
        if name and name not in seen:
            seen.add(name)
            paths.append(name)

    def walk(node: Any) -> None:
        fd = _filter_item_as_dict(node)
        if not fd:
            return
        if "and" in fd:
            for child in fd.get("and") or []:
                walk(child)
            return
        if "or" in fd:
            for child in fd.get("or") or []:
                walk(child)
            return
        if "not" in fd:
            walk(fd.get("not"))
            return
        add_path(str(fd.get("target_property") or "").strip(), str(fd.get("property_scope", "view")))

    for raw in filters or []:
        walk(raw)
    return paths
