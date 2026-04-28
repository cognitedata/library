"""Build ``cognite.client.data_modeling.filters`` from scope ``source_views[].filters`` YAML.

Boolean composition follows the CDF filter grammar (``and``, ``or``, ``not``); see
https://docs.cognite.com/dev/guides/advanced-query/filtering

Leaf entries use the module YAML shape: ``operator``, ``target_property``,
``property_scope`` (``view`` | ``node``), ``values`` (optional), optional ``negate``.
``RANGE`` also accepts ``gt``, ``gte``, ``lt``, ``lte`` at the leaf object root.

Example::

    filters:
      - and:
          - operator: EQUALS
            target_property: equipmentType
            values: [pump]
          - or:
              - operator: EQUALS
                target_property: zone
                values: [A]
              - operator: EQUALS
                target_property: zone
                values: [B]
      - not:
          operator: EQUALS
          target_property: deprecated
          values: [true]
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes.filters import Filter

from .dm_filter_utils import property_reference_for_filter

PropertyRef = Union[tuple[str, str], list[str]]


def _filter_item_as_dict(item: Any) -> Dict[str, Any] | None:
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


def _normalize_operator(raw: Any) -> str:
    if raw is None:
        return "EQUALS"
    val = getattr(raw, "value", raw)
    return str(val or "EQUALS").strip().upper()


def _as_prop_ref(view_id: Any, target_property: str, property_scope: str) -> PropertyRef:
    return property_reference_for_filter(
        view_id, target_property, str(property_scope or "view").lower()
    )


def _coerce_values_list(values: Any) -> List[Any]:
    if values is None:
        return []
    if isinstance(values, list):
        return list(values)
    return [values]


def _leaf_dict_to_dm(view_id: Any, d: dict[str, Any]) -> Filter:
    """Convert a leaf filter dict to a DM ``Filter`` (not a boolean group)."""
    op = _normalize_operator(d.get("operator"))
    target_property = str(d.get("target_property") or "").strip()
    if not target_property:
        raise ValueError("Leaf filter requires non-empty target_property")
    property_scope = str(d.get("property_scope", "view")).lower()
    prop_ref = _as_prop_ref(view_id, target_property, property_scope)
    values = d.get("values")
    negate = bool(d.get("negate", False))

    filt: Filter

    if op == "IN" and isinstance(values, list):
        filt = dm.filters.In(property=prop_ref, values=values)
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
    elif op == "RANGE":
        gt = d.get("gt")
        gte = d.get("gte")
        lt = d.get("lt")
        lte = d.get("lte")
        if all(x is None for x in (gt, gte, lt, lte)):
            raise ValueError(
                "RANGE requires at least one of gt, gte, lt, lte on the filter object"
            )
        filt = dm.filters.Range(
            property=prop_ref, gt=gt, gte=gte, lt=lt, lte=lte
        )
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
    """Map one YAML filter node (leaf or ``and`` / ``or`` / ``not`` group) to a DM filter."""
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


def build_source_view_user_filters(view_id: Any, filters: Sequence[Any] | None) -> Filter:
    """
    Combine only user-configured nodes with ``And`` (no ``HasData``).

    Used by ``SourceViewConfig.build_filter()`` — the extraction pipeline wraps this with
    ``HasData`` in ``_build_filter``. When ``filters`` is empty, returns ``HasData`` to match
    legacy behavior.
    """
    parts: List[Filter] = []
    for raw in filters or []:
        fd = _filter_item_as_dict(raw)
        if fd is not None:
            parts.append(filter_dict_to_dm(view_id, fd))
    if not parts:
        return dm.filters.HasData(views=[view_id])
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)


def build_source_view_query_filter(view_id: Any, filters: Sequence[Any] | None) -> Filter:
    """
    Full filter for ``instances.list``: ``HasData(views=[view_id])`` AND each top-level node.

    Each entry may be a leaf mapping or a boolean group (``and`` / ``or`` / ``not``).
    """
    parts: List[Filter] = [dm.filters.HasData(views=[view_id])]
    for raw in filters or []:
        fd = _filter_item_as_dict(raw)
        if fd is not None:
            parts.append(filter_dict_to_dm(view_id, fd))
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)
