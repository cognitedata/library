"""Load ``scope_hierarchy`` and list ``dimensions`` registry."""

from __future__ import annotations

import itertools
from typing import Any, Dict, Iterator, List, Mapping

HIERARCHY = "hierarchy"
LIST = "list"
_SCOPE_HIERARCHY = "scope_hierarchy"


def get_scope_hierarchy(doc: Mapping[str, Any]) -> Dict[str, Any]:
    sh = doc.get(_SCOPE_HIERARCHY)
    if not isinstance(sh, dict):
        raise ValueError(
            f"Missing top-level '{_SCOPE_HIERARCHY}' mapping "
            f"(expected {_SCOPE_HIERARCHY}.levels and {_SCOPE_HIERARCHY}.locations)"
        )
    if sh.get("type") != HIERARCHY:
        raise ValueError(f"{_SCOPE_HIERARCHY} must have type: hierarchy")
    return dict(sh)


def get_dimensions(doc: Mapping[str, Any]) -> Dict[str, Any]:
    d = doc.get("dimensions")
    if not isinstance(d, dict):
        raise ValueError("dimensions must be a mapping")
    return dict(d)


def require_list_dimension(dimensions: Mapping[str, Any], name: str) -> Dict[str, Any]:
    block = dimensions.get(name)
    if not isinstance(block, dict):
        raise ValueError(f"dimensions.{name} must be a mapping")
    if block.get("type") != LIST:
        raise ValueError(f"dimensions.{name} must have type: list")
    items = block.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError(f"dimensions.{name}.items must be a non-empty list")
    for i, it in enumerate(items):
        if isinstance(it, dict) and str(it.get("id", "")).strip():
            continue
        raise ValueError(f"dimensions.{name}.items[{i}] needs a non-empty 'id'")
    return block


def list_dimension_items(dimensions: Mapping[str, Any], name: str) -> List[Dict[str, Any]]:
    block = require_list_dimension(dimensions, name)
    return [dict(x) for x in block.get("items", []) if isinstance(x, dict)]


def cartesian_list_combos(
    dimensions: Mapping[str, Any], combine_names: List[str]
) -> Iterator[Dict[str, Dict[str, Any]]]:
    """Yield dict mapping list-dimension name → item object for each combo."""
    if not combine_names:
        yield {}
        return
    pools: List[List[Dict[str, Any]]] = []
    for name in combine_names:
        pools.append(list_dimension_items(dimensions, name))
    for combo_tuple in itertools.product(*pools):
        yield {name: item for name, item in zip(combine_names, combo_tuple)}
