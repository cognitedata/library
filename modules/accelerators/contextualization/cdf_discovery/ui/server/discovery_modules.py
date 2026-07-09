"""Discovery shell — registered top-level modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ui.server.tree_node_ids import (
    CONNECTION_ROOT_CHILD_ORDER,
    DATA_ROOT,
    EXTRACT_ROOT,
    FUSION_ROOT,
    GOVERNANCE_ROOT,
    INDEX_ROOT,
    MONITOR_ROOT,
    TRANSFORM_ROOT,
)

TreeNodeOut = Dict[str, Any]


@dataclass(frozen=True)
class DiscoveryModuleDef:
    id: str
    tree_root_id: str
    label: str
    kind: str
    has_children: bool
    meta: Dict[str, Any]


def _module(
    *,
    id: str,
    tree_root_id: str,
    label: str,
    kind: str,
    has_children: bool,
    meta: Optional[Dict[str, Any]] = None,
) -> DiscoveryModuleDef:
    return DiscoveryModuleDef(
        id=id,
        tree_root_id=tree_root_id,
        label=label,
        kind=kind,
        has_children=has_children,
        meta=dict(meta or {}),
    )


DISCOVERY_MODULES: tuple[DiscoveryModuleDef, ...] = (
    _module(
        id="data",
        tree_root_id=DATA_ROOT,
        label="Data",
        kind="folder",
        has_children=True,
        meta={"domain": "data"},
    ),
    _module(
        id="fusion",
        tree_root_id=FUSION_ROOT,
        label="Fusion",
        kind="folder",
        has_children=True,
        meta={"domain": "fusion"},
    ),
    _module(
        id="governance",
        tree_root_id=GOVERNANCE_ROOT,
        label="Governance",
        kind="folder",
        has_children=True,
        meta={"domain": "governance", "governance_workspace": "scope"},
    ),
    _module(
        id="extract",
        tree_root_id=EXTRACT_ROOT,
        label="Extract",
        kind="extract",
        has_children=False,
        meta={"domain": "extract"},
    ),
    _module(
        id="transform",
        tree_root_id=TRANSFORM_ROOT,
        label="Transform",
        kind="folder",
        has_children=True,
        meta={"domain": "transform"},
    ),
    _module(
        id="inverted_index",
        tree_root_id=INDEX_ROOT,
        label="Indexing",
        kind="folder",
        has_children=True,
        meta={"domain": "inverted_index"},
    ),
    _module(
        id="monitor",
        tree_root_id=MONITOR_ROOT,
        label="Monitor",
        kind="monitor",
        has_children=False,
        meta={"domain": "monitor"},
    ),
)

_MODULE_BY_ROOT: Dict[str, DiscoveryModuleDef] = {m.tree_root_id: m for m in DISCOVERY_MODULES}


def connection_root_child_order() -> tuple[str, ...]:
    return CONNECTION_ROOT_CHILD_ORDER


def module_for_tree_root(tree_root_id: str) -> Optional[DiscoveryModuleDef]:
    return _MODULE_BY_ROOT.get(tree_root_id)


def module_for_node_id(node_id: str) -> Optional[DiscoveryModuleDef]:
    from ui.server.discovery_tree import parse_node_id

    kind, _segs = parse_node_id(node_id)
    if kind == "connection":
        return None
    return _MODULE_BY_ROOT.get(kind)


def connection_root_nodes() -> List[TreeNodeOut]:
    """Build connection-root folder nodes in registry order."""
    order = {nid: i for i, nid in enumerate(CONNECTION_ROOT_CHILD_ORDER)}
    nodes: List[TreeNodeOut] = []
    for mod in sorted(DISCOVERY_MODULES, key=lambda m: order.get(m.tree_root_id, 999)):
        nodes.append(
            {
                "id": mod.tree_root_id,
                "label": mod.label,
                "kind": mod.kind,
                "has_children": mod.has_children,
                "meta": mod.meta,
            }
        )
    return nodes
