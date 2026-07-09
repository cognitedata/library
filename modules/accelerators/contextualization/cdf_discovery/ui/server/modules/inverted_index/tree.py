"""Indexing module — lazy object tree children."""

from __future__ import annotations

from typing import Any, Dict, List

from ui.server.tree_node_ids import INDEX_ROOT

TreeNodeOut = Dict[str, Any]


def _node(
    *,
    id: str,
    label: str,
    kind: str,
    has_children: bool,
    meta: Dict[str, Any] | None = None,
) -> TreeNodeOut:
    out: TreeNodeOut = {
        "id": id,
        "label": label,
        "kind": kind,
        "has_children": has_children,
    }
    if meta:
        out["meta"] = meta
    return out


_INDEX_NAV: tuple[tuple[str, str, str, bool], ...] = (
    ("index:dashboard", "Dashboard", "inverted_index_dashboard", False),
    ("index:config", "Configuration", "inverted_index_configuration", False),
    ("index:ops", "Operations", "folder", True),
    ("index:query", "Query", "inverted_index_query", False),
    ("index:file", "File context", "inverted_index_file_context", False),
    ("index:tag-reuse", "Tag reuse audit", "inverted_index_tag_reuse", False),
)

_OPS_CHILDREN: tuple[tuple[str, str, str], ...] = (
    ("index:ops:build-metadata", "Build metadata", "inverted_index_build_metadata"),
    ("index:ops:build-annotations", "Build annotations", "inverted_index_build_annotations"),
    ("index:ops:target-driven", "Target driven", "inverted_index_target_driven"),
)


def list_children(_client: Any, node_id: str) -> List[TreeNodeOut] | None:
    """Return children for index-module nodes, or None if not owned."""
    if node_id == INDEX_ROOT:
        return [
            _node(id=nid, label=label, kind=kind, has_children=has_children, meta={"domain": "inverted_index"})
            for nid, label, kind, has_children in _INDEX_NAV
        ]
    if node_id == "index:ops":
        return [
            _node(id=nid, label=label, kind=kind, has_children=False, meta={"domain": "inverted_index"})
            for nid, label, kind in _OPS_CHILDREN
        ]
    if node_id == INDEX_ROOT or node_id.startswith("index:"):
        return []
    return None
