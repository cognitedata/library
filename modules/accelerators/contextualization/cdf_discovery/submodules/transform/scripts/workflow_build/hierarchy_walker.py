"""Scope hierarchy leaf walk (vendored semantics)."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

_LOCATIONS = "locations"
_WHITESPACE_RUN = re.compile(r"\s+")
_SEG_INVALID = re.compile(r"[/\\\0]")


def slugify_display_name(name: str) -> str:
    s = _WHITESPACE_RUN.sub("_", name.strip())
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unnamed"


def validate_segment_id(segment_id: str, *, where: str) -> None:
    if not segment_id or not segment_id.strip():
        raise ValueError(f"Empty identifier segment {where}")
    if segment_id in (".", ".."):
        raise ValueError(f"Invalid identifier {where!r}: {segment_id!r}")
    if _SEG_INVALID.search(segment_id):
        raise ValueError(f"Identifier {where!r} contains forbidden characters: {segment_id!r}")


def node_segment_id(node: Dict[str, Any], *, where: str) -> str:
    raw_id = node.get("id")
    if raw_id is not None:
        sid = str(raw_id).strip()
        validate_segment_id(sid, where=where)
        return sid
    name = node.get("name")
    if name is None or str(name).strip() == "":
        raise ValueError(f"Location {where} needs non-empty ``name`` or ``id`` for identifier")
    slug = slugify_display_name(str(name))
    validate_segment_id(slug, where=where)
    return slug


def cdf_external_id_suffix(scope_id: str) -> str:
    return scope_id.lower().replace("__", "_")


def _walk(
    nodes: Any,
    *,
    depth: int,
    ancestors: List[Dict[str, Any]],
    ancestor_segments: List[str],
    leaves_only: bool,
) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    if not isinstance(nodes, list):
        raise ValueError(f"{_LOCATIONS} must be a list")
    rows: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"{_LOCATIONS}[{i}] must be a mapping")
        where = f"{_LOCATIONS}[{i}] at depth {depth}"
        seg = node_segment_id(node, where=where)
        chain = ancestors + [node]
        segments = ancestor_segments + [seg]
        children = node.get(_LOCATIONS) or []
        if not isinstance(children, list):
            raise ValueError(f"{where}: {_LOCATIONS} must be a list when present")
        if leaves_only:
            if len(children) == 0:
                rows.append(("__".join(segments), chain, segments))
            else:
                rows.extend(
                    _walk(
                        children,
                        depth=depth + 1,
                        ancestors=chain,
                        ancestor_segments=segments,
                        leaves_only=True,
                    )
                )
        else:
            rows.append(("__".join(segments), chain, segments))
            if children:
                rows.extend(
                    _walk(
                        children,
                        depth=depth + 1,
                        ancestors=chain,
                        ancestor_segments=segments,
                        leaves_only=False,
                    )
                )
    return rows


def collect_leaves(hierarchy_block: Dict[str, Any]) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    levels = hierarchy_block.get("levels")
    if not isinstance(levels, list) or not levels:
        raise ValueError("scope_hierarchy.levels must be a non-empty list")
    loc = hierarchy_block.get(_LOCATIONS)
    if loc is None:
        return []
    return _walk(loc, depth=0, ancestors=[], ancestor_segments=[], leaves_only=True)
