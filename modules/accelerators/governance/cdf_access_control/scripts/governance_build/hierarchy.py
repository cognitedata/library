"""Hierarchy tree walks (all nodes vs leaves) — aligned with KEA scope_hierarchy semantics."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from governance_build.context import PathStep, ScopeBinding

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
        raise ValueError(
            f"Identifier {where!r} contains forbidden characters: {segment_id!r}"
        )


def node_segment_id(node: Dict[str, Any], *, where: str) -> str:
    raw_id = node.get("id")
    if raw_id is not None:
        sid = str(raw_id).strip()
        validate_segment_id(sid, where=where)
        return sid
    name = node.get("name")
    if name is None or str(name).strip() == "":
        raise ValueError(
            f"Location {where} needs non-empty ``name`` or ``id`` for identifier"
        )
    slug = slugify_display_name(str(name))
    validate_segment_id(slug, where=where)
    return slug


def display_name(node: Dict[str, Any]) -> str:
    n = node.get("name")
    if n is not None and str(n).strip() != "":
        return str(n)
    raw_id = node.get("id")
    if raw_id is not None and str(raw_id).strip() != "":
        return str(raw_id).strip()
    return ""


def parse_levels(hierarchy_block: Dict[str, Any]) -> List[str]:
    levels = hierarchy_block.get("levels")
    if not isinstance(levels, list) or not levels:
        raise ValueError("hierarchy levels must be a non-empty list")
    out: List[str] = []
    for i, lv in enumerate(levels):
        if not isinstance(lv, str) or not lv.strip():
            raise ValueError(f"levels[{i}] must be a non-empty string")
        out.append(lv.strip())
    return out


def path_step_level_name(levels: List[str], depth: int) -> str:
    if depth < len(levels):
        return levels[depth]
    return f"level_{depth + 1}"


def _walk(
    nodes: Any,
    *,
    depth: int,
    ancestors: List[Dict[str, Any]],
    ancestor_segments: List[str],
    leaves_only: bool,
) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    if not isinstance(nodes, list):
        raise ValueError("locations must be a list")
    rows: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"locations[{i}] must be a mapping")
        where = f"locations[{i}] at depth {depth}"
        seg = node_segment_id(node, where=where)
        chain = ancestors + [node]
        segments = ancestor_segments + [seg]
        children = node.get(_LOCATIONS)
        if children is None:
            children = []
        if not isinstance(children, list):
            raise ValueError(f"{where}: locations must be a list when present")
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


def _dedupe_check(rows: List[Tuple[str, List[Dict[str, Any]], List[str]]]) -> None:
    seen: Dict[str, int] = {}
    for scope_id, _, _ in rows:
        seen[scope_id] = seen.get(scope_id, 0) + 1
    dupes = [k for k, v in seen.items() if v > 1]
    if dupes:
        raise ValueError(f"Duplicate scope_id after composition: {sorted(dupes)}")


def collect_scope_rows(
    hierarchy_block: Dict[str, Any], *, nodes_mode: str
) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    """Return (scope_id, node_chain, segment_ids) for each binding row."""
    parse_levels(hierarchy_block)
    loc = hierarchy_block.get(_LOCATIONS)
    if loc is None:
        return []
    leaves_only = nodes_mode == "leaves"
    raw = _walk(loc, depth=0, ancestors=[], ancestor_segments=[], leaves_only=leaves_only)
    _dedupe_check(raw)
    return raw


def synthetic_scope_binding(
    levels: List[str], *, scope_id: str, name: str, description: str | None
) -> ScopeBinding:
    seg = str(scope_id).strip()
    validate_segment_id(seg, where="synthetic scope_id")
    segments = [seg]
    sid = "__".join(segments)
    node: Dict[str, Any] = {"id": seg, "name": name, "description": description}
    level_name = levels[0] if levels else "scope"
    desc_str = description if isinstance(description, str) else None
    path_steps = [
        PathStep(
            level=level_name,
            name=display_name(node),
            description=desc_str,
            segment_id=seg,
            node=dict(node),
        )
    ]
    return ScopeBinding(scope_id=sid, path=path_steps, segments=segments)


def scope_binding_from_row(
    levels: List[str], row: Tuple[str, List[Dict[str, Any]], List[str]]
) -> ScopeBinding:
    scope_id, chain, segments = row
    path_steps: List[PathStep] = []
    for depth, node in enumerate(chain):
        desc = node.get("description")
        desc_str = desc if isinstance(desc, str) else None
        path_steps.append(
            PathStep(
                level=path_step_level_name(levels, depth),
                name=display_name(node),
                description=desc_str,
                segment_id=segments[depth],
                node=dict(node),
            )
        )
    return ScopeBinding(scope_id=scope_id, path=path_steps, segments=segments)
