"""Load and validate hierarchy YAML under top-level ``aliasing_scope_hierarchy`` (``levels`` + ``locations``).

``aliasing_scope_hierarchy.levels`` names the first path segments (site, plant, …).
``aliasing_scope_hierarchy.locations`` is the root list of scope nodes. Each node may nest child nodes
under ``locations``. Leaves may appear at any depth; a shorter tree does not need to use every
declared level name, and a deeper tree uses synthetic labels ``level_{n}`` past the end of
``levels``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from scope_build.context import PathStep, ScopeBuildContext

_SEGMENT_INVALID = re.compile(r"[/\\\0]")
_WHITESPACE_RUN = re.compile(r"\s+")

_SCOPE_HIERARCHY = "aliasing_scope_hierarchy"
_LEVELS = "levels"
_LOCATIONS = "locations"


def slugify_display_name(name: str) -> str:
    """Turn a human ``name`` (may include spaces) into a single path segment."""
    s = _WHITESPACE_RUN.sub("_", name.strip())
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unnamed"


def validate_segment_id(segment_id: str, *, where: str) -> None:
    if not segment_id or not segment_id.strip():
        raise ValueError(f"Empty identifier segment {where}")
    if segment_id in (".", ".."):
        raise ValueError(f"Invalid identifier {where!r}: {segment_id!r}")
    if _SEGMENT_INVALID.search(segment_id):
        raise ValueError(
            f"Identifier {where!r} contains forbidden characters: {segment_id!r}"
        )


def node_segment_id(node: Dict[str, Any], *, where: str) -> str:
    """Prefer explicit ``id``; otherwise slug ``name``."""
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


def load_hierarchy_doc(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Hierarchy file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError("Hierarchy root must be a mapping")
    return doc


def _scope_hierarchy_section(doc: Dict[str, Any]) -> Dict[str, Any]:
    sh = doc.get(_SCOPE_HIERARCHY)
    if not isinstance(sh, dict):
        raise ValueError(
            "Missing top-level 'aliasing_scope_hierarchy' mapping (expected aliasing_scope_hierarchy.levels and "
            "aliasing_scope_hierarchy.locations)"
        )
    return sh


def parse_levels(doc: Dict[str, Any]) -> List[str]:
    sh = _scope_hierarchy_section(doc)
    levels = sh.get(_LEVELS)
    if not isinstance(levels, list) or not levels:
        raise ValueError("aliasing_scope_hierarchy.levels must be a non-empty list")
    out: List[str] = []
    for i, lv in enumerate(levels):
        if not isinstance(lv, str) or not lv.strip():
            raise ValueError(f"aliasing_scope_hierarchy.levels[{i}] must be a non-empty string")
        out.append(lv.strip())
    return out


def path_step_level_name(levels: List[str], depth: int) -> str:
    """Human-readable level tag for ``depth`` (0 = root). Extra tree depth uses ``level_{n}``."""
    if depth < len(levels):
        return levels[depth]
    return f"level_{depth + 1}"


def _walk(
    nodes: Any,
    *,
    depth: int,
    ancestors: List[Dict[str, Any]],
    ancestor_segments: List[str],
) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    """Return list of (scope_id, node_chain, segment_ids). Leaves may appear at any depth."""
    if not isinstance(nodes, list):
        raise ValueError("aliasing_scope_hierarchy.locations must be a list")
    leaves: List[Tuple[str, List[Dict[str, Any]], List[str]]] = []
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"aliasing_scope_hierarchy.locations[{i}] must be a mapping")
        where = f"aliasing_scope_hierarchy.locations[{i}] at depth {depth}"
        seg = node_segment_id(node, where=where)
        chain = ancestors + [node]
        segments = ancestor_segments + [seg]
        children = node.get(_LOCATIONS)
        if children is None:
            children = []
        if not isinstance(children, list):
            raise ValueError(f"{where}: locations must be a list when present")
        if len(children) == 0:
            scope_id = "__".join(segments)
            leaves.append((scope_id, chain, segments))
        else:
            leaves.extend(
                _walk(
                    children,
                    depth=depth + 1,
                    ancestors=chain,
                    ancestor_segments=segments,
                )
            )
    return leaves


def collect_leaves(doc: Dict[str, Any]) -> List[Tuple[str, List[Dict[str, Any]], List[str]]]:
    parse_levels(doc)  # validate aliasing_scope_hierarchy.levels exists
    sh = _scope_hierarchy_section(doc)
    loc = sh.get(_LOCATIONS)
    if loc is None:
        return []
    raw = _walk(loc, depth=0, ancestors=[], ancestor_segments=[])
    seen: Dict[str, int] = {}
    for scope_id, _, _ in raw:
        seen[scope_id] = seen.get(scope_id, 0) + 1
    dupes = [k for k, v in seen.items() if v > 1]
    if dupes:
        raise ValueError(f"Duplicate scope_id after composition: {sorted(dupes)}")
    return raw


def build_contexts(
    *,
    module_root: Path,
    doc: Dict[str, Any],
    dry_run: bool,
) -> List[ScopeBuildContext]:
    levels = parse_levels(doc)
    contexts: List[ScopeBuildContext] = []
    for scope_id, chain, segments in collect_leaves(doc):
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
        contexts.append(
            ScopeBuildContext(
                module_root=module_root,
                scope_id=scope_id,
                levels=levels,
                path=path_steps,
                dry_run=dry_run,
            )
        )
    return contexts
