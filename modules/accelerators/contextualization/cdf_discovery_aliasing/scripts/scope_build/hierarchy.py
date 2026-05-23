"""Load and validate hierarchy YAML under top-level ``scope_hierarchy`` (``levels`` + ``locations``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence

import yaml

from scope_build.context import PathStep, ScopeBuildContext
from scope_build.hierarchy_walker import (
    collect_leaves as _collect_leaves_block,
    display_name,
    node_segment_id,
    parse_levels as _parse_levels_block,
    path_step_level_name,
    slugify_display_name,
    validate_segment_id,
)

_SCOPE_HIERARCHY = "scope_hierarchy"
_LEVELS = "levels"
_LOCATIONS = "locations"

__all__ = [
    "slugify_display_name",
    "validate_segment_id",
    "node_segment_id",
    "display_name",
    "workflow_display_name_from_path",
    "load_hierarchy_doc",
    "parse_levels",
    "path_step_level_name",
    "collect_leaves",
    "build_contexts",
]


def workflow_display_name_from_path(path: Sequence[PathStep]) -> str | None:
    """Toolkit Workflow ``name`` (display): ``<first> - <leaf>`` when depth ≥ 2, else the leaf name."""
    if not path:
        return None
    first = (path[0].name or "").strip()
    leaf = (path[-1].name or "").strip()
    if len(path) == 1:
        single = leaf or first
        return single if single else None
    if first and leaf:
        return f"{first} - {leaf}" if first != leaf else first
    return leaf or first or None


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
            f"Missing top-level '{_SCOPE_HIERARCHY}' mapping (expected {_SCOPE_HIERARCHY}.levels and "
            f"{_SCOPE_HIERARCHY}.locations)"
        )
    return sh


def parse_levels(doc: Dict[str, Any]) -> List[str]:
    sh = _scope_hierarchy_section(doc)
    return _parse_levels_block(sh, key_prefix=_SCOPE_HIERARCHY)


def collect_leaves(doc: Dict[str, Any]) -> List[tuple[str, List[Dict[str, Any]], List[str]]]:
    sh = _scope_hierarchy_section(doc)
    return _collect_leaves_block(sh, key_prefix=_SCOPE_HIERARCHY)


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
