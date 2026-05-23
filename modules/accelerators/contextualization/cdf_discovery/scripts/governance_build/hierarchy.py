"""Hierarchy tree walks (all nodes vs leaves) — vendored scope_hierarchy semantics."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from governance_build.context import PathStep, ScopeBinding
from governance_build.hierarchy_walker import (
    collect_scope_rows,
    display_name,
    node_segment_id,
    parse_levels,
    path_step_level_name,
    slugify_display_name,
    validate_segment_id,
)

__all__ = [
    "slugify_display_name",
    "validate_segment_id",
    "node_segment_id",
    "display_name",
    "parse_levels",
    "path_step_level_name",
    "collect_scope_rows",
    "synthetic_scope_binding",
    "scope_binding_from_row",
]


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
