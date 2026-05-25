"""Resolve ScopedWorkflowTarget list from CLI / API options."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from workflow_build.hierarchy_walker import collect_leaves
from workflow_build.targets import ScopedWorkflowTarget


def load_scope_hierarchy(module_root, config: dict) -> Dict[str, Any]:
    block = config.get("scope_hierarchy")
    return block if isinstance(block, dict) else {}


def scope_targets_for_source(
    *,
    workflow_id: str,
    source_kind: str,
    module_root,
    config: dict,
    scoped: bool,
    scope_suffix: str | None,
) -> List[ScopedWorkflowTarget]:
    hierarchy = load_scope_hierarchy(module_root, config)
    levels = [str(x).strip() for x in (hierarchy.get("levels") or []) if str(x).strip()]

    if scope_suffix is not None and str(scope_suffix).strip():
        ss = str(scope_suffix).strip()
        if ss == "all":
            return [
                ScopedWorkflowTarget(
                    workflow_id=workflow_id,
                    scope_suffix="all",
                    scope_id="all",
                    node_chain=[],
                    segment_ids=[],
                    source_kind=source_kind,
                )
            ]
        if hierarchy:
            for scope_id, chain, segments in collect_leaves(hierarchy):
                from workflow_build.hierarchy_walker import cdf_external_id_suffix

                if cdf_external_id_suffix(scope_id) == ss:
                    return [
                        ScopedWorkflowTarget(
                            workflow_id=workflow_id,
                            scope_suffix=ss,
                            scope_id=scope_id,
                            node_chain=chain,
                            segment_ids=segments,
                            source_kind=source_kind,
                        )
                    ]
        return [
            ScopedWorkflowTarget(
                workflow_id=workflow_id,
                scope_suffix=ss,
                scope_id=ss,
                node_chain=[],
                segment_ids=[],
                source_kind=source_kind,
            )
        ]

    if scoped and hierarchy:
        leaves = collect_leaves(hierarchy)
        if leaves:
            from workflow_build.hierarchy_walker import cdf_external_id_suffix

            return [
                ScopedWorkflowTarget(
                    workflow_id=workflow_id,
                    scope_suffix=cdf_external_id_suffix(scope_id),
                    scope_id=scope_id,
                    node_chain=chain,
                    segment_ids=segments,
                    source_kind=source_kind,
                )
                for scope_id, chain, segments in leaves
            ]

    return [
        ScopedWorkflowTarget(
            workflow_id=workflow_id,
            scope_suffix="all",
            scope_id="all",
            node_chain=[],
            segment_ids=[],
            source_kind=source_kind,
        )
    ]
