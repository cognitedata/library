"""Resolve ScopedWorkflowTarget list from CLI / API options."""

from __future__ import annotations

from typing import List

from workflow_build.paths import normalize_scope_suffix
from workflow_build.targets import ScopedWorkflowTarget


def scope_targets_for_source(
    *,
    workflow_id: str,
    source_kind: str,
    module_root,
    config: dict,
    scope_suffix: str | None,
) -> List[ScopedWorkflowTarget]:
    del module_root, config  # unscoped builds only; scope_suffix selects artifact folder when set
    if scope_suffix is not None and str(scope_suffix).strip():
        ss = normalize_scope_suffix(scope_suffix)
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

    return [
        ScopedWorkflowTarget(
            workflow_id=workflow_id,
            scope_suffix="",
            scope_id="",
            node_chain=[],
            segment_ids=[],
            source_kind=source_kind,
        )
    ]
