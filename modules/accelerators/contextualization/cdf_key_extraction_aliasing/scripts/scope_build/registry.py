"""Ordered registry of scope artifact builders."""

from __future__ import annotations

from pathlib import Path
from typing import List, Protocol, Sequence

from scope_build.builders.workflow_definitions import (
    RootWorkflowDefinitionsBuilder,
    ScopedWorkflowDefinitionsBuilder,
)
from scope_build.builders.workflow_triggers import WorkflowTriggersBuilder
from scope_build.context import ScopeBuildContext
from scope_build.mode import ScopeBuildMode


class ScopeArtifactBuilder(Protocol):
    name: str

    def run(self, ctx: ScopeBuildContext) -> None: ...


def default_builders(
    *,
    scope_build_mode: ScopeBuildMode,
    workflow_base: str,
    scope_document_path: Path,
    workflow_trigger_template_path: Path | None = None,
    workflow_template_path: Path | None = None,
    workflow_version_template_path: Path | None = None,
    overwrite: bool = False,
) -> List[ScopeArtifactBuilder]:
    triggers = WorkflowTriggersBuilder(
        template_path=workflow_trigger_template_path,
        scope_document_path=scope_document_path,
        mode=scope_build_mode,
        workflow_base=workflow_base,
        overwrite=overwrite,
    )
    if scope_build_mode == "trigger_only":
        return [
            RootWorkflowDefinitionsBuilder(
                workflow_base=workflow_base,
                workflow_template_path=workflow_template_path,
                workflow_version_template_path=workflow_version_template_path,
                overwrite=overwrite,
            ),
            triggers,
        ]
    return [
        ScopedWorkflowDefinitionsBuilder(
            workflow_base=workflow_base,
            workflow_template_path=workflow_template_path,
            workflow_version_template_path=workflow_version_template_path,
            overwrite=overwrite,
        ),
        triggers,
    ]


def filter_builders(
    builders: Sequence[ScopeArtifactBuilder],
    only: Sequence[str] | None,
) -> List[ScopeArtifactBuilder]:
    """Return a subset of ``builders`` by name (preserves first occurrence order in ``only``)."""
    if not only:
        return list(builders)
    by_name = {b.name: b for b in builders}
    seen: set[str] = set()
    out: List[ScopeArtifactBuilder] = []
    for name in only:
        if name in seen:
            continue
        seen.add(name)
        if name not in by_name:
            known = ", ".join(sorted(by_name))
            raise ValueError(
                f"Unknown builder {name!r}. Known builders: {known}. Use --list-builders."
            )
        out.append(by_name[name])
    return out
