"""Resolve built workflow Toolkit YAML paths for deploy and CDF runs."""

from __future__ import annotations

from pathlib import Path

from workflow_artifact_paths import (
    artifact_basename,
    normalize_scope_suffix,
    workflow_artifacts_output_dir,
)


def resolve_workflow_artifacts(
    transform_root: Path,
    workflow_id: str,
    scope_suffix: str | None = None,
) -> dict[str, Path]:
    suffix = normalize_scope_suffix(scope_suffix)
    out_dir = workflow_artifacts_output_dir(transform_root, suffix)
    base = artifact_basename(workflow_id, suffix)
    return {
        "dir": out_dir,
        "workflow": out_dir / f"{base}.Workflow.yaml",
        "workflow_version": out_dir / f"{base}.WorkflowVersion.yaml",
        "trigger": out_dir / f"{base}.WorkflowTrigger.yaml",
    }


def workflow_trigger_rel(
    discovery_root: Path,
    workflow_id: str,
    scope_suffix: str | None = None,
) -> str:
    transform_root = discovery_root / "transform"
    paths = resolve_workflow_artifacts(transform_root, workflow_id, scope_suffix)
    return str(paths["trigger"].relative_to(discovery_root.resolve())).replace("\\", "/")
