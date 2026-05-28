"""Paths for CDF Toolkit workflow deploy artifacts (module package root)."""

from __future__ import annotations

from pathlib import Path

from workflow_artifact_paths import (
    artifact_basename,
    artifact_filename,
    is_scoped_build,
    normalize_scope_suffix,
    workflow_artifacts_output_dir,
    workflow_artifacts_root,
)

__all__ = [
    "WORKFLOW_ARTIFACTS_REL",
    "artifact_basename",
    "artifact_filename",
    "is_scoped_build",
    "normalize_scope_suffix",
    "workflow_artifacts_output_dir",
    "workflow_artifacts_root",
    "workflow_artifacts_scope_dir",
]

WORKFLOW_ARTIFACTS_REL = Path("workflows")


def workflow_artifacts_scope_dir(transform_module_root: Path, scope_suffix: str) -> Path:
    return workflow_artifacts_output_dir(transform_module_root, scope_suffix)
