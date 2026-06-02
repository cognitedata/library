"""Built workflow YAML paths (flat ``workflows/`` or ``workflows/<scope>/``)."""

from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_ARTIFACTS_DIR_NAME = "workflows"

_BUILT_CONFIG_RE = re.compile(
    r"^etl_(?P<workflow_id>[a-z][a-z0-9_]+)(?:\.(?P<scope_suffix>[^.]+))?\.config\.yaml$"
)


def normalize_scope_suffix(scope_suffix: str | None) -> str:
    return str(scope_suffix or "").strip()


def is_scoped_build(scope_suffix: str | None) -> bool:
    return bool(normalize_scope_suffix(scope_suffix))


def artifact_basename(workflow_id: str, scope_suffix: str | None = None) -> str:
    wid = str(workflow_id).strip()
    suffix = normalize_scope_suffix(scope_suffix)
    if suffix:
        return f"etl_{wid}.{suffix}"
    return f"etl_{wid}"


def artifact_filename(workflow_id: str, scope_suffix: str | None, kind: str) -> str:
    """``kind`` e.g. ``Workflow.yaml``, ``config.yaml``."""
    base = artifact_basename(workflow_id, scope_suffix)
    return f"{base}.{kind}"


def parse_built_config_filename(name: str) -> tuple[str, str] | None:
    m = _BUILT_CONFIG_RE.match(name)
    if not m:
        return None
    scope = normalize_scope_suffix(m.group("scope_suffix") or "")
    return m.group("workflow_id"), scope


def workflow_artifacts_root(module_root: Path) -> Path:
    """``workflows/`` under cdf_discovery when ``module_root`` is ``transform/``."""
    root = module_root.resolve()
    parent = root.parent
    if root.name == "transform" and (parent / "transform").resolve() == root:
        return parent / WORKFLOW_ARTIFACTS_DIR_NAME
    return root / WORKFLOW_ARTIFACTS_DIR_NAME


def workflow_artifacts_output_dir(module_root: Path, scope_suffix: str | None = None) -> Path:
    root = workflow_artifacts_root(module_root)
    suffix = normalize_scope_suffix(scope_suffix)
    return root / suffix if suffix else root
