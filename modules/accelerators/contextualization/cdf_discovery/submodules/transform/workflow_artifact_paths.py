"""Built workflow YAML paths (Toolkit ``workflows/`` vs local-run ``workflow_scopes/``)."""

from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_ARTIFACTS_DIR_NAME = "workflows"
WORKFLOW_SCOPES_DIR_NAME = "workflow_scopes"

_BUILT_CONFIG_RE = re.compile(
    r"^etl_(?P<workflow_id>[a-z][a-z0-9_]+)(?:_(?P<scope_suffix>[^.]+))?_scope\.yaml$"
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
    """``kind`` e.g. ``Workflow.yaml``, ``scope.yaml`` (local run document, not a CDF resource)."""
    if kind == "scope.yaml":
        base = artifact_basename(workflow_id, scope_suffix).replace(".", "_")
        return f"{base}_scope.yaml"
    base = artifact_basename(workflow_id, scope_suffix)
    return f"{base}.{kind}"


def parse_scoped_scope_filename(name: str, scope_suffix: str) -> str | None:
    """Return workflow_id from ``etl_*_scope.yaml`` when *scope_suffix* is known."""
    if not name.startswith("etl_") or not name.endswith("_scope.yaml"):
        return None
    stem = name[len("etl_") : -len("_scope.yaml")]
    suffix = normalize_scope_suffix(scope_suffix)
    if suffix:
        scope_token = suffix.replace(".", "_")
        if not stem.endswith(f"_{scope_token}"):
            return None
        wid = stem[: -(len(scope_token) + 1)]
        return wid if wid else None
    return stem or None


def parse_built_config_filename(name: str) -> tuple[str, str] | None:
    parsed = parse_scoped_scope_filename(name, "")
    if parsed is not None:
        return parsed, ""
    m = _BUILT_CONFIG_RE.match(name)
    if not m:
        return None
    scope = normalize_scope_suffix(m.group("scope_suffix") or "")
    return m.group("workflow_id"), scope


def discovery_module_root(module_root: Path) -> Path:
    """Resolve cdf_discovery module root from transform package root or module root."""
    root = module_root.resolve()
    if root.name == "transform" and root.parent.name == "submodules":
        return root.parent.parent
    if (root / "submodules" / "transform").is_dir():
        return root
    if root.name == "transform" and (root.parent / "module.py").is_file():
        return root.parent
    return root


def workflow_artifacts_root(module_root: Path) -> Path:
    return discovery_module_root(module_root) / WORKFLOW_ARTIFACTS_DIR_NAME


def workflow_scopes_root(module_root: Path) -> Path:
    return discovery_module_root(module_root) / WORKFLOW_SCOPES_DIR_NAME


def workflow_artifacts_output_dir(module_root: Path, scope_suffix: str | None = None) -> Path:
    root = workflow_artifacts_root(module_root)
    suffix = normalize_scope_suffix(scope_suffix)
    return root / suffix if suffix else root


def workflow_scopes_output_dir(module_root: Path, scope_suffix: str | None = None) -> Path:
    root = workflow_scopes_root(module_root)
    suffix = normalize_scope_suffix(scope_suffix)
    return root / suffix if suffix else root


def artifact_output_dir(module_root: Path, scope_suffix: str | None, kind: str) -> Path:
    if kind == "scope.yaml":
        return workflow_scopes_output_dir(module_root, scope_suffix)
    return workflow_artifacts_output_dir(module_root, scope_suffix)
