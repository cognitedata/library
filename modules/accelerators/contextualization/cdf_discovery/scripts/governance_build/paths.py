"""Paths for governance config (declared root) vs Toolkit deploy artifacts."""

from __future__ import annotations

from pathlib import Path

GOVERNANCE_CONFIG_DIR_NAME = "governance"
STATE_NAME = "access_governance_state.json"


def discovery_package_root(governance_config_root: Path) -> Path:
    """cdf_discovery module root when config lives under ``governance/``."""
    root = governance_config_root.resolve()
    if root.name == GOVERNANCE_CONFIG_DIR_NAME:
        return root.parent
    return root


def governance_artifacts_root(governance_config_root: Path) -> Path:
    """Generated ``spaces/`` and ``auth/`` YAML (Toolkit deploy targets)."""
    return discovery_package_root(governance_config_root)


def resolve_artifact_path(governance_config_root: Path, rel: str) -> Path:
    return governance_artifacts_root(governance_config_root) / rel.replace("\\", "/")


def state_file_path(governance_config_root: Path) -> Path:
    return governance_config_root.resolve() / STATE_NAME
