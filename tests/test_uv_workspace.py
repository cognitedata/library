"""Tests for the uv workspace and deploy requirements export."""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from generate_uv_member_projects import PACKAGE_SPECS


def _deploy_dependencies(spec: dict[str, object]) -> list[str]:
    deploy = spec.get("deploy_dependencies")
    if deploy is not None:
        return list(deploy)  # type: ignore[arg-type]
    return list(spec["dependencies"])  # type: ignore[arg-type]


def test_deploy_targets_have_pyproject_and_requirements() -> None:
    for spec in PACKAGE_SPECS:
        rel_path = str(spec["path"])
        package_dir = REPO_ROOT / rel_path
        assert (package_dir / "pyproject.toml").is_file(), rel_path
        requirements = package_dir / "requirements.txt"
        assert requirements.is_file(), rel_path
        lines = [line for line in requirements.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert lines == _deploy_dependencies(spec), rel_path
        assert not any(line.lstrip().startswith("# via ") for line in lines), rel_path


def test_workspace_members_include_deploy_targets() -> None:
    manifest = json.loads((REPO_ROOT / "scripts" / "uv_workspace_members.json").read_text(encoding="utf-8"))
    for spec in PACKAGE_SPECS:
        assert str(spec["path"]) in manifest
