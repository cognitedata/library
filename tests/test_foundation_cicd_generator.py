"""Tests for the Foundation Deployment Pack CI/CD generator module."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import tomllib

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_foundation_cicd"
PACKAGES_TOML = REPO_ROOT / "modules" / "packages.toml"
GENERATE_ACTIONS = MODULE_ROOT / "scripts" / "generate_actions.py"
GENERATE_ENV = MODULE_ROOT / "scripts" / "generate_env_configs.py"
TEMPLATES = MODULE_ROOT / "templates" / "github"


def test_foundation_package_includes_cicd_module() -> None:
    data = tomllib.loads(PACKAGES_TOML.read_text(encoding="utf-8"))
    modules = data["packages"]["foundation"]["modules"]
    assert "common/cdf_foundation_cicd" in modules


def test_workflow_templates_exist() -> None:
    for name in (
        "dry-run.yml",
        "deploy-dev.yml",
        "deploy-test.yml",
        "deploy-prod.yml",
        "prepare-toolkit-project.sh",
        "FOUNDATION_CICD.md",
    ):
        assert (TEMPLATES / name).is_file(), f"missing template {name}"


def test_discover_modules_excludes_cicd_tooling() -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    from generate_env_configs import discover_foundation_module_paths

    paths = discover_foundation_module_paths(REPO_ROOT / "modules", REPO_ROOT)
    assert "common/cdf_foundation_cicd" not in paths
    assert "sourcesystem/cdf_pi_foundation" in paths


def test_generate_actions_writes_workflows(tmp_path: Path) -> None:
    org_dir = "industrial"
    (tmp_path / "cdf.toml").write_text(
        f"""
[cdf]
default_organization_dir = "{org_dir}"

[modules]
version = "0.7.220"
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "modules").mkdir()
    # Minimal module stub so config generation finds at least one module.
    mod = tmp_path / "modules" / "sourcesystem" / "cdf_pi_foundation"
    mod.mkdir(parents=True)
    (mod / "module.toml").write_text(
        'id = "cdf_pi_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    (mod / "default.config.yaml").write_text("location: site1\n", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
            "--enterprise",
            "acme",
            "--repo-root",
            str(tmp_path),
            "--force",
        ],
        check=True,
        cwd=tmp_path,
    )

    assert (tmp_path / ".github" / "workflows" / "dry-run.yml").is_file()
    assert (tmp_path / ".github" / "workflows" / "deploy-dev.yml").is_file()
    assert (tmp_path / org_dir / "config.dev.yaml").is_file()
    content = (tmp_path / org_dir / "config.dev.yaml").read_text(encoding="utf-8")
    assert "acme-dev" in content
    assert "modules/sourcesystem/cdf_pi_foundation" in content
    assert "modules/common/cdf_foundation_cicd" not in content
