"""Smoke tests for Foundation Deployment Pack CI scaffolding."""

from __future__ import annotations

from pathlib import Path

import tomllib

REPO_ROOT = Path(__file__).resolve().parents[1]
FDP_ROOT = REPO_ROOT / "foundation-deployment-pack"
PACKAGES_TOML = REPO_ROOT / "modules" / "packages.toml"


def test_fdp_config_files_exist() -> None:
    for env in ("dev", "test", "prod"):
        path = FDP_ROOT / f"config.{env}.yaml"
        assert path.is_file(), f"missing {path}"


def test_cdf_toml_points_at_fdp() -> None:
    cdf_toml = REPO_ROOT / "cdf.toml"
    data = tomllib.loads(cdf_toml.read_text(encoding="utf-8"))
    assert data["cdf"]["default_organization_dir"] == "foundation-deployment-pack"


def test_foundation_package_registered() -> None:
    data = tomllib.loads(PACKAGES_TOML.read_text(encoding="utf-8"))
    assert "foundation" in data["packages"]
    assert data["packages"]["foundation"]["id"] == "dp:foundation"


def test_workflow_files_exist() -> None:
    assert (REPO_ROOT / ".github/workflows/dry-run.yml").is_file()
    assert (REPO_ROOT / ".github/workflows/deploy.yml").is_file()
