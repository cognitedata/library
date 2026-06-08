"""Tests for Foundation Deployment Pack CI/CD generator (cdf_project_foundation)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_project_foundation"
GENERATE_ACTIONS = MODULE_ROOT / "scripts" / "generate_actions.py"
TEMPLATES = MODULE_ROOT / "templates" / "github"


def test_generator_scripts_exist() -> None:
    assert (MODULE_ROOT / "scripts" / "generate_actions.py").is_file()
    assert (TEMPLATES / "dry-run.yml").is_file()


def test_discover_foundation_modules_includes_project_foundation() -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    from generate_actions import discover_foundation_module_paths

    paths = discover_foundation_module_paths(REPO_ROOT / "modules", REPO_ROOT)
    assert "common/cdf_project_foundation" in paths
    assert "sourcesystem/cdf_pi_foundation" in paths


def test_generate_actions_writes_workflows_and_docs(tmp_path: Path) -> None:
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
    mod = tmp_path / org_dir / "modules" / "sourcesystem" / "cdf_pi_foundation"
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
            "--force",
        ],
        check=True,
        cwd=tmp_path,
    )

    assert (tmp_path / ".github" / "workflows" / "dry-run.yml").is_file()
    assert (tmp_path / ".github" / "workflows" / "deploy-dev.yml").is_file()
    assert (tmp_path / ".github" / "workflows" / "deploy-test.yml").is_file()
    assert (tmp_path / ".github" / "workflows" / "deploy-prod.yml").is_file()
    assert (tmp_path / "docs" / "FOUNDATION_CICD.md").is_file()
    assert not (tmp_path / org_dir / "config.dev.yaml").exists()

    dry_run = (tmp_path / ".github" / "workflows" / "dry-run.yml").read_text(
        encoding="utf-8"
    )
    assert "'industrial/config*.yaml'" in dry_run
    assert "'industrial/modules/sourcesystem/cdf_pi_foundation/'" in dry_run
