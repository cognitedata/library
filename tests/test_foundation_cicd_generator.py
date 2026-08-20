"""Tests for Foundation Deployment Pack CI/CD generator (cdf_project_foundation)."""


import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_project_foundation"
GENERATE_ACTIONS = MODULE_ROOT / "scripts" / "generate_actions.py"
TEMPLATES = MODULE_ROOT / "templates" / "github"


def test_generator_scripts_exist() -> None:
    assert (MODULE_ROOT / "scripts" / "generate_actions.py").is_file()
    assert (TEMPLATES / "dry-run.yml").is_file()


def test_dry_run_environment_rejects_more_than_two_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    monkeypatch.setattr(generate_actions, "deployable_envs", lambda projects: ["dev", "test", "qa"])
    monkeypatch.setitem(generate_actions.DEPLOY_BRANCHES, "qa", "qa")

    with pytest.raises(ValueError, match="Unsupported number of deployable branches: 3"):
        generate_actions.dry_run_environment({"dev": "acme-dev", "test": "acme-test", "qa": "acme-qa"})


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
    mod = tmp_path / org_dir / "modules" / "sourcesystem" / "cdf_pi_extractor"
    mod.mkdir(parents=True)
    (mod / "module.toml").write_text(
        'id = "cdf_pi_extractor"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    (mod / "default.config.yaml").write_text("location: site1\n", encoding="utf-8")
    for env in ("dev", "test", "prod"):
        (tmp_path / org_dir / f"config.{env}.yaml").write_text(
            f"""
environment:
  name: {env}
  project: acme-{env}
""".lstrip(),
            encoding="utf-8",
        )

    subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
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

    dry_run = (tmp_path / ".github" / "workflows" / "dry-run.yml").read_text(
        encoding="utf-8"
    )
    assert "'industrial/config*.yaml'" in dry_run
    assert "'industrial/modules/sourcesystem/cdf_pi_extractor/'" not in dry_run
    assert "No .pre-commit-config.yaml found; skipping pre-commit config lint." in dry_run
    assert "ruff check" in dry_run
    assert "pyright --pythonversion 3.13" in dry_run
    assert "No Python found under functions/; skipping ruff and pyright." in dry_run
    assert "cdf build --env dev" in dry_run
    assert "cdf deploy --dry-run | tee dryrun-output.txt" in dry_run
    assert "cdf deploy --dry-run --env" not in dry_run
    assert (
        "run: python industrial/modules/common/cdf_project_foundation/scripts/"
        "setup_project.py --check"
    ) in dry_run
    # The check step must run before the "cdf build" step, not after.
    assert dry_run.index("Verify project config is in sync") < dry_run.index("- name: cdf build")

    deploy_dev = (tmp_path / ".github" / "workflows" / "deploy-dev.yml").read_text(
        encoding="utf-8"
    )
    assert "name: Deploy to acme-dev" in deploy_dev
    assert "run: cdf build --env dev" in deploy_dev
    assert "run: cdf deploy" in deploy_dev
    assert "cdf deploy --env" not in deploy_dev
    assert "ADMIN_SOURCE_ID: ${{ vars.ADMIN_SOURCE_ID }}" in deploy_dev
    assert "CONSUMER_SOURCE_ID: ${{ vars.CONSUMER_SOURCE_ID }}" in deploy_dev
    assert "PRODUCER_SOURCE_ID: ${{ vars.PRODUCER_SOURCE_ID }}" in deploy_dev
    assert (
        "run: python industrial/modules/common/cdf_project_foundation/scripts/"
        "setup_project.py --check"
    ) in deploy_dev
    assert deploy_dev.index("Verify project config is in sync") < deploy_dev.index("- name: cdf build")

    deploy_prod = (tmp_path / ".github" / "workflows" / "deploy-prod.yml").read_text(
        encoding="utf-8"
    )
    assert (
        "run: python industrial/modules/common/cdf_project_foundation/scripts/"
        "setup_project.py --check"
    ) in deploy_prod

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`acme-dev`" in cicd_docs
    assert "`acme-test`" in cicd_docs
    assert "`acme-prod`" in cicd_docs
    assert "`ADMIN_SOURCE_ID`" in cicd_docs
    assert "`CONSUMER_SOURCE_ID`" in cicd_docs
    assert "`PRODUCER_SOURCE_ID`" in cicd_docs
    assert "skips the pre-commit config lint step" in cicd_docs
    assert "ruff check` and `pyright`" in cicd_docs


def test_generate_actions_validates_environment_name(tmp_path: Path) -> None:
    (tmp_path / "cdf.toml").write_text(
        """
[modules]
version = "0.7.220"
""".strip(),
        encoding="utf-8",
    )
    modules = tmp_path / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    for env in ("dev", "test", "prod"):
        name = "prod" if env == "dev" else env
        (tmp_path / f"config.{env}.yaml").write_text(
            f"""
environment:
  name: {name}
  project: acme-{env}
""".lstrip(),
            encoding="utf-8",
        )

    result = subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
            "--force",
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "environment.name='prod'; expected 'dev'" in result.stderr


def test_generate_actions_uses_config_flag_for_toolkit_0_8(tmp_path: Path) -> None:
    org_dir = "industrial"
    (tmp_path / "cdf.toml").write_text(
        f"""
[cdf]
default_organization_dir = "{org_dir}"

[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    modules = tmp_path / org_dir / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    for env in ("dev", "test", "prod"):
        (tmp_path / org_dir / f"config.{env}.yaml").write_text(
            f"""
environment:
  name: {env}
  project: acme-{env}
""".lstrip(),
            encoding="utf-8",
        )

    subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
            "--force",
        ],
        check=True,
        cwd=tmp_path,
    )

    dry_run = (tmp_path / ".github" / "workflows" / "dry-run.yml").read_text(
        encoding="utf-8"
    )
    assert "cdf build -c industrial/config.dev.yaml" in dry_run
    assert "cdf build -c industrial/config.test.yaml" in dry_run
    assert 'case "$GITHUB_BASE_REF" in' in dry_run
    assert "Unsupported base branch $GITHUB_BASE_REF" in dry_run
    assert "cdf build --env" not in dry_run

    deploy_prod = (tmp_path / ".github" / "workflows" / "deploy-prod.yml").read_text(
        encoding="utf-8"
    )
    assert "run: cdf build -c industrial/config.prod.yaml" in deploy_prod
    assert "run: cdf deploy" in deploy_prod
    assert "cdf deploy --env" not in deploy_prod


def test_generate_actions_skips_test_workflow_when_test_config_missing(tmp_path: Path) -> None:
    org_dir = "industrial"
    (tmp_path / "cdf.toml").write_text(
        f"""
[cdf]
default_organization_dir = "{org_dir}"

[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    modules = tmp_path / org_dir / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    for env in ("dev", "prod"):
        (tmp_path / org_dir / f"config.{env}.yaml").write_text(
            f"""
environment:
  name: {env}
  project: acme-{env}
""".lstrip(),
            encoding="utf-8",
        )
    stale_test_workflow = tmp_path / ".github" / "workflows" / "deploy-test.yml"
    stale_test_workflow.parent.mkdir(parents=True)
    stale_test_workflow.write_text("stale\n", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
            "--force",
        ],
        check=True,
        cwd=tmp_path,
    )

    assert (tmp_path / ".github" / "workflows" / "dry-run.yml").is_file()
    assert (tmp_path / ".github" / "workflows" / "deploy-dev.yml").is_file()
    assert not stale_test_workflow.exists()
    assert (tmp_path / ".github" / "workflows" / "deploy-prod.yml").is_file()

    dry_run = (tmp_path / ".github" / "workflows" / "dry-run.yml").read_text(
        encoding="utf-8"
    )
    assert "      - dev" in dry_run
    assert "      - main" not in dry_run
    assert "deploy-test.yml" not in dry_run
    assert "test-toolkit-credentials" not in dry_run
    assert "config.test.yaml" not in dry_run
    assert "cdf build -c industrial/config.dev.yaml" in dry_run

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`acme-dev`" in cicd_docs
    assert "`acme-prod`" in cicd_docs
    assert "`acme-test`" not in cicd_docs
    assert "config.test.yaml" not in cicd_docs


def test_generate_actions_supports_dev_only_config(tmp_path: Path) -> None:
    (tmp_path / "cdf.toml").write_text(
        """
[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    modules = tmp_path / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    (tmp_path / "config.dev.yaml").write_text(
        """
environment:
  name: dev
  project: acme-dev
""".lstrip(),
        encoding="utf-8",
    )
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    stale_test_workflow = workflows / "deploy-test.yml"
    stale_prod_workflow = workflows / "deploy-prod.yml"
    stale_test_workflow.write_text("stale\n", encoding="utf-8")
    stale_prod_workflow.write_text("stale\n", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(GENERATE_ACTIONS),
            "--force",
        ],
        check=True,
        cwd=tmp_path,
    )

    assert (workflows / "dry-run.yml").is_file()
    assert (workflows / "deploy-dev.yml").is_file()
    assert not stale_test_workflow.exists()
    assert not stale_prod_workflow.exists()

    dry_run = (workflows / "dry-run.yml").read_text(encoding="utf-8")
    assert "      - dev" in dry_run
    assert "      - main" not in dry_run
    assert "deploy-test.yml" not in dry_run
    assert "deploy-prod.yml" not in dry_run
    assert "test-toolkit-credentials" not in dry_run
    assert "prod-toolkit-credentials" not in dry_run
    assert "config.test.yaml" not in dry_run
    assert "config.prod.yaml" not in dry_run
    assert "cdf build -c config.dev.yaml" in dry_run

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`acme-dev`" in cicd_docs
    assert "`acme-test`" not in cicd_docs
    assert "`acme-prod`" not in cicd_docs
    assert "config.test.yaml" not in cicd_docs
    assert "config.prod.yaml" not in cicd_docs


def test_generate_actions_supports_selected_environment_combinations(tmp_path: Path) -> None:
    cases = [
        ("dev",),
        ("test",),
        ("prod",),
        ("dev", "test"),
        ("dev", "prod"),
        ("test", "prod"),
        ("dev", "test", "prod"),
    ]
    deployable = {"dev", "test"}
    all_workflows = {
        "dry-run.yml",
        "deploy-dev.yml",
        "deploy-test.yml",
        "deploy-prod.yml",
    }

    for envs in cases:
        case_dir = tmp_path / "-".join(envs)
        case_dir.mkdir()
        (case_dir / "cdf.toml").write_text(
            """
[modules]
version = "0.8.0"
""".strip(),
            encoding="utf-8",
        )
        modules = case_dir / "modules" / "common" / "cdf_project_foundation"
        modules.mkdir(parents=True)
        (modules / "module.toml").write_text(
            'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
            encoding="utf-8",
        )
        for env in envs:
            (case_dir / f"config.{env}.yaml").write_text(
                f"""
environment:
  name: {env}
  project: acme-{env}
""".lstrip(),
                encoding="utf-8",
            )

        workflows = case_dir / ".github" / "workflows"
        workflows.mkdir(parents=True)
        for name in all_workflows:
            (workflows / name).write_text("stale\n", encoding="utf-8")

        subprocess.run(
            [
                sys.executable,
                str(GENERATE_ACTIONS),
                "--force",
            ],
            check=True,
            cwd=case_dir,
        )

        expected = set()
        if deployable.intersection(envs):
            expected.add("dry-run.yml")
        for env in envs:
            expected.add(f"deploy-{env}.yml")

        for name in all_workflows:
            path = workflows / name
            if name in expected:
                assert path.is_file(), f"{envs}: expected {name}"
            else:
                assert not path.exists(), f"{envs}: unexpected stale {name}"

        docs = (case_dir / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
        for env in envs:
            assert f"`acme-{env}`" in docs
            assert f"`config.{env}.yaml`" in docs
        for env in {"dev", "test", "prod"} - set(envs):
            assert f"`acme-{env}`" not in docs
            assert f"`config.{env}.yaml`" not in docs


def _scaffold_dev_only_project(project_dir: Path) -> None:
    (project_dir / "cdf.toml").write_text(
        """
[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    modules = project_dir / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    (project_dir / "config.dev.yaml").write_text(
        """
environment:
  name: dev
  project: acme-dev
""".lstrip(),
        encoding="utf-8",
    )


def test_generate_actions_explicit_provider_github_is_byte_identical_to_default(tmp_path: Path) -> None:
    default_dir = tmp_path / "default"
    explicit_dir = tmp_path / "explicit"
    default_dir.mkdir()
    explicit_dir.mkdir()
    _scaffold_dev_only_project(default_dir)
    _scaffold_dev_only_project(explicit_dir)

    subprocess.run([sys.executable, str(GENERATE_ACTIONS), "--force"], check=True, cwd=default_dir)
    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "github"],
        check=True,
        cwd=explicit_dir,
    )

    default_workflows = default_dir / ".github" / "workflows"
    explicit_workflows = explicit_dir / ".github" / "workflows"
    for name in ("dry-run.yml", "deploy-dev.yml"):
        default_content = (default_workflows / name).read_text(encoding="utf-8")
        explicit_content = (explicit_workflows / name).read_text(encoding="utf-8")
        assert default_content == explicit_content

    default_docs = (default_dir / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    explicit_docs = (explicit_dir / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert default_docs == explicit_docs

    # Nothing provider-specific should leak into GitHub's own output tree.
    assert not (explicit_dir / ".devops").exists()


def test_generate_actions_rejects_invalid_provider(tmp_path: Path) -> None:
    _scaffold_dev_only_project(tmp_path)

    result = subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "gitlab"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "invalid choice: 'gitlab'" in result.stderr
    assert not (tmp_path / ".github").exists()
    assert not (tmp_path / ".devops").exists()


def test_generate_actions_provider_ado_fails_with_missing_template_error(tmp_path: Path) -> None:
    _scaffold_dev_only_project(tmp_path)

    result = subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "Missing template" in result.stderr
    assert "templates/ado" in result.stderr
    # No-op: nothing should be written for a provider whose templates don't exist yet.
    assert not (tmp_path / ".devops").exists()
    assert not (tmp_path / ".github").exists()
    assert not (tmp_path / "docs" / "FOUNDATION_CICD.md").exists()


def _make_foundation_module(modules_root: Path) -> None:
    module_dir = modules_root / "common" / "cdf_project_foundation" / "scripts"
    module_dir.mkdir(parents=True)
    (module_dir / "setup_project.py").write_text("", encoding="utf-8")


def test_setup_project_check_cmd_org_dir_set_but_modules_at_repo_root(tmp_path: Path) -> None:
    """org_dir being configured in cdf.toml doesn't guarantee modules/ is nested
    under it — resolve_modules_root checks the repo root first. The generated
    command must point at wherever modules/ actually is, not blindly prefix org_dir."""
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    _make_foundation_module(tmp_path / "modules")

    cmd = generate_actions.setup_project_check_cmd(tmp_path, "industrial")
    assert cmd == "python modules/common/cdf_project_foundation/scripts/setup_project.py --check"


def test_setup_project_check_cmd_modules_nested_under_org_dir(tmp_path: Path) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    _make_foundation_module(tmp_path / "industrial" / "modules")

    cmd = generate_actions.setup_project_check_cmd(tmp_path, "industrial")
    assert cmd == "python industrial/modules/common/cdf_project_foundation/scripts/setup_project.py --check"
