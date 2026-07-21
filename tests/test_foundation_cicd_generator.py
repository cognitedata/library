"""Tests for Foundation Deployment Pack CI/CD generator (cdf_project_foundation)."""


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
    from generate_actions import discover_foundation_module_paths  # pyright: ignore[reportMissingImports]

    paths = discover_foundation_module_paths(REPO_ROOT / "modules", REPO_ROOT)
    assert "common/cdf_project_foundation" in paths
    assert "sourcesystem/cdf_pi_extractor" in paths


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
    assert "cdf build --env dev" in dry_run
    assert "cdf deploy --dry-run | tee dryrun-output.txt" in dry_run
    assert "cdf deploy --dry-run --env" not in dry_run

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

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`acme-dev`" in cicd_docs
    assert "`acme-test`" in cicd_docs
    assert "`acme-prod`" in cicd_docs
    assert "`ADMIN_SOURCE_ID`" in cicd_docs
    assert "`CONSUMER_SOURCE_ID`" in cicd_docs
    assert "`PRODUCER_SOURCE_ID`" in cicd_docs
    assert "skips the pre-commit config lint step" in cicd_docs


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
    stale_staging_workflow = workflows / "deploy-staging.yml"
    stale_prod_workflow = workflows / "deploy-prod.yml"
    stale_test_workflow.write_text("stale\n", encoding="utf-8")
    stale_staging_workflow.write_text("stale\n", encoding="utf-8")
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
    assert not stale_staging_workflow.exists()
    assert not stale_prod_workflow.exists()

    dry_run = (workflows / "dry-run.yml").read_text(encoding="utf-8")
    assert "      - dev" in dry_run
    assert "      - main" not in dry_run
    assert "deploy-test.yml" not in dry_run
    assert "deploy-staging.yml" not in dry_run
    assert "deploy-prod.yml" not in dry_run
    assert "test-toolkit-credentials" not in dry_run
    assert "staging-toolkit-credentials" not in dry_run
    assert "prod-toolkit-credentials" not in dry_run
    assert "config.test.yaml" not in dry_run
    assert "config.staging.yaml" not in dry_run
    assert "config.prod.yaml" not in dry_run
    assert "cdf build -c config.dev.yaml" in dry_run

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`acme-dev`" in cicd_docs
    assert "`acme-test`" not in cicd_docs
    assert "`acme-staging`" not in cicd_docs
    assert "`acme-prod`" not in cicd_docs
    assert "config.test.yaml" not in cicd_docs
    assert "config.staging.yaml" not in cicd_docs
    assert "config.prod.yaml" not in cicd_docs


def test_generate_actions_uses_staging_when_staging_config_exists(tmp_path: Path) -> None:
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
    for env in ("dev", "staging", "prod"):
        (tmp_path / f"config.{env}.yaml").write_text(
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

    workflows = tmp_path / ".github" / "workflows"
    assert (workflows / "deploy-staging.yml").is_file()
    assert not (workflows / "deploy-test.yml").exists()

    dry_run = (workflows / "dry-run.yml").read_text(encoding="utf-8")
    assert "deploy-staging.yml" in dry_run
    assert "deploy-test.yml" not in dry_run
    assert "staging-toolkit-credentials" in dry_run
    assert "test-toolkit-credentials" not in dry_run
    assert "cdf build -c config.staging.yaml" in dry_run
    assert "cdf build -c config.test.yaml" not in dry_run

    deploy_staging = (workflows / "deploy-staging.yml").read_text(encoding="utf-8")
    assert "name: Deploy to acme-staging" in deploy_staging
    assert "environment: staging-toolkit-credentials" in deploy_staging
    assert "run: cdf build -c config.staging.yaml" in deploy_staging

    cicd_docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "`staging-toolkit-credentials`" in cicd_docs
    assert "`config.staging.yaml`" in cicd_docs
    assert "`config.test.yaml`" not in cicd_docs


def test_generate_actions_supports_selected_environment_combinations(tmp_path: Path) -> None:
    cases = [
        ("dev",),
        ("test",),
        ("staging",),
        ("prod",),
        ("dev", "test"),
        ("dev", "staging"),
        ("dev", "prod"),
        ("test", "prod"),
        ("staging", "prod"),
        ("dev", "test", "prod"),
        ("dev", "staging", "prod"),
    ]
    deployable = {"dev", "test", "staging"}
    all_workflows = {
        "dry-run.yml",
        "deploy-dev.yml",
        "deploy-test.yml",
        "deploy-staging.yml",
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
        for env in {"dev", "test", "staging", "prod"} - set(envs):
            assert f"`acme-{env}`" not in docs
            assert f"`config.{env}.yaml`" not in docs
