"""Tests for Foundation Deployment Pack CI/CD generator (cdf_project_foundation)."""


import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules" / "common" / "cdf_project_foundation"
GENERATE_ACTIONS = MODULE_ROOT / "scripts" / "generate_actions.py"
TEMPLATES = MODULE_ROOT / "templates" / "github"


def test_generator_scripts_exist() -> None:
    assert (MODULE_ROOT / "scripts" / "generate_actions.py").is_file()
    assert (TEMPLATES / "dry-run.yml").is_file()


def test_github_dry_run_environment_rejects_more_than_two_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    monkeypatch.setattr(generate_actions, "deployable_envs", lambda projects: ["dev", "test", "qa"])
    monkeypatch.setitem(generate_actions.DEPLOY_BRANCHES, "qa", "qa")

    with pytest.raises(ValueError, match="Unsupported number of deployable branches: 3"):
        generate_actions.github_dry_run_environment({"dev": "acme-dev", "test": "acme-test", "qa": "acme-qa"})


def test_ado_dry_run_jobs_rejects_more_than_two_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    monkeypatch.setattr(generate_actions, "deployable_envs", lambda projects: ["dev", "test", "qa"])
    monkeypatch.setitem(generate_actions.DEPLOY_BRANCHES, "qa", "qa")

    with pytest.raises(ValueError, match="Unsupported number of deployable branches: 3"):
        generate_actions.ado_dry_run_jobs(
            "0.8.0",
            None,
            "python scripts/setup_project.py --check",
            {"dev": "acme-dev", "test": "acme-test", "qa": "acme-qa"},
        )


def test_workflows_output_dir_rejects_unsupported_provider(tmp_path: Path) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    with pytest.raises(ValueError, match="Unsupported provider: gitlab"):
        generate_actions.workflows_output_dir("gitlab", tmp_path)


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


def _scaffold_project_with_envs(project_dir: Path, envs: tuple[str, ...], org_dir: str | None = None) -> None:
    (project_dir / "cdf.toml").write_text(
        """
[modules]
version = "0.8.0"
""".strip(),
        encoding="utf-8",
    )
    base = (project_dir / org_dir) if org_dir else project_dir
    modules = base / "modules" / "common" / "cdf_project_foundation"
    modules.mkdir(parents=True)
    (modules / "module.toml").write_text(
        'id = "cdf_project_foundation"\npackage_id = "dp:foundation"\n',
        encoding="utf-8",
    )
    for env in envs:
        (base / f"config.{env}.yaml").write_text(
            f"""
environment:
  name: {env}
  project: acme-{env}
""".lstrip(),
            encoding="utf-8",
        )


def test_generate_actions_ado_writes_pipelines_and_docs(tmp_path: Path) -> None:
    _scaffold_project_with_envs(tmp_path, ("dev", "test", "prod"))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    dry_run_path = tmp_path / ".devops" / "dry-run-pipeline.yml"
    deploy_path = tmp_path / ".devops" / "deploy-pipeline.yml"
    assert dry_run_path.is_file()
    assert deploy_path.is_file()
    assert (tmp_path / "docs" / "FOUNDATION_CICD.md").is_file()
    # Nothing GitHub-specific should leak into ado's own output tree.
    assert not (tmp_path / ".github").exists()

    dry_run_yaml = yaml.safe_load(dry_run_path.read_text(encoding="utf-8"))
    deploy_yaml = yaml.safe_load(deploy_path.read_text(encoding="utf-8"))
    job_names = {job["job"] for job in dry_run_yaml["jobs"]}
    assert job_names == {"lint", "dry_run_dev", "dry_run_test"}
    deploy_job_names = {job["job"] for job in deploy_yaml["jobs"]}
    assert deploy_job_names == {"deploy_dev", "deploy_test", "deploy_prod"}

    # Azure Repos doesn't support YAML `pr:` triggers — a `pr:` block here would be
    # dead config that misleads readers into thinking it controls PR execution.
    assert "pr" not in dry_run_yaml
    dry_run_raw = dry_run_path.read_text(encoding="utf-8")
    assert "Build Validation branch policy" in dry_run_raw

    for job in dry_run_yaml["jobs"]:
        if job["job"] in ("dry_run_dev", "dry_run_test"):
            # An explicit condition replaces the default succeeded() gate in Azure
            # Pipelines — without succeeded() here, a PR could still build/deploy
            # against live CDF credentials even when the lint job failed.
            assert job["condition"].startswith("and(succeeded(), ")

    dry_run_text = dry_run_path.read_text(encoding="utf-8")
    assert "GITHUB_BASE_REF" not in dry_run_text
    assert "github." not in dry_run_text
    assert "System.PullRequest.TargetBranch" in dry_run_text
    assert "dev-toolkit-credentials" in dry_run_text
    assert "test-toolkit-credentials" in dry_run_text

    deploy_text = deploy_path.read_text(encoding="utf-8")
    assert "startsWith(variables['Build.SourceBranch'], 'refs/tags/v')" in deploy_text
    assert "prod-toolkit-credentials" in deploy_text
    assert "Enforce release tag pattern" in deploy_text
    for job in deploy_yaml["jobs"]:
        # Same succeeded() requirement as the dry-run jobs: an explicit condition
        # without it would let a deploy job start after a canceled run.
        assert job["condition"].startswith("and(succeeded(), ")
    # Azure Pipelines does not auto-map secret variable-group values into script
    # environments (only plain variables get that treatment) — cdf deploy needs
    # IDP_CLIENT_SECRET mapped explicitly or authentication fails.
    assert deploy_text.count("IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)") == 4
    assert "IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)" in dry_run_text
    # checkout doesn't persist git credentials by default — the prod job's own
    # `git fetch origin main` step needs them to authenticate.
    prod_job = next(job for job in deploy_yaml["jobs"] if job["job"] == "deploy_prod")
    assert prod_job["steps"][0]["checkout"] == "self"
    assert prod_job["steps"][0]["persistCredentials"] is True
    # A shallow/single-branch CI checkout may not have origin/main as a valid
    # remote-tracking ref; FETCH_HEAD is always populated by the preceding fetch.
    assert "git merge-base --is-ancestor HEAD FETCH_HEAD" in deploy_text
    assert "origin/main; then" not in deploy_text
    assert all(len(line) <= 120 for line in deploy_text.splitlines())
    assert all(len(line) <= 120 for line in dry_run_text.splitlines())

    docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "toolkit-deploy-dev" in docs
    assert "toolkit-deploy-test" in docs
    assert "toolkit-deploy-prod" in docs
    assert "Build Validation" in docs
    assert "Branch control" in docs
    assert "Pipeline permissions alone are not sufficient" in docs
    assert "IDP_TOKEN_URL" in docs
    assert "GitHub Release" not in docs


def test_generate_actions_ado_dev_only_has_branch_condition(tmp_path: Path) -> None:
    """Even with a single deployable branch, the dry-run job must stay gated on
    System.PullRequest.TargetBranch -- otherwise a manual or non-PR pipeline run
    would load live credentials and run cdf deploy --dry-run unconditionally."""
    _scaffold_project_with_envs(tmp_path, ("dev",))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    dry_run_path = tmp_path / ".devops" / "dry-run-pipeline.yml"
    deploy_path = tmp_path / ".devops" / "deploy-pipeline.yml"
    dry_run_yaml = yaml.safe_load(dry_run_path.read_text(encoding="utf-8"))
    deploy_yaml = yaml.safe_load(deploy_path.read_text(encoding="utf-8"))

    dry_run_dev_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "dry_run_dev")
    assert dry_run_dev_job["condition"] == (
        "and(succeeded(), eq(variables['System.PullRequest.TargetBranch'], 'refs/heads/dev'))"
    )
    assert {job["job"] for job in deploy_yaml["jobs"]} == {"deploy_dev"}
    # The promotion-flow guard only applies to the main/test job.
    assert "Enforce promotion flow" not in dry_run_path.read_text(encoding="utf-8")


def test_generate_actions_ado_test_only_promotion_guard_skips_non_pr_runs(tmp_path: Path) -> None:
    """With only config.test.yaml present, dry_run_test is the sole branch (main). The
    job-level condition already keeps non-PR runs from starting, but the promotion
    guard script is kept as defense-in-depth and must not blow up on an empty HEAD."""
    _scaffold_project_with_envs(tmp_path, ("test",))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    dry_run_path = tmp_path / ".devops" / "dry-run-pipeline.yml"
    dry_run_yaml = yaml.safe_load(dry_run_path.read_text(encoding="utf-8"))
    dry_run_test_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "dry_run_test")
    assert dry_run_test_job["condition"] == (
        "and(succeeded(), eq(variables['System.PullRequest.TargetBranch'], 'refs/heads/main'))"
    )

    dry_run_text = dry_run_path.read_text(encoding="utf-8")
    assert "Enforce promotion flow" in dry_run_text
    assert 'if [ -n "${HEAD}" ]; then' in dry_run_text


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


def test_generate_actions_ado_missing_dry_run_template_fails_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    _scaffold_dev_only_project(project_dir)

    # An ado template set missing dry-run-pipeline.yml — simulates an incomplete install.
    ado_templates = MODULE_ROOT / "templates" / "ado"
    fake_templates_root = tmp_path / "templates"
    fake_ado_dir = fake_templates_root / "ado"
    fake_ado_dir.mkdir(parents=True)
    for name in ("deploy-pipeline.yml", "FOUNDATION_CICD.md"):
        (fake_ado_dir / name).write_text((ado_templates / name).read_text(encoding="utf-8"), encoding="utf-8")

    monkeypatch.setattr(generate_actions, "TEMPLATES_ROOT", fake_templates_root)
    monkeypatch.setattr(sys, "argv", ["generate_actions.py", "--force", "--provider", "ado"])
    monkeypatch.chdir(project_dir)

    with pytest.raises(SystemExit) as exc_info:
        generate_actions.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Missing template" in captured.err
    assert "dry-run-pipeline.yml" in captured.err
    assert not (project_dir / ".devops").exists()


def test_generate_actions_missing_readme_template_fails_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sys.path.insert(0, str(MODULE_ROOT / "scripts"))
    import generate_actions  # pyright: ignore[reportMissingImports]

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    _scaffold_dev_only_project(project_dir)

    # A github template set missing FOUNDATION_CICD.md — everything else is present.
    fake_templates_root = tmp_path / "templates"
    fake_github_dir = fake_templates_root / "github"
    fake_github_dir.mkdir(parents=True)
    for name in ("dry-run.yml", "deploy.yml"):
        (fake_github_dir / name).write_text((TEMPLATES / name).read_text(encoding="utf-8"), encoding="utf-8")

    monkeypatch.setattr(generate_actions, "TEMPLATES_ROOT", fake_templates_root)
    monkeypatch.setattr(sys, "argv", ["generate_actions.py", "--force", "--provider", "github"])
    monkeypatch.chdir(project_dir)

    with pytest.raises(SystemExit) as exc_info:
        generate_actions.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Missing template" in captured.err
    assert "FOUNDATION_CICD.md" in captured.err
    assert not (project_dir / "docs" / "FOUNDATION_CICD.md").exists()
    # Workflow files ahead of the README step should still have been written.
    assert (project_dir / ".github" / "workflows" / "dry-run.yml").is_file()


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
