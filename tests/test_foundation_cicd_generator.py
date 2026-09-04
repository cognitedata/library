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


def test_generate_actions_force_regenerates_tag_pinned_workflows_to_digests(tmp_path: Path) -> None:
    """An existing customer repo generated before actions were digest-pinned must get
    digest-pinned workflows after re-running the generator with --force."""
    _scaffold_dev_only_project(tmp_path)
    workflows_dir = tmp_path / ".github" / "workflows"
    workflows_dir.mkdir(parents=True)
    (workflows_dir / "dry-run.yml").write_text(
        "        - uses: actions/checkout@v4\n        - uses: actions/setup-python@v5\n",
        encoding="utf-8",
    )

    subprocess.run([sys.executable, str(GENERATE_ACTIONS), "--force"], check=True, cwd=tmp_path)

    regenerated = (workflows_dir / "dry-run.yml").read_text(encoding="utf-8")
    assert "actions/checkout@v4\n" not in regenerated
    assert "actions/setup-python@v5\n" not in regenerated
    assert "actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4" in regenerated
    assert "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5" in regenerated


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
    deploy_dev_path = tmp_path / ".devops" / "deploy-dev-pipeline.yml"
    deploy_test_path = tmp_path / ".devops" / "deploy-test-pipeline.yml"
    deploy_prod_path = tmp_path / ".devops" / "deploy-prod-pipeline.yml"
    assert dry_run_path.is_file()
    assert deploy_dev_path.is_file()
    assert deploy_test_path.is_file()
    assert deploy_prod_path.is_file()
    assert (tmp_path / "docs" / "FOUNDATION_CICD.md").is_file()
    # Nothing GitHub-specific should leak into ado's own output tree.
    assert not (tmp_path / ".github").exists()

    dry_run_yaml = yaml.safe_load(dry_run_path.read_text(encoding="utf-8"))
    deploy_dev_yaml = yaml.safe_load(deploy_dev_path.read_text(encoding="utf-8"))
    deploy_test_yaml = yaml.safe_load(deploy_test_path.read_text(encoding="utf-8"))
    deploy_prod_yaml = yaml.safe_load(deploy_prod_path.read_text(encoding="utf-8"))
    job_names = {job["job"] for job in dry_run_yaml["jobs"]}
    assert job_names == {"lint", "source_branch_guard", "dry_run_dev", "dry_run_test"}
    # Each environment gets its own pipeline file with a single job, so Azure's
    # resource authorization for one registration never needs another env's group.
    assert {job["job"] for job in deploy_dev_yaml["jobs"]} == {"deploy_dev"}
    assert {job["job"] for job in deploy_test_yaml["jobs"]} == {"deploy_test"}
    assert {job["job"] for job in deploy_prod_yaml["jobs"]} == {"deploy_prod"}

    # Azure Repos doesn't support YAML `pr:` triggers — a `pr:` block here would be
    # dead config that misleads readers into thinking it controls PR execution.
    assert "pr" not in dry_run_yaml
    dry_run_raw = dry_run_path.read_text(encoding="utf-8")
    assert "Build Validation branch policy" in dry_run_raw
    # dry-run-pipeline.yml is only ever invoked via a Build Validation policy, not
    # a push trigger, so it correctly keeps trigger: none.
    assert dry_run_yaml["trigger"] == "none"

    lint_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "lint")
    # lint must stay unconditioned -- that's what guarantees the target-branch check
    # inside it always runs, even when the value is something no job condition
    # recognizes. A future refactor adding a condition here would silently
    # reintroduce the false-green bug item 4 fixed.
    assert "condition" not in lint_job

    for job in dry_run_yaml["jobs"]:
        if job["job"] in ("dry_run_dev", "dry_run_test", "source_branch_guard"):
            # An explicit condition replaces the default succeeded() gate in Azure
            # Pipelines — without succeeded() here, a PR could still build/deploy
            # against live CDF credentials even when the lint job failed.
            assert job["condition"].startswith("and(succeeded(), ")
    dry_run_dev_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "dry_run_dev")
    dry_run_test_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "dry_run_test")
    assert set(dry_run_dev_job["dependsOn"]) == {"lint"}
    assert set(dry_run_test_job["dependsOn"]) == {"lint", "source_branch_guard"}
    source_branch_guard_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "source_branch_guard")
    assert source_branch_guard_job["dependsOn"] == "lint"
    # Neither job needs repo content -- an implicit checkout would be a wasted full
    # clone just to compare short strings, doubling agent time on constrained orgs.
    assert source_branch_guard_job["steps"][0]["checkout"] == "none"

    dry_run_text = dry_run_path.read_text(encoding="utf-8")
    assert "GITHUB_BASE_REF" not in dry_run_text
    assert "github." not in dry_run_text
    assert "System.PullRequest.TargetBranch" in dry_run_text
    # PR validation only loads the non-secret config group -- never credentials,
    # since a Build Validation run compiles this file's own YAML from the PR's
    # merge ref, which the PR author controls.
    assert "dev-toolkit-config" in dry_run_text
    assert "test-toolkit-config" in dry_run_text
    assert "dev-toolkit-credentials" not in dry_run_text
    assert "test-toolkit-credentials" not in dry_run_text
    assert "IDP_CLIENT_SECRET" not in dry_run_text
    assert "cdf deploy --dry-run" not in dry_run_text
    assert "cdf build" in dry_run_text
    # Azure's script: task runs cmd.exe on a Windows agent; these scripts rely on
    # bash-only syntax ([[ ]], set -euo pipefail), so they must use bash: instead.
    assert "- script:" not in dry_run_text
    # The target-branch check (now lint's first step) must fail loudly rather than
    # silently pass when the value doesn't match a known branch — otherwise the
    # required check reports success having validated nothing.
    assert "Unsupported target branch" in dry_run_text
    assert "exit 1" in dry_run_text
    # It must run before the repo is even checked out, not after.
    assert dry_run_text.index("Enforce known target branch") < dry_run_text.index("checkout: self")

    deploy_dev_text = deploy_dev_path.read_text(encoding="utf-8")
    deploy_test_text = deploy_test_path.read_text(encoding="utf-8")
    deploy_prod_text = deploy_prod_path.read_text(encoding="utf-8")
    assert "- script:" not in deploy_dev_text
    assert "- script:" not in deploy_test_text
    assert "- script:" not in deploy_prod_text
    # Deploy pipelines load both groups -- config (for cdf build) and credentials
    # (for cdf deploy --dry-run / cdf deploy), since this is the pipeline whose
    # YAML a pull request can't modify.
    assert "dev-toolkit-config" in deploy_dev_text
    assert "dev-toolkit-credentials" in deploy_dev_text
    assert "test-toolkit-credentials" not in deploy_dev_text
    assert "prod-toolkit-credentials" not in deploy_dev_text
    assert "test-toolkit-config" in deploy_test_text
    assert "test-toolkit-credentials" in deploy_test_text
    assert "dev-toolkit-credentials" not in deploy_test_text
    # cdf deploy --dry-run now runs in every environment's deploy pipeline, not
    # just prod -- that's the whole point of moving it out of PR validation.
    assert "cdf deploy --dry-run" in deploy_dev_text
    assert "cdf deploy --dry-run" in deploy_test_text
    assert "startsWith(variables['Build.SourceBranch'], 'refs/tags/v')" in deploy_prod_text
    assert "prod-toolkit-credentials" in deploy_prod_text
    assert "dev-toolkit-credentials" not in deploy_prod_text
    assert "Enforce release tag pattern" in deploy_prod_text
    # The prod tag name comes from the full ref, not just its last segment — a tag
    # like `x/v1.0.0` must not slip through as if it were `v1.0.0` -- and the
    # unset-variable case must still be a clean "tag must match" failure, not a
    # bare `set -u` crash.
    assert 'SRC="${BUILD_SOURCEBRANCH:-}"' in deploy_prod_text
    assert 'TAG="${SRC#refs/tags/}"' in deploy_prod_text
    assert "BUILD_SOURCEBRANCHNAME" not in deploy_prod_text
    # Each deploy pipeline declares its own trigger directly, no manual UI override.
    assert deploy_dev_yaml["trigger"] == {"branches": {"include": ["dev"]}}
    assert deploy_test_yaml["trigger"] == {"branches": {"include": ["main"]}}
    assert deploy_prod_yaml["trigger"] == {"branches": {"exclude": ["*"]}, "tags": {"include": ["v*"]}}
    # On a GitHub/Bitbucket-hosted repo, an absent `pr:` block defaults to PR
    # validation on every branch -- without `pr: none`, every PR would queue a
    # toolkit-deploy-prod run (harmlessly skipped, but alarming to see).
    assert deploy_dev_yaml["pr"] == "none"
    assert deploy_test_yaml["pr"] == "none"
    assert deploy_prod_yaml["pr"] == "none"
    for deploy_yaml in (deploy_dev_yaml, deploy_test_yaml, deploy_prod_yaml):
        for job in deploy_yaml["jobs"]:
            # Same succeeded() requirement as the dry-run jobs: an explicit condition
            # without it would let a deploy job start after a canceled run.
            assert job["condition"].startswith("and(succeeded(), ")
    # Azure Pipelines does not auto-map secret variable-group values into script
    # environments (only plain variables get that treatment) — cdf deploy needs
    # IDP_CLIENT_SECRET mapped explicitly or authentication fails. Every deploy
    # pipeline now runs both cdf deploy --dry-run and cdf deploy, so each needs
    # the mapping twice.
    assert deploy_dev_text.count("IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)") == 2
    assert deploy_test_text.count("IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)") == 2
    assert deploy_prod_text.count("IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)") == 2
    # checkout doesn't persist git credentials by default — the prod job's own
    # `git fetch origin main` step needs them to authenticate.
    prod_job = next(job for job in deploy_prod_yaml["jobs"] if job["job"] == "deploy_prod")
    assert prod_job["steps"][0]["checkout"] == "self"
    assert prod_job["steps"][0]["persistCredentials"] is True
    # A shallow/single-branch CI checkout may not have origin/main as a valid
    # remote-tracking ref; FETCH_HEAD is always populated by the preceding fetch.
    assert "git merge-base --is-ancestor HEAD FETCH_HEAD" in deploy_prod_text
    assert "origin/main; then" not in deploy_prod_text
    for text in (deploy_dev_text, deploy_test_text, deploy_prod_text, dry_run_text):
        assert all(len(line) <= 120 for line in text.splitlines())

    docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "toolkit-deploy-dev" in docs
    assert "toolkit-deploy-test" in docs
    assert "toolkit-deploy-prod" in docs
    assert "deploy-dev-pipeline.yml" in docs
    assert "deploy-test-pipeline.yml" in docs
    assert "deploy-prod-pipeline.yml" in docs
    assert "Build Validation" in docs
    # Branch control is no longer part of the story: PR validation never loads
    # a secret, so there's nothing for the (broken) Branch control workaround
    # to protect. Philippe's finding is resolved by removing the exposure, not
    # by patching around it.
    assert "Branch control" not in docs
    assert "Pipeline permissions are sufficient" in docs
    assert "toolkit-config" in docs
    assert "IDP_TOKEN_URL" in docs
    assert "GitHub Release" not in docs
    # ADO's Build Validation policy references a pipeline, not a job display name
    # inside it — the branch-protection table must not carry over GitHub wording.
    assert "Source branch guardrail" not in docs
    assert "cdf build & deploy --dry-run" not in docs
    assert "toolkit-pr-validate` (Build Validation)" in docs
    assert "Minimum number of reviewers" in docs
    assert "Required reviewers" not in docs
    # ADO's reviewer-count policy is either off or >=1 -- there's no "0" setting,
    # unlike GitHub's approval count.
    assert "none (policy not enabled)" in docs
    assert "| dev | 0 |" not in docs
    # Documents the GitHub/Bitbucket-hosted-repo default PR trigger and the
    # expected failure message on a manual smoke-test run.
    assert "Unsupported target branch" in docs
    assert "falls back to its default of validating PRs to any branch" in docs
    # ADO's PR check never runs cdf deploy --dry-run anymore -- that's the point
    # of the #3 fix. GitHub's branching-model wording is unaffected (separate,
    # not-yet-fixed exposure), so only assert on ADO's own row here.
    assert "| PR → `dev` | `acme-dev` | Validate (`cdf build`) |" in docs
    assert "| PR → `main` | `acme-test` | Validate (`cdf build`) |" in docs
    assert "cdf deploy --dry-run" not in docs.split("## Branch policies")[0]


def test_generate_actions_ado_prod_only_has_no_dry_run_pipeline(tmp_path: Path) -> None:
    """A prod-only project (no dev/test) has no dry-run pipeline at all -- the docs
    must not reference toolkit-pr-validate/dry-run-pipeline.yml as if it exists, or
    give a `dev-toolkit-credentials` example when no dev environment is configured."""
    _scaffold_project_with_envs(tmp_path, ("prod",))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    assert not (tmp_path / ".devops" / "dry-run-pipeline.yml").exists()
    assert (tmp_path / ".devops" / "deploy-prod-pipeline.yml").is_file()

    docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "toolkit-pr-validate" not in docs
    assert "dry-run-pipeline.yml" not in docs
    assert "dev-toolkit-credentials" not in docs
    assert "dev-toolkit-config" not in docs
    assert "No dry-run pipeline is generated without a dev or test environment" in docs
    # The prod-only example must name a group/pipeline that's actually configured.
    assert "the `prod-toolkit-config` and `prod-toolkit-credentials` groups should each grant" in docs
    assert "access to `toolkit-deploy-prod` only" in docs
    assert "`prod-toolkit-credentials` only ever needs to be authorized for `toolkit-deploy-prod`" in docs


def test_generate_actions_ado_test_and_prod_examples_do_not_mention_dev(tmp_path: Path) -> None:
    """With test+prod configured (no dev), the doc's illustrative examples must name
    an environment that actually exists -- a hardcoded 'dev-toolkit-credentials'
    example would refer to a variable group this project never creates."""
    _scaffold_project_with_envs(tmp_path, ("test", "prod"))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    docs = (tmp_path / "docs" / "FOUNDATION_CICD.md").read_text(encoding="utf-8")
    assert "dev-toolkit-credentials" not in docs
    assert "dev-toolkit-config" not in docs
    assert "toolkit-deploy-dev" not in docs
    # Only the non-secret config group grants access to toolkit-pr-validate --
    # the credentials group is deploy-only.
    assert "the `test-toolkit-config` group should grant access to `toolkit-pr-validate`" in docs
    assert "`test-toolkit-credentials` should grant access to `toolkit-deploy-test` only" in docs
    assert "`test-toolkit-credentials` only ever needs to be authorized for `toolkit-deploy-test`" in docs
    # Only `main` is a deployable branch here (test only) -- the row must not claim
    # a `dev` PR trigger that was never generated.
    assert "| `toolkit-pr-validate` | `.devops/dry-run-pipeline.yml` | PR to `main`" in docs
    assert "PR to `dev`" not in docs


def test_generate_actions_ado_dev_only_has_branch_condition(tmp_path: Path) -> None:
    """Even with a single deployable branch, the dry-run job must stay gated on
    System.PullRequest.TargetBranch -- otherwise a manual or non-PR pipeline run
    would load the non-secret config group and run `cdf build` unconditionally."""
    _scaffold_project_with_envs(tmp_path, ("dev",))

    subprocess.run(
        [sys.executable, str(GENERATE_ACTIONS), "--force", "--provider", "ado"],
        check=True,
        cwd=tmp_path,
    )

    dry_run_path = tmp_path / ".devops" / "dry-run-pipeline.yml"
    deploy_dev_path = tmp_path / ".devops" / "deploy-dev-pipeline.yml"
    dry_run_yaml = yaml.safe_load(dry_run_path.read_text(encoding="utf-8"))
    deploy_dev_yaml = yaml.safe_load(deploy_dev_path.read_text(encoding="utf-8"))

    dry_run_dev_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "dry_run_dev")
    assert dry_run_dev_job["condition"] == (
        "and(succeeded(), "
        "in(variables['System.PullRequest.TargetBranch'], 'refs/heads/dev', 'dev'))"
    )
    # No test/main branch configured, so the promotion-flow guard job is never
    # generated at all -- the target-branch check lives inside lint regardless.
    assert {job["job"] for job in dry_run_yaml["jobs"]} == {"lint", "dry_run_dev"}
    assert {job["job"] for job in deploy_dev_yaml["jobs"]} == {"deploy_dev"}
    # Only the dev environment was configured — test and prod deploy pipelines
    # must not be written at all.
    assert not (tmp_path / ".devops" / "deploy-test-pipeline.yml").exists()
    assert not (tmp_path / ".devops" / "deploy-prod-pipeline.yml").exists()
    # The promotion-flow guard only applies to the main/test job.
    assert "Enforce promotion flow" not in dry_run_path.read_text(encoding="utf-8")


def test_generate_actions_ado_test_only_promotion_guard_skips_non_pr_runs(tmp_path: Path) -> None:
    """With only config.test.yaml present, dry_run_test is the sole branch (main), and
    the promotion guard now lives in its own source_branch_guard job rather than a
    step inside dry_run_test. Its script must not blow up on an empty HEAD (manual
    or non-PR runs), even though the job condition already keeps those from mattering."""
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
        "and(succeeded(), "
        "in(variables['System.PullRequest.TargetBranch'], 'refs/heads/main', 'main'))"
    )
    assert set(dry_run_test_job["dependsOn"]) == {"lint", "source_branch_guard"}
    source_branch_guard_job = next(job for job in dry_run_yaml["jobs"] if job["job"] == "source_branch_guard")
    assert source_branch_guard_job["condition"] == (
        "and(succeeded(), "
        "in(variables['System.PullRequest.TargetBranch'], 'refs/heads/main', 'main'))"
    )

    dry_run_text = dry_run_path.read_text(encoding="utf-8")
    assert "Enforce promotion flow" in dry_run_text
    assert 'if [ -n "${HEAD}" ]; then' in dry_run_text
    # The guard is its own job now, not a step buried inside the credentialed
    # dry_run_test job -- a PR can no longer delete it by editing that job away.
    assert "source_branch_guard" in {job["job"] for job in dry_run_yaml["jobs"]}


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
