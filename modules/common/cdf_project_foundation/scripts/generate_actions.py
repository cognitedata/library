#!/usr/bin/env python3
"""
Generate CI/CD for a Toolkit project using the Foundation Deployment Pack.

Implements the branching model and workflows from sop-cdf-project-setup.md (Step 5):
  - PR to dev, and PR to main when config.test.yaml exists → dry-run
  - Push to dev → deploy to config.dev.yaml's environment.project
  - Push to main → deploy to config.test.yaml's environment.project when present
  - Release published from main → deploy to config.prod.yaml's environment.project

Supports multiple CI/CD providers via --provider (default: github). Provider-specific
templates live under templates/<provider>/; output is written to a provider-specific
directory (.github/workflows for GitHub, .devops for Azure DevOps).

Run from the Toolkit project root after `cdf modules add -d dp:foundation`:

  python modules/common/cdf_project_foundation/scripts/generate_actions.py
  python modules/common/cdf_project_foundation/scripts/generate_actions.py --provider ado
"""


import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

MODULE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_ROOT = MODULE_DIR / "templates"
ENVIRONMENTS = ("dev", "test", "prod")
DEPLOY_BRANCHES = {"dev": "dev", "test": "main"}
ENV_LABELS = {"dev": "Dev", "test": "Test"}
CONFIG_FLAG_MIN_VERSION = (0, 8, 0)

PROVIDERS = ("github", "ado")
DEFAULT_PROVIDER = "github"
PROVIDER_WORKFLOWS_DIR = {
    "github": Path(".github") / "workflows",
    "ado": Path(".devops"),
}


def templates_dir(provider: str) -> Path:
    """Provider-specific template directory (e.g. templates/github, templates/ado)."""
    return TEMPLATES_ROOT / provider


def workflows_output_dir(provider: str, repo_root: Path) -> Path:
    """Where generated CI/CD files are written for this provider."""
    if provider not in PROVIDER_WORKFLOWS_DIR:
        raise ValueError(f"Unsupported provider: {provider}")
    return repo_root / PROVIDER_WORKFLOWS_DIR[provider]


def resolve_modules_root(repo_root: Path, org_dir: str | None) -> Path:
    """Toolkit modules/ at repo root, or nested under the organization directory."""
    candidates = [repo_root / "modules"]
    if org_dir:
        candidates.append(repo_root / org_dir / "modules")
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    searched = ", ".join(str(c) for c in candidates)
    raise FileNotFoundError(f"No modules directory found under: {searched}")


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "cdf.toml").is_file():
            return candidate
    raise FileNotFoundError("cdf.toml not found — run from a Cognite Toolkit project root")


def load_cdf_toml(repo_root: Path) -> dict[str, Any]:
    return tomllib.loads((repo_root / "cdf.toml").read_text(encoding="utf-8"))


def render_template(path: Path, values: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(f"{{{{{key}}}}}", value)
    remaining = re.findall(r"\{\{[A-Z0-9_]+\}\}", text)
    if remaining:
        raise ValueError(f"Unfilled placeholders in {path.name}: {remaining}")
    return text


def write_file(path: Path, content: str, force: bool) -> None:
    if path.exists() and not force:
        try:
            answer = input(f"{path} already exists. Overwrite? [y/N] ").strip().lower()
        except EOFError:
            answer = "n"
        if answer not in ("y", "yes"):
            print(f"Skipped {path}")
            return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"Wrote {path}")


def remove_file(path: Path) -> None:
    if path.exists():
        path.unlink()
        print(f"Removed {path}")


def build_lint_paths(org_dir: str | None, provider: str) -> str:
    """Return git pathspecs for generated workflow linting.

    Keep this scoped to committed project-level files. Deployment modules often
    ship README files, notebooks, and generated Python that are not intended to
    satisfy the destination repository's pre-commit hooks.
    """
    ci_scripts_dir = ".github/scripts/" if provider == "github" else ".devops/scripts/"
    entries: list[str] = [
        "'cdf.toml'",
        "'.pre-commit-config.yaml'",
        f"'{ci_scripts_dir}'",
    ]
    if org_dir:
        entries.insert(0, f"'{org_dir}/config*.yaml'")
    else:
        entries.insert(0, "'config*.yaml'")
    return " \\\n            ".join(entries)


def parse_version(version: str) -> tuple[int, int, int]:
    parts = re.match(r"^(\d+)\.(\d+)\.(\d+)", version)
    if not parts:
        return (0, 0, 0)
    return tuple(int(part) for part in parts.groups())


def config_arg(org_dir: str | None, env: str) -> str:
    config = f"config.{env}.yaml"
    if org_dir:
        config = f"{org_dir}/{config}"
    return f"-c {config}"


def setup_project_check_cmd(repo_root: Path, org_dir: str | None) -> str:
    """Command to verify config.<env>.yaml is in sync with the installed variant
    and pack before ``cdf build`` runs — catches drift (e.g. a new variable added
    to a module since the config was last generated) with an actionable message
    instead of a raw Toolkit "undefined variable" build failure.
    """
    modules_root = resolve_modules_root(repo_root, org_dir)
    script_path = modules_root / "common" / "cdf_project_foundation" / "scripts" / "setup_project.py"
    return f"python {script_path.relative_to(repo_root).as_posix()} --check"


def build_args(toolkit_version: str, org_dir: str | None, env: str) -> str:
    if parse_version(toolkit_version) >= CONFIG_FLAG_MIN_VERSION:
        return config_arg(org_dir, env)
    return f"--env {env}"


def config_path(repo_root: Path, org_dir: str | None, env: str) -> Path:
    base = repo_root / org_dir if org_dir else repo_root
    return base / f"config.{env}.yaml"


def load_environment_projects(repo_root: Path, org_dir: str | None) -> dict[str, str]:
    """Read CDF project names from config.<env>.yaml files."""
    projects: dict[str, str] = {}
    for env in ENVIRONMENTS:
        path = config_path(repo_root, org_dir, env)
        if not path.is_file():
            continue

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        environment = data.get("environment") or {}
        name = environment.get("name")
        project = environment.get("project")
        if name != env:
            raise ValueError(
                f"{path.relative_to(repo_root)} has environment.name={name!r}; expected {env!r}."
            )
        if not project:
            raise ValueError(f"{path.relative_to(repo_root)} is missing environment.project.")
        projects[env] = str(project)
    if not projects:
        raise FileNotFoundError(
            "No config.<env>.yaml files found. Run setup_project.py before generating workflows."
        )
    return projects


def deployable_envs(projects: dict[str, str]) -> list[str]:
    return [env for env in ("dev", "test") if env in projects]


def branch_envs(projects: dict[str, str]) -> dict[str, str]:
    envs_by_branch: dict[str, str] = {}
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        if branch in envs_by_branch:
            raise ValueError(
                f"Both {envs_by_branch[branch]!r} and {env!r} map to branch {branch!r}."
            )
        envs_by_branch[branch] = env
    return envs_by_branch


def pr_branches(projects: dict[str, str]) -> str:
    branches = branch_envs(projects)
    return "\n".join(f"      - {branch}" for branch in branches)


def github_dry_run_environment(projects: dict[str, str]) -> str:
    """GitHub Actions ``environment:`` expression for the dry-run job.

    GitHub Actions-specific: relies on the ``github.base_ref`` expression context,
    which has no equivalent on other providers. See ``ado_dry_run_jobs`` for the
    Azure DevOps approach (one conditioned job per target branch).
    """
    branches = branch_envs(projects)
    if not branches:
        return ""
    if len(branches) == 1:
        env = next(iter(branches.values()))
        return f"{env}-toolkit-credentials"
    if len(branches) > 2:
        raise ValueError(f"Unsupported number of deployable branches: {len(branches)}")
    first_branch, first_env = next(iter(branches.items()))
    fallback_env = next(env for branch, env in branches.items() if branch != first_branch)
    return (
        f"${{{{ github.base_ref == '{first_branch}' && "
        f"'{first_env}-toolkit-credentials' || '{fallback_env}-toolkit-credentials' }}}}"
    )


def github_dry_run_build_script(toolkit_version: str, org_dir: str | None, projects: dict[str, str]) -> str:
    """Bash build script for the dry-run job, keyed on ``$GITHUB_BASE_REF``.

    GitHub Actions-specific: ``GITHUB_BASE_REF`` is a GitHub Actions runner
    environment variable. See ``ado_dry_run_jobs`` for the Azure DevOps approach.
    """
    branches = branch_envs(projects)
    if not branches:
        return ""
    if len(branches) == 1:
        env = next(iter(branches.values()))
        return f"cdf build {build_args(toolkit_version, org_dir, env)} | tee build-output.txt"
    cases: list[str] = ['case "$GITHUB_BASE_REF" in']
    for branch, env in branches.items():
        cases.extend(
            [
                f"  {branch})",
                f"    cdf build {build_args(toolkit_version, org_dir, env)} | tee build-output.txt",
                "    ;;",
            ]
        )
    cases.extend(
        [
            "  *)",
            '    echo "::error::Unsupported base branch $GITHUB_BASE_REF"',
            "    exit 1",
            "    ;;",
            "esac",
        ]
    )
    return "\n".join(cases)


def _ado_promotion_guard_lines() -> list[str]:
    return [
        "      - script: |",
        "          set -euo pipefail",
        '          HEAD="$(System.PullRequest.SourceBranch)"',
        '          HEAD="${HEAD#refs/heads/}"',
        '          if [ "${HEAD}" != "dev" ] && [[ "${HEAD}" != hotfix/* ]]; then',
        "            echo \"##vso[task.logissue type=error]PRs to 'main' must come from 'dev' or 'hotfix/*' only (head: ${HEAD})\"",
        "            exit 1",
        "          fi",
        '          echo "Promotion flow OK: ${HEAD} -> main"',
        "        displayName: 'Enforce promotion flow (main <- dev or hotfix/* only)'",
        "",
    ]


def ado_dry_run_jobs(toolkit_version: str, org_dir: str | None, setup_check_cmd: str, projects: dict[str, str]) -> str:
    """Azure DevOps jobs for the dry-run pipeline: one job per deployable branch,
    each scoped to its own ``<env>-toolkit-credentials`` variable group and gated
    on ``System.PullRequest.TargetBranch`` when more than one branch is deployable.
    Avoids ever loading two environments' variable groups into the same job.
    """
    branches = branch_envs(projects)
    if not branches:
        return ""
    if len(branches) > 2:
        raise ValueError(f"Unsupported number of deployable branches: {len(branches)}")
    gate_on_branch = len(branches) > 1

    jobs: list[str] = []
    for branch, env in branches.items():
        lines = [
            f"  - job: dry_run_{env}",
            f"    displayName: 'cdf build & deploy --dry-run ({env})'",
            "    dependsOn: lint",
        ]
        if gate_on_branch:
            lines.append(f"    condition: eq(variables['System.PullRequest.TargetBranch'], 'refs/heads/{branch}')")
        lines.extend(
            [
                "    variables:",
                f"      - group: {env}-toolkit-credentials",
                "    steps:",
                "      - checkout: self",
                "",
            ]
        )
        if branch == "main":
            lines.extend(_ado_promotion_guard_lines())
        lines.extend(
            [
                "      - script: rm -f .env",
                "        displayName: 'Remove local .env'",
                "",
                "      - task: UsePythonVersion@0",
                "        inputs:",
                "          versionSpec: '3.13'",
                "",
                f'      - script: pip install "cognite-toolkit=={toolkit_version}"',
                "        displayName: 'Install Cognite Toolkit'",
                "",
                f"      - script: {setup_check_cmd}",
                "        displayName: 'Verify project config is in sync'",
                "",
                f"      - script: cdf build {build_args(toolkit_version, org_dir, env)}",
                "        displayName: 'cdf build'",
                "",
                "      - script: cdf deploy --dry-run",
                "        displayName: 'cdf deploy --dry-run'",
            ]
        )
        jobs.append("\n".join(lines))
    return "\n\n".join(jobs)


ADO_DEPLOY_PIPELINE_NAMES = {"dev": "toolkit-deploy-dev", "test": "toolkit-deploy-test", "prod": "toolkit-deploy-prod"}
ADO_DEPLOY_PIPELINE_TRIGGERS = {
    "dev": "Push to `dev`",
    "test": "Push to `main`",
    "prod": "Git tag `vX.Y.Z` from `main`",
}


def ado_deploy_pipeline_rows(projects: dict[str, str]) -> str:
    rows = [
        f"| `{ADO_DEPLOY_PIPELINE_NAMES[env]}` | `.devops/deploy-pipeline.yml` | {ADO_DEPLOY_PIPELINE_TRIGGERS[env]} |"
        for env in ENVIRONMENTS
        if env in projects
    ]
    return "\n".join(rows)


def _ado_deploy_condition(env: str) -> str:
    if env == "dev":
        return "eq(variables['Build.SourceBranch'], 'refs/heads/dev')"
    if env == "test":
        return "eq(variables['Build.SourceBranch'], 'refs/heads/main')"
    return "startsWith(variables['Build.SourceBranch'], 'refs/tags/v')"


def ado_deploy_jobs(toolkit_version: str, org_dir: str | None, setup_check_cmd: str, projects: dict[str, str]) -> str:
    """Azure DevOps jobs for the single, reusable deploy-pipeline.yml: one job per
    environment present in config.<env>.yaml, each gated on the branch or tag that
    should trigger it and scoped to its own ``<env>-toolkit-credentials`` group.
    Register the generated file three times in Azure DevOps (toolkit-deploy-dev,
    toolkit-deploy-test, toolkit-deploy-prod) so history and permissions stay
    per-environment; the job conditions make each registration a no-op outside its
    own trigger.
    """
    envs = [env for env in ENVIRONMENTS if env in projects]
    jobs: list[str] = []
    for env in envs:
        lines = [
            f"  - job: deploy_{env}",
            f"    displayName: 'Deploy to {projects[env]}'",
            f"    condition: {_ado_deploy_condition(env)}",
            "    variables:",
            f"      - group: {env}-toolkit-credentials",
            "    steps:",
        ]
        if env == "prod":
            lines.extend(
                [
                    "      - checkout: self",
                    "        fetchDepth: 0",
                    "",
                    "      - script: |",
                    "          set -euo pipefail",
                    '          TAG="$(Build.SourceBranchName)"',
                    '          if [[ ! "$TAG" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+$ ]]; then',
                    '            echo "##vso[task.logissue type=error]Release tag must match vX.Y.Z (got: $TAG)"',
                    "            exit 1",
                    "          fi",
                    "        displayName: 'Enforce release tag pattern'",
                    "",
                    "      - script: |",
                    "          set -euo pipefail",
                    "          git fetch origin main",
                    "          if ! git merge-base --is-ancestor HEAD origin/main; then",
                    '            echo "##vso[task.logissue type=error]Production releases must be published from a commit reachable from main"',
                    "            exit 1",
                    "          fi",
                    "        displayName: 'Reject releases not reachable from main'",
                    "",
                ]
            )
        else:
            lines.extend(["      - checkout: self", ""])
        lines.extend(
            [
                "      - script: rm -f .env",
                "        displayName: 'Remove local .env'",
                "",
                "      - task: UsePythonVersion@0",
                "        inputs:",
                "          versionSpec: '3.13'",
                "",
                f'      - script: pip install "cognite-toolkit=={toolkit_version}"',
                "        displayName: 'Install Cognite Toolkit'",
                "",
                f"      - script: {setup_check_cmd}",
                "        displayName: 'Verify project config is in sync'",
                "",
                f"      - script: cdf build {build_args(toolkit_version, org_dir, env)}",
                "        displayName: 'cdf build'",
            ]
        )
        if env == "prod":
            lines.extend(
                [
                    "",
                    "      - script: cdf deploy --dry-run",
                    "        displayName: 'cdf deploy --dry-run'",
                ]
            )
        lines.extend(
            [
                "",
                "      - script: cdf deploy",
                "        displayName: 'cdf deploy'",
            ]
        )
        jobs.append("\n".join(lines))
    return "\n\n".join(jobs)


def branching_rows(projects: dict[str, str], provider: str) -> str:
    rows: list[str] = []
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        rows.extend(
            [
                f"| PR → `{branch}` | `{projects[env]}` | Dry-run (`cdf build`, `cdf deploy --dry-run`) |",
                f"| Push to `{branch}` | `{projects[env]}` | Deploy |",
            ]
        )
    if "prod" in projects:
        prod_trigger = "GitHub Release (tag `vX.Y.Z` from `main`)" if provider == "github" else "Git tag `vX.Y.Z` pushed to `main`"
        rows.append(f"| {prod_trigger} | `{projects['prod']}` | Deploy |")
    return "\n".join(rows or ["| *(none)* | *(none)* | No CI/CD workflows generated |"])


def branch_protection_rows(projects: dict[str, str]) -> str:
    rows: list[str] = []
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        if branch == "main":
            rows.append("| `main` | 1 | `Source branch guardrail`, `cdf build & deploy --dry-run` |")
        else:
            rows.append(f"| `{branch}` | none | `cdf build & deploy --dry-run` |")
    return "\n".join(rows or ["| *(none)* | *(none)* | No PR workflow generated |"])


def branch_protection_note(projects: dict[str, str]) -> str:
    branches = {DEPLOY_BRANCHES[env] for env in deployable_envs(projects)}
    if not branches:
        return "No PR workflow is generated without a dev or test environment configured."
    if branches == {"dev"}:
        return "PRs to `dev` only run dry-run CI (0 reviewers)."
    if branches == {"main"}:
        return (
            "PRs to `main` require a reviewer and the `Source branch guardrail` check, which"
            " enforces that changes are promoted from `dev` or `hotfix/*`, in addition to dry-run CI."
        )
    return (
        "PRs to `dev` only run dry-run CI (0 reviewers). The `Source branch guardrail` check does"
        " not run on `dev` — it only applies to PRs targeting `main`, where it enforces that changes"
        " are promoted from `dev` or `hotfix/*`."
    )


def indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else line for line in text.splitlines())


def environment_rows(projects: dict[str, str], provider: str) -> str:
    rows: list[str] = []
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        rows.append(f"| `{env}-toolkit-credentials` | PR → {branch}, push `{branch}` | `{projects[env]}` |")
    if "prod" in projects:
        prod_trigger = "Release published" if provider == "github" else "Tag pushed"
        rows.append(f"| `prod-toolkit-credentials` | {prod_trigger} | `{projects['prod']}` |")
    return "\n".join(rows)


def env_config_list(projects: dict[str, str]) -> str:
    configs = [f"`config.{env}.yaml`" for env in ENVIRONMENTS if env in projects]
    if len(configs) == 1:
        return configs[0]
    return ", ".join(configs[:-1]) + f", or {configs[-1]}"


def example_build_args(toolkit_version: str, org_dir: str | None, projects: dict[str, str]) -> str:
    env = next(env for env in ENVIRONMENTS if env in projects)
    return build_args(toolkit_version, org_dir, env)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--org-dir",
        help="Organization directory (default: cdf.toml default_organization_dir)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite generated files without prompting",
    )
    parser.add_argument(
        "--provider",
        choices=PROVIDERS,
        default=DEFAULT_PROVIDER,
        help=f"CI/CD provider to generate for (default: {DEFAULT_PROVIDER})",
    )
    args = parser.parse_args()

    templates = templates_dir(args.provider)
    repo_root = find_repo_root(Path.cwd())
    cdf = load_cdf_toml(repo_root)
    org_dir: str | None = args.org_dir or cdf.get("cdf", {}).get("default_organization_dir") or None
    projects = load_environment_projects(repo_root, org_dir)
    workflows_dir = workflows_output_dir(args.provider, repo_root)

    toolkit_version = cdf.get("modules", {}).get("version", "0.7.220")
    resolve_modules_root(repo_root, org_dir)

    setup_check_cmd = setup_project_check_cmd(repo_root, org_dir)
    base_values: dict[str, str] = {
        "PR_BRANCHES": pr_branches(projects),
        "BRANCHING_ROWS": branching_rows(projects, args.provider),
        "ENVIRONMENT_ROWS": environment_rows(projects, args.provider),
        "EXAMPLE_BUILD_ARGS": example_build_args(str(toolkit_version), org_dir, projects),
        "ENV_CONFIG_LIST": env_config_list(projects),
        "BRANCH_PROTECTION_ROWS": branch_protection_rows(projects),
        "BRANCH_PROTECTION_NOTE": branch_protection_note(projects),
        "TOOLKIT_VERSION": str(toolkit_version),
        "LINT_PATHS": build_lint_paths(org_dir, args.provider),
        "SETUP_PROJECT_CHECK_CMD": setup_check_cmd,
    }

    if args.provider == "github":
        base_values["DRY_RUN_ENVIRONMENT"] = github_dry_run_environment(projects)
        base_values["DRY_RUN_BUILD_SCRIPT"] = indent(
            github_dry_run_build_script(str(toolkit_version), org_dir, projects), 10
        )

        if deployable_envs(projects):
            template = templates / "dry-run.yml"
            if not template.is_file():
                print(f"Missing template: {template}", file=sys.stderr)
                sys.exit(1)
            write_file(
                workflows_dir / "dry-run.yml",
                render_template(template, base_values),
                args.force,
            )
        else:
            remove_file(workflows_dir / "dry-run.yml")

        if "prod" in projects:
            deploy_prod_template = templates / "deploy-prod.yml"
            if not deploy_prod_template.is_file():
                print(f"Missing template: {deploy_prod_template}", file=sys.stderr)
                sys.exit(1)
            prod_values = {
                **base_values,
                "PROD_PROJECT": projects["prod"],
                "PROD_BUILD_ARGS": build_args(str(toolkit_version), org_dir, "prod"),
            }
            write_file(
                workflows_dir / "deploy-prod.yml",
                render_template(deploy_prod_template, prod_values),
                args.force,
            )
        else:
            remove_file(workflows_dir / "deploy-prod.yml")

        deploy_template = templates / "deploy.yml"
        if not deploy_template.is_file():
            print(f"Missing template: {deploy_template}", file=sys.stderr)
            sys.exit(1)
        for env in ("dev", "test"):
            if env not in projects:
                remove_file(workflows_dir / f"deploy-{env}.yml")
                continue
            merged = {
                **base_values,
                "ENV": env,
                "BRANCH": DEPLOY_BRANCHES[env],
                "ENV_LABEL": ENV_LABELS[env],
                "PROJECT": projects[env],
                "BUILD_ARGS": build_args(str(toolkit_version), org_dir, env),
            }
            out = workflows_dir / f"deploy-{env}.yml"
            write_file(out, render_template(deploy_template, merged), args.force)

    elif args.provider == "ado":
        base_values["DRY_RUN_JOBS"] = ado_dry_run_jobs(str(toolkit_version), org_dir, setup_check_cmd, projects)
        base_values["DEPLOY_JOBS"] = ado_deploy_jobs(str(toolkit_version), org_dir, setup_check_cmd, projects)
        base_values["DEPLOY_PIPELINE_ROWS"] = ado_deploy_pipeline_rows(projects)

        if deployable_envs(projects):
            dry_run_template = templates / "dry-run-pipeline.yml"
            if not dry_run_template.is_file():
                print(f"Missing template: {dry_run_template}", file=sys.stderr)
                sys.exit(1)
            write_file(
                workflows_dir / "dry-run-pipeline.yml",
                render_template(dry_run_template, base_values),
                args.force,
            )
        else:
            remove_file(workflows_dir / "dry-run-pipeline.yml")

        deploy_pipeline_template = templates / "deploy-pipeline.yml"
        if not deploy_pipeline_template.is_file():
            print(f"Missing template: {deploy_pipeline_template}", file=sys.stderr)
            sys.exit(1)
        write_file(
            workflows_dir / "deploy-pipeline.yml",
            render_template(deploy_pipeline_template, base_values),
            args.force,
        )

    readme_template = templates / "FOUNDATION_CICD.md"
    if not readme_template.is_file():
        print(f"Missing template: {readme_template}", file=sys.stderr)
        sys.exit(1)
    cicd_readme = repo_root / "docs" / "FOUNDATION_CICD.md"
    write_file(
        cicd_readme,
        render_template(readme_template, base_values),
        args.force,
    )

    print()
    environments = [f"{env}-toolkit-credentials" for env in ENVIRONMENTS if env in projects]
    print("Next steps:")
    if args.provider == "github":
        print(f"  1. Create GitHub Environments: {', '.join(environments)}")
        print("     (see docs/FOUNDATION_CICD.md)")
        print("  2. Create and protect the branches used by the generated workflows")
        if deployable_envs(projects):
            branches = ", ".join(branch_envs(projects))
            print(f"  3. Open a PR to {branches} to validate dry-run.yml")
    else:
        pipeline_names = [ADO_DEPLOY_PIPELINE_NAMES[env] for env in ENVIRONMENTS if env in projects]
        print(f"  1. Create Azure DevOps variable groups: {', '.join(environments)}")
        print("     (see docs/FOUNDATION_CICD.md)")
        print(f"  2. Register deploy-pipeline.yml as: {', '.join(pipeline_names)}")
        if deployable_envs(projects):
            branches = ", ".join(branch_envs(projects))
            print(f"  3. Add dry-run-pipeline.yml as a Build Validation policy on {branches}")


if __name__ == "__main__":
    main()
