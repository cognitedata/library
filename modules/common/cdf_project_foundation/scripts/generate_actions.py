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


def _ado_target_branch_condition(branch: str) -> str:
    """System.PullRequest.TargetBranch is documented as refs/heads/<branch>, but
    some ADO/GitHub-repo integrations surface it without the refs/heads/ prefix.
    Tolerating both forms here (rather than only detecting the mismatch after the
    fact) keeps a format quirk from silently skipping every dry_run_<env> job.
    """
    return f"in(variables['System.PullRequest.TargetBranch'], 'refs/heads/{branch}', '{branch}')"


def ado_target_branch_guard_step(branches: dict[str, str]) -> str:
    """First step of the (always-unconditioned) lint job: fails the whole run loudly
    if System.PullRequest.TargetBranch doesn't match any deployable branch. Without
    this, a value the per-branch job conditions don't recognize makes every
    dry_run_<env> job skip while lint still succeeds, so the required check reports
    success having validated nothing. Lives inside lint (rather than its own job) so
    it doesn't cost a second implicit full-repo checkout for a plain string compare.
    """
    case_lines: list[str] = []
    for branch in branches:
        case_lines.append(f"            refs/heads/{branch}|{branch})")
        case_lines.append(f'              echo "Target branch OK: {branch}"')
        case_lines.append("              ;;")
    lines = [
        "      - bash: |",
        "          set -euo pipefail",
        '          TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-}"',
        '          case "${TARGET}" in',
        *case_lines,
        "            *)",
        '              MSG="Unsupported target branch: ${TARGET:-<empty>}"',
        '              echo "##vso[task.logissue type=error]${MSG}"',
        "              exit 1",
        "              ;;",
        "          esac",
        "        displayName: 'Enforce known target branch'",
    ]
    return "\n".join(lines)


def _ado_source_branch_guard_job() -> str:
    """Own job (mirrors GitHub's separate source-branch-guard job) instead of a step
    nested inside a credentialed dry_run_<env> job. That way the check runs before
    any variable group is loaded, and a PR can't delete the guard along with the
    rest of a job it also controls. dependsOn: lint makes the succeeded() below mean
    something (skip this check if lint already failed) instead of trivially passing.
    checkout: none — the script only inspects a variable, so no repo content is needed.
    """
    return "\n".join(
        [
            "  - job: source_branch_guard",
            "    displayName: 'Source branch guardrail'",
            "    dependsOn: lint",
            f"    condition: and(succeeded(), {_ado_target_branch_condition('main')})",
            "    steps:",
            "      - checkout: none",
            "",
            "      - bash: |",
            "          set -euo pipefail",
            '          HEAD="${SYSTEM_PULLREQUEST_SOURCEBRANCH:-}"',
            '          if [ -n "${HEAD}" ]; then',
            '            HEAD="${HEAD#refs/heads/}"',
            '            if [ "${HEAD}" != "dev" ] && [[ "${HEAD}" != hotfix/* ]]; then',
            "              MSG=\"PRs to 'main' must come from 'dev' or 'hotfix/*' only (head: ${HEAD})\"",
            '              echo "##vso[task.logissue type=error]${MSG}"',
            "              exit 1",
            "            fi",
            '            echo "Promotion flow OK: ${HEAD} -> main"',
            "          fi",
            "        displayName: 'Enforce promotion flow (main <- dev or hotfix/* only)'",
        ]
    )


def ado_dry_run_jobs(toolkit_version: str, org_dir: str | None, setup_check_cmd: str, projects: dict[str, str]) -> str:
    """Azure DevOps jobs for the dry-run pipeline: one job per deployable branch,
    each scoped to its own ``<env>-toolkit-credentials`` variable group and gated
    on ``System.PullRequest.TargetBranch``. Avoids ever loading two environments'
    variable groups into the same job.
    """
    branches = branch_envs(projects)
    if not branches:
        return ""
    if len(branches) > 2:
        raise ValueError(f"Unsupported number of deployable branches: {len(branches)}")

    jobs: list[str] = []
    if "main" in branches:
        jobs.append(_ado_source_branch_guard_job())

    for branch, env in branches.items():
        depends_on = ["lint"]
        if branch == "main":
            depends_on.append("source_branch_guard")
        lines = [
            f"  - job: dry_run_{env}",
            f"    displayName: 'cdf build & deploy --dry-run ({env})'",
            "    dependsOn:",
            *[f"      - {dep}" for dep in depends_on],
            f"    condition: and(succeeded(), {_ado_target_branch_condition(branch)})",
        ]
        lines.extend(
            [
                "    variables:",
                f"      - group: {env}-toolkit-credentials",
                "    steps:",
                "      - checkout: self",
                "",
                "      - bash: rm -f .env",
                "        displayName: 'Remove local .env'",
                "",
                "      - task: UsePythonVersion@0",
                "        inputs:",
                "          versionSpec: '3.13'",
                "",
                f'      - bash: pip install "cognite-toolkit=={toolkit_version}"',
                "        displayName: 'Install Cognite Toolkit'",
                "",
                f"      - bash: {setup_check_cmd}",
                "        displayName: 'Verify project config is in sync'",
                "",
                f"      - bash: cdf build {build_args(toolkit_version, org_dir, env)}",
                "        displayName: 'cdf build'",
                "",
                "      - bash: cdf deploy --dry-run",
                "        displayName: 'cdf deploy --dry-run'",
                "        env:",
                "          IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)",
            ]
        )
        jobs.append("\n".join(lines))
    return "\n\n".join(jobs)


ADO_DEPLOY_PIPELINE_NAMES = {"dev": "toolkit-deploy-dev", "test": "toolkit-deploy-test", "prod": "toolkit-deploy-prod"}
ADO_DEPLOY_PIPELINE_FILES = {
    "dev": "deploy-dev-pipeline.yml",
    "test": "deploy-test-pipeline.yml",
    "prod": "deploy-prod-pipeline.yml",
}
ADO_DEPLOY_PIPELINE_TRIGGERS = {
    "dev": "Push to `dev`",
    "test": "Push to `main`",
    "prod": "Git tag `vX.Y.Z` from `main`",
}


def ado_pipeline_rows(projects: dict[str, str]) -> str:
    """Full 'Pipelines to register' table: toolkit-pr-validate only when a
    dry-run pipeline is actually generated (a dev-only, test-only, or prod-only
    project may have none), plus one row per configured deploy environment.
    """
    rows: list[str] = []
    branches = branch_envs(projects)
    if branches:
        branches_text = " or ".join(f"`{branch}`" for branch in branches)
        rows.append(
            f"| `toolkit-pr-validate` | `.devops/dry-run-pipeline.yml` | PR to {branches_text}"
            " (via Build Validation policy above) |"
        )
    rows.extend(
        f"| `{ADO_DEPLOY_PIPELINE_NAMES[env]}` | `.devops/{ADO_DEPLOY_PIPELINE_FILES[env]}` | "
        f"{ADO_DEPLOY_PIPELINE_TRIGGERS[env]} |"
        for env in ENVIRONMENTS
        if env in projects
    )
    return "\n".join(rows)


def ado_dry_run_registration_notes(projects: dict[str, str]) -> str:
    """Registration guidance for toolkit-pr-validate. Only relevant when a
    dry-run pipeline is actually generated -- referencing a file and pipeline
    that don't exist (e.g. on a prod-only, or dev+prod-without-test project)
    would be actively misleading rather than just unhelpful.
    """
    if not branch_envs(projects):
        return (
            "No dry-run pipeline is generated without a dev or test environment"
            " configured, so there is nothing to register as a Build Validation policy."
        )
    register_note = (
        "Register `dry-run-pipeline.yml` (the `toolkit-pr-validate` pipeline, see below)"
        " as a **Build Validation** policy on every branch listed above, so it runs"
        " automatically as a required check on each PR."
    )
    hosted_repo_note = (
        "If this repository is hosted on GitHub or Bitbucket Cloud with Azure Pipelines"
        " as the CI (rather than Azure Repos Git), `dry-run-pipeline.yml` also runs"
        " automatically on every pull request even before you register the Build"
        " Validation policy above — it has no `pr:` block, so Azure Pipelines falls back"
        " to its default of validating PRs to any branch. Register the Build Validation"
        " policy anyway, since that's what makes it a *required* check rather than an"
        " informational one."
    )
    manual_run_note = (
        "A manual or non-PR run of `toolkit-pr-validate` is expected to fail with"
        " `Unsupported target branch: <empty>` — `System.PullRequest.TargetBranch` is"
        " only populated for an actual PR-triggered run. That's not a broken pipeline;"
        " it's the guard working as intended."
    )
    return "\n\n".join([register_note, hosted_repo_note, manual_run_note])


def ado_branch_control_warning(projects: dict[str, str]) -> str:
    """Only relevant when toolkit-pr-validate actually exists -- a prod-only (or
    otherwise dry-run-less) project has no PR-validation pipeline for a PR author
    to exfiltrate secrets through, so the warning would refer to nothing.

    NOTE: the warning's content (Branch control) is not itself a fix -- Philippe's
    review established it doesn't provide the protection this text claims. Leave
    the wording as-is pending that decision; this only controls whether it's shown.
    """
    if not branch_envs(projects):
        return ""
    return (
        "**Pipeline permissions alone are not sufficient for `toolkit-pr-validate`.** It runs as a"
        " Build Validation policy, which executes the pipeline YAML as modified by the pull request"
        " itself — a PR author who edits `.devops/dry-run-pipeline.yml` could otherwise use that"
        " access to exfiltrate the loaded secrets, including `IDP_CLIENT_SECRET`. On every variable"
        " group, also add an **Approvals and checks → Branch control** check (and/or a required"
        " approval) under **Pipelines → Library**, so secrets are only released to runs building from"
        " a trusted target branch and pipeline definition, not to arbitrary PR-modified YAML. Do this"
        " before using any of these variable groups against a real customer project."
    )


def ado_dry_run_trigger_note(projects: dict[str, str]) -> str:
    """Only relevant when dry-run-pipeline.yml is actually generated."""
    if not branch_envs(projects):
        return ""
    return (
        " `dry-run-pipeline.yml` still sets `trigger: none`, since a Build Validation"
        " policy invokes it directly rather than a push trigger."
    )


def ado_deploy_authorization_example(projects: dict[str, str]) -> str:
    """Concrete example for the 'a shared file would force over-authorizing'
    sentence. Must name a group/pipeline pair that's actually configured --
    hardcoding `dev-toolkit-credentials`/`toolkit-deploy-dev` is simply wrong
    on a project with no dev environment.
    """
    env = next(iter(projects))
    return f"`{env}-toolkit-credentials` only ever needs to be authorized for `{ADO_DEPLOY_PIPELINE_NAMES[env]}`"


def ado_variable_group_scoping_example(projects: dict[str, str]) -> str:
    """Concrete example for the 'scope each group narrowly' sentence. Must name a
    group that's actually configured for this project -- a hardcoded 'dev-toolkit-
    credentials' example is simply wrong on a project with no dev environment
    (prod-only, or test+prod without dev).
    """
    dry_run_envs = deployable_envs(projects)
    if dry_run_envs:
        env = dry_run_envs[0]
        return (
            f"the `{env}-toolkit-credentials` group should grant access to "
            f"`toolkit-pr-validate` and `{ADO_DEPLOY_PIPELINE_NAMES[env]}` only"
        )
    env = next(iter(projects))
    return f"the `{env}-toolkit-credentials` group should grant access to `{ADO_DEPLOY_PIPELINE_NAMES[env]}` only"


def _ado_deploy_condition(env: str) -> str:
    if env == "dev":
        return "eq(variables['Build.SourceBranch'], 'refs/heads/dev')"
    if env == "test":
        return "eq(variables['Build.SourceBranch'], 'refs/heads/main')"
    return "startsWith(variables['Build.SourceBranch'], 'refs/tags/v')"


def ado_deploy_job(env: str, toolkit_version: str, org_dir: str | None, setup_check_cmd: str, project: str) -> str:
    """Azure DevOps job for one environment's own deploy-<env>-pipeline.yml, scoped
    only to that environment's ``<env>-toolkit-credentials`` variable group. Each
    environment gets its own file (and its own pipeline registration) so Azure's
    resource authorization — which covers every variable group referenced anywhere
    in a pipeline's YAML, not just the group a runtime condition ends up using —
    never requires authorizing a group outside the pipeline that actually needs it.

    The branch/tag `condition:` looks redundant with the file's own `trigger:` block
    (only the right branch/tag ever triggers a run), but don't drop it: it's what
    turns an accidental manual "Save and run" during pipeline registration into a
    harmlessly skipped job instead of a real deploy.
    """
    lines = [
        f"  - job: deploy_{env}",
        f"    displayName: 'Deploy to {project}'",
        f"    condition: and(succeeded(), {_ado_deploy_condition(env)})",
        "    variables:",
        f"      - group: {env}-toolkit-credentials",
        "    steps:",
    ]
    if env == "prod":
        lines.extend(
            [
                "      - checkout: self",
                "        fetchDepth: 0",
                "        persistCredentials: true",
                "",
                "      - bash: |",
                "          set -euo pipefail",
                '          SRC="${BUILD_SOURCEBRANCH:-}"',
                '          TAG="${SRC#refs/tags/}"',
                '          if [[ ! "$TAG" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+$ ]]; then',
                '            MSG="Release tag must match vX.Y.Z (got: $TAG)"',
                '            echo "##vso[task.logissue type=error]${MSG}"',
                "            exit 1",
                "          fi",
                "        displayName: 'Enforce release tag pattern'",
                "",
                "      - bash: |",
                "          set -euo pipefail",
                "          git fetch origin main",
                "          if ! git merge-base --is-ancestor HEAD FETCH_HEAD; then",
                '            MSG="Production releases must be published from a commit reachable from main"',
                '            echo "##vso[task.logissue type=error]${MSG}"',
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
            "      - bash: rm -f .env",
            "        displayName: 'Remove local .env'",
            "",
            "      - task: UsePythonVersion@0",
            "        inputs:",
            "          versionSpec: '3.13'",
            "",
            f'      - bash: pip install "cognite-toolkit=={toolkit_version}"',
            "        displayName: 'Install Cognite Toolkit'",
            "",
            f"      - bash: {setup_check_cmd}",
            "        displayName: 'Verify project config is in sync'",
            "",
            f"      - bash: cdf build {build_args(toolkit_version, org_dir, env)}",
            "        displayName: 'cdf build'",
        ]
    )
    if env == "prod":
        lines.extend(
            [
                "",
                "      - bash: cdf deploy --dry-run",
                "        displayName: 'cdf deploy --dry-run'",
                "        env:",
                "          IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)",
            ]
        )
    lines.extend(
        [
            "",
            "      - bash: cdf deploy",
            "        displayName: 'cdf deploy'",
            "        env:",
            "          IDP_CLIENT_SECRET: $(IDP_CLIENT_SECRET)",
        ]
    )
    return "\n".join(lines)


def ado_deploy_trigger(env: str) -> str:
    """Real trigger embedded in each deploy-<env>-pipeline.yml, instead of
    `trigger: none` plus a manual 'override the CI trigger in the UI' step. A UI
    override isn't visible in git and is lost if the pipeline is ever re-registered;
    with one file per environment there's no longer a reason to rely on it.
    """
    if env == "prod":
        return "trigger:\n  branches:\n    exclude:\n      - '*'\n  tags:\n    include:\n      - v*"
    return f"trigger:\n  branches:\n    include:\n      - {DEPLOY_BRANCHES[env]}"


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
        prod_trigger = (
            "GitHub Release (tag `vX.Y.Z` from `main`)"
            if provider == "github"
            else "Git tag `vX.Y.Z` pushed to `main`"
        )
        rows.append(f"| {prod_trigger} | `{projects['prod']}` | Deploy |")
    return "\n".join(rows or ["| *(none)* | *(none)* | No CI/CD workflows generated |"])


def branch_protection_rows(projects: dict[str, str], provider: str) -> str:
    """Required-check names are provider-specific: GitHub registers individual job
    names as status checks, but ADO's Build Validation policy references a pipeline
    (toolkit-pr-validate), not a job name inside it — job names aren't registrable
    checks there at all.
    """
    rows: list[str] = []
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        if provider == "github":
            reviewers = "1" if branch == "main" else "none"
            checks = (
                "`Source branch guardrail`, `cdf build & deploy --dry-run`"
                if branch == "main"
                else "`cdf build & deploy --dry-run`"
            )
        else:
            # ADO's reviewer-count policy is either off or >=1 -- there's no "0"
            # setting to configure, unlike GitHub's approval count.
            reviewers = "1" if branch == "main" else "none (policy not enabled)"
            checks = "`toolkit-pr-validate` (Build Validation)"
        rows.append(f"| `{branch}` | {reviewers} | {checks} |")
    return "\n".join(rows or ["| *(none)* | *(none)* | No PR workflow generated |"])


def branch_protection_note(projects: dict[str, str], provider: str) -> str:
    branches = {DEPLOY_BRANCHES[env] for env in deployable_envs(projects)}
    if not branches:
        return "No PR workflow is generated without a dev or test environment configured."
    if provider == "github":
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
    if branches == {"dev"}:
        return (
            "PRs to `dev` only run the `toolkit-pr-validate` Build Validation policy"
            " (no reviewer requirement enabled)."
        )
    if branches == {"main"}:
        return (
            "PRs to `main` require 1 reviewer and the `toolkit-pr-validate` Build Validation policy, whose"
            " `source_branch_guard` job enforces that changes are promoted from `dev` or `hotfix/*`."
        )
    return (
        "PRs to `dev` only run the `toolkit-pr-validate` Build Validation policy (no reviewer requirement"
        " enabled). Its `source_branch_guard` job only enforces the promotion rule for PRs targeting"
        " `main`, not `dev`."
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
        "BRANCH_PROTECTION_ROWS": branch_protection_rows(projects, args.provider),
        "BRANCH_PROTECTION_NOTE": branch_protection_note(projects, args.provider),
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
        base_values["TARGET_BRANCH_GUARD_STEP"] = ado_target_branch_guard_step(branch_envs(projects))
        base_values["DRY_RUN_JOBS"] = ado_dry_run_jobs(str(toolkit_version), org_dir, setup_check_cmd, projects)
        base_values["PIPELINE_ROWS"] = ado_pipeline_rows(projects)
        base_values["DRY_RUN_REGISTRATION_NOTES"] = ado_dry_run_registration_notes(projects)
        base_values["VARIABLE_GROUP_SCOPING_EXAMPLE"] = ado_variable_group_scoping_example(projects)
        base_values["DEPLOY_AUTHORIZATION_EXAMPLE"] = ado_deploy_authorization_example(projects)
        base_values["BRANCH_CONTROL_WARNING"] = ado_branch_control_warning(projects)
        base_values["DRY_RUN_TRIGGER_NOTE"] = ado_dry_run_trigger_note(projects)

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
        for env in ENVIRONMENTS:
            out = workflows_dir / ADO_DEPLOY_PIPELINE_FILES[env]
            if env not in projects:
                remove_file(out)
                continue
            env_values = {
                **base_values,
                "DEPLOY_JOBS": ado_deploy_job(env, str(toolkit_version), org_dir, setup_check_cmd, projects[env]),
                "DEPLOY_TRIGGER": ado_deploy_trigger(env),
            }
            write_file(out, render_template(deploy_pipeline_template, env_values), args.force)

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
        pipeline_registrations = [
            f"{ADO_DEPLOY_PIPELINE_NAMES[env]} ({ADO_DEPLOY_PIPELINE_FILES[env]})"
            for env in ENVIRONMENTS
            if env in projects
        ]
        print(f"  1. Create Azure DevOps variable groups: {', '.join(environments)}")
        print("     (see docs/FOUNDATION_CICD.md)")
        print(f"  2. Register each deploy pipeline: {', '.join(pipeline_registrations)}")
        if deployable_envs(projects):
            branches = ", ".join(branch_envs(projects))
            print(f"  3. Add dry-run-pipeline.yml as a Build Validation policy on {branches}")


if __name__ == "__main__":
    main()
