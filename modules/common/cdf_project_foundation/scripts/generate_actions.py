#!/usr/bin/env python3
"""
Generate GitHub Actions CI/CD for a Toolkit project using the Foundation Deployment Pack.

Implements the branching model and workflows from sop-cdf-project-setup.md (Step 5):
  - PR to dev, and PR to main when config.test.yaml exists → dry-run
  - Push to dev → deploy to config.dev.yaml's environment.project
  - Push to main → deploy to config.test.yaml's environment.project when present
  - Release published from main → deploy to config.prod.yaml's environment.project

Run from the Toolkit project root after `cdf modules add -d dp:foundation`:

  python modules/common/cdf_project_foundation/scripts/generate_actions.py
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
TEMPLATES_DIR = MODULE_DIR / "templates" / "github"
ENVIRONMENTS = ("dev", "test", "prod")
DEPLOY_BRANCHES = {"dev": "dev", "test": "main"}
ENV_LABELS = {"dev": "Dev", "test": "Test"}
CONFIG_FLAG_MIN_VERSION = (0, 8, 0)


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


def build_lint_paths(org_dir: str | None) -> str:
    """Return git pathspecs for generated workflow linting.

    Keep this scoped to committed project-level files. Deployment modules often
    ship README files, notebooks, and generated Python that are not intended to
    satisfy the destination repository's pre-commit hooks.
    """
    entries: list[str] = [
        "'cdf.toml'",
        "'.pre-commit-config.yaml'",
        "'.github/scripts/'",
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


def dry_run_environment(projects: dict[str, str]) -> str:
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


def dry_run_build_script(toolkit_version: str, org_dir: str | None, projects: dict[str, str]) -> str:
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


def branching_rows(projects: dict[str, str]) -> str:
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
        rows.append(f"| GitHub Release (tag `vX.Y.Z` from `main`) | `{projects['prod']}` | Deploy |")
    return "\n".join(
        rows or ["| *(none)* | *(none)* | No CI/CD workflows generated |"]
    )


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


def environment_rows(projects: dict[str, str]) -> str:
    rows: list[str] = []
    for env in deployable_envs(projects):
        branch = DEPLOY_BRANCHES[env]
        rows.append(f"| `{env}-toolkit-credentials` | PR → {branch}, push `{branch}` | `{projects[env]}` |")
    if "prod" in projects:
        rows.append(f"| `prod-toolkit-credentials` | Release published | `{projects['prod']}` |")
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
    args = parser.parse_args()

    repo_root = find_repo_root(Path.cwd())
    cdf = load_cdf_toml(repo_root)
    org_dir: str | None = args.org_dir or cdf.get("cdf", {}).get("default_organization_dir") or None
    projects = load_environment_projects(repo_root, org_dir)

    toolkit_version = cdf.get("modules", {}).get("version", "0.7.220")
    resolve_modules_root(repo_root, org_dir)

    base_values: dict[str, str] = {
        "PR_BRANCHES": pr_branches(projects),
        "DRY_RUN_ENVIRONMENT": dry_run_environment(projects),
        "DRY_RUN_BUILD_SCRIPT": indent(dry_run_build_script(str(toolkit_version), org_dir, projects), 10),
        "BRANCHING_ROWS": branching_rows(projects),
        "ENVIRONMENT_ROWS": environment_rows(projects),
        "EXAMPLE_BUILD_ARGS": example_build_args(str(toolkit_version), org_dir, projects),
        "ENV_CONFIG_LIST": env_config_list(projects),
        "BRANCH_PROTECTION_ROWS": branch_protection_rows(projects),
        "BRANCH_PROTECTION_NOTE": branch_protection_note(projects),
        "TOOLKIT_VERSION": str(toolkit_version),
        "LINT_PATHS": build_lint_paths(org_dir),
        "SETUP_PROJECT_CHECK_CMD": setup_project_check_cmd(repo_root, org_dir),
    }

    if deployable_envs(projects):
        template = TEMPLATES_DIR / "dry-run.yml"
        if not template.is_file():
            print(f"Missing template: {template}", file=sys.stderr)
            sys.exit(1)
        write_file(
            repo_root / ".github" / "workflows" / "dry-run.yml",
            render_template(template, base_values),
            args.force,
        )
    else:
        remove_file(repo_root / ".github" / "workflows" / "dry-run.yml")

    if "prod" in projects:
        deploy_prod_template = TEMPLATES_DIR / "deploy-prod.yml"
        if not deploy_prod_template.is_file():
            print(f"Missing template: {deploy_prod_template}", file=sys.stderr)
            sys.exit(1)
        prod_values = {
            **base_values,
            "PROD_PROJECT": projects["prod"],
            "PROD_BUILD_ARGS": build_args(str(toolkit_version), org_dir, "prod"),
        }
        write_file(
            repo_root / ".github" / "workflows" / "deploy-prod.yml",
            render_template(deploy_prod_template, prod_values),
            args.force,
        )
    else:
        remove_file(repo_root / ".github" / "workflows" / "deploy-prod.yml")

    deploy_template = TEMPLATES_DIR / "deploy.yml"
    if not deploy_template.is_file():
        print(f"Missing template: {deploy_template}", file=sys.stderr)
        sys.exit(1)
    for env in ("dev", "test"):
        if env not in projects:
            remove_file(repo_root / ".github" / "workflows" / f"deploy-{env}.yml")
            continue
        merged = {
            **base_values,
            "ENV": env,
            "BRANCH": DEPLOY_BRANCHES[env],
            "ENV_LABEL": ENV_LABELS[env],
            "PROJECT": projects[env],
            "BUILD_ARGS": build_args(str(toolkit_version), org_dir, env),
        }
        out = repo_root / ".github" / "workflows" / f"deploy-{env}.yml"
        write_file(out, render_template(deploy_template, merged), args.force)

    cicd_readme = repo_root / "docs" / "FOUNDATION_CICD.md"
    write_file(
        cicd_readme,
        render_template(TEMPLATES_DIR / "FOUNDATION_CICD.md", base_values),
        args.force,
    )

    print()
    environments = [f"{env}-toolkit-credentials" for env in ENVIRONMENTS if env in projects]
    print("Next steps:")
    print(f"  1. Create GitHub Environments: {', '.join(environments)}")
    print("     (see docs/FOUNDATION_CICD.md)")
    print("  2. Create and protect the branches used by the generated workflows")
    if deployable_envs(projects):
        branches = ", ".join(branch_envs(projects))
        print(f"  3. Open a PR to {branches} to validate dry-run.yml")


if __name__ == "__main__":
    main()
