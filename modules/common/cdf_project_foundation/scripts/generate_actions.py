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

from __future__ import annotations

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
REQUIRED_ENVIRONMENTS = ("dev", "prod")
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


def discover_foundation_module_paths(modules_root: Path, repo_root: Path | None = None) -> list[str]:
    """Resolve deployable dp:foundation module paths from packages.toml or module.toml scan."""
    root = repo_root or modules_root.parent
    packages_toml = root / "modules" / "packages.toml"
    if packages_toml.is_file():
        data = tomllib.loads(packages_toml.read_text(encoding="utf-8"))
        listed = data.get("packages", {}).get("foundation", {}).get("modules") or []
        if listed:
            return list(listed)

    paths: list[str] = []
    for module_toml in sorted(modules_root.rglob("module.toml")):
        text = module_toml.read_text(encoding="utf-8")
        if 'package_id = "dp:foundation"' not in text:
            continue
        rel = module_toml.parent.relative_to(modules_root)
        paths.append(rel.as_posix())
    return paths


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
            if env in REQUIRED_ENVIRONMENTS:
                raise FileNotFoundError(
                    f"Missing {path.relative_to(repo_root)}. Run setup_project.py before generating workflows."
                )
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
    return projects


def workflow_file_list(include_test: bool) -> str:
    files = [
        '".github/workflows/dry-run.yml"',
        '".github/workflows/deploy-dev.yml"',
    ]
    if include_test:
        files.append('".github/workflows/deploy-test.yml"')
    files.append('".github/workflows/deploy-prod.yml"')
    return "\n".join(f"              {path}," for path in files)


def pr_branches(include_test: bool) -> str:
    branches = ["dev"]
    if include_test:
        branches.append("main")
    return "\n".join(f"      - {branch}" for branch in branches)


def dry_run_environment(include_test: bool) -> str:
    if include_test:
        return "${{ github.base_ref == 'dev' && 'dev-toolkit-credentials' || 'test-toolkit-credentials' }}"
    return "dev-toolkit-credentials"


def dry_run_build_script(toolkit_version: str, org_dir: str | None, include_test: bool) -> str:
    dev_build = f"cdf build {build_args(toolkit_version, org_dir, 'dev')} | tee build-output.txt"
    if not include_test:
        return dev_build
    test_build = f"cdf build {build_args(toolkit_version, org_dir, 'test')} | tee build-output.txt"
    return "\n".join(
        [
            'if [ "${{ github.base_ref }}" = "dev" ]; then',
            f"  {dev_build}",
            "else",
            f"  {test_build}",
            "fi",
        ]
    )


def indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else line for line in text.splitlines())


def test_branching_rows(projects: dict[str, str]) -> str:
    if "test" not in projects:
        return ""
    return "\n".join(
        [
            f"| PR → `main` | `{projects['test']}` | Dry-run |",
            f"| Push to `main` | `{projects['test']}` | Deploy |",
        ]
    )


def test_environment_row(projects: dict[str, str]) -> str:
    if "test" not in projects:
        return ""
    return f"| `test-toolkit-credentials` | PR → main, push `main` | `{projects['test']}` |"


def env_config_list(projects: dict[str, str]) -> str:
    configs = [f"`config.{env}.yaml`" for env in ENVIRONMENTS if env in projects]
    if len(configs) == 1:
        return configs[0]
    return ", ".join(configs[:-1]) + f", or {configs[-1]}"


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
    include_test = "test" in projects

    toolkit_version = cdf.get("modules", {}).get("version", "0.7.220")
    resolve_modules_root(repo_root, org_dir)

    base_values: dict[str, str] = {
        "DEV_PROJECT": projects["dev"],
        "PROD_PROJECT": projects["prod"],
        "DEV_BUILD_ARGS": build_args(str(toolkit_version), org_dir, "dev"),
        "PROD_BUILD_ARGS": build_args(str(toolkit_version), org_dir, "prod"),
        "WORKFLOW_FILES": workflow_file_list(include_test),
        "PR_BRANCHES": pr_branches(include_test),
        "DRY_RUN_ENVIRONMENT": dry_run_environment(include_test),
        "DRY_RUN_BUILD_SCRIPT": indent(dry_run_build_script(str(toolkit_version), org_dir, include_test), 10),
        "TEST_BRANCHING_ROWS": test_branching_rows(projects),
        "TEST_ENVIRONMENT_ROW": test_environment_row(projects),
        "ENV_CONFIG_LIST": env_config_list(projects),
        "TOOLKIT_VERSION": str(toolkit_version),
        "LINT_PATHS": build_lint_paths(org_dir),
    }
    if include_test:
        base_values.update(
            {
                "TEST_PROJECT": projects["test"],
                "TEST_BUILD_ARGS": build_args(str(toolkit_version), org_dir, "test"),
            }
        )

    for name in ("dry-run.yml", "deploy-prod.yml"):
        template = TEMPLATES_DIR / name
        if not template.is_file():
            print(f"Missing template: {template}", file=sys.stderr)
            sys.exit(1)
        out = repo_root / ".github" / "workflows" / name
        write_file(out, render_template(template, base_values), args.force)

    deploy_template = TEMPLATES_DIR / "deploy.yml"
    if not deploy_template.is_file():
        print(f"Missing template: {deploy_template}", file=sys.stderr)
        sys.exit(1)
    deploy_envs = [("dev", "dev", "Dev")]
    if include_test:
        deploy_envs.append(("test", "main", "Test"))
    else:
        remove_file(repo_root / ".github" / "workflows" / "deploy-test.yml")
    for env, branch, label in deploy_envs:
        merged = {
            **base_values,
            "ENV": env,
            "BRANCH": branch,
            "ENV_LABEL": label,
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
    environments = ["dev-toolkit-credentials"]
    if include_test:
        environments.append("test-toolkit-credentials")
    environments.append("prod-toolkit-credentials")
    print("Next steps:")
    print(f"  1. Create GitHub Environments: {', '.join(environments)}")
    print("     (see docs/FOUNDATION_CICD.md)")
    print("  2. Create and protect the branches used by the generated workflows")
    print("  3. Open a PR to dev to validate dry-run.yml")


if __name__ == "__main__":
    main()
