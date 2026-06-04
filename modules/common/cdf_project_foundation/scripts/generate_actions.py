#!/usr/bin/env python3
"""
Generate GitHub Actions CI/CD for a Toolkit project using the Foundation Deployment Pack.

Implements the branching model and workflows from sop-cdf-project-setup.md (Step 5):
  - PR to dev or main → dry-run (lint, test, cdf build, cdf deploy --dry-run)
  - Push to dev → deploy to {enterprise}-dev
  - Push to main → deploy to {enterprise}-test
  - Release published from main → deploy to {enterprise}-prod

Run from the Toolkit project root after `cdf modules add -d dp:foundation`:

  python modules/common/cdf_project_foundation/scripts/generate_actions.py --enterprise acme
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

MODULE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = MODULE_DIR / "templates" / "github"
GENERATE_ENV_SCRIPT = MODULE_DIR / "scripts" / "generate_env_configs.py"


def resolve_modules_root(repo_root: Path, org_dir: str) -> Path:
    """Toolkit modules/ at repo root, or nested under the organization directory."""
    for candidate in (repo_root / "modules", repo_root / org_dir / "modules"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        f"No modules directory found under {repo_root / 'modules'} "
        f"or {repo_root / org_dir / 'modules'}"
    )


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
        raise FileExistsError(f"{path} exists — pass --force to overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"Wrote {path}")


def build_lint_paths(org_dir: str, module_paths: list[str]) -> str:
    entries = [
        f"'{org_dir}/'",
        "'modules/packages.toml'",
        "'cdf.toml'",
        "'.pre-commit-config.yaml'",
        "'.github/scripts/'",
    ]
    for path in module_paths:
        entries.append(f"'modules/{path}/'")
    return " \\\n            ".join(entries)


def discover_module_paths(modules_root: Path, repo_root: Path) -> list[str]:
    from generate_env_configs import discover_foundation_module_paths

    return discover_foundation_module_paths(modules_root, repo_root)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enterprise",
        required=True,
        help="Enterprise slug for CDF projects ({enterprise}-dev|test|prod)",
    )
    parser.add_argument(
        "--org-dir",
        help="Organization directory (default: cdf.toml default_organization_dir)",
    )
    parser.add_argument(
        "--toolkit-version",
        help="cognite-toolkit pip version (default: cdf.toml [modules].version)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        help="Toolkit project root (default: directory containing cdf.toml)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite generated files",
    )
    parser.add_argument(
        "--skip-configs",
        action="store_true",
        help="Only generate workflows, not config.*.yaml",
    )
    args = parser.parse_args()

    repo_root = (args.repo_root or find_repo_root(Path.cwd())).resolve()
    cdf = load_cdf_toml(repo_root)
    org_dir = args.org_dir or cdf.get("cdf", {}).get("default_organization_dir")
    if not org_dir:
        print("Set --org-dir or default_organization_dir in cdf.toml", file=sys.stderr)
        sys.exit(1)

    toolkit_version = args.toolkit_version or cdf.get("modules", {}).get("version", "0.7.220")
    modules_root = resolve_modules_root(repo_root, org_dir)
    module_paths = discover_module_paths(modules_root, repo_root)

    template_values = {
        "ENTERPRISE": args.enterprise,
        "ORG_DIR": org_dir,
        "TOOLKIT_VERSION": str(toolkit_version),
        "LINT_PATHS": build_lint_paths(org_dir, module_paths),
    }

    workflow_names = [
        "dry-run.yml",
        "deploy-dev.yml",
        "deploy-test.yml",
        "deploy-prod.yml",
    ]
    for name in workflow_names:
        template = TEMPLATES_DIR / name
        if not template.is_file():
            print(f"Missing template: {template}", file=sys.stderr)
            sys.exit(1)
        out = repo_root / ".github" / "workflows" / name
        write_file(out, render_template(template, template_values), args.force)

    prepare_src = TEMPLATES_DIR / "prepare-toolkit-project.sh"
    prepare_out = repo_root / ".github" / "scripts" / "prepare-toolkit-project.sh"
    write_file(prepare_out, render_template(prepare_src, template_values), args.force)
    prepare_out.chmod(0o755)

    # Copy env config generator into org dir for local refresh.
    org_scripts = repo_root / org_dir / "scripts"
    org_scripts.mkdir(parents=True, exist_ok=True)
    env_gen_dest = org_scripts / "generate_env_configs.py"
    if env_gen_dest.exists() and not args.force:
        print(f"Keeping existing {env_gen_dest}")
    else:
        shutil.copy2(GENERATE_ENV_SCRIPT, env_gen_dest)
        print(f"Wrote {env_gen_dest}")

    cicd_readme = repo_root / "docs" / "FOUNDATION_CICD.md"
    write_file(
        cicd_readme,
        render_template(TEMPLATES_DIR / "FOUNDATION_CICD.md", template_values),
        args.force,
    )

    if not args.skip_configs:
        subprocess.run(
            [
                sys.executable,
                str(GENERATE_ENV_SCRIPT),
                "--enterprise",
                args.enterprise,
                "--org-dir",
                org_dir,
                "--repo-root",
                str(repo_root),
            ],
            check=True,
        )

    print()
    print("Next steps:")
    print("  1. Create GitHub Environments: dev-toolkit-credentials, test-toolkit-credentials,")
    print("     prod-toolkit-credentials (see docs/FOUNDATION_CICD.md)")
    print("  2. Create branches dev and main; protect them")
    print("  3. Open a PR to dev to validate dry-run.yml")


if __name__ == "__main__":
    # Allow importing generate_env_configs from the same directory.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    main()
