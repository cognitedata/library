#!/usr/bin/env python3
"""Read-only preflight for installing the Contextualization Quality Dashboard.

Usage (from the Toolkit project root):
    python3 <skill-dir>/scripts/preflight.py [--project-dir .] [--app-dir <app folder>] [--scope all|backend|app]

Prints a PASS / FAIL / SKIP table and exits 1 if any check fails. It never prints, logs or writes the value of an
environment variable: it only says whether each required variable is set.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

type Status = Literal["PASS", "FAIL", "SKIP"]
type Version = tuple[int, int, int]

MIN_TOOLKIT: Version = (0, 8, 0)
RECORD_VIEWS_STABLE: Version = (0, 9, 0)
MIN_NODE_MAJOR = 20
ENV_NAMES = (
    "CQ_FUNCTION_GROUP_SOURCE_ID",
    "CQ_DASHBOARD_GROUP_SOURCE_ID",
    "CQ_FUNCTION_CLIENT_ID",
    "CQ_FUNCTION_CLIENT_SECRET",
)
PLACEHOLDER_PROJECT = "your-project"
SCOPES = ("all", "backend", "app")
VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")


@dataclass(frozen=True)
class Check:
    name: str
    status: Status
    detail: str


def parse_version(output: str) -> Version | None:
    match = VERSION.search(output)
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


def format_version(version: Version) -> str:
    return ".".join(str(part) for part in version)


def run_command(command: Sequence[str]) -> str | None:
    """Return the command's output, or None when it is not installed or fails."""
    executable = shutil.which(command[0])
    if executable is None:
        return None
    try:
        completed = subprocess.run(  # noqa: S603
            [executable, *command[1:]], capture_output=True, text=True, timeout=120, check=True
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    return completed.stdout + completed.stderr


def toolkit_command(project_dir: Path) -> list[str]:
    if (project_dir / "pyproject.toml").is_file() or (project_dir / "uv.lock").is_file():
        return ["uv", "run", "cdf", "--version"]
    return ["cdf", "--version"]


def check_toolkit(output: str | None) -> Check:
    version = parse_version(output) if output else None
    if version is None:
        return Check("Toolkit", "FAIL", "`cdf --version` did not run; install the Cognite Toolkit")
    if version < MIN_TOOLKIT:
        return Check("Toolkit", "FAIL", f"{format_version(version)} is older than {format_version(MIN_TOOLKIT)}")
    return Check("Toolkit", "PASS", format_version(version))


def check_record_views(cdf_toml: Path, toolkit: Version | None) -> Check:
    if not cdf_toml.is_file():
        return Check("record_views", "FAIL", f"no cdf.toml in {cdf_toml.parent}")
    if toolkit is not None and toolkit >= RECORD_VIEWS_STABLE:
        return Check("record_views", "SKIP", "not checked on Toolkit 0.9 or later; see SKILL.md step 1")
    try:
        flags = tomllib.loads(cdf_toml.read_text(encoding="utf-8")).get("alpha_flags", {})
    except (OSError, UnicodeDecodeError) as error:
        return Check("record_views", "FAIL", f"cannot read cdf.toml: {error}")
    except tomllib.TOMLDecodeError as error:
        return Check("record_views", "FAIL", f"cdf.toml does not parse: {error}")
    if isinstance(flags, dict) and flags.get("record_views") is True:
        return Check("record_views", "PASS", "[alpha_flags] record_views = true")
    return Check("record_views", "FAIL", "add `record_views = true` under [alpha_flags] in cdf.toml")


def check_node(output: str | None) -> Check:
    version = parse_version(output) if output else None
    if version is None:
        return Check("Node", "FAIL", "`node --version` did not run; install Node.js 20 or later")
    if version[0] < MIN_NODE_MAJOR:
        return Check("Node", "FAIL", f"{format_version(version)} is older than {MIN_NODE_MAJOR}")
    return Check("Node", "PASS", format_version(version))


def names_set_in(env_file: Path) -> set[str]:
    """Names that have a non-empty value in a dotenv file. Values are read only to test emptiness."""
    if not env_file.is_file():
        return set()
    assignment = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")
    names: set[str] = set()
    try:
        content = env_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return names
    for line in content.splitlines():
        match = assignment.match(line)
        if match and match.group(2).strip("'\""):
            names.add(match.group(1))
    return names


def check_env_vars(env_file: Path, environ: Mapping[str, str]) -> Check:
    in_file = names_set_in(env_file)
    missing = [name for name in ENV_NAMES if name not in in_file and not environ.get(name)]
    if missing:
        return Check("CQ_* variables", "FAIL", f"missing or empty: {', '.join(missing)}")
    return Check("CQ_* variables", "PASS", "all four are set (values not shown)")


def check_app_json(app_dir: Path | None) -> Check:
    if app_dir is None:
        return Check("app.json", "SKIP", "no --app-dir given")
    path = app_dir / "app.json"
    try:
        loaded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return Check("app.json", "FAIL", f"cannot read {path}: {error}")
    deployments = None
    if isinstance(loaded, dict):
        deployments = loaded.get("deployments")
    first = None
    if isinstance(deployments, list) and deployments:
        first = deployments[0]
    project = None
    if isinstance(first, dict):
        project = first.get("project")
    if not isinstance(project, str) or project in ("", PLACEHOLDER_PROJECT):
        return Check("app.json", "FAIL", "set deployments[0].project to your CDF project")
    return Check("app.json", "PASS", f"project {project}")


def format_table(checks: Sequence[Check]) -> str:
    width = max(len(check.name) for check in checks)
    lines = [f"{'Check'.ljust(width)}  Status  Detail", f"{'-' * width}  ------  ------"]
    lines += [f"{check.name.ljust(width)}  {check.status.ljust(6)}  {check.detail}" for check in checks]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Toolkit project root (holds cdf.toml)")
    parser.add_argument("--app-dir", type=Path, default=None, help="the dashboard app folder (holds app.json)")
    parser.add_argument("--scope", choices=SCOPES, default="all", help="what is being installed (default: all)")
    args = parser.parse_args(argv)
    project_dir: Path = args.project_dir
    scope: str = args.scope

    def skipped(name: str) -> Check:
        return Check(name, "SKIP", f"not needed for --scope {scope}")

    if scope == "app":
        backend_checks = [skipped("Toolkit"), skipped("record_views"), skipped("CQ_* variables")]
    else:
        toolkit_output = run_command(toolkit_command(project_dir))
        backend_checks = [
            check_toolkit(toolkit_output),
            check_record_views(project_dir / "cdf.toml", parse_version(toolkit_output) if toolkit_output else None),
            check_env_vars(project_dir / ".env", os.environ),
        ]
    if scope == "backend":
        app_checks = [skipped("Node"), skipped("app.json")]
    else:
        app_checks = [check_node(run_command(["node", "--version"])), check_app_json(args.app_dir)]
    checks = [*backend_checks, *app_checks]
    print(format_table(checks))
    return 1 if any(check.status == "FAIL" for check in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
