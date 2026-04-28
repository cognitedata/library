"""
Post-write verification helpers for the Quickstart DP setup wizard.

Runs ``cdf build`` and optionally ``cdf deploy --dry-run`` / live deploy
after the wizard has written its changes, and prints a consolidated failure
summary when the build fails.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

from . import _style as style
from ._constants import _CONFIG_FLAG_VERSION, DATA_UPLOAD_DIRS
from ._messages import (
    SEC_POST_VERIFY,
    VERIFY_BUILD_FAIL,
    VERIFY_BUILD_OK,
    VERIFY_BUILD_START,
    VERIFY_DATA_FAIL,
    VERIFY_DATA_INTRO,
    VERIFY_DATA_OK,
    VERIFY_DATA_SKIP,
    VERIFY_DATA_UPLOAD,
    VERIFY_DRY_FAIL,
    VERIFY_DRY_OK,
    VERIFY_DRY_START,
    VERIFY_LIVE_OK,
    VERIFY_LIVE_SKIP,
)
from ._prompts import prompt_yes_no

# Flag selection

def _cdf_env_args(
    env: str,
    config_arg: str,
    toolkit_version: tuple[int, int, int] | None,
) -> list[str]:
    """Return the env/config flag(s) for ``cdf build`` / ``cdf deploy`` commands.

    - Toolkit ``< 0.8.0``  →  ``["--env=<env>"]``
    - Toolkit ``≥ 0.8.0``  →  ``["-c", "<config_arg>"]``
    - Version unknown (``None``) defaults to the newer ``-c`` form.
    """
    if toolkit_version is not None and toolkit_version < _CONFIG_FLAG_VERSION:
        return [f"--env={env}"]
    return ["-c", config_arg]


# Failure summary

def _summarise_build_failure(stderr: str, build_cmd: str) -> None:
    """Print a consolidated build-failure summary from *stderr*.

    Shows up to ten lines that look like actionable errors.  Falls back to
    the last five non-empty lines when nothing recognisable is found.
    Always ends with concrete suggested next steps.
    """
    error_lines = [
        ln for ln in stderr.splitlines()
        if re.search(r"error|Error|ERROR|failed|Failed|FAILED|invalid|Invalid|not found", ln)
        and ln.strip()
    ]
    if error_lines:
        style.error("\n  Errors detected:")
        for ln in error_lines[:10]:
            style.error(f"    {ln.strip()}")
        if len(error_lines) > 10:
            style.hint(f"    … and {len(error_lines) - 10} more — run with --verbose for full output.")
    else:
        tail = [ln for ln in stderr.splitlines() if ln.strip()][-5:]
        if tail:
            style.hint("\n  Build output (last lines):")
            for ln in tail:
                style.hint(f"    {ln.strip()}")

    print(
        f"\n  {style.BOLD}Suggested actions:{style.RESET}\n"
        f"    1. Inspect full output : {build_cmd} --verbose\n"
        f"    2. Verify credentials  : cdf auth verify\n"
        f"    3. Check alpha flags   : [alpha_flags] deployment-pack = true  in cdf.toml\n"
        f"    4. Restore if needed   : copy the timestamped .bak file over the config"
    )


# Post-write verification

def run_post_write_verification(
    repo_root: Path,
    env: str,
    config_arg: str,
    toolkit_version: tuple[int, int, int] | None = None,
    project_name: str | None = None,
    org_dir: str | None = None,
) -> None:
    """Run ``cdf build`` then offer ``cdf deploy --dry-run`` and live deploy.

    The env/config flag passed to ``cdf`` depends on the installed toolkit version:

    - ``< 0.8.0``  →  ``--env=<env>``
    - ``≥ 0.8.0``  →  ``-c <config_arg>``
    """
    env_args = _cdf_env_args(env, config_arg, toolkit_version)
    build_cmd = "cdf build " + " ".join(env_args)

    style.section(SEC_POST_VERIFY)

    # --- Step 1: cdf build --------------------------------------------------
    style.hint(VERIFY_BUILD_START)
    style.hint(f"        Running: {build_cmd}")
    build = subprocess.run(
        ["cdf", "build"] + env_args,
        capture_output=True, text=True, cwd=str(repo_root),
    )
    if build.returncode != 0:
        style.error(VERIFY_BUILD_FAIL)
        _summarise_build_failure(build.stderr + build.stdout, build_cmd)
        return
    style.success(VERIFY_BUILD_OK)

    # --- Step 2: cdf deploy --dry-run ---------------------------------------
    style.hint(VERIFY_DRY_START)
    style.hint("        Running: cdf deploy --dry-run")
    dry = subprocess.run(
        ["cdf", "deploy", "--dry-run"],
        capture_output=True, text=True, cwd=str(repo_root),
    )
    if dry.stdout:
        for line in dry.stdout.splitlines():
            print(f"  {line}")
    if dry.returncode != 0:
        style.warning(VERIFY_DRY_FAIL)
        return
    style.success(VERIFY_DRY_OK)

    # --- Step 3: live deploy (optional) -------------------------------------
    deploy_target = project_name or env
    style.hint(f"\n  [3/4] Live deploy to '{deploy_target}' (optional).")
    if prompt_yes_no(f"  Proceed with live deploy to '{deploy_target}'?", default=False):
        style.hint("        Running: cdf deploy")
        subprocess.run(["cdf", "deploy"], cwd=str(repo_root))
        style.success(VERIFY_LIVE_OK)
    else:
        style.hint(VERIFY_LIVE_SKIP)
        return

    # --- Step 4: synthetic data upload (optional) ---------------------------
    style.hint(VERIFY_DATA_INTRO)
    if prompt_yes_no("\n  Upload synthetic test data now?", default=False):
        style.hint(VERIFY_DATA_UPLOAD)
        failed = False
        for upload_dir in DATA_UPLOAD_DIRS:
            full_dir = f"{org_dir}/{upload_dir}" if org_dir else upload_dir
            style.hint(f"        Running: cdf data upload dir {full_dir}")
            result = subprocess.run(
                ["cdf", "data", "upload", "dir", full_dir],
                cwd=str(repo_root),
            )
            if result.returncode != 0:
                failed = True
        if failed:
            style.warning(VERIFY_DATA_FAIL)
        else:
            style.success(VERIFY_DATA_OK)
    else:
        style.hint(VERIFY_DATA_SKIP)
