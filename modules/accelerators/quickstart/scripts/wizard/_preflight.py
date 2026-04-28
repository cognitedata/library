"""
Pre-flight checks for the Quickstart DP setup wizard.

Verifies that:
- The ``cdf`` CLI is installed and meets the minimum version requirement.
- ``cdf.toml`` exists and contains the required ``[alpha_flags]``.
- The organisation directory (if any) can be resolved from ``cdf.toml``.
- ``.env`` is present in ``.gitignore`` to avoid accidental secret commits.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from ._constants import MIN_TOOLKIT_VERSION
from . import _style as style


# Version helpers

def _parse_version(version_str: str) -> tuple[int, int, int] | None:
    """Extract the first semver triplet from a string, e.g. ``'cdf 0.7.34'`` → ``(0, 7, 34)``."""
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", version_str)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def _version_str(v: tuple[int, int, int]) -> str:
    return ".".join(str(x) for x in v)


# Toolkit version check

def check_toolkit_version() -> tuple[int, int, int] | None:
    """Shell out to ``cdf --version`` and enforce the minimum supported version.

    Exits with code 1 if the installed version is below the minimum.
    Returns the parsed version tuple, or ``None`` if it could not be determined
    (timeout or unparseable output — execution continues with a warning).
    """
    try:
        result = subprocess.run(
            ["cdf", "--version"], capture_output=True, text=True, timeout=10
        )
        raw = (result.stdout + result.stderr).strip()
    except FileNotFoundError:
        style.error(
            "Error: 'cdf' command not found.\n"
            f"  Install Cognite Toolkit ≥ {_version_str(MIN_TOOLKIT_VERSION)} and retry.\n"
            "  See: https://developer.cognite.com/sdks/toolkit/"
        )
        sys.exit(1)
    except subprocess.TimeoutExpired:
        style.warning("Warning: 'cdf --version' timed out — skipping version check.")
        return None

    parsed = _parse_version(raw)
    if parsed is None:
        style.warning(
            f"Warning: could not parse Toolkit version from: {raw!r} — skipping version check."
        )
        return None

    min_s = _version_str(MIN_TOOLKIT_VERSION)
    cur_s = _version_str(parsed)

    if parsed < MIN_TOOLKIT_VERSION:
        style.error(
            f"Error: Toolkit {cur_s} is below the minimum required version {min_s}.\n"
            f"  Upgrade with:  pip install --upgrade cognite-toolkit>={min_s}\n"
            "  See: https://developer.cognite.com/sdks/toolkit/"
        )
        sys.exit(1)

    style.success(f"Toolkit version {cur_s} — OK (minimum: {min_s}).")
    return parsed


# cdf.toml check

def _ensure_toml_flag(lines: list[str], section_header: str, flag: str) -> str | None:
    """Ensure ``flag = true`` exists in *section_header* of a TOML file (in-place on *lines*).

    - Already ``= true``           → no-op, returns ``None``.
    - Exists with a different value → updated in place, returns change description.
    - Missing from section          → inserted after section header, returns change description.
    - Section itself missing        → section + flag appended at end of file, returns change description.
    """
    section_idx: int | None = None
    flag_idx: int | None = None
    in_section = False

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == section_header:
            in_section = True
            section_idx = idx
        elif stripped.startswith("[") and stripped != section_header:
            in_section = False
        if in_section and re.match(rf"{re.escape(flag)}\s*=", stripped):
            flag_idx = idx
            break

    if flag_idx is not None:
        if re.match(rf"{re.escape(flag)}\s*=\s*true", lines[flag_idx].strip()):
            return None  # already correct
        lines[flag_idx] = f"{flag} = true\n"
        return f"updated {flag} = true  (in {section_header})"

    if section_idx is not None:
        lines.insert(section_idx + 1, f"{flag} = true\n")
    else:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(f"\n{section_header}\n{flag} = true\n")
    return f"added {flag} = true  (in {section_header})"


def check_cdf_toml(repo_root: Path) -> None:
    """Verify ``cdf.toml`` exists and contains the required flags.

    Ensures ``deployment-pack = true`` under ``[alpha_flags]`` and
    ``data = true`` under ``[plugins]``.  Each flag is added if absent or
    corrected if present with a non-true value.  Exits with code 1 only
    if the file is absent.
    """
    toml_path = repo_root / "cdf.toml"
    if not toml_path.exists():
        style.error(
            f"Error: cdf.toml not found at {repo_root}\n"
            "  Ensure you are running this wizard from inside a valid Cognite Toolkit project."
        )
        sys.exit(1)

    lines = toml_path.read_text(encoding="utf-8").splitlines(keepends=True)

    changes: list[str] = []
    for result in [
        _ensure_toml_flag(lines, "[alpha_flags]", "deployment-pack"),
        _ensure_toml_flag(lines, "[plugins]", "data"),
    ]:
        if result:
            changes.append(result)

    if changes:
        toml_path.write_text("".join(lines), encoding="utf-8")
        style.warning(
            "  Updated cdf.toml:\n"
            + "".join(f"    {c}\n" for c in changes)
        )


# cdf.toml helpers

def _get_org_dir(repo_root: Path) -> str | None:
    """Return the ``organization_dir`` value from ``cdf.toml``, or ``None``.

    Looks for ``organization_dir = "some_dir"`` (any section, any quote style).
    """
    toml_path = repo_root / "cdf.toml"
    if not toml_path.exists():
        return None
    content = toml_path.read_text(encoding="utf-8")
    m = re.search(r"""organization_dir\s*=\s*["']([^"']+)["']""", content)
    return m.group(1) if m else None


# .gitignore safety check

def _check_gitignore(repo_root: Path) -> None:
    """Warn if ``.env`` is not listed in ``.gitignore``."""
    gitignore = repo_root / ".gitignore"
    if gitignore.exists():
        content = gitignore.read_text(encoding="utf-8")
        if not any(line.strip() in {".env", "*.env"} for line in content.splitlines()):
            style.warning(
                "Warning: .env does not appear to be in .gitignore "
                "— secrets may be committed accidentally."
            )
