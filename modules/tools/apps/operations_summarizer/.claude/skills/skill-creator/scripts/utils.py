"""Shared utilities for skill-creator scripts."""

import os
import shutil
from pathlib import Path


# ---------------------------------------------------------------------------
# Platform detection & helpers
# ---------------------------------------------------------------------------

PLATFORMS = ("claude", "cursor")


def detect_platform() -> str:
    """Auto-detect the current platform (claude or cursor).

    Checks environment variables first, then falls back to CLI availability.
    """
    if os.environ.get("CLAUDECODE"):
        return "claude"
    if os.environ.get("CURSOR_SESSION"):
        return "cursor"
    # Fall back to CLI availability
    if shutil.which("cursor"):
        return "cursor"
    return "claude"


def get_platform_name(platform: str) -> str:
    """Human-readable platform name for display and prompts."""
    return {"claude": "Claude Code", "cursor": "Cursor"}.get(platform, platform)


def get_skills_dir(platform: str) -> str:
    """Return the conventional skills directory for a platform."""
    return {"claude": ".claude/skills/", "cursor": ".cursor/skills/"}.get(
        platform, ".claude/skills/"
    )


_BASE_FRONTMATTER_KEYS = {
    "name", "description", "license", "allowed-tools", "metadata", "compatibility",
}


def get_allowed_frontmatter_keys(platform: str) -> set[str]:
    """Return the set of valid SKILL.md frontmatter keys for a platform."""
    keys = set(_BASE_FRONTMATTER_KEYS)
    if platform == "cursor":
        keys.add("disable-model-invocation")
    return keys


# ---------------------------------------------------------------------------
# SKILL.md parsing
# ---------------------------------------------------------------------------

def parse_skill_md(skill_path: Path) -> dict:
    """Parse a SKILL.md file.

    Returns a dict with keys: name, description, content.
    """
    content = (skill_path / "SKILL.md").read_text()
    lines = content.split("\n")

    if lines[0].strip() != "---":
        raise ValueError("SKILL.md missing frontmatter (no opening ---)")

    end_idx = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_idx = i
            break

    if end_idx is None:
        raise ValueError("SKILL.md missing frontmatter (no closing ---)")

    name = ""
    description = ""
    frontmatter_lines = lines[1:end_idx]
    i = 0
    while i < len(frontmatter_lines):
        line = frontmatter_lines[i]
        if line.startswith("name:"):
            name = line[len("name:"):].strip().strip('"').strip("'")
        elif line.startswith("description:"):
            value = line[len("description:"):].strip()
            # Handle YAML multiline indicators (>, |, >-, |-)
            if value in (">", "|", ">-", "|-"):
                continuation_lines: list[str] = []
                i += 1
                while i < len(frontmatter_lines) and (frontmatter_lines[i].startswith("  ") or frontmatter_lines[i].startswith("\t")):
                    continuation_lines.append(frontmatter_lines[i].strip())
                    i += 1
                description = " ".join(continuation_lines)
                continue
            else:
                description = value.strip('"').strip("'")
        i += 1

    return {"name": name, "description": description, "content": content}
