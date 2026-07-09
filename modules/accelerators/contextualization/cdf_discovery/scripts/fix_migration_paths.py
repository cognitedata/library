#!/usr/bin/env python3
"""Fix incorrect path substitutions from modular migration."""

from __future__ import annotations

import shutil
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "node_modules", "__pycache__", ".cache", "build"}
TEXT_EXTENSIONS = {".py", ".yaml", ".yml", ".md", ".ts", ".tsx", ".json", ".toml", ".txt", ".ini", ".css", ".html"}


def _replace_in_file(path: Path) -> bool:
    if path.suffix not in TEXT_EXTENSIONS:
        return False
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return False
    original = text
    replacements = [
        ("submodules/", "submodules/"),
        ("components/governance/", "components/governance/"),
        ("components/transform/", "components/transform/"),
        ("./governance/", "./governance/"),
        ("./transform/", "./transform/"),
        ("../governance/", "../governance/"),
        ("../transform/", "../transform/"),
        ("../../components/governance/", "../../components/governance/"),
        ("../../components/transform/", "../../components/transform/"),
        ("../components/governance/", "../components/governance/"),
        ("../components/transform/", "../components/transform/"),
        ("/api/transform/", "/api/transform/"),
        ("/api/governance/", "/api/governance/"),
        ("/api/cdf/governance/", "/api/cdf/governance/"),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def _remove_duplicate_roots() -> None:
    for name in ("functions", "transform"):
        src = MODULE_ROOT / name
        if src.is_dir():
            shutil.rmtree(src)
            print(f"removed duplicate {name}/")


def main() -> int:
    count = 0
    for path in MODULE_ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file() and _replace_in_file(path):
            count += 1
    _remove_duplicate_roots()
    print(f"fixed {count} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
