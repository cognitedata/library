#!/usr/bin/env python3
"""Fix double submodules paths and remove resurrected duplicate dirs."""

from __future__ import annotations

import shutil
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "node_modules", "__pycache__", ".cache", "build"}
TEXT_EXTENSIONS = {".py", ".yaml", ".yml", ".md", ".ts", ".tsx", ".json", ".toml", ".txt", ".ini"}


def _fix_file(path: Path) -> bool:
    if path.suffix not in TEXT_EXTENSIONS:
        return False
    text = path.read_text(encoding="utf-8")
    original = text
    replacements = [
        ("submodules/", "submodules/"),
        ('from "./governance/module"', 'from "./governance/module"'),
        ('from "./transform/module"', 'from "./transform/module"'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> int:
    for name in ("functions", "transform"):
        dup = MODULE / name
        if dup.is_dir():
            shutil.rmtree(dup)
            print(f"removed duplicate {name}/")

    count = 0
    for path in MODULE.rglob("*"):
        if any(p in SKIP_DIRS for p in path.parts):
            continue
        if path.is_file() and _fix_file(path):
            count += 1
    print(f"fixed {count} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
