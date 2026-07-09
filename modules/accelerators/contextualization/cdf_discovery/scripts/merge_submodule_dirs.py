#!/usr/bin/env python3
"""Merge top-level transform/inverted_index into submodules/."""

from __future__ import annotations

import shutil
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parents[1]


def _merge_tree(src: Path, dst: Path) -> None:
    if not src.is_dir():
        return
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            if target.exists():
                _merge_tree(item, target)
            else:
                shutil.move(str(item), str(target))
        elif not target.exists():
            shutil.move(str(item), str(target))
    if src.is_dir() and not any(src.iterdir()):
        src.rmdir()


def main() -> int:
    for name in ("transform", "inverted_index"):
        src = MODULE_ROOT / name
        dst = MODULE_ROOT / "submodules" / name
        if not src.is_dir():
            print(f"skip missing {name}")
            continue
        print(f"merge {name} -> submodules/{name}")
        _merge_tree(src, dst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
