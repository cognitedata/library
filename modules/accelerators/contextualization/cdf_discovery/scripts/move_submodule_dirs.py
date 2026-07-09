#!/usr/bin/env python3
"""Move remaining top-level submodule dirs into submodules/."""

from __future__ import annotations

import shutil
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parents[1]

MOVES = ("governance", "transform", "inverted_index")


def main() -> int:
    for name in MOVES:
        src = MODULE_ROOT / name
        dst = MODULE_ROOT / "submodules" / name
        if not src.is_dir():
            print(f"skip missing {name}")
            continue
        if dst.exists():
            # empty placeholder from initial mkdir — remove and move
            if dst.is_dir() and not any(dst.iterdir()):
                dst.rmdir()
            elif dst.is_dir():
                print(f"skip {name}: submodules/{name} already has content")
                continue
        print(f"move {name} -> submodules/{name}")
        shutil.move(str(src), str(dst))

    # Merge inverted_index library from root if duplicate
    root_idx = MODULE_ROOT / "inverted_index"
    sub_idx = MODULE_ROOT / "submodules" / "inverted_index"
    if root_idx.is_dir() and sub_idx.is_dir():
        for item in root_idx.iterdir():
            if item.name in ("functions", "raw", "data_modeling"):
                continue
            target = sub_idx / item.name
            if target.exists():
                continue
            shutil.move(str(item), str(target))
        if not any(root_idx.iterdir()):
            root_idx.rmdir()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
