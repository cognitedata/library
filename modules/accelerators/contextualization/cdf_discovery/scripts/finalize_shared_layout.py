#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1]
root = MODULE / "shared" / "cdf_fn_common"
nested = root / "cdf_fn_common"
if nested.is_dir():
    for item in nested.iterdir():
        dest = root / item.name
        if not dest.exists():
            shutil.move(str(item), str(dest))
    if nested.is_dir() and not any(nested.iterdir()):
        nested.rmdir()
    print("flattened cdf_fn_common")

idx = MODULE / "submodules" / "inverted_index"
manifest = idx / "functions.Function.yaml"
target = idx / "submodules/transform/functions" / "functions.Function.yaml"
if manifest.is_file():
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        manifest.unlink()
    else:
        shutil.move(str(manifest), str(target))
    print("idx manifest ok")
