#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in ROOT.rglob("*"):
    if not path.is_file() or path.suffix != ".py":
        continue
    if "submodules/transform" not in str(path):
        continue
    text = path.read_text(encoding="utf-8")
    new = text.replace('ROOT / "submodules/transform/functions"', 'ROOT / "functions"')
    new = new.replace("ROOT / 'submodules/transform/functions'", "ROOT / 'functions'")
    new = new.replace('_TRANSFORM_ROOT / "submodules/transform/functions"', '_TRANSFORM_ROOT / "functions"')
    if new != text:
        path.write_text(new, encoding="utf-8")
        print(path.relative_to(ROOT))
