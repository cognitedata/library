"""Pytest path setup for inverted_index tests (distinct ``local_runner`` package)."""

from __future__ import annotations

import sys
from pathlib import Path

_IDX_ROOT = Path(__file__).resolve().parent.parent
_MODULE_ROOT = _IDX_ROOT.parent.parent

for key in list(sys.modules):
    if key == "local_runner" or key.startswith("local_runner."):
        del sys.modules[key]

for _p in (
    str(_IDX_ROOT),
    str(_IDX_ROOT / "functions"),
    str(_MODULE_ROOT / "scripts"),
):
    while _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

_submodules = str(_IDX_ROOT.parent)
if _submodules not in sys.path:
    sys.path.insert(0, _submodules)
