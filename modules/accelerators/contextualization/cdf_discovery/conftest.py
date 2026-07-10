"""Pytest path setup for cdf_discovery modular layout."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent

for _p in (
    str(_ROOT),
    str(_ROOT / "shared"),
    str(_ROOT / "shared" / "python"),
    str(_ROOT / "submodules"),
    str(_ROOT / "scripts"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)
