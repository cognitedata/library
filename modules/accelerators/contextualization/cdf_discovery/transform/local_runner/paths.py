"""Local runner paths and PYTHONPATH bootstrap."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parent.parent


def module_root() -> Path:
    return _MODULE_ROOT


def ensure_paths() -> None:
    functions = _MODULE_ROOT / "functions"
    for p in (str(_MODULE_ROOT), str(functions)):
        if p not in sys.path:
            sys.path.insert(0, p)
