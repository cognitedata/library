"""Paths for the local ``module.py`` CLI."""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent.parent


def ensure_module_on_path() -> None:
    """Put repo root and this package on ``sys.path``."""
    for p in (REPO_ROOT, SCRIPT_DIR):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
