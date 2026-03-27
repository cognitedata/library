"""Paths for the local `main.py` CLI (repo root on sys.path for `modules.*` imports)."""

import sys
from pathlib import Path

# cdf_key_extraction_aliasing/
SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent.parent


def ensure_repo_on_path() -> None:
    """Put repo root and this package directory on ``sys.path`` (for ``modules.*`` and ``local_runner``)."""
    for p in (REPO_ROOT, SCRIPT_DIR):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
