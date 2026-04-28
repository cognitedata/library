"""Paths for the local `module.py` CLI (repo root on sys.path for `modules.*` imports)."""

import sys
from pathlib import Path

# cdf_key_extraction_aliasing/
SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent.parent


def ensure_repo_on_path() -> None:
    """Put repo root, this package directory, and ``functions/`` on ``sys.path``.

    ``functions/`` is required for top-level ``cdf_fn_common`` imports used by Cognite
    function code.
    """
    functions_dir = SCRIPT_DIR / "functions"
    for p in (REPO_ROOT, SCRIPT_DIR, functions_dir):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
