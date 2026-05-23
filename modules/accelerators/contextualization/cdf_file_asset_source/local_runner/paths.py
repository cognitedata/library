"""Module and repository path helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parent.parent
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent

DEFAULT_CONFIG_REL = "default.config.yaml"
WORKFLOW_TRIGGER_REL = "workflows/create_asset_hierarchy_from_files.WorkflowTrigger.yaml"

PIPELINE_STEPS: tuple[str, ...] = ("extract", "create", "write")


def get_module_root() -> Path:
    return Path(os.environ.get("CDF_FILE_ASSET_SOURCE_ROOT", _MODULE_ROOT)).resolve()


def get_repo_root() -> Path:
    return _REPO_ROOT.resolve()


def ensure_import_paths() -> Path:
    """Insert module root and ``functions/`` on ``sys.path`` for ``fn_dm_*`` imports."""
    root = get_module_root()
    functions = root / "functions"
    repo = get_repo_root()
    for p in (repo, root, functions):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)
    return root
