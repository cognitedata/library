"""Local runner paths and PYTHONPATH bootstrap."""

from __future__ import annotations

import sys
from pathlib import Path

from workflow_artifact_paths import workflow_scopes_output_dir

_TRANSFORM_ROOT = Path(__file__).resolve().parent.parent
_DISCOVERY_ROOT = _TRANSFORM_ROOT.parent.parent


def module_root() -> Path:
    return _TRANSFORM_ROOT


def discovery_root() -> Path:
    return _DISCOVERY_ROOT


def built_workflow_scope_dir(scope_suffix: str) -> Path:
    """Built workflow scope config under ``workflow_scopes/`` (flat) or ``workflow_scopes/<scope>/``."""
    return workflow_scopes_output_dir(_TRANSFORM_ROOT, scope_suffix)


def ensure_paths() -> None:
    functions = _TRANSFORM_ROOT / "functions"
    shared = _DISCOVERY_ROOT / "shared"
    for p in (str(_TRANSFORM_ROOT), str(functions), str(shared)):
        if p not in sys.path:
            sys.path.insert(0, p)
