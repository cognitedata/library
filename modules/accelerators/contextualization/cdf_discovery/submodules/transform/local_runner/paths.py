"""Local runner paths and PYTHONPATH bootstrap."""

from __future__ import annotations

import sys
from pathlib import Path

from workflow_artifact_paths import workflow_artifacts_output_dir

_TRANSFORM_ROOT = Path(__file__).resolve().parent.parent
_DISCOVERY_ROOT = _TRANSFORM_ROOT.parent


def module_root() -> Path:
    return _TRANSFORM_ROOT


def discovery_root() -> Path:
    return _DISCOVERY_ROOT


def built_workflow_scope_dir(scope_suffix: str) -> Path:
    """Built workflow config under ``workflows/`` (flat) or ``workflows/<scope>/``."""
    return workflow_artifacts_output_dir(_TRANSFORM_ROOT, scope_suffix)


def ensure_paths() -> None:
    functions = _DISCOVERY_ROOT / "functions"
    for p in (str(_TRANSFORM_ROOT), str(functions)):
        if p not in sys.path:
            sys.path.insert(0, p)
