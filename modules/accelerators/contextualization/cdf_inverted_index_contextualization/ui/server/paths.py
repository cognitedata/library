"""Shared module paths for the operator API."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_MODULE_DEFAULT = str(Path(__file__).resolve().parent.parent.parent)
MODULE_ROOT = Path(os.environ.get("CDF_INVERTED_INDEX_ROOT") or _MODULE_DEFAULT).resolve()
_mod_root_str = str(MODULE_ROOT)
if _mod_root_str not in sys.path:
    sys.path.insert(0, _mod_root_str)

CONFIG_PATH = MODULE_ROOT / "default.config.yaml"
WORKSPACE_PATH = MODULE_ROOT / ".ui_workspace.json"
