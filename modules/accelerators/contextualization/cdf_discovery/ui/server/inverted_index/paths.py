"""Shared paths for the inverted index operator API (under cdf_discovery)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_DISCOVERY_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_MODULE_DEFAULT = _DISCOVERY_ROOT / "submodules" / "inverted_index"
MODULE_ROOT = Path(os.environ.get("CDF_INVERTED_INDEX_ROOT") or _MODULE_DEFAULT).resolve()
_SUBMODULES_ROOT = MODULE_ROOT.parent
for _p in (str(_SUBMODULES_ROOT), str(_DISCOVERY_ROOT / "shared" / "python"), str(_DISCOVERY_ROOT / "shared" / "cdf_fn_common")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

CONFIG_PATH = MODULE_ROOT / "default.config.yaml"
# Indexing module tabs persist via discovery.local.config.yaml workspace (no separate file).
WORKSPACE_PATH = MODULE_ROOT / ".ui_workspace.json"
