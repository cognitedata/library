"""Pytest path setup for transform tests (ETL ``local_runner`` package)."""

from __future__ import annotations

import sys
from pathlib import Path

_TRANSFORM_ROOT = Path(__file__).resolve().parent.parent
_MODULE_ROOT = _TRANSFORM_ROOT.parent.parent

from ui.server.etl_syspath import prepare_etl_local_runner  # noqa: E402

prepare_etl_local_runner(_MODULE_ROOT)

for _p in (
    str(_TRANSFORM_ROOT),
    str(_TRANSFORM_ROOT / "functions"),
):
    while _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

for key in list(sys.modules):
    if key == "local_runner" or key.startswith("local_runner."):
        mod = sys.modules.get(key)
        mod_file = getattr(mod, "__file__", None) if mod else None
        if mod_file and "inverted_index" in str(mod_file).replace("\\", "/"):
            del sys.modules[key]
