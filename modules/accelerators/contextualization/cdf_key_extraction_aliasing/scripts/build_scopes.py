#!/usr/bin/env python3
"""CLI: build config/scopes/<leaf>/key_extraction_aliasing.yaml from scope_hierarchy.yaml."""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_PKG = _SCRIPTS.parent
for _p in (_PKG, _SCRIPTS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from scope_build.orchestrate import main


if __name__ == "__main__":
    raise SystemExit(main())
