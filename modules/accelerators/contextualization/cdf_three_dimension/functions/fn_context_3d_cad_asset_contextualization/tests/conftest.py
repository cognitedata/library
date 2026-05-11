"""Pytest fixtures for 3D contextualization tests."""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure function package is importable when running from project root or function dir
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
