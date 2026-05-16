"""Tests for ``cdf_fn_common.property_path``."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.property_path import get_property_path  # noqa: E402


def test_get_property_path_nested() -> None:
    assert get_property_path({"a": {"b": 3}}, "a.b") == 3
    assert get_property_path({"items": ["x", "y"]}, "items.1") == "y"
