"""Tests for scope_build.naming.cdf_external_id_suffix."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
for _p in (_PKG, _SCRIPTS):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

from scope_build.naming import cdf_external_id_suffix, leaf_level_filename_segment


def test_cdf_external_id_suffix_double_underscore_to_single() -> None:
    assert cdf_external_id_suffix("MAIN_SITE__PLANT_A") == "main_site_plant_a"


def test_cdf_external_id_suffix_empty_after_strip() -> None:
    assert cdf_external_id_suffix("___") == "scope"


def test_leaf_level_filename_segment_lowercase_and_safe() -> None:
    assert leaf_level_filename_segment("System") == "system"
    assert leaf_level_filename_segment("  Plant_A  ") == "plant_a"


def test_leaf_level_filename_segment_empty_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        leaf_level_filename_segment("   ")
