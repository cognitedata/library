"""Tests for ``inverted_index_naming`` helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.inverted_index_naming import (  # noqa: E402
    inverted_index_raw_table_from_key_extraction_table,
)


def test_inverted_index_raw_table_from_key_extraction_table() -> None:
    assert inverted_index_raw_table_from_key_extraction_table("") == "inverted_index"
    assert inverted_index_raw_table_from_key_extraction_table("key_extraction_state") == "inverted_index"
    assert inverted_index_raw_table_from_key_extraction_table("site_key_extraction_state") == "site_inverted_index"
