"""Unit tests for build_index handler registry and property_token_index handler."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_build_index.pipeline import resolve_build_index_config  # noqa: E402
from cdf_fn_common.etl_build_index.handlers.property_token_index import (  # noqa: E402
    PropertyTokenIndexHandler,
    normalize_lookup_key_for_handler,
)
from cdf_fn_common.etl_inverted_index import parse_index_kinds_config  # noqa: E402


def test_resolve_build_index_config_defaults() -> None:
    handler_id, handler_cls, resolved = resolve_build_index_config(
        {"index_kinds": {"metadata": ["indexKey"]}}
    )
    assert handler_id == "property_token_index"
    assert handler_cls is PropertyTokenIndexHandler
    assert resolved["lookup_key_normalization"] == "strip_casefold"
    assert resolved["token_initial_confidence"] == 1.0
    assert resolved["row_key_template"] == "{index_kind}:{lookup_key}"
    assert parse_index_kinds_config(resolved) == [("metadata", "indexKey")]


def test_resolve_build_index_config_handler_block_overrides() -> None:
    _handler_id, _handler_cls, resolved = resolve_build_index_config(
        {
            "handler_id": "property_token_index",
            "index_kinds": {"metadata": ["indexKey"]},
            "property_token_index": {
                "lookup_key_normalization": "strip",
                "token_initial_confidence": 0.5,
                "row_key_template": "{lookup_key}",
                "query_source": "custom_index",
            },
        }
    )
    assert resolved["lookup_key_normalization"] == "strip"
    assert resolved["token_initial_confidence"] == 0.5
    assert resolved["row_key_template"] == "{lookup_key}"
    assert resolved["query_source"] == "custom_index"


def test_normalize_lookup_key_modes() -> None:
    assert normalize_lookup_key_for_handler("  AbC ", "strip_casefold") == "abc"
    assert normalize_lookup_key_for_handler("  AbC ", "strip") == "AbC"
    assert normalize_lookup_key_for_handler("  AbC ", "none") == "  AbC "
