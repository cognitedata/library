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
    assert resolved["row_key_template"] == "{lookup_key}:{scope}|{index_kind}"
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


def test_format_inverted_index_row_key_default_template() -> None:
    from cdf_fn_common.etl_inverted_index import format_inverted_index_row_key

    assert (
        format_inverted_index_row_key("metadata", "p-101", "{lookup_key}:{scope}|{index_kind}", "sp")
        == "p-101:sp|metadata"
    )


def test_normalize_lookup_key_modes() -> None:
    assert normalize_lookup_key_for_handler("  AbC ", "strip_casefold") == "abc"
    assert normalize_lookup_key_for_handler("  AbC ", "strip") == "AbC"
    assert normalize_lookup_key_for_handler("  AbC ", "none") == "  AbC "


def test_resolve_annotation_vertex_index_handler() -> None:
    from cdf_fn_common.etl_build_index.handlers.annotation_vertex_index import (
        AnnotationVertexIndexHandler,
    )
    from cdf_fn_common.etl_inverted_index import posting_dedupe_key

    handler_id, handler_cls, resolved = resolve_build_index_config(
        {
            "handler_id": "annotation_vertex_index",
            "index_kinds": {"annotation": ["text"]},
        }
    )
    assert handler_id == "annotation_vertex_index"
    assert handler_cls is AnnotationVertexIndexHandler
    assert resolved["row_key_template"] == "{lookup_key}:{scope}|{index_kind}"
    assert resolved["lookup_key_normalization"] == "strip"
    key = posting_dedupe_key(
        {
            "index_kind": "annotation",
            "file_ref": {"file_id": 1, "page_number": 2},
            "lookup_key": "p-101",
            "region": {"vertices": [{"x": 0.1, "y": 0.2}]},
        }
    )
    assert key[0] == "annotation"


def test_build_annotation_index_posting_text_lookup_and_results_json() -> None:
    from cdf_fn_common.etl_build_index.handlers.annotation_vertex_index import (
        AnnotationVertexIndexHandler,
        convert_cohort_annotation_to_posting,
    )
    from cdf_fn_common.etl_inverted_index import build_inverted_index_rows

    cols = {
        "NODE_INSTANCE_ID": "sp:file-1",
        "EXTERNAL_ID": "file-1",
        "VIEW_SPACE": "cdf_cdm",
        "VIEW_EXTERNAL_ID": "CogniteFile",
        "VIEW_VERSION": "v1",
        "ENTITY_TYPE": "CogniteFile",
    }
    props = {
        "text": "00-X-00",
        "confidence": 0.91,
        "region": {"vertices": [{"x": 0.1, "y": 0.2}]},
        "file_ref": {"file_id": 42, "page_number": 3},
        "annotation": {
            "text": "00-X-00",
            "confidence": 0.91,
            "region": {"vertices": [{"x": 0.1, "y": 0.2}]},
            "entities": [{"name": "00-X-00", "sample": "00-X-00"}],
        },
    }
    resolved = AnnotationVertexIndexHandler.default_block()
    posting = convert_cohort_annotation_to_posting(
        cols=cols,
        props=props,
        lookup_key="00-X-00",
        run_id="run-1",
        resolved=resolved,
    )
    assert posting["lookup_key"] == "00-X-00"
    assert posting["text"] == "00-X-00"
    assert posting["results_json"]["text"] == "00-X-00"
    assert posting["results_json"]["entities"][0]["name"] == "00-X-00"
    assert posting["results_json"]["file_ref"]["file_id"] == 42

    rows = AnnotationVertexIndexHandler.build_rows(
        {("annotation", "00-X-00"): [posting]},
        resolved=resolved,
        run_id="run-1",
        canvas_node_id="build_index",
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "00-X-00:sp|annotation"
    assert rows[0]["columns"]["SCOPE"] == "sp"
    import json

    postings = json.loads(rows[0]["columns"]["POSTINGS_JSON"])
    assert postings[0]["lookup_key"] == "00-X-00"
    assert postings[0]["results_json"]["text"] == "00-X-00"
