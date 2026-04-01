"""Unit tests for scope YAML merge helpers (no CDF client)."""

from __future__ import annotations

import pytest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.scope_document_dm import (
    build_aliasing_workflow_config,
    build_key_extraction_workflow_config,
    build_reference_index_config_block,
    ensure_instance_space_from_scope_document,
    read_enable_reference_index,
    reference_index_raw_table_key_from_scope,
    resolve_instance_space_from_scope_document,
)


def test_build_key_extraction_merges_runtime() -> None:
    doc = {
        "key_extraction": {
            "externalId": "ctx_key_extraction_x",
            "config": {
                "parameters": {
                    "debug": True,
                    "raw_db": "db_key_extraction",
                    "raw_table_key": "my_suffix_key_extraction_state",
                },
                "data": {
                    "source_views": [
                        {"view_external_id": "CogniteFile", "instance_space": "old"}
                    ],
                    "extraction_rules": [],
                },
            },
        }
    }
    out = build_key_extraction_workflow_config(
        doc,
        full_rescan=True,
        instance_space="sp1",
        incremental_change_processing=True,
    )
    assert out["externalId"] == "ctx_key_extraction_x"
    inner = out["config"]
    assert inner["parameters"]["raw_table_key"] == "my_suffix_key_extraction_state"
    assert inner["parameters"]["full_rescan"] is True
    assert inner["parameters"]["incremental_change_processing"] is True
    assert inner["data"]["source_views"][0]["instance_space"] == "sp1"


def test_build_aliasing_merges_raw_tables() -> None:
    doc = {
        "aliasing": {
            "externalId": "ctx_aliasing_x",
            "config": {
                "parameters": {
                    "debug": True,
                    "raw_table_aliases": "a_aliases",
                    "raw_table_state": "a_state",
                },
                "data": {"aliasing_rules": [{"name": "r1", "type": "regex_substitution"}]},
            },
        }
    }
    out = build_aliasing_workflow_config(
        doc,
        instance_space="sp2",
    )
    assert out["externalId"] == "ctx_aliasing_x"
    p = out["config"]["parameters"]
    assert p["raw_table_aliases"] == "a_aliases"
    assert p["raw_table_state"] == "a_state"
    assert p["raw_db"] == "db_tag_aliasing"


def test_reference_index_raw_table_key_from_scope_convention() -> None:
    assert reference_index_raw_table_key_from_scope({}, "key_extraction_state") == "reference_index"
    assert (
        reference_index_raw_table_key_from_scope({}, "site_a_key_extraction_state")
        == "site_a_reference_index"
    )
    assert reference_index_raw_table_key_from_scope({}, "other") == "other_reference_index"


def test_reference_index_raw_table_key_from_scope_explicit() -> None:
    assert (
        reference_index_raw_table_key_from_scope(
            {"reference_index_raw_table_key": "custom_idx"}, "site_a_key_extraction_state"
        )
        == "custom_idx"
    )


def test_read_enable_reference_index() -> None:
    assert read_enable_reference_index({}) is False
    assert (
        read_enable_reference_index(
            {"key_extraction": {"config": {"parameters": {"enable_reference_index": True}}}}
        )
        is True
    )


def _doc_with_source_views(views: list) -> dict:
    return {
        "key_extraction": {
            "config": {
                "parameters": {"raw_table_key": "x_key_extraction_state"},
                "data": {"source_views": views, "extraction_rules": []},
            }
        }
    }


def test_resolve_instance_space_from_view_field() -> None:
    doc = _doc_with_source_views(
        [{"view_external_id": "CogniteFile", "instance_space": "  sp-x  "}]
    )
    assert resolve_instance_space_from_scope_document(doc) == "sp-x"


def test_resolve_instance_space_from_equals_filter() -> None:
    doc = _doc_with_source_views(
        [
            {
                "view_external_id": "CogniteFile",
                "filters": [
                    {
                        "property_scope": "node",
                        "target_property": "space",
                        "operator": "EQUALS",
                        "values": ["dm-space"],
                    }
                ],
            }
        ]
    )
    assert resolve_instance_space_from_scope_document(doc) == "dm-space"


def test_resolve_instance_space_from_in_filter_single_value() -> None:
    doc = _doc_with_source_views(
        [
            {
                "view_external_id": "CogniteFile",
                "filters": [
                    {
                        "property_scope": "node",
                        "target_property": "space",
                        "operator": "IN",
                        "values": ["only-one"],
                    }
                ],
            }
        ]
    )
    assert resolve_instance_space_from_scope_document(doc) == "only-one"


def test_resolve_instance_space_missing_raises() -> None:
    doc = _doc_with_source_views([{"view_external_id": "CogniteFile", "filters": []}])
    with pytest.raises(ValueError, match="Cannot derive instance_space"):
        resolve_instance_space_from_scope_document(doc)


def test_ensure_instance_space_prefers_data_then_doc() -> None:
    doc = _doc_with_source_views(
        [{"view_external_id": "CogniteFile", "instance_space": "from-doc"}]
    )
    data: dict = {"scope_document": doc, "instance_space": "  explicit  "}
    assert ensure_instance_space_from_scope_document(data) == "explicit"
    assert data["instance_space"] == "explicit"

    data2: dict = {"scope_document": doc}
    assert ensure_instance_space_from_scope_document(data2) == "from-doc"
    assert data2["instance_space"] == "from-doc"


def test_build_reference_index_config_block() -> None:
    doc = {
        "aliasing": {
            "config": {
                "parameters": {"debug": True},
                "data": {"aliasing_rules": [], "validation": {}},
            }
        }
    }
    blk = build_reference_index_config_block(doc)
    assert "config" in blk
    assert blk["config"]["data"]["aliasing_rules"] == []
