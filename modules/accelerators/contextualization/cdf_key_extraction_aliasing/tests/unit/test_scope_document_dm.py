"""Unit tests for scope YAML merge helpers (no CDF client)."""

from __future__ import annotations

import pytest

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.scope_document_dm import (
    _filter_aliasing_rules_on_data,
    narrow_aliasing_engine_config_for_inline_rule_names,
    build_aliasing_workflow_config,
    build_key_extraction_workflow_config,
    build_inverted_index_config_block,
    ensure_aliasing_config_from_scope_dm,
    ensure_instance_space_from_scope_document,
    ensure_key_extraction_config_from_scope_dm,
    read_enable_inverted_index,
    inverted_index_raw_table_key_from_scope,
    resolve_instance_space_from_scope_document,
    resolve_scope_document_source_views,
    restrict_aliasing_pathways_to_wanted_names,
)


def test_build_key_extraction_merges_runtime() -> None:
    doc = {
        # Runtime instance_space is merged only when the view omits instance_space
        "source_views": [{"view_external_id": "CogniteFile"}],
        "key_extraction": {
            "externalId": "ctx_key_extraction_x",
            "config": {
                "parameters": {
                    "debug": True,
                    "raw_db": "db_key_extraction",
                    "raw_table_key": "my_suffix_key_extraction_state",
                },
                "data": {
                    "extraction_rules": [],
                },
            },
        },
    }
    out = build_key_extraction_workflow_config(
        doc,
        run_all=True,
        instance_space="sp1",
        incremental_change_processing=True,
    )
    assert out["externalId"] == "ctx_key_extraction_x"
    inner = out["config"]
    assert inner["parameters"]["raw_table_key"] == "my_suffix_key_extraction_state"
    assert inner["parameters"]["run_all"] is True
    assert inner["parameters"]["incremental_change_processing"] is True
    assert inner["data"]["source_views"][0]["instance_space"] == "sp1"


def test_restrict_pathways_keeps_only_wanted_rule_names() -> None:
    sec = {
        "pathways": {
            "steps": [
                {
                    "mode": "sequential",
                    "rules": [
                        {"name": "keep_a", "handler": "character_substitution"},
                        {"name": "drop_b", "handler": "character_substitution"},
                    ],
                }
            ]
        }
    }
    restrict_aliasing_pathways_to_wanted_names(sec, {"keep_a"})
    steps = sec["pathways"]["steps"]
    assert len(steps) == 1
    assert [r["name"] for r in steps[0]["rules"]] == ["keep_a"]


def test_narrow_engine_config_for_inline_rule_names() -> None:
    eng = {
        "rules": [{"name": "r1", "handler": "character_substitution"}],
        "pathways": {
            "steps": [
                {
                    "mode": "sequential",
                    "rules": [
                        {"name": "p1", "handler": "character_substitution"},
                        {"name": "p2", "handler": "character_substitution"},
                    ],
                }
            ]
        },
        "validation": {},
    }
    out = narrow_aliasing_engine_config_for_inline_rule_names(eng, ["p2"])
    assert out["rules"] == []
    assert [r["name"] for r in out["pathways"]["steps"][0]["rules"]] == ["p2"]
    assert len(eng["pathways"]["steps"][0]["rules"]) == 2


def test_filter_aliasing_rules_on_data_slices_pathways_for_task_payload() -> None:
    cfg = {
        "config": {
            "data": {
                "aliasing_rules": [],
                "pathways": {
                    "steps": [
                        {
                            "mode": "sequential",
                            "rules": [
                                {"name": "A", "handler": "character_substitution"},
                                {"name": "B", "handler": "character_substitution"},
                            ],
                        }
                    ]
                },
            }
        }
    }
    data = {"aliasing_rule_names": ["B"]}
    _filter_aliasing_rules_on_data(data, cfg)
    names = [r["name"] for r in cfg["config"]["data"]["pathways"]["steps"][0]["rules"]]
    assert names == ["B"]


def test_restrict_pathways_removes_key_when_no_rule_matches() -> None:
    sec = {
        "pathways": {
            "steps": [
                {
                    "mode": "sequential",
                    "rules": [{"name": "only_x", "handler": "character_substitution"}],
                }
            ]
        }
    }
    restrict_aliasing_pathways_to_wanted_names(sec, {"other"})
    assert "pathways" not in sec


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
                "data": {"aliasing_rules": [{"name": "r1", "handler": "regex_substitution"}]},
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


def test_ensure_aliasing_rebuilds_when_stub_config_missing_raw_parameters() -> None:
    """A non-empty ``data['config']`` stub must not skip scope merge (RAW db + tables)."""
    doc = {
        "source_views": [{"view_external_id": "CogniteFile", "instance_space": "sp1"}],
        "key_extraction": {
            "config": {
                "parameters": {
                    "raw_db": "db_key_extraction",
                    "raw_table_key": "key_extraction_state",
                },
                "data": {"extraction_rules": []},
            }
        },
        "aliasing": {
            "externalId": "ctx_aliasing_x",
            "config": {
                "parameters": {
                    "raw_db": "db_tag_aliasing",
                    "raw_table_aliases": "default_aliases",
                    "raw_table_state": "tag_aliasing_state",
                },
                "data": {
                    "pathways": {
                        "steps": [
                            {
                                "mode": "sequential",
                                "rules": [
                                    {
                                        "name": "Strip Delimiter",
                                        "handler": "regex_substitution",
                                    }
                                ],
                            }
                        ]
                    }
                },
            },
        },
    }
    stub = {
        "externalId": "noise",
        "config": {
            "data": {"pathways": {"steps": []}},
            "parameters": {},
        },
    }
    data: dict = {"configuration": doc, "config": stub}
    ensure_aliasing_config_from_scope_dm(data, None)
    inner = data["config"]["config"]
    assert inner["parameters"]["raw_db"] == "db_tag_aliasing"
    assert inner["parameters"]["raw_table_aliases"] == "default_aliases"
    assert data["config"]["externalId"] == "ctx_aliasing_x"


def test_inverted_index_raw_table_key_from_scope_convention() -> None:
    assert inverted_index_raw_table_key_from_scope({}, "key_extraction_state") == "inverted_index"
    assert (
        inverted_index_raw_table_key_from_scope({}, "site_a_key_extraction_state")
        == "site_a_inverted_index"
    )
    assert inverted_index_raw_table_key_from_scope({}, "other") == "other_inverted_index"


def test_inverted_index_raw_table_key_from_scope_explicit() -> None:
    assert (
        inverted_index_raw_table_key_from_scope(
            {"inverted_index_raw_table_key": "custom_idx"}, "site_a_key_extraction_state"
        )
        == "custom_idx"
    )


def test_read_enable_inverted_index() -> None:
    assert read_enable_inverted_index({}) is False
    assert (
        read_enable_inverted_index(
            {"key_extraction": {"config": {"parameters": {"enable_inverted_index": True}}}}
        )
        is True
    )


def _doc_with_source_views(views: list) -> dict:
    return {
        "source_views": views,
        "key_extraction": {
            "config": {
                "parameters": {"raw_table_key": "x_key_extraction_state"},
                "data": {"extraction_rules": []},
            }
        },
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
    data: dict = {"configuration": doc, "instance_space": "  explicit  "}
    assert ensure_instance_space_from_scope_document(data) == "explicit"
    assert data["instance_space"] == "explicit"

    data2: dict = {"configuration": doc}
    assert ensure_instance_space_from_scope_document(data2) == "from-doc"
    assert data2["instance_space"] == "from-doc"


def test_ensure_instance_space_accepts_legacy_scope_document_key() -> None:
    doc = _doc_with_source_views(
        [{"view_external_id": "CogniteFile", "instance_space": "legacy-sp"}]
    )
    data: dict = {"scope_document": doc}
    assert ensure_instance_space_from_scope_document(data) == "legacy-sp"


def test_build_inverted_index_config_block() -> None:
    doc = {
        "aliasing": {
            "config": {
                "parameters": {"debug": True},
                "data": {"aliasing_rules": [], "validation": {}},
            }
        }
    }
    blk = build_inverted_index_config_block(doc)
    assert "config" in blk
    assert blk["config"]["data"]["aliasing_rules"] == []

def test_ensure_key_extraction_materializes_configuration_when_config_preset() -> None:
    """Embedded ``config`` on the task must not block expanding ``configuration`` confidence refs."""
    data = {
        "configuration": {
            "source_views": [{"view_external_id": "CogniteAsset", "instance_space": "sp1"}],
            "validation_rule_definitions": {
                "bl": {
                    "name": "bl",
                    "priority": 1,
                    "match": {"keywords": ["bad"]},
                    "confidence_modifier": {"mode": "explicit", "value": 0},
                },
            },
            "key_extraction": {
                "externalId": "ke",
                "config": {
                    "parameters": {"raw_db": "d", "raw_table_key": "k"},
                    "data": {
                        "extraction_rules": [],
                        "validation": {"validation_rules": ["bl"]},
                    },
                },
            },
        },
        "config": {"prebuilt": True},
    }
    ensure_key_extraction_config_from_scope_dm(data, None, incremental_change_processing=True)
    conf = data["configuration"]
    assert "validation_rule_definitions" not in conf
    rules = conf["key_extraction"]["config"]["data"]["validation"]["validation_rules"]
    assert isinstance(rules[0], dict)
    assert rules[0].get("name") == "bl"


def test_resolve_scope_document_source_views_requires_non_empty_list() -> None:
    with pytest.raises(ValueError, match="top-level source_views"):
        resolve_scope_document_source_views({})
    with pytest.raises(ValueError, match="top-level source_views"):
        resolve_scope_document_source_views({"source_views": []})
    out = resolve_scope_document_source_views({"source_views": [{"view_external_id": "X"}]})
    assert len(out) == 1
    assert out[0]["view_external_id"] == "X"

