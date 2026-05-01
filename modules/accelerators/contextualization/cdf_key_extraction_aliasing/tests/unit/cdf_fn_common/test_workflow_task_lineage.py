"""Tests for workflow predecessor extraction lineage."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_task_lineage import (  # noqa: E402
    allowed_extraction_rule_names_for_task,
    apply_predecessor_extraction_allowlist_to_task_data,
    filter_entities_keys_extracted_by_rules,
    raw_row_allowed_for_predecessor_extraction_rules,
    transitive_predecessor_task_ids,
)
from cdf_fn_common.workflow_compile.legacy_ir import TASK_INCREMENTAL  # noqa: E402


def _cw_two_ke_one_al() -> dict:
    return {
        "tasks": [
            {
                "id": TASK_INCREMENTAL,
                "function_external_id": "fn_dm_incremental_state_update",
                "depends_on": [],
                "payload": {},
            },
            {
                "id": "kea__ex_a",
                "function_external_id": "fn_dm_key_extraction",
                "depends_on": [TASK_INCREMENTAL],
                "payload": {"extraction_rule_names": ["rule_a"]},
            },
            {
                "id": "kea__ex_b",
                "function_external_id": "fn_dm_key_extraction",
                "depends_on": [TASK_INCREMENTAL],
                "payload": {"extraction_rule_names": ["rule_b"]},
            },
            {
                "id": "kea__al",
                "function_external_id": "fn_dm_aliasing",
                "depends_on": ["kea__ex_a"],
                "payload": {"aliasing_rule_names": ["x"]},
            },
        ]
    }


def test_transitive_predecessors_include_incremental_and_ke() -> None:
    cw = _cw_two_ke_one_al()
    preds = transitive_predecessor_task_ids(cw, "kea__al")
    assert TASK_INCREMENTAL in preds
    assert "kea__ex_a" in preds
    assert "kea__ex_b" not in preds


def test_allowed_rules_union_from_ke_predecessors() -> None:
    cw = _cw_two_ke_one_al()
    assert allowed_extraction_rule_names_for_task(cw, "kea__al") == {"rule_a"}


def test_filter_entities_keys_extracted() -> None:
    eke = {
        "e1": {
            "keys": {
                "f": {
                    "tag1": {"rule_name": "rule_a", "extraction_type": "candidate_key"},
                    "tag2": {"rule_name": "rule_b", "extraction_type": "candidate_key"},
                }
            },
            "foreign_key_references": [{"value": "v1", "rule_id": "rule_b"}],
            "document_references": [],
        }
    }
    out = filter_entities_keys_extracted_by_rules(eke, {"rule_a"})
    assert "e1" in out
    assert list(out["e1"]["keys"]["f"].keys()) == ["tag1"]
    assert out["e1"]["foreign_key_references"] == []


def test_raw_row_allowed_intersection() -> None:
    cols = {"RULES_USED_JSON": '["rule_a"]'}
    assert raw_row_allowed_for_predecessor_extraction_rules(
        cols, fk_list=None, doc_list=None, allowed={"rule_a", "rule_c"}
    )
    assert not raw_row_allowed_for_predecessor_extraction_rules(
        cols, fk_list=None, doc_list=None, allowed={"rule_b"}
    )


def test_apply_predecessor_drops_entities_with_only_non_predecessor_rules() -> None:
    cw = _cw_two_ke_one_al()
    data = {
        "task_id": "kea__al",
        "compiled_workflow": cw,
        "entities_keys_extracted": {
            "e1": {
                "keys": {
                    "f": {
                        "tag_b": {"rule_name": "rule_b", "extraction_type": "candidate_key"},
                    }
                },
                "foreign_key_references": [],
                "document_references": [],
            },
        },
    }
    apply_predecessor_extraction_allowlist_to_task_data(data)
    assert data["entities_keys_extracted"] == {}


def test_apply_predecessor_filters_to_predecessor_rules() -> None:
    cw = _cw_two_ke_one_al()
    data = {
        "task_id": "kea__al",
        "compiled_workflow": cw,
        "entities_keys_extracted": {
            "e1": {
                "keys": {
                    "f": {
                        "ok": {"rule_name": "rule_a", "extraction_type": "candidate_key"},
                        "no": {"rule_name": "rule_b", "extraction_type": "candidate_key"},
                    }
                },
                "foreign_key_references": [],
                "document_references": [],
            },
        },
    }
    apply_predecessor_extraction_allowlist_to_task_data(data)
    assert list(data["entities_keys_extracted"]["e1"]["keys"]["f"].keys()) == ["ok"]
