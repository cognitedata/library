"""Tests for local Kahn-style workflow helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
_SCRIPTS = _MODULE_ROOT / "scripts"
for _p in (str(_FUNCS), str(_SCRIPTS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cdf_fn_common.workflow_compile.legacy_ir import TASK_INCREMENTAL  # noqa: E402
from cdf_fn_common.workflow_task_lineage import (  # noqa: E402
    apply_predecessor_extraction_allowlist_to_task_data,
    allowed_extraction_rule_names_for_task,
)
from local_runner.kahn_workflow_executor import (  # noqa: E402
    _alias_persistence_topological_layers,
    _inverted_index_aliasing_data_from_engine_config,
)


def test_inverted_index_payload_includes_pathways_when_flat_rules_empty() -> None:
    acfg = {
        "rules": [],
        "validation": {"min_confidence": 0.01},
        "pathways": {
            "steps": [
                {
                    "mode": "sequential",
                    "rules": [
                        {
                            "name": "asset_only",
                            "handler": "character_substitution",
                            "config": {"substitutions": {"-": "_"}},
                        }
                    ],
                }
            ]
        },
    }
    data = _inverted_index_aliasing_data_from_engine_config(acfg)
    assert data["aliasing_rules"] == []
    assert "pathways" in data
    assert len((data["pathways"]["steps"][0]["rules"])) == 1


def test_predecessor_allowlist_keeps_only_dag_predecessor_extraction_rules() -> None:
    """Aliasing task that depends only on extraction A must not drop A's rules when strict is on."""
    cw = {
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
                "payload": {},
            },
        ]
    }
    assert allowed_extraction_rule_names_for_task(cw, "kea__al") == {"rule_a"}
    merged = {
        "e1": {
            "keys": {
                "f": {
                    "t_a": {"rule_name": "rule_a", "extraction_type": "candidate_key"},
                    "t_b": {"rule_name": "rule_b", "extraction_type": "candidate_key"},
                }
            },
            "foreign_key_references": [],
            "document_references": [],
        }
    }
    alias_data = {
        "task_id": "kea__al",
        "compiled_workflow": cw,
        "entities_keys_extracted": merged,
    }
    apply_predecessor_extraction_allowlist_to_task_data(alias_data)
    keys = alias_data["entities_keys_extracted"]["e1"]["keys"]["f"]
    assert set(keys) == {"t_a"}


def test_alias_persistence_layers_respect_depends_on_not_alphabetical_order() -> None:
    """Deferred persistence must follow DAG edges; id sort would run ``a_second`` before ``z_first``."""
    cw = {
        "tasks": [
            {
                "id": "a_second",
                "function_external_id": "fn_dm_alias_persistence",
                "depends_on": ["z_first"],
            },
            {
                "id": "z_first",
                "function_external_id": "fn_dm_alias_persistence",
                "depends_on": [],
            },
        ]
    }
    layers = _alias_persistence_topological_layers(cw)
    assert layers == [["z_first"], ["a_second"]]
