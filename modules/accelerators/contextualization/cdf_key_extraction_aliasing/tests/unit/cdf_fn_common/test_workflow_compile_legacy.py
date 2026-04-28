"""Tests for ``cdf_fn_common.workflow_compile`` legacy IR + codegen."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402
from cdf_fn_common.workflow_compile.legacy_ir import (  # noqa: E402
    TASK_ALIAS_PERSISTENCE,
    TASK_ALIASING,
    TASK_INCREMENTAL,
    TASK_KEY_EXTRACTION,
    TASK_REFERENCE_INDEX,
    compile_legacy_configuration,
    compiled_workflow_signature,
)


@pytest.fixture
def template_scope() -> dict:
    root = Path(__file__).resolve().parents[3]
    p = root / "workflow_template" / "workflow.template.config.yaml"
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def test_compile_legacy_task_order_and_functions(template_scope: dict) -> None:
    cw = compile_legacy_configuration(template_scope)
    assert cw["schemaVersion"] == 1
    assert cw.get("dag_source") == "legacy"
    ids = [t["id"] for t in cw["tasks"]]
    assert ids == [
        TASK_INCREMENTAL,
        TASK_KEY_EXTRACTION,
        TASK_REFERENCE_INDEX,
        TASK_ALIASING,
        TASK_ALIAS_PERSISTENCE,
    ]
    assert cw["tasks"][1]["depends_on"] == [TASK_INCREMENTAL]
    assert cw["tasks"][2]["depends_on"] == [TASK_KEY_EXTRACTION]
    assert cw["tasks"][3]["depends_on"] == [TASK_KEY_EXTRACTION]
    assert cw["tasks"][4]["depends_on"] == [TASK_ALIASING]


def test_compile_legacy_reference_index_persistence(template_scope: dict) -> None:
    cw = compile_legacy_configuration(template_scope)
    ref = next(t for t in cw["tasks"] if t["id"] == TASK_REFERENCE_INDEX)
    assert ref["function_external_id"] == "fn_dm_reference_index"
    pers = ref.get("persistence") or {}
    assert pers.get("source_raw_db")
    assert pers.get("source_raw_table_key")
    assert "enable_reference_index" in pers


def test_compiled_workflow_signature_stable(template_scope: dict) -> None:
    a = compiled_workflow_signature(compile_legacy_configuration(template_scope))
    b = compiled_workflow_signature(compile_legacy_configuration(template_scope))
    assert a == b


def test_canvas_persistence_overrides_alias_task(template_scope: dict) -> None:
    doc = dict(template_scope)
    doc["canvas"] = {
        "schemaVersion": 1,
        "nodes": [
            {
                "id": "n_ap",
                "kind": "alias_persistence",
                "position": {"x": 0, "y": 0},
                "data": {
                    "persistence_config": {
                        "kind": "alias_persistence",
                        "raw_db": "db_custom_aliases",
                    },
                },
            }
        ],
        "edges": [],
    }
    cw = compile_legacy_configuration(doc)
    ap = next(t for t in cw["tasks"] if t["id"] == TASK_ALIAS_PERSISTENCE)
    assert ap["persistence"]["raw_db"] == "db_custom_aliases"


def test_build_workflow_version_document_tasks(template_scope: dict) -> None:
    cw = compile_legacy_configuration(template_scope)
    wv = build_workflow_version_document(
        workflow_external_id="key_extraction_aliasing",
        version="v5",
        compiled_workflow=cw,
    )
    assert wv["version"] == "v5"
    tasks = wv["workflowDefinition"]["tasks"]
    assert len(tasks) == 5
    assert tasks[0]["externalId"] == TASK_INCREMENTAL
    assert tasks[0]["parameters"]["function"]["externalId"] == "fn_dm_incremental_state_update"
    assert tasks[1]["dependsOn"] == [{"externalId": TASK_INCREMENTAL}]
