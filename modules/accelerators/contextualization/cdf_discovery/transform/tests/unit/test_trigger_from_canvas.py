from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from workflow_build.trigger_from_canvas import (  # noqa: E402
    apply_start_trigger_to_workflow_trigger,
    read_start_trigger_config,
)


def test_read_start_trigger_schedule_cron() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {
                    "config": {
                        "trigger_type": "schedule",
                        "cron_expression": "15 3 * * *",
                        "workflow_version": "2",
                        "incremental_change_processing": False,
                    }
                },
            }
        ]
    }
    cfg = read_start_trigger_config(canvas, default_cron="0 2 * * *")
    assert cfg["trigger_rule"] == {"triggerType": "schedule", "cronExpression": "15 3 * * *"}
    assert cfg["workflow_version"] == "2"
    assert cfg["incremental_change_processing"] is False


def test_apply_start_trigger_patches_workflow_trigger_doc() -> None:
    doc: dict = {
        "externalId": "trg_old",
        "workflowExternalId": "wf_old",
        "workflowVersion": "1",
        "triggerRule": {"triggerType": "schedule", "cronExpression": "0 2 * * *"},
        "input": {"run_id": "", "configuration": {}},
    }
    cfg = {
        "workflow_version": "3",
        "trigger_rule": {"triggerType": "schedule", "cronExpression": "0 6 * * *"},
        "incremental_change_processing": False,
        "cohort_write_batch_size": 250,
    }
    apply_start_trigger_to_workflow_trigger(doc, workflow_external_id="wf_test", trigger_cfg=cfg)
    assert doc["externalId"] == "trg_wf_test"
    assert doc["workflowExternalId"] == "wf_test"
    assert doc["workflowVersion"] == "3"
    assert doc["triggerRule"]["cronExpression"] == "0 6 * * *"
    assert doc["input"]["incremental_change_processing"] is False
    assert doc["input"]["cohort_write_batch_size"] == 250


def test_apply_start_trigger_does_not_override_existing_incremental_flag() -> None:
    doc: dict = {
        "externalId": "trg_old",
        "workflowExternalId": "wf_old",
        "workflowVersion": "1",
        "triggerRule": {"triggerType": "schedule", "cronExpression": "0 2 * * *"},
        "input": {"incremental_change_processing": True, "run_id": "", "configuration": {}},
    }
    cfg = {
        "workflow_version": "3",
        "trigger_rule": {"triggerType": "schedule", "cronExpression": "0 6 * * *"},
        "incremental_change_processing": False,
    }
    apply_start_trigger_to_workflow_trigger(doc, workflow_external_id="wf_test", trigger_cfg=cfg)
    assert doc["input"]["incremental_change_processing"] is True


def test_read_start_trigger_record_stream() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {
                    "config": {
                        "trigger_type": "recordStream",
                        "stream_external_id": "stream-1",
                        "batch_size": 50,
                        "batch_timeout": 120,
                    }
                },
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    rule = cfg["trigger_rule"]
    assert rule["triggerType"] == "recordStream"
    assert rule["streamExternalId"] == "stream-1"
    assert rule["batchSize"] == 50
    assert rule["batchTimeout"] == 120


def test_read_start_trigger_data_modeling() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {
                    "config": {
                        "trigger_type": "dataModeling",
                        "batch_size": 25,
                        "batch_timeout": 30,
                        "data_modeling_query": {"with": {}, "select": {}},
                    }
                },
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    rule = cfg["trigger_rule"]
    assert rule["triggerType"] == "dataModeling"
    assert rule["batchSize"] == 25
    assert rule["batchTimeout"] == 30
    assert isinstance(rule["dataModelingQuery"], dict)


def test_read_start_trigger_parses_string_false_incremental_flag() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {"config": {"incremental_change_processing": "false"}},
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    assert cfg["incremental_change_processing"] is False


def test_read_start_trigger_includes_cohort_write_batch_size() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {"config": {"cohort_write_batch_size": "123"}},
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    assert cfg["cohort_write_batch_size"] == 123


def test_read_start_trigger_includes_workflow_scope() -> None:
    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {"config": {"workflow_scope": "aliasing_workflow"}},
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    assert cfg["workflow_scope"] == "aliasing_workflow"
