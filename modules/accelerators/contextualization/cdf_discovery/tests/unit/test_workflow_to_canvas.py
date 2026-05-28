"""Tests for CDF workflow → transform canvas import."""

from __future__ import annotations

from ui.server.workflow_to_canvas import (
    cdf_trigger_to_start_config,
    suggest_pipeline_id,
    workflow_graph_to_canvas,
)


def test_suggest_pipeline_id_strips_wf_prefix() -> None:
    assert suggest_pipeline_id("wf_all_etl_demo") == "all_etl_demo"


def test_workflow_graph_to_canvas_maps_function_tasks_and_edges() -> None:
    graph = {
        "workflow": {
            "external_id": "wf_demo",
            "version": "2",
            "description": "Demo workflow",
        },
        "tasks": [
            {
                "external_id": "query_assets",
                "name": "Query assets",
                "type": "function",
                "description": "Load assets",
                "parameters": {
                    "function": {
                        "externalId": "fn_etl_view_query",
                        "data": {"task_id": "query_assets"},
                    }
                },
            },
            {
                "external_id": "save_rows",
                "name": "Save rows",
                "type": "function",
                "parameters": {
                    "function": {
                        "externalId": "fn_etl_view_save",
                        "data": {"task_id": "save_rows"},
                    }
                },
            },
        ],
        "edges": [
            {"id": "query_assets->save_rows", "from": "query_assets", "to": "save_rows", "label": ""},
        ],
    }
    canvas = workflow_graph_to_canvas(graph, workflow_external_id="wf_demo")
    kinds = {n["id"]: n["kind"] for n in canvas["nodes"]}
    assert kinds["start"] == "start"
    assert kinds["end"] == "end"
    assert kinds["query_assets"] == "query_view"
    assert kinds["save_rows"] == "save_view"

    start_cfg = next(n for n in canvas["nodes"] if n["id"] == "start")["data"]["config"]
    assert start_cfg["workflow_external_id"] == "wf_demo"
    assert start_cfg["workflow_version"] == "2"

    edge_pairs = {(e["source"], e["target"]) for e in canvas["edges"]}
    assert ("start", "query_assets") in edge_pairs
    assert ("query_assets", "save_rows") in edge_pairs
    assert ("save_rows", "end") in edge_pairs


def test_cdf_trigger_to_start_config_schedule() -> None:
    trigger = {
        "external_id": "trg_wf_demo",
        "workflow_external_id": "wf_demo",
        "workflow_version": "3",
        "input": {"incremental_change_processing": False, "run_id": "run-1"},
        "trigger_rule": {
            "trigger_type": "schedule",
            "cron_expression": "0 4 * * *",
        },
    }
    cfg = cdf_trigger_to_start_config(
        trigger,
        workflow_external_id="wf_demo",
        workflow_version="1",
        description="From CDF",
    )
    assert cfg["trigger_type"] == "schedule"
    assert cfg["cron_expression"] == "0 4 * * *"
    assert cfg["trigger_external_id"] == "trg_wf_demo"
    assert cfg["workflow_version"] == "3"
    assert cfg["incremental_change_processing"] is False
    assert cfg["run_id"] == "run-1"
    assert cfg["description"] == "From CDF"


def test_cdf_trigger_to_start_config_record_stream() -> None:
    trigger = {
        "external_id": "trg_wf_rs",
        "trigger_rule": {
            "triggerType": "recordStream",
            "streamExternalId": "my-stream",
            "batchSize": 200,
            "batchTimeout": 120,
        },
    }
    cfg = cdf_trigger_to_start_config(
        trigger,
        workflow_external_id="wf_rs",
        workflow_version="1",
    )
    assert cfg["trigger_type"] == "recordStream"
    assert cfg["stream_external_id"] == "my-stream"
    assert cfg["batch_size"] == 200
    assert cfg["batch_timeout"] == 120


def test_workflow_graph_to_canvas_uses_trigger_config() -> None:
    graph = {
        "workflow": {"external_id": "wf_demo", "version": "2", "description": "Demo"},
        "tasks": [],
        "edges": [],
    }
    trigger = {
        "external_id": "trg_wf_demo",
        "trigger_rule": {"trigger_type": "schedule", "cron_expression": "15 6 * * *"},
        "input": {"incremental_change_processing": True},
    }
    canvas = workflow_graph_to_canvas(
        graph,
        workflow_external_id="wf_demo",
        workflow_trigger=trigger,
    )
    start_cfg = next(n for n in canvas["nodes"] if n["id"] == "start")["data"]["config"]
    assert start_cfg["cron_expression"] == "15 6 * * *"
    assert start_cfg["trigger_external_id"] == "trg_wf_demo"


def test_workflow_graph_to_canvas_json_mapping_task() -> None:
    graph = {
        "workflow": {"external_id": "wf_map", "version": "1"},
        "tasks": [
            {
                "external_id": "map_step",
                "type": "jsonMapping",
                "parameters": {
                    "jsonMapping": {
                        "input": {"x": 1},
                        "expression": "x + 1",
                    }
                },
            }
        ],
        "edges": [],
    }
    canvas = workflow_graph_to_canvas(graph, workflow_external_id="wf_map")
    node = next(n for n in canvas["nodes"] if n["id"] == "map_step")
    assert node["kind"] == "json_mapping"
    assert node["data"]["config"]["expression"] == "x + 1"
