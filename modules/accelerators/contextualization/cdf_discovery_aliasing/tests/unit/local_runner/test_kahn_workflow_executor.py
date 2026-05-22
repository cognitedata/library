"""Tests for local Kahn-style workflow helpers."""

from __future__ import annotations

import logging
import sys
from argparse import Namespace
from pathlib import Path
from threading import Lock
from typing import Any
from unittest.mock import patch

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
_SCRIPTS = _MODULE_ROOT / "scripts"
for _p in (str(_FUNCS), str(_SCRIPTS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cdf_fn_common.workflow_execution_graph import ExecutionGraph  # noqa: E402
from local_runner.kahn_run_context import KahnRunContext  # noqa: E402
from local_runner.kahn_workflow_executor import (  # noqa: E402
    _discovery_cohort_row_index_getter,
    _discovery_raw_hash_index_getter,
    _dispatch_task,
    _dispatch_task_tracked,
    should_validate_macro_execution_graph,
)


def test_discovery_raw_hash_index_getter_builds_once_per_table(monkeypatch) -> None:
    """Parallel view-query tasks should share one build_latest_hash_index_for_table call per sink."""
    build_calls = {"n": 0}

    def _fake_build(_client, _db, _tbl, *, workflow_scope="", chunk_size=2500):
        del _client, chunk_size
        build_calls["n"] += 1
        return {"sk": {"n1": f"h1_{workflow_scope}"}}

    monkeypatch.setattr(
        "cdf_fn_common.incremental_scope.build_latest_hash_index_for_table",
        _fake_build,
    )
    ctx = KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_hash_cache"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={},
        run_id="r",
    )
    getter = _discovery_raw_hash_index_getter(ctx)
    assert getter(None, "db1", "t1", "wf_a") == {"sk": {"n1": "h1_wf_a"}}
    assert getter(None, "db1", "t1", "wf_a") == {"sk": {"n1": "h1_wf_a"}}
    assert build_calls["n"] == 1
    getter(None, "db1", "t1", "wf_b")
    assert build_calls["n"] == 2
    getter(None, "db2", "t2", "")
    assert build_calls["n"] == 3


def test_discovery_cohort_row_index_getter_builds_once_per_table(monkeypatch) -> None:
    build_calls = {"n": 0}

    def _fake_build(_client, _db, _tbl, *, chunk_size=2500):
        del _client, chunk_size
        build_calls["n"] += 1
        return {"rk": object()}

    monkeypatch.setattr(
        "cdf_fn_common.cohort_storage.build_cohort_row_index",
        _fake_build,
    )
    ctx = KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_cohort_cache"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={},
        run_id="r",
    )
    getter = _discovery_cohort_row_index_getter(ctx)
    assert getter(None, "db1", "t1")
    assert getter(None, "db1", "t1")
    assert build_calls["n"] == 1
    getter(None, "db2", "t2")
    assert build_calls["n"] == 2


def test_should_validate_macro_execution_graph_true_for_template_dag(monkeypatch) -> None:
    """Macro graph check compares task ids to ``load_execution_graph`` nodes (stub graph, not repo file)."""

    def _stub_execution_graph(_path: Path) -> ExecutionGraph:
        return ExecutionGraph(
            schema_version=1,
            description="stub for test",
            nodes=["kea__vq", "kea__tr", "kea__va", "kea__ii", "kea__discovery_raw_cleanup"],
            node_roles={},
            edges=[],
        )

    monkeypatch.setattr(
        "local_runner.kahn_workflow_executor.load_execution_graph",
        _stub_execution_graph,
    )
    cw = {
        "tasks": [
            {"id": "kea__vq", "function_external_id": "fn_dm_view_query", "depends_on": []},
            {"id": "kea__tr", "function_external_id": "fn_dm_transform", "depends_on": ["kea__vq"]},
            {"id": "kea__va", "function_external_id": "fn_dm_validate", "depends_on": ["kea__tr"]},
            {"id": "kea__ii", "function_external_id": "fn_dm_inverted_index", "depends_on": ["kea__va"]},
            {
                "id": "kea__discovery_raw_cleanup",
                "function_external_id": "fn_dm_discovery_raw_cleanup",
                "depends_on": ["kea__ii"],
            },
        ]
    }
    assert should_validate_macro_execution_graph(cw) is True


def test_should_validate_macro_execution_graph_false_for_partial_dag() -> None:
    cw = {
        "tasks": [
            {
                "id": "kea__vq",
                "function_external_id": "fn_dm_view_query",
                "depends_on": [],
            }
        ]
    }
    assert should_validate_macro_execution_graph(cw) is False


def test_dispatch_task_routes_discovery_view_query_to_pipeline() -> None:
    recorded: list[tuple[str, Any]] = []

    class _FakePipeline:
        @staticmethod
        def query_view(client: Any, logger: Any, data: dict, cdf_config: Any) -> None:
            del client, logger, cdf_config
            recorded.append((str(data.get("task_id") or ""), data.get("config")))

    def _import(name: str):
        if name == "fn_dm_view_query.pipeline":
            return _FakePipeline()
        raise AssertionError(f"unexpected import {name!r}")

    ctx = KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_kahn_discovery"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "kea__vq",
                    "function_external_id": "fn_dm_view_query",
                    "depends_on": [],
                    "payload": {
                        "config": {
                            "description": "stub",
                            "view_external_id": "CogniteFile",
                            "view_space": "cdf_cdm",
                            "view_version": "v1",
                        }
                    },
                    "pipeline_node_id": "kea__vq",
                }
            ]
        },
        run_id="run_discovery_test",
    )
    with patch("local_runner.kahn_workflow_executor.importlib.import_module", side_effect=_import):
        _dispatch_task(ctx, "kea__vq", Lock())
    assert recorded == [
        (
            "kea__vq",
            {
                "description": "stub",
                "view_external_id": "CogniteFile",
                "view_space": "cdf_cdm",
                "view_version": "v1",
            },
        )
    ]
    assert "kea__vq" in ctx.discovery_task_outputs
    out = ctx.discovery_task_outputs["kea__vq"]
    assert set(out.keys()) == {"status", "message"}
    assert ctx.handler_data_snapshots == {}


def test_dispatch_task_captures_view_save_handler_data() -> None:
    class _FakeSave:
        @staticmethod
        def save_view(client: Any, logger: Any, data: dict, cdf_config: Any) -> None:
            del client, logger, cdf_config
            data["status"] = "succeeded"
            data["message"] = "{}"

    def _import(name: str):
        if name == "fn_dm_view_save.pipeline":
            return _FakeSave()
        raise AssertionError(f"unexpected import {name!r}")

    ctx = KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_kahn_view_save"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={"x": 1},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "vs1",
                    "function_external_id": "fn_dm_view_save",
                    "depends_on": [],
                    "payload": {
                        "config": {
                            "view_external_id": "CogniteDescribable",
                            "view_space": "cdf_cdm",
                            "view_version": "v1",
                        }
                    },
                }
            ]
        },
        run_id="run_save_test",
    )
    with patch("local_runner.kahn_workflow_executor.importlib.import_module", side_effect=_import):
        _dispatch_task(ctx, "vs1", Lock())
    cap = ctx.handler_data_snapshots["vs1"]
    assert cap["task_id"] == "vs1"
    assert cap["function_external_id"] == "fn_dm_view_save"
    assert cap["handler_data"]["task_id"] == "vs1"
    assert cap["handler_data"]["config"]["view_external_id"] == "CogniteDescribable"
    assert "cohort_snapshot" in cap


def test_dispatch_task_captures_inverted_index_handler_data() -> None:
    class _FakeII:
        @staticmethod
        def inverted_index(client: Any, logger: Any, data: dict, cdf_config: Any) -> None:
            del client, logger, cdf_config
            data["status"] = "succeeded"
            data["message"] = '{"stub":true}'

    def _import(name: str):
        if name == "fn_dm_inverted_index.pipeline":
            return _FakeII()
        raise AssertionError(f"unexpected import {name!r}")

    ctx = KahnRunContext(
        args=Namespace(run_all=False),
        logger=logging.getLogger("test_kahn_ii"),
        client=None,
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "ii1",
                    "function_external_id": "fn_dm_inverted_index",
                    "depends_on": [],
                }
            ]
        },
        run_id="run_ii_test",
    )
    with patch("local_runner.kahn_workflow_executor.importlib.import_module", side_effect=_import):
        _dispatch_task(ctx, "ii1", Lock())
    cap = ctx.handler_data_snapshots["ii1"]
    assert cap["task_id"] == "ii1"
    assert cap["function_external_id"] == "fn_dm_inverted_index"
    assert cap["handler_data"]["task_id"] == "ii1"
    assert "cohort_snapshot" in cap
    assert "inverted_index_persistence" in cap["cohort_snapshot"]


def test_dispatch_cleanup_snapshots_raw_results_before_purge() -> None:
    order: list[str] = []

    class _FakeCleanup:
        @staticmethod
        def discovery_raw_cleanup(client: Any, logger: Any, data: dict, cdf_config: Any) -> None:
            del client, logger, cdf_config
            order.append("cleanup")
            data["status"] = "succeeded"
            data["message"] = "{}"

    def _import(name: str):
        if name == "fn_dm_discovery_raw_cleanup.pipeline":
            return _FakeCleanup()
        raise AssertionError(f"unexpected import {name!r}")

    def _snapshot(ctx: KahnRunContext) -> None:
        order.append("snapshot")
        ctx.raw_results_snapshot = {"tables": [{"row_count": 3}]}

    ctx = KahnRunContext(
        args=Namespace(run_all=True, raw_results_rows=10, raw_results_max_tables=5),
        logger=logging.getLogger("test_kahn_cleanup"),
        client=object(),
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "clean1",
                    "function_external_id": "fn_dm_discovery_raw_cleanup",
                    "depends_on": [],
                }
            ]
        },
        run_id="run_cleanup_test",
    )
    with (
        patch(
            "local_runner.kahn_workflow_executor.snapshot_raw_results_for_ctx",
            side_effect=_snapshot,
        ),
        patch("local_runner.kahn_workflow_executor.importlib.import_module", side_effect=_import),
    ):
        _dispatch_task(ctx, "clean1", Lock())
    assert order == ["snapshot", "cleanup"]
    assert ctx.raw_results_snapshot == {"tables": [{"row_count": 3}]}


def test_dispatch_task_tracked_retries_then_succeeds(monkeypatch) -> None:
    attempts = {"n": 0}

    def _import(name: str):
        if name == "fn_dm_transform.pipeline":

            class _M:
                @staticmethod
                def transform(client, logger, data, cdf_config):
                    del client, logger, cdf_config
                    attempts["n"] += 1
                    if attempts["n"] < 2:
                        raise RuntimeError("transient")
                    data["status"] = "succeeded"
                    data["message"] = "{}"

            return _M()
        raise AssertionError(name)

    monkeypatch.setenv("KEA_LOCAL_TASK_RETRY_DELAY_SEC", "0")
    ctx = KahnRunContext(
        args=Namespace(run_all=False, local_task_retries=2),
        logger=logging.getLogger("test_kahn_retry"),
        client=object(),
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "tr1",
                    "function_external_id": "fn_dm_transform",
                    "depends_on": [],
                }
            ]
        },
        run_id="run_retry",
    )
    with patch(
        "local_runner.kahn_workflow_executor.importlib.import_module",
        side_effect=_import,
    ):
        _dispatch_task_tracked(ctx, "tr1", Lock())
    assert attempts["n"] == 2
    assert ctx.local_run_tasks[-1]["status"] == "succeeded"


def test_dispatch_cleanup_skip_task_continues_after_failure(monkeypatch) -> None:
    def _import(name: str):
        if name == "fn_dm_discovery_raw_cleanup.pipeline":

            class _M:
                @staticmethod
                def discovery_raw_cleanup(client, logger, data, cdf_config):
                    del client, logger, cdf_config
                    raise RuntimeError("cleanup failed")

            return _M()
        raise AssertionError(name)

    monkeypatch.setenv("KEA_LOCAL_TASK_RETRY_DELAY_SEC", "0")
    ctx = KahnRunContext(
        args=Namespace(run_all=False, local_task_retries=0),
        logger=logging.getLogger("test_kahn_skip"),
        client=object(),
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp_test",
        source_views=[],
        cdf_config=None,
        compiled_workflow={
            "tasks": [
                {
                    "id": "clean1",
                    "function_external_id": "fn_dm_discovery_raw_cleanup",
                    "depends_on": [],
                }
            ]
        },
        run_id="run_skip",
    )
    with (
        patch(
            "local_runner.kahn_workflow_executor.snapshot_raw_results_for_ctx",
        ),
        patch(
            "local_runner.kahn_workflow_executor.importlib.import_module",
            side_effect=_import,
        ),
    ):
        _dispatch_task_tracked(ctx, "clean1", Lock())
    assert ctx.local_run_tasks[-1]["status"] == "completed_with_errors"
    assert ctx.pipeline_warnings
