"""Tests for local DAG parallelism and thread-safe progress."""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from local_runner.kahn_workflow_executor import (  # noqa: E402
    _seed_in_memory_predecessors,
    run_compiled_workflow_dag,
)
from local_runner.parallel import resolve_max_workers, run_parallel  # noqa: E402
from local_runner.progress_writer import emit_ui_progress_locked  # noqa: E402


def test_resolve_max_workers_defaults() -> None:
    assert resolve_max_workers(layer_size=10, override=None) == 4
    assert resolve_max_workers(layer_size=2, override=None) == 2
    assert resolve_max_workers(layer_size=10, override=1) == 1
    assert resolve_max_workers(layer_size=10, override=0) == 4


def test_resolve_max_workers_from_configuration() -> None:
    assert resolve_max_workers(
        layer_size=8,
        override=None,
        configuration={"parameters": {"local_max_workers": 3}},
    ) == 3


def test_run_parallel_invokes_all_workers() -> None:
    seen: list[int] = []
    lock = threading.Lock()

    def worker(n: int) -> int:
        with lock:
            seen.append(n)
        return n * 2

    out = run_parallel([1, 2, 3], worker, max_workers=3)
    assert sorted(seen) == [1, 2, 3]
    assert out == [2, 4, 6]


def test_seed_in_memory_predecessors_from_buffers() -> None:
    data: dict = {
        "compiled_workflow": {
            "tasks": [
                {"id": "left", "depends_on": []},
                {"id": "right", "depends_on": ["left"]},
            ]
        },
        "etl_task_row_buffers": {
            "left": [{"columns": {}, "properties": {"x": 1}}],
        },
    }
    _seed_in_memory_predecessors(data, "right")
    rows = data.get("_predecessor_rows")
    assert isinstance(rows, list) and len(rows) == 1
    assert rows[0]["properties"]["x"] == 1


def test_parallel_layer_runs_sibling_query_tasks() -> None:
    compiled = {
        "schema_version": 1,
        "tasks": [
            {
                "id": "q1",
                "task_type": "function",
                "executable_kind": "query_view",
                "function_external_id": "fn_etl_view_query",
                "depends_on": [],
                "payload": {"config": {"view_external_id": "Asset"}},
            },
            {
                "id": "q2",
                "task_type": "function",
                "executable_kind": "query_view",
                "function_external_id": "fn_etl_view_query",
                "depends_on": [],
                "payload": {"config": {"view_external_id": "Asset"}},
            },
        ],
    }
    shared = {
        "configuration": {},
        "compiled_workflow": compiled,
        "dry_run": True,
        "local_predecessor_mode": "in_memory",
    }
    summaries = run_compiled_workflow_dag(
        compiled,
        client=None,
        logger=__import__("logging").getLogger("test"),
        shared_data=shared,
        dry_run=True,
        max_workers=4,
    )
    assert "q1" in summaries and "q2" in summaries


def test_emit_ui_progress_locked_writes_valid_json_lines(monkeypatch) -> None:
    monkeypatch.setenv("KEA_UI_PROGRESS_FD", "1")
    lines: list[bytes] = []
    lock = threading.Lock()

    def fake_write(fd: int, data: bytes) -> int:
        del fd
        with lock:
            lines.append(data)
        return len(data)

    monkeypatch.setattr("local_runner.progress_writer.os.write", fake_write)

    def worker(i: int) -> None:
        emit_ui_progress_locked("task_progress", task_id=f"t{i}", progress_current=i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(lines) == 20
    for raw in lines:
        obj = json.loads(raw.decode("utf-8").strip())
        assert obj["event"] == "task_progress"
        assert "task_id" in obj


def test_dynamic_fanout_parallel_children() -> None:
    from local_runner import dynamic_fanout

    calls: list[str] = []
    call_lock = threading.Lock()

    def fake_fn(fn_ext: str, data: dict, client: Any, logger: Any) -> dict:
        del fn_ext, client, logger
        tid = str(data.get("task_id") or "")
        with call_lock:
            calls.append(tid)
        return {"status": "ok", "annotation_rows": 1}

    compiled_child_specs = [
        {
            "externalId": "child_a",
            "parameters": {
                "function": {
                    "externalId": "fn_etl_file_annotation",
                    "data": {"task_id": "child_a"},
                }
            },
        },
        {
            "externalId": "child_b",
            "parameters": {
                "function": {
                    "externalId": "fn_etl_file_annotation",
                    "data": {"task_id": "child_b"},
                }
            },
        },
    ]
    task = {
        "id": "fanout",
        "canvas_node_id": "fanout",
        "payload": {"config": {"generator_task_id": "plan"}},
    }
    summaries = {
        "plan": {"tasks": compiled_child_specs},
    }
    shared: dict = {"run_id": "run1", "configuration": {}}

    with patch.object(dynamic_fanout, "etl_local_pipeline_specs", return_value={"fn_etl_file_annotation": ("x", "y")}), patch.object(
        dynamic_fanout.importlib, "import_module", return_value=type("M", (), {"y": staticmethod(fake_fn)})()
    ), patch.object(
        dynamic_fanout, "_merge_child_cohort_into_fanout", return_value=2
    ):
        result = dynamic_fanout.run_local_dynamic_fanout(
            task,
            summaries=summaries,
            shared_data=shared,
            client=object(),
            logger=__import__("logging").getLogger("test"),
            max_workers=2,
        )

    assert sorted(calls) == ["child_a", "child_b"]
    assert result["children_run"] == 2
