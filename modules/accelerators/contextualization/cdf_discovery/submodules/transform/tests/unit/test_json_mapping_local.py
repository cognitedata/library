"""Local jsonMapping task execution."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.kuiper_eval import evaluate_kuiper_expression, kuiper_available  # noqa: E402
from local_runner.json_mapping import (  # noqa: E402
    resolve_json_mapping_input,
    run_local_json_mapping_task,
)


def test_resolve_json_mapping_input_refs():
    summaries = {"q1": {"output": {"rows": [{"id": 1}]}}}
    inp = {"rows": "${q1.output.rows}"}
    resolved = resolve_json_mapping_input(inp, summaries)
    assert resolved["rows"] == [{"id": 1}]


@pytest.mark.skipif(not kuiper_available(), reason="cognite-kuiper not installed")
def test_evaluate_json_mapping_expression_input_paths():
    data = {"rows": [1, 2], "meta": {"n": 2}}
    assert evaluate_kuiper_expression("input", data) == data
    assert evaluate_kuiper_expression("input.rows", data) == [1, 2]


def test_run_local_json_mapping_passthrough_dry_run():
    task = {
        "id": "map1",
        "task_type": "jsonMapping",
        "depends_on": [],
        "payload": {
            "config": {
                "mapper_kind": "custom",
                "input": {},
                "expression": "input",
            }
        },
    }
    summary = run_local_json_mapping_task(
        task,
        summaries={},
        shared_data={},
        client=None,
        logger=__import__("logging").getLogger("test"),
        dry_run=True,
    )
    assert summary["status"] == "skipped"
    assert summary["reason"] == "dry_run"
