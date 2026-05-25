"""In-memory join handler tests."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_join_orchestration import etl_handle_join_in_memory  # noqa: E402


def test_join_in_memory_inner_match() -> None:
    data = {
        "task_id": "jn",
        "join_left_task_id": "left_q",
        "join_right_task_id": "right_q",
        "local_predecessor_mode": "in_memory",
        "etl_task_row_buffers": {
            "left_q": [{"columns": {"external_id": "A1"}, "properties": {"name": "P-101"}}],
            "right_q": [
                {"columns": {"key": "x"}, "properties": {"raw_columns": {"name": "P-101", "aliases": "EXP-P-101"}}}
            ],
        },
    }
    cfg = {
        "join_on": {
            "operator": "IEQUALS",
            "left_property": "name",
            "right_property": "raw_columns.name",
        }
    }
    summary = etl_handle_join_in_memory("fn_etl_join", data, cfg, task_id="jn", run_id="run1", log=None)
    assert summary["rows_written"] == 1
    out = data["_predecessor_rows"]
    assert len(out) == 1
    assert out[0]["properties"]["raw_columns"]["aliases"] == "EXP-P-101"
