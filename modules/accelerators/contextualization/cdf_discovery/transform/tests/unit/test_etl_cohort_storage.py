from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_cohort_storage import predecessor_canvas_node_ids  # noqa: E402


def test_predecessor_canvas_node_ids_falls_back_to_explicit_predecessors() -> None:
    data = {
        "task_id": "transform_a",
        "predecessors": ["query_a", {"task_id": "query_b"}],
    }
    out = predecessor_canvas_node_ids(data, "transform_a")
    assert out == ["query_a", "query_b"]

