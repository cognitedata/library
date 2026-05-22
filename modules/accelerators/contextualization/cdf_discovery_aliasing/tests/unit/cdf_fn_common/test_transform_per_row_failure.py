"""Transform orchestration continues after per-row failure."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fn_dm_transform.engine.orchestration import discovery_handle_transform  # noqa: E402


def test_transform_continues_after_row_error() -> None:
    data = {
        "task_id": "kea__tr",
        "run_id": "run-1",
        "config": {
            "handler_id": "trim_whitespace",
            "steps": [{"handler_id": "trim_whitespace", "config": {"fields": ["name"]}}],
        },
        "configuration": {"parameters": {}},
    }
    client = MagicMock()
    log = MagicMock()

    rows = [
        ("rk1", {"NODE_INSTANCE_ID": "sp:1", "RAW_ROW_KEY": "rk1"}),
        ("rk2", {"NODE_INSTANCE_ID": "sp:2", "RAW_ROW_KEY": "rk2"}),
    ]

    def _iter_rows(*_a, **_k):
        for rk, cols in rows:
            yield rk, cols

    call_count = {"n": 0}

    def _apply(_props, _cfg):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ValueError("bad row")
        return [{"aliases": ["a"]}]

    with patch(
        "fn_dm_transform.engine.orchestration.iter_unique_predecessor_entity_rows",
        _iter_rows,
    ), patch(
        "fn_dm_transform.engine.orchestration.build_transform_table_indexes",
        return_value={},
    ), patch(
        "fn_dm_transform.engine.orchestration.resolve_cumulative_input_props",
        return_value={},
    ), patch(
        "fn_dm_transform.engine.orchestration.apply_transform_steps_to_props",
        _apply,
    ), patch(
        "fn_dm_transform.engine.orchestration.build_entity_failure_recorder"
    ) as mock_rec:
        from cdf_fn_common.discovery_record_failure import EntityFailureRecorder

        rec = EntityFailureRecorder(
            client=client, raw_db="db", raw_table="tbl", kd_backend=None
        )
        mock_rec.return_value = rec
        summary = discovery_handle_transform("fn_dm_transform", data, client, log)

    assert summary["entities_failed"] == 1
    assert summary["rows_written"] == 1
