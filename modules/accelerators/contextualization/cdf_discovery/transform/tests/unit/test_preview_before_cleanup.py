"""Preview snapshots must run before raw_cleanup deletes cohort tables."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from local_runner.kahn_workflow_executor import (  # noqa: E402
    _LayerTaskWork,
    _maybe_run_preview_snapshots_before_cleanup,
)


def test_preview_snapshots_run_once_before_raw_cleanup_layer() -> None:
    shared: dict[str, Any] = {"configuration": {"canvas": {"nodes": [], "edges": []}}}
    summaries = {"q1": {"status": "succeeded"}}
    work = [
        _LayerTaskWork(
            task_id="end",
            task={
                "id": "end",
                "executable_kind": "raw_cleanup",
                "function_external_id": "fn_etl_raw_cleanup",
            },
            skipped=False,
            skip_summary=None,
        )
    ]
    client = MagicMock()
    logger = MagicMock()

    with patch(
        "local_runner.preview_nodes.run_canvas_preview_snapshots",
        return_value=[{"preview_node_id": "pv1", "rows_written": 2}],
    ) as mock_preview:
        _maybe_run_preview_snapshots_before_cleanup(
            work_items=work,
            shared_data=shared,
            summaries=summaries,
            client=client,
            dry_run=False,
            logger=logger,
        )
        mock_preview.assert_called_once()
        _maybe_run_preview_snapshots_before_cleanup(
            work_items=work,
            shared_data=shared,
            summaries=summaries,
            client=client,
            dry_run=False,
            logger=logger,
        )
        mock_preview.assert_called_once()

    assert shared["_preview_snapshots_done"] is True
    assert shared["_preview_snapshots"] == [{"preview_node_id": "pv1", "rows_written": 2}]


def test_preview_snapshots_skipped_when_layer_has_no_cleanup() -> None:
    shared: dict[str, Any] = {"configuration": {"canvas": {"nodes": [], "edges": []}}}
    work = [
        _LayerTaskWork(
            task_id="q1",
            task={"id": "q1", "executable_kind": "query_view"},
            skipped=False,
            skip_summary=None,
        )
    ]
    with patch("local_runner.preview_nodes.run_canvas_preview_snapshots") as mock_preview:
        _maybe_run_preview_snapshots_before_cleanup(
            work_items=work,
            shared_data=shared,
            summaries={},
            client=MagicMock(),
            dry_run=False,
            logger=MagicMock(),
        )
        mock_preview.assert_not_called()
