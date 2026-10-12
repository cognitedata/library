"""Tests for turning extraction pipeline runs into the run charts."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent))

from data_processor import DataProcessor  # isort: skip


def _run(caller: str, total: int, created_time: int) -> MagicMock:
    message = (
        f"(caller:{caller}, function_id:1, call_id:2) - total files processed: {total} - "
        f"successful files: {total} - failed files: 0"
    )
    return MagicMock(status="success", message=message, created_time=created_time)


def test_every_stage_is_counted_in_the_run_charts() -> None:
    runs = [_run(caller, 3, 0) for caller in ("Prepare", "Launch", "Finalize", "Promote")]

    counts = DataProcessor.process_runs_for_graphing(runs).groupby("type")["count"].sum().to_dict()

    assert counts == {"Prepare": 3, "Launch": 3, "Finalize": 3, "Promote": 3}
