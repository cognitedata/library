"""Tests for RAW row attachment to local run JSON."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
for _p in (str(_FUNCS), str(_MODULE_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from local_runner.kahn_run_context import KahnRunContext  # noqa: E402
from local_runner.raw_results_attachment import (  # noqa: E402
    build_raw_results_bundle,
    collect_raw_locations_from_task_outputs,
    snapshot_raw_results_for_ctx,
)
from argparse import Namespace  # noqa: E402


def test_collect_raw_locations_from_task_outputs() -> None:
    msg = json.dumps({"raw_db": "db1", "raw_table": "t1", "instances_written": 3})
    task_outputs = {
        "task_a": {"status": "succeeded", "message": msg},
        "task_b": {"status": "succeeded", "message": "not json"},
    }
    locs = collect_raw_locations_from_task_outputs(task_outputs)
    assert locs == {("db1", "t1"): {"task_a"}}


def test_collect_raw_locations_sink_aliases() -> None:
    msg = json.dumps({"sink_raw_db": "db2", "sink_raw_table": "t2"})
    locs = collect_raw_locations_from_task_outputs({"x": {"message": msg}})
    assert locs == {("db2", "t2"): {"x"}}


def test_build_raw_results_bundle_reads_rows() -> None:
    class _Row:
        def __init__(self, key: str, columns: dict) -> None:
            self.key = key
            self.columns = columns

    task_outputs = {
        "q": {
            "status": "succeeded",
            "message": json.dumps({"raw_db": "dbx", "raw_table": "tbl"}),
        }
    }
    client = MagicMock()
    log = MagicMock()
    with patch(
        "local_runner.raw_results_attachment.iter_raw_table_rows_chunked",
        return_value=[_Row("a", {"COL": "v", "RUN_ID": "run1"})],
    ) as mock_iter:
        bundle = build_raw_results_bundle(
            client,
            task_outputs,
            row_limit=10,
            max_tables=5,
            logger=log,
            run_id="run1",
        )
    mock_iter.assert_called_once_with(client, "dbx", "tbl")
    assert bundle["tables"][0]["raw_db"] == "dbx"
    assert bundle["tables"][0]["rows"][0]["key"] == "a"
    assert bundle["tables"][0]["rows"][0]["columns"] == {"COL": "v", "RUN_ID": "run1"}
    assert bundle.get("run_id") == "run1"


def test_build_raw_results_bundle_run_id_filters_other_runs() -> None:
    class _Row:
        def __init__(self, key: str, columns: dict) -> None:
            self.key = key
            self.columns = columns

    rows_in = [
        _Row("old", {"RUN_ID": "other"}),
        _Row("new", {"RUN_ID": "want"}),
    ]
    task_outputs = {
        "q": {"status": "succeeded", "message": json.dumps({"raw_db": "d", "raw_table": "t"})},
    }
    with patch(
        "local_runner.raw_results_attachment.iter_raw_table_rows_chunked",
        return_value=rows_in,
    ):
        bundle = build_raw_results_bundle(
            MagicMock(),
            task_outputs,
            row_limit=10,
            max_tables=5,
            logger=None,
            run_id="want",
        )
    assert len(bundle["tables"][0]["rows"]) == 1
    assert bundle["tables"][0]["rows"][0]["key"] == "new"


def test_build_raw_results_bundle_disabled() -> None:
    bundle = build_raw_results_bundle(
        None,
        {},
        row_limit=0,
        max_tables=10,
        logger=None,
    )
    assert bundle["tables"] == []


def test_snapshot_raw_results_for_ctx_caches_on_ctx() -> None:
    ctx = KahnRunContext(
        args=Namespace(raw_results_rows=10, raw_results_max_tables=5),
        logger=None,
        client=MagicMock(),
        pipe_logger=None,
        scope_yaml_path=Path("/tmp/scope.yaml"),
        scope_document={},
        wf_instance_space="sp",
        source_views=[],
        cdf_config=None,
        run_id="run1",
    )
    bundle = {"tables": [{"row_count": 2}]}
    with patch(
        "local_runner.raw_results_attachment.build_raw_results_bundle",
        return_value=bundle,
    ) as mock_build:
        out1 = snapshot_raw_results_for_ctx(ctx)
        out2 = snapshot_raw_results_for_ctx(ctx)
    assert out1 is bundle
    assert out2 is bundle
    mock_build.assert_called_once()


def test_build_raw_results_bundle_raw_scan_truncated() -> None:
    """When RUN_ID filter is sparse, stop examining RAW after max_raw_rows_scanned."""
    class _Row:
        def __init__(self, key: str, columns: dict) -> None:
            self.key = key
            self.columns = columns

    # No row matches until after the scan cap — walk stops with raw_scan_truncated.
    rows_in = [_Row(str(i), {"RUN_ID": "other"}) for i in range(20)]
    task_outputs = {
        "q": {"status": "succeeded", "message": json.dumps({"raw_db": "d", "raw_table": "t"})},
    }
    with patch(
        "local_runner.raw_results_attachment.iter_raw_table_rows_chunked",
        return_value=rows_in,
    ):
        bundle = build_raw_results_bundle(
            MagicMock(),
            task_outputs,
            row_limit=10,
            max_tables=5,
            logger=None,
            run_id="want",
            max_raw_rows_scanned=5,
        )
    t0 = bundle["tables"][0]
    assert t0["raw_scan_truncated"] is True
    assert t0["rows_examined"] == 6
    assert t0["row_count"] == 0
