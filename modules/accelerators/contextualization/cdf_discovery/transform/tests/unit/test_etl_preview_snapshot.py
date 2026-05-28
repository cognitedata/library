from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

import cdf_fn_common.etl_preview_snapshot as preview_snapshot  # noqa: E402
from cdf_fn_common.etl_preview_snapshot import (  # noqa: E402
    build_preview_snapshot_row,
    preview_row_key,
    resolve_preview_sink,
    snapshot_predecessor_to_preview,
)


def test_resolve_preview_sink_defaults() -> None:
    db, table = resolve_preview_sink({"parameters": {}})
    assert db == "etl_staging"
    assert table == "etl_preview"


def test_build_preview_snapshot_row_tags_columns() -> None:
    row = build_preview_snapshot_row(
        run_id="run-1",
        preview_node_id="pv1",
        source_canvas_node_id="q1",
        source_cols={"RECORD_KIND": "entity", "RAW_ROW_KEY": "k1"},
        source_props={"name": "x"},
        source_row_key="k1",
    )
    assert row["columns"]["PREVIEW_NODE_ID"] == "pv1"
    assert row["columns"]["SOURCE_CANVAS_NODE_ID"] == "q1"
    assert row["columns"]["RUN_ID"] == "run-1"
    assert row["key"] == preview_row_key("run-1", "pv1", "k1")


def test_resolve_preview_sink_node_override() -> None:
    db, table = resolve_preview_sink(
        {"parameters": {"raw_db": "etl_staging", "preview_raw_table_key": "global_table"}},
        {"preview_raw_table_key": "preview_node_table"},
    )
    assert db == "etl_staging"
    assert table == "preview_node_table"


def test_snapshot_truncates_preview_table_once_per_run(monkeypatch) -> None:
    client = Mock()
    shared = {"configuration": {"parameters": {"raw_db": "etl_staging", "preview_raw_table_key": "etl_preview"}}}

    monkeypatch.setattr(
        preview_snapshot,
        "_iter_rows_from_memory",
        lambda _shared, _src: iter([({"RAW_ROW_KEY": "rk1", "RECORD_KIND": "entity"}, {}, "rk1")]),
    )

    def _fake_flush(_queue, _raw_db, _raw_table, pending, *, client=None):  # noqa: ANN001
        pending.clear()

    monkeypatch.setattr(preview_snapshot, "_flush_rows", _fake_flush)
    monkeypatch.setattr(preview_snapshot, "create_table_if_not_exists", lambda *_a, **_k: None)

    snapshot_predecessor_to_preview(
        client,
        shared,
        run_id="run-1",
        preview_node_id="pv1",
        source_canvas_node_id="q1",
        preview_config={"preview_raw_table_key": "preview_node_table"},
    )
    snapshot_predecessor_to_preview(
        client,
        shared,
        run_id="run-1",
        preview_node_id="pv2",
        source_canvas_node_id="q2",
        preview_config={"preview_raw_table_key": "preview_node_table"},
    )

    client.raw.tables.delete.assert_called_once_with("etl_staging", "preview_node_table")
