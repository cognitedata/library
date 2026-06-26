"""Unit tests for ETL save apply (view / raw / classic)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_save_apply import (  # noqa: E402
    DEFAULT_SAVE_BATCH_SIZE,
    _coerce_dm_list_property_value,
    _prepare_view_apply_properties,
    _resolve_save_batch_size,
    etl_apply_classic_save,
    etl_apply_view_save,
    etl_replicate_raw_save,
)
from cdf_fn_common.etl_discovery_query_shared import (  # noqa: E402
    INSTANCE_SPACE_COLUMN,
    cohort_instance_space_and_external_id,
    resolve_raw_save_sink,
)


def test_cohort_instance_space_prefers_row_over_pipeline_default() -> None:
    cols = {
        "EXTERNAL_ID": "ext1",
        "INSTANCE_SPACE": "real_sp",
        "NODE_INSTANCE_ID": "real_sp:ext1",
    }
    inst_space, ext_id = cohort_instance_space_and_external_id(
        cols,
        data={"instance_space": "inst_assets"},
    )
    assert inst_space == "real_sp"
    assert ext_id == "ext1"


def test_cohort_instance_space_falls_back_to_pipeline_default() -> None:
    cols = {"EXTERNAL_ID": "ext1"}
    inst_space, ext_id = cohort_instance_space_and_external_id(
        cols,
        data={"instance_space": "inst_assets"},
    )
    assert inst_space == "inst_assets"
    assert ext_id == "ext1"


def test_coerce_dm_list_property_value() -> None:
    assert _coerce_dm_list_property_value(["a", "b"]) == ["a", "b"]
    assert _coerce_dm_list_property_value([{"value": "a", "confidence": 0.9}]) == ["a"]
    assert _prepare_view_apply_properties(
        {"aliases": "", "name": "n"},
        list_properties=frozenset({"aliases"}),
    ) == {"name": "n"}
    assert _prepare_view_apply_properties(
        {"aliases": ["a"], "aliases_score": [0.8], "name": "n"},
        list_properties=frozenset({"aliases"}),
    ) == {"aliases": ["a"], "name": "n"}


def test_resolve_save_batch_size_defaults_to_500() -> None:
    assert _resolve_save_batch_size({}) == DEFAULT_SAVE_BATCH_SIZE
    assert _resolve_save_batch_size({"batch_size": 250}) == 250


def test_resolve_save_batch_size_rejects_invalid() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        _resolve_save_batch_size({"batch_size": 0})


def test_view_save_requires_view_external_id() -> None:
    with pytest.raises(ValueError, match="view_external_id"):
        etl_apply_view_save(
            "fn_etl_view_save",
            {"task_id": "t1", "run_id": "r1", "config": {}},
            MagicMock(),
            None,
        )


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_view_save_uses_cohort_space_over_pipeline_default(
    mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_cohort_space"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "real_sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        INSTANCE_SPACE_COLUMN: "real_sp",
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": ["TAG-1"], "name": "N"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "instance_space": "inst_assets",
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
        },
    }
    summary = etl_apply_view_save("fn_etl_view_save", data, client, None)
    assert summary["instances_applied"] == 1
    apply_call = client.data_modeling.instances.apply.call_args
    nodes = apply_call.kwargs.get("nodes") or apply_call.args[0]
    assert nodes[0].space == "real_sp"


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_view_save_dry_run_counts_without_apply(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    run_id = "run_test_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        "PROPERTIES_JSON": json.dumps({"name": "N", "description": "D"}),
    }
    mock_iter_rows.return_value = [(0, cols, {"name": "N", "description": "D"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "dry_run": True,
        "config": {
            "view_space": "vs",
            "view_external_id": "VE",
            "view_version": "v1",
            "instance_space": "sp",
        },
    }
    summary = etl_apply_view_save("fn_etl_view_save", data, client, None)
    assert summary["instances_applied"] == 1
    assert summary["dry_run"] is True
    client.data_modeling.instances.apply.assert_not_called()


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_view_save_applies_when_not_dry_run(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    run_id = "run_live_1"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        INSTANCE_SPACE_COLUMN: "sp",
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": ["TAG-1"], "name": "N"})]
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
        },
    }
    summary = etl_apply_view_save("fn_etl_view_save", data, client, None)
    assert summary["instances_applied"] == 1
    client.data_modeling.instances.apply.assert_called_once()


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_view_save_batches_apply_calls(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    run_id = "run_batch"
    rows = []
    for i in range(3):
        cols = {
            "RECORD_KIND": "entity",
            "RUN_ID": run_id,
            "NODE_INSTANCE_ID": f"sp:11111111-1111-1111-1111-11111111111{i}",
            "EXTERNAL_ID": f"ext{i}",
            INSTANCE_SPACE_COLUMN: "sp",
        }
        rows.append((0, cols, {"aliases": [f"TAG-{i}"], "name": f"N{i}"}))
    mock_iter_rows.return_value = rows
    client = MagicMock()
    data = {
        "task_id": "save1",
        "run_id": run_id,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteDescribable",
            "view_version": "v1",
            "batch_size": 2,
        },
    }
    summary = etl_apply_view_save("fn_etl_view_save", data, client, None)
    assert summary["instances_applied"] == 3
    assert client.data_modeling.instances.apply.call_count == 2


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[])
def test_view_save_in_memory_via_predecessor_rows(_mock_pred: MagicMock) -> None:
    data = {
        "task_id": "save1",
        "run_id": "run_mem",
        "local_predecessor_mode": "in_memory",
        "_predecessor_rows": [
            {
                "columns": {
                    "EXTERNAL_ID": "e1",
                    "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
                },
                "properties": {"aliases": ["a"], "name": "n"},
            }
        ],
        "config": {
            "view_external_id": "VE",
            "view_space": "vs",
            "view_version": "v1",
        },
    }
    summary = etl_apply_view_save("fn_etl_view_save", data, None, None)
    assert summary["rows_read"] == 1
    assert summary["instances_applied"] == 1


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_classic_save_dry_run(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    cols = {"EXTERNAL_ID": "asset-1", "RECORD_KIND": "entity"}
    mock_iter_rows.return_value = [(0, cols, {"name": "Updated"})]
    client = MagicMock()
    data = {
        "task_id": "c1",
        "run_id": "r1",
        "dry_run": True,
        "config": {"resource_type": "assets"},
    }
    summary = etl_apply_classic_save("fn_etl_classic_save", data, client, None)
    assert summary["updates_applied"] == 1
    client.assets.update.assert_not_called()


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[("db", "src")])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
def test_classic_save_batches_updates(mock_iter_rows: MagicMock, _mock_pred: MagicMock) -> None:
    rows = []
    for i in range(3):
        cols = {"EXTERNAL_ID": f"asset-{i}", "RECORD_KIND": "entity"}
        rows.append((0, cols, {"name": f"Updated-{i}"}))
    mock_iter_rows.return_value = rows
    client = MagicMock()
    data = {
        "task_id": "c1",
        "run_id": "r1",
        "config": {"resource_type": "assets", "batch_size": 2},
    }
    summary = etl_apply_classic_save("fn_etl_classic_save", data, client, None)
    assert summary["updates_applied"] == 3
    assert client.assets.update.call_count == 2


def test_resolve_raw_save_sink_from_source_fields() -> None:
    db, tbl = resolve_raw_save_sink(
        {"source_raw_db": "db_discovery", "source_raw_table_key": "test_dump"}
    )
    assert db == "db_discovery"
    assert tbl == "test_dump"


def test_resolve_raw_save_sink_requires_db_and_table() -> None:
    with pytest.raises(ValueError, match="save_raw requires"):
        resolve_raw_save_sink({"source_raw_db": "db_discovery"})


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
@patch("cdf_fn_common.etl_save_apply._flush_rows")
def test_raw_save_writes_to_configured_sink(
    mock_flush: MagicMock, mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    run_id = "run_raw_save"
    cols = {
        "RECORD_KIND": "entity",
        "RUN_ID": run_id,
        "NODE_INSTANCE_ID": "sp:11111111-1111-1111-1111-111111111111",
        "EXTERNAL_ID": "ext1",
        INSTANCE_SPACE_COLUMN: "sp",
    }
    mock_iter_rows.return_value = [(0, cols, {"aliases": ["TAG-1"]})]
    client = MagicMock()
    data = {
        "task_id": "save_raw_1",
        "run_id": run_id,
        "config": {
            "source_raw_db": "db_discovery",
            "source_raw_table_key": "test_dump",
        },
    }
    summary = etl_replicate_raw_save("fn_etl_raw_save", data, client, None)
    assert summary["raw_db"] == "db_discovery"
    assert summary["raw_table"] == "test_dump"
    assert summary["rows_written"] == 1
    mock_flush.assert_called_once()
    assert mock_flush.call_args[0][1] == "db_discovery"
    assert mock_flush.call_args[0][2] == "test_dump"


@patch("cdf_fn_common.etl_save_apply.iter_predecessor_raw_locations", return_value=[])
@patch("cdf_fn_common.etl_save_apply._iter_entity_rows_for_save")
@patch("cdf_fn_common.etl_save_apply._flush_rows")
def test_raw_save_flushes_at_configured_batch_size(
    mock_flush: MagicMock, mock_iter_rows: MagicMock, _mock_pred: MagicMock
) -> None:
    def _clear_pending(_queue, _db, _tbl, rows, **kwargs) -> None:
        del kwargs
        rows.clear()

    mock_flush.side_effect = _clear_pending
    run_id = "run_raw_batch"
    rows = []
    for i in range(3):
        cols = {
            "RECORD_KIND": "entity",
            "RUN_ID": run_id,
            "NODE_INSTANCE_ID": f"sp:11111111-1111-1111-1111-11111111111{i}",
            "EXTERNAL_ID": f"ext{i}",
            INSTANCE_SPACE_COLUMN: "sp",
        }
        rows.append((0, cols, {"aliases": [f"TAG-{i}"]}))
    mock_iter_rows.return_value = rows
    client = MagicMock()
    data = {
        "task_id": "save_raw_1",
        "run_id": run_id,
        "config": {
            "source_raw_db": "db_discovery",
            "source_raw_table_key": "test_dump",
            "batch_size": 2,
        },
    }
    summary = etl_replicate_raw_save("fn_etl_raw_save", data, client, None)
    assert summary["rows_written"] == 3
    assert mock_flush.call_count == 2
