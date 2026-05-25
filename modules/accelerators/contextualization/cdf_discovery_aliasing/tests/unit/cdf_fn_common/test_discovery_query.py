"""Unit tests for discovery query handlers (view / RAW / classic)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_cohort import _props_from_row_columns
from cdf_fn_common.discovery_query import (
    build_entity_cohort_row,
    discovery_handle_classic_query,
    discovery_handle_raw_query,
    discovery_handle_view_query,
    discovery_query_handle_cdf,
    resolve_query_sink,
)
from cdf_fn_common.discovery_query_shared import (
    CONFIDENCE_COLUMN,
    PROPERTIES_JSON_COLUMN,
    resolve_raw_save_sink,
)


class _FakeInstance:
    def __init__(
        self,
        *,
        external_id: str,
        space: str = "sp1",
        instance_id: str = "uuid-1",
        properties: Dict[str, Any] | None = None,
        last_updated_time: int = 1_700_000_000_000,
    ) -> None:
        self.external_id = external_id
        self.space = space
        self.instance_id = instance_id
        self.last_updated_time = last_updated_time
        self._properties = properties or {"name": external_id}

    def dump(self) -> Dict[str, Any]:
        return {
            "externalId": self.external_id,
            "space": self.space,
            "lastUpdatedTime": self.last_updated_time,
            "properties": {
                "cdf_cdm": {
                    "CogniteAsset/v1": self._properties,
                }
            },
        }


def test_resolve_query_sink_requires_run_id_and_task_id() -> None:
    with pytest.raises(ValueError, match="run_id"):
        resolve_query_sink({"task_id": "kea__vq"})
    with pytest.raises(ValueError, match="task_id"):
        resolve_query_sink({"run_id": "run1"})


def test_resolve_query_sink_node_table() -> None:
    rid = "20260101T000000.000000Z-abc123def456"
    db, tbl = resolve_query_sink(
        {
            "run_id": rid,
            "task_id": "kea__vq",
            "compiled_workflow": {
                "tasks": [{"id": "kea__vq", "canvas_node_id": "vq_eq"}]
            },
            "config": {"raw_db": "db_x", "raw_table_key": "tbl_y"},
            "persistence": {},
        }
    )
    assert db == "db_x"
    assert tbl.startswith("tbl_y__")
    assert "__vq_eq" in tbl or tbl.endswith("__vq_eq")


def test_resolve_raw_save_sink_from_source_fields() -> None:
    db, tbl = resolve_raw_save_sink(
        {"source_raw_db": "db_discovery", "source_raw_table_key": "test_dump"}
    )
    assert db == "db_discovery"
    assert tbl == "test_dump"


def test_resolve_raw_save_sink_requires_db_and_table() -> None:
    with pytest.raises(ValueError, match="save_raw requires"):
        resolve_raw_save_sink({"source_raw_table_key": "test_dump"})


def test_build_entity_cohort_row_shape() -> None:
    cn = "n_vq_eq"
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        canvas_node_id=cn,
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"name": "A-1"},
    )
    assert row["key"] == "scope1:sp1:uuid-1"
    cols = row["columns"]
    assert cols["RECORD_KIND"] == "entity"
    assert cols["WORKFLOW_STATUS"] == "detected"
    assert cols["QUERY_SOURCE"] == "view"
    assert json.loads(cols["PROPERTIES_JSON"]) == {"name": "A-1"}
    assert "CONFIDENCE" not in cols


def test_cohort_row_from_columns_preserves_extraction_inputs_hash() -> None:
    from cdf_fn_common.discovery_cohort import _cohort_row_from_columns
    from cdf_fn_common.discovery_query_shared import EXTRACTION_INPUTS_HASH_COLUMN

    cols = {
        "NODE_INSTANCE_ID": "sp1:uuid-1",
        "EXTERNAL_ID": "FILE-1",
        "SCOPE_KEY": "scope_abc",
        "ENTITY_TYPE": "file",
        "VIEW_SPACE": "cdf_cdm",
        "VIEW_EXTERNAL_ID": "CogniteFile",
        "VIEW_VERSION": "v1",
        EXTRACTION_INPUTS_HASH_COLUMN: "abc123hash",
        "LAST_UPDATED_TIME_MS": 1_700_000_000_000,
        "PROPERTIES_JSON": '{"name": "doc.pdf"}',
    }
    row = _cohort_row_from_columns(
        cols=cols,
        row_key="ignored",
        run_id="run_new",
        canvas_node_id="tr",
        properties={"name": "doc.pdf", "aliases": ["DOC"]},
    )
    assert row["columns"][EXTRACTION_INPUTS_HASH_COLUMN] == "abc123hash"
    assert row["columns"]["LAST_UPDATED_TIME_MS"] == 1_700_000_000_000


def test_build_entity_cohort_row_optional_extraction_inputs_hash() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        canvas_node_id="n_vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"name": "A-1"},
        extraction_inputs_hash="deadbeef",
    )
    cols = row["columns"]
    assert cols.get("EXTRACTION_INPUTS_HASH") == "deadbeef"
    assert json.loads(cols["PROPERTIES_JSON"]) == {"name": "A-1"}
    assert "CONFIDENCE" not in cols


def test_build_entity_cohort_row_moves_confidence_to_column() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        canvas_node_id="n_vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"indexKey": ["K1"], "indexKey_confidence": [0.88], "name": "A-1"},
        value_field="indexKey",
    )
    cols = row["columns"]
    assert json.loads(cols["PROPERTIES_JSON"]) == {"indexKey": ["K1"], "name": "A-1"}
    assert "indexKey_confidence" not in json.loads(cols["PROPERTIES_JSON"])
    assert json.loads(cols["CONFIDENCE"]) == [0.88]


def test_props_from_row_columns_merges_confidence_column() -> None:
    cols = {
        PROPERTIES_JSON_COLUMN: json.dumps({"indexKey": ["K1"], "name": "x"}),
        CONFIDENCE_COLUMN: "[0.25]",
    }
    props = _props_from_row_columns(cols)
    assert props["indexKey"] == ["K1"]
    assert props["aliases_confidence"] == [0.25]


def test_build_entity_strips_indexKey_confidence_from_properties_json() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        canvas_node_id="n_vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"indexKey": ["K1"], "indexKey_confidence": [0.9], "name": "A-1"},
    )
    cols = row["columns"]
    assert "CONFIDENCE" not in cols
    body = json.loads(cols["PROPERTIES_JSON"])
    assert "indexKey_confidence" not in body
    assert body == {"indexKey": ["K1"], "name": "A-1"}


def test_view_query_incremental_paginates_beyond_batch_size(monkeypatch) -> None:
    """``batch_size`` is page size only; incremental listing must not stop after one page."""
    listed = 0

    def _fake_list_all_instances(_client, **kwargs):
        nonlocal listed
        limit = int(kwargs.get("limit") or 1000)
        for i in range(limit + 200):
            listed += 1
            yield _FakeInstance(
                external_id=f"P-{i}",
                instance_id=f"uuid-{i}",
                properties={"name": f"P-{i}", "externalId": f"P-{i}"},
            )

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        _fake_list_all_instances,
    )

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *_a, **_k):
            return None

    client = MagicMock()
    client.raw.rows = _Rows()

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "batch_size": 1000,
            "incremental_change_processing": True,
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    summary = ViewQueryHandler.run("fn_dm_view_query", data, client, None)
    assert listed == 1200
    assert summary["instances_listed"] == 1200
    assert summary["instances_written"] == 1200
    assert summary["incremental"] is True


def test_view_query_incremental_applies_watermark_filter(monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    class _WmRow:
        columns = {"HIGH_WATERMARK_MS": 1_700_000_000_000}

    class _Rows:
        def retrieve(self, *_a, **_k):
            return _WmRow()

        def insert(self, *_a, **_k):
            return None

    def _fake_list_all_instances(_client, **kwargs):
        captured["filter"] = kwargs.get("filter")
        return iter(())

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        _fake_list_all_instances,
    )

    client = MagicMock()
    client.raw.rows = _Rows()

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "incremental_change_processing": True,
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    summary = ViewQueryHandler.run("fn_dm_view_query", data, client, None)
    assert captured.get("filter") is not None
    assert summary["list_complete"] is True
    assert summary["rows_truncated"] is False


def test_view_query_run_all_no_watermark_filter(monkeypatch) -> None:
    wm_reads: List[Any] = []
    captured: Dict[str, Any] = {}

    def _track_read_wm(*_a, **_k):
        wm_reads.append(1)
        return 1_700_000_000_000

    def _fake_list_all_instances(_client, **kwargs):
        captured["filter"] = kwargs.get("filter")
        return iter(())

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.read_listing_watermark_ms",
        _track_read_wm,
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        _fake_list_all_instances,
    )

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *_a, **_k):
            return None

    client = MagicMock()
    client.raw.rows = _Rows()

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "run_all": True,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "incremental_change_processing": True,
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    summary = ViewQueryHandler.run("fn_dm_view_query", data, client, None)
    assert wm_reads == []
    filt = captured.get("filter")
    assert filt is not None
    filt_dump = filt.dump() if hasattr(filt, "dump") else str(filt)
    assert "lastUpdatedTime" not in json.dumps(filt_dump)
    assert summary["run_all"] is True
    assert summary["incremental"] is True
    assert summary["listing_watermark_applied"] is False


def test_view_query_run_all_writes_watermark_and_hashes(monkeypatch) -> None:
    wm_writes: List[int] = []
    hash_upserts: List[List[Dict[str, Any]]] = []

    def _fake_write_wm(_client, *, high_ms: int, **_k):
        wm_writes.append(high_ms)

    def _fake_upsert_hashes(_client, *, items: List[Dict[str, Any]], **_k):
        hash_upserts.append(list(items))

    instances = [
        _FakeInstance(external_id="P-1", instance_id="uuid-1", last_updated_time=1_700_000_000_100),
        _FakeInstance(external_id="P-2", instance_id="uuid-2", last_updated_time=1_700_000_000_200),
    ]

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        lambda _c, **kw: iter(instances),
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.write_listing_watermark_raw",
        _fake_write_wm,
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.upsert_incremental_entity_hashes_raw",
        _fake_upsert_hashes,
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.compute_extraction_inputs_hash_from_entity_row",
        lambda *_a, **_k: "hash_run_all",
    )

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *_a, **_k):
            return None

    client = MagicMock()
    client.raw.rows = _Rows()

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "run_all": True,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "incremental_change_processing": True,
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    summary = ViewQueryHandler.run("fn_dm_view_query", data, client, None)
    assert summary["instances_written"] == 2
    assert summary["instances_skipped_unchanged_hash"] == 0
    assert wm_writes == [1_700_000_000_200]
    assert len(hash_upserts) == 1
    assert len(hash_upserts[0]) == 2
    assert all(it["extraction_inputs_hash"] == "hash_run_all" for it in hash_upserts[0])


def test_view_query_run_all_emits_all_cohort_rows(monkeypatch) -> None:
    fixed_hash = "unchanged_hash_value"

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.load_hash_by_node_for_scope",
        lambda *_a, **_k: {
            "sp1:uuid-1": fixed_hash,
            "sp1:uuid-2": fixed_hash,
        },
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.compute_extraction_inputs_hash_from_entity_row",
        lambda *_a, **_k: fixed_hash,
    )
    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        lambda _c, **kw: iter(
            [
                _FakeInstance(external_id="P-1", instance_id="uuid-1"),
                _FakeInstance(external_id="P-2", instance_id="uuid-2"),
            ]
        ),
    )

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *_a, **_k):
            return None

    client = MagicMock()
    client.raw.rows = _Rows()

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "run_all": True,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "incremental_change_processing": True,
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    summary = ViewQueryHandler.run("fn_dm_view_query", data, client, None)
    assert summary["instances_written"] == 2
    assert summary["instances_skipped_unchanged_hash"] == 0
    assert summary["incremental_skip_unchanged_source_inputs"] is False


def test_discovery_handle_view_query_writes_raw(monkeypatch) -> None:
    inserted: List[Dict[str, Any]] = []

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            inserted.append({"db": db_name, "table": table_name, "row": dict(row)})

    client = MagicMock()
    client.raw.rows = _Rows()

    instances = [
        _FakeInstance(
            external_id="P-1",
            instance_id="uuid-1",
            properties={"name": "Pump 1", "externalId": "P-1"},
        ),
        _FakeInstance(
            external_id="P-2",
            instance_id="uuid-2",
            properties={"name": "Pump 2", "externalId": "P-2"},
        ),
    ]

    def _fake_list_all_instances(_client, **kwargs):
        del _client, kwargs
        yield from instances

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        _fake_list_all_instances,
    )

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "include_properties": ["name", "externalId"],
            "filters": [],
        },
        "configuration": {
            "source_views": [
                {
                    "view_external_id": "CogniteAsset",
                    "entity_type": "asset",
                }
            ]
        },
    }
    summary = discovery_handle_view_query("fn_dm_view_query", data, client, None)
    assert summary["instances_written"] == 2
    assert summary.get("list_page_count") is not None
    assert summary.get("list_duration_sec") is not None
    assert "list_sort" in summary
    assert summary["raw_db"] == "db_discovery"
    assert "__" in summary["raw_table"]
    assert len(inserted) == 1
    row_map = inserted[0]["row"]
    assert len(row_map) == 2


@pytest.mark.parametrize("instance_space_kw", ["", "all_spaces"])
def test_view_query_empty_instance_space_passes_none_to_list_all_instances(
    monkeypatch, instance_space_kw: str
) -> None:
    """Unrestricted instance space must not be passed to ``instances.list`` as a literal ``space``."""
    captured: Dict[str, Any] = {}

    def _capture_list(_client: Any, **kwargs: Any):
        captured["kwargs"] = kwargs
        yield from ()

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        _capture_list,
    )

    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    data: Dict[str, Any] = {
        "task_id": "kea__vq",
        "run_id": "run_test",
        "instance_space": instance_space_kw,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteAsset",
            "view_version": "v1",
            "filters": [],
        },
        "configuration": {},
    }
    ViewQueryHandler.run("fn_dm_view_query", data, MagicMock(), None)
    assert captured.get("kwargs", {}).get("space") is None


def test_discovery_handle_raw_query_copies_rows() -> None:
    inserted: List[Dict[str, Any]] = []

    class _Row:
        def __init__(self, key: str, columns: Dict[str, Any]) -> None:
            self.key = key
            self.columns = columns

    class _Rows:
        def __call__(self, *_a, **_k):
            yield _Row(
                "src-key",
                {
                    "RECORD_KIND": "entity",
                    "RUN_ID": "old_run",
                    "NODE_INSTANCE_ID": "sp1:uuid-9",
                    "EXTERNAL_ID": "TAG-9",
                    "SCOPE_KEY": "abc",
                    "PROPERTIES_JSON": json.dumps({"name": "TAG-9"}),
                },
            )

        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            inserted.append({"db": db_name, "table": table_name, "row": dict(row)})

    client = MagicMock()
    client.raw.rows = _Rows()

    data = {
        "task_id": "kea__rq",
        "run_id": "run_new",
        "config": {
            "source_raw_db": "db_src",
            "source_raw_table": "src_tbl",
            "raw_db": "db_sink",
            "raw_table": "sink_tbl",
        },
    }
    summary = discovery_handle_raw_query("fn_dm_raw_query", data, client, None)
    assert summary["instances_written"] == 1
    assert inserted[0]["db"] == "db_sink"
    assert summary["raw_table"].startswith("sink_tbl__")
    cols = next(iter(inserted[0]["row"].values()))
    assert cols["RUN_ID"] == "run_new"
    assert cols["QUERY_SOURCE"] == "raw"


def test_discovery_handle_classic_query_assets() -> None:
    inserted: List[Dict[str, Any]] = []

    class _Asset:
        external_id = "CL-1"

        def dump(self) -> Dict[str, Any]:
            return {"externalId": self.external_id, "name": "Classic asset"}

    class _Rows:
        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            inserted.append({"db": db_name, "table": table_name, "row": dict(row)})

    client = MagicMock()
    client.assets.list.return_value = [_Asset()]
    client.raw.rows = _Rows()

    data = {
        "task_id": "kea__cq",
        "run_id": "run_classic",
        "config": {"resource_type": "assets", "limit": 10},
    }
    summary = discovery_handle_classic_query("fn_dm_classic_query", data, client, None)
    assert summary["instances_written"] == 1
    client.assets.list.assert_called_once()
    cols = next(iter(inserted[0]["row"].values()))
    assert cols["EXTERNAL_ID"] == "CL-1"
    assert cols["QUERY_SOURCE"] == "classic"


def test_discovery_query_handle_cdf_view(monkeypatch) -> None:
    called: Dict[str, Any] = {}

    def _fake_run(fn_external_id, data, client, log):
        called["fn"] = fn_external_id
        return {"function_external_id": fn_external_id, "instances_written": 0}

    monkeypatch.setattr(
        "fn_dm_view_query.engine.orchestration.discovery_handle_view_query",
        _fake_run,
    )
    out = discovery_query_handle_cdf(
        "fn_dm_view_query",
        {"config": {}},
        MagicMock(),
        None,
    )
    assert out["function_external_id"] == "fn_dm_view_query"
    assert called["fn"] == "fn_dm_view_query"


def test_view_query_injects_instance_space_without_include_properties() -> None:
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    inst = _FakeInstance(external_id="A-1", space="sp-inject", properties={"name": "n"})
    picked = ViewQueryHandler._pick_properties({"name": "n"}, ["name"])
    out = ViewQueryHandler._inject_instance_space_on_properties(picked, inst)
    assert out["name"] == "n"
    assert out["instance_space"] == "sp-inject"
