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
from cdf_fn_common.discovery_query_shared import CONFIDENCE_COLUMN, PROPERTIES_JSON_COLUMN


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


def test_resolve_query_sink_defaults() -> None:
    db, tbl = resolve_query_sink({})
    assert db == "db_discovery"
    assert tbl == "discovery_state"


def test_resolve_query_sink_from_config() -> None:
    db, tbl = resolve_query_sink(
        {"config": {"raw_db": "db_x", "raw_table_key": "tbl_y"}, "persistence": {}}
    )
    assert db == "db_x"
    assert tbl == "tbl_y"


def test_build_entity_cohort_row_shape() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        task_id="kea__vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"name": "A-1"},
    )
    assert row["key"] == "run1:scope1:sp1:uuid-1"
    cols = row["columns"]
    assert cols["RECORD_KIND"] == "entity"
    assert cols["WORKFLOW_STATUS"] == "detected"
    assert cols["QUERY_SOURCE"] == "view"
    assert json.loads(cols["PROPERTIES_JSON"]) == {"name": "A-1"}
    assert "CONFIDENCE" not in cols


def test_build_entity_cohort_row_optional_extraction_inputs_hash() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        task_id="kea__vq",
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
        task_id="kea__vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"discoveredKey": ["K1"], "confidence": [0.88], "name": "A-1"},
    )
    cols = row["columns"]
    assert json.loads(cols["PROPERTIES_JSON"]) == {"discoveredKey": ["K1"], "name": "A-1"}
    assert "confidence" not in json.loads(cols["PROPERTIES_JSON"])
    assert json.loads(cols["CONFIDENCE"]) == [0.88]


def test_props_from_row_columns_merges_confidence_column() -> None:
    cols = {
        PROPERTIES_JSON_COLUMN: json.dumps({"discoveredKey": ["K1"], "name": "x"}),
        CONFIDENCE_COLUMN: "[0.25]",
    }
    props = _props_from_row_columns(cols)
    assert props["discoveredKey"] == ["K1"]
    assert props["confidence"] == [0.25]


def test_build_entity_strips_discoveredKey_confidence_from_properties_json() -> None:
    row = build_entity_cohort_row(
        run_id="run1",
        scope_key="scope1",
        task_id="kea__vq",
        query_source="view",
        node_instance_id="sp1:uuid-1",
        external_id="A-1",
        entity_type="asset",
        view_space="cdf_cdm",
        view_external_id="CogniteAsset",
        view_version="v1",
        properties={"discoveredKey": ["K1"], "discoveredKey_confidence": [0.9], "name": "A-1"},
    )
    cols = row["columns"]
    assert "CONFIDENCE" not in cols
    body = json.loads(cols["PROPERTIES_JSON"])
    assert "discoveredKey_confidence" not in body
    assert body == {"discoveredKey": ["K1"], "name": "A-1"}


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
    assert summary["raw_db"] == "db_discovery"
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
