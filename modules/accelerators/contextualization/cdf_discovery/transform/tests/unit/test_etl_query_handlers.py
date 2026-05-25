from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_filter_eval import row_passes_filter  # noqa: E402
from cdf_fn_common.etl_raw_read import (  # noqa: E402
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
)
from cdf_fn_common.query_preview import run_classic_query_preview, run_raw_query_preview  # noqa: E402
from fn_etl_classic_query.handler import etl_handle_query_classic  # noqa: E402
from fn_etl_raw_query.handler import etl_handle_query_raw  # noqa: E402
from fn_etl_sql_query.handler import etl_handle_query_sql  # noqa: E402


def _raw_row(key: str, cols: dict) -> SimpleNamespace:
    return SimpleNamespace(key=key, columns=cols)


def test_etl_handle_query_raw_filters_and_populates_predecessor_rows() -> None:
    rows_iter = [
        _raw_row(
            "k1",
            {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                RUN_ID_COLUMN: "run-src",
                EXTERNAL_ID_COLUMN: "E1",
                NODE_INSTANCE_ID_COLUMN: "sp:E1",
                PROPERTIES_JSON_COLUMN: json.dumps({"name": "keep"}),
            },
        ),
        _raw_row(
            "k2",
            {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                RUN_ID_COLUMN: "run-other",
                EXTERNAL_ID_COLUMN: "E2",
                PROPERTIES_JSON_COLUMN: json.dumps({"name": "wrong-run"}),
            },
        ),
        _raw_row(
            "k3",
            {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                RUN_ID_COLUMN: "run-src",
                EXTERNAL_ID_COLUMN: "E3",
                PROPERTIES_JSON_COLUMN: json.dumps({"name": "drop"}),
            },
        ),
    ]

    client = MagicMock()
    client.raw.rows = MagicMock(return_value=iter(rows_iter))

    data = {
        "task_id": "raw_q",
        "run_id": "run-new",
        "config": {
            "source_raw_db": "db_src",
            "source_raw_table": "tbl",
            "source_run_id": "run-src",
            "filters": [
                {"operator": "EQUALS", "target_property": "name", "values": ["keep"]},
            ],
        },
    }
    summary = etl_handle_query_raw("fn_etl_raw_query", data, client, None)
    assert summary["instances_written"] == 1
    assert len(data["_predecessor_rows"]) == 1
    row = data["_predecessor_rows"][0]
    assert row["columns"]["external_id"] == "E1"
    assert row["properties"]["name"] == "keep"


def test_etl_handle_query_classic_applies_filters() -> None:
    class _Asset:
        external_id = "CL-1"

        def dump(self) -> dict:
            return {"externalId": self.external_id, "name": "Alpha"}

    class _Asset2:
        external_id = "CL-2"

        def dump(self) -> dict:
            return {"externalId": self.external_id, "name": "Beta"}

    client = MagicMock()
    client.assets.list.return_value = [_Asset(), _Asset2()]

    data = {
        "task_id": "classic_q",
        "run_id": "run-c",
        "config": {
            "resource_type": "assets",
            "filters": [
                {"operator": "EQUALS", "target_property": "name", "values": ["Alpha"]},
            ],
        },
    }
    summary = etl_handle_query_classic("fn_etl_classic_query", data, client, None)
    assert summary["instances_written"] == 1
    assert data["_predecessor_rows"][0]["columns"]["external_id"] == "CL-1"
    assert data["_predecessor_rows"][0]["properties"]["name"] == "Alpha"


def test_etl_handle_query_sql_maps_external_id_column() -> None:
    preview = SimpleNamespace(
        schema=[],
        results=[
            {"asset_id": "SQL-1", "value": 10},
            {"asset_id": "SQL-2", "value": 20},
        ],
    )
    client = MagicMock()
    client.transformations.preview.return_value = preview

    data = {
        "task_id": "sql_q",
        "run_id": "run-s",
        "config": {
            "sql_query": "SELECT * FROM assets",
            "external_id_column": "asset_id",
            "limit": 100,
        },
    }
    summary = etl_handle_query_sql("fn_etl_sql_query", data, client, None)
    assert summary["instances_written"] == 2
    assert data["_predecessor_rows"][0]["columns"]["external_id"] == "SQL-1"
    assert data["_predecessor_rows"][1]["properties"]["value"] == 20


def test_run_raw_query_preview_applies_filters() -> None:
    rows_iter = [
        _raw_row(
            "k1",
            {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                PROPERTIES_JSON_COLUMN: json.dumps({"zone": "A"}),
            },
        ),
        _raw_row(
            "k2",
            {
                RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                PROPERTIES_JSON_COLUMN: json.dumps({"zone": "B"}),
            },
        ),
    ]
    client = MagicMock()
    client.raw.rows = MagicMock(return_value=iter(rows_iter))

    out = run_raw_query_preview(
        client,
        {
            "raw_db": "db",
            "raw_table": "tbl",
            "filters": [{"operator": "EQUALS", "target_property": "zone", "values": ["A"]}],
            "limit": 10,
        },
    )
    assert out["row_count"] == 1


def test_run_classic_query_preview_applies_filters() -> None:
    class _Asset:
        external_id = "X-1"

        def dump(self) -> dict:
            return {"name": "match"}

    class _Asset2:
        external_id = "X-2"

        def dump(self) -> dict:
            return {"name": "other"}

    client = MagicMock()
    client.assets.list.return_value = [_Asset(), _Asset2()]

    out = run_classic_query_preview(
        client,
        {
            "resource_type": "assets",
            "filters": [{"operator": "EQUALS", "target_property": "name", "values": ["match"]}],
        },
        limit=10,
    )
    assert out["row_count"] == 1
    assert out["items"][0]["external_id"] == "X-1"


def test_classic_filter_on_dumped_props() -> None:
    props = {"name": "Pump-101", "externalId": "P-101"}
    assert row_passes_filter(props, [{"operator": "EQUALS", "target_property": "name", "values": ["Pump-101"]}])
