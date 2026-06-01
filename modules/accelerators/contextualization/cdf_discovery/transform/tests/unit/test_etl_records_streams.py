"""Unit tests for Streams/Records API wrapper and handlers."""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from cdf_fn_common.etl_records_cohort import build_record_cohort_row, record_node_instance_id
from cdf_fn_common.etl_streams_records_api import (
    StreamsRecordsAPIError,
    build_records_request_body,
    build_stream_create_body,
    cohort_row_to_record_item,
    list_streams,
    sync_records,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload: Dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> Dict[str, Any]:
        return self._payload


def test_build_records_request_body_filter_and_limit() -> None:
    body = build_records_request_body(
        {
            "batch_size": 500,
            "filter": {"space": {"eq": "my-space"}},
            "sources": [{"container": {"space": "s", "externalId": "c", "version": "v1"}}],
            "include_tombstones": True,
            "cursor": "abc",
        }
    )
    assert body["limit"] == 500
    assert body["filter"]["space"]["eq"] == "my-space"
    assert body["includeTombstones"] is True
    assert body["cursor"] == "abc"


def test_build_stream_create_body_structured() -> None:
    body = build_stream_create_body(
        {
            "stream_external_id": "evt-stream",
            "stream_space": "my-space",
            "name": "Events",
            "template": "MutableStream",
            "mutable": True,
        }
    )
    assert body["externalId"] == "evt-stream"
    assert body["space"] == "my-space"
    assert body["mutable"] is True


def test_list_streams_success() -> None:
    client = MagicMock()
    client.config.project = "test-project"
    client.get.return_value = _FakeResponse(200, {"items": [{"externalId": "s1"}], "nextCursor": None})
    out = list_streams(client, limit=100)
    assert out["items"][0]["externalId"] == "s1"
    assert client.get.called


def test_list_streams_error_raises() -> None:
    client = MagicMock()
    client.config.project = "test-project"
    client.get.return_value = _FakeResponse(403, {"error": "denied"})
    with pytest.raises(StreamsRecordsAPIError):
        list_streams(client)


def test_record_node_instance_id() -> None:
    assert record_node_instance_id("sp", "ext") == "sp:ext"


def test_cohort_row_round_trip() -> None:
    row = build_record_cohort_row(
        run_id="run-1",
        scope_key="records:stream-a",
        canvas_node_id="n1",
        stream_external_id="stream-a",
        record_space="sp",
        external_id="r1",
        properties={"temperature": 42},
        sources=[{"container": {"space": "sp", "externalId": "c", "version": "v1"}, "properties": {}}],
    )
    cols = row.get("columns") or {}
    assert cols.get("RECORD_KIND") == "record"
    item = cohort_row_to_record_item(cols, {"temperature": 42})
    assert item.get("externalId") == "r1"
    assert item.get("space") == "sp"


def test_etl_handle_query_records_in_memory() -> None:
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "functions"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from fn_etl_records_query.handler import etl_handle_query_records

    pages = [
        {
            "items": [
                {
                    "space": "sp",
                    "externalId": "r1",
                    "sources": [{"properties": {"x": 1}}],
                }
            ],
            "nextCursor": None,
        }
    ]

    client = MagicMock()
    client.config.project = "p"
    call_count = {"n": 0}

    def fake_sync(_c: Any, _s: str, body: Dict[str, Any]) -> Dict[str, Any]:
        idx = call_count["n"]
        call_count["n"] += 1
        return pages[min(idx, len(pages) - 1)]

    import cdf_fn_common.etl_streams_records_api as api_mod

    api_mod.sync_records = fake_sync  # type: ignore[method-assign]
    api_mod.filter_records = fake_sync  # type: ignore[method-assign]

    data: Dict[str, Any] = {
        "run_id": "00000000-0000-4000-8000-000000000002",
        "task_id": "t1",
        "config": {
            "stream_external_id": "stream-a",
            "read_mode": "sync",
            "local_predecessor_mode": "in_memory",
        },
        "local_predecessor_mode": "in_memory",
    }
    summary = etl_handle_query_records("fn_etl_records_query", data, client, None)
    assert summary["rows_read"] >= 1
    assert len(data.get("_predecessor_rows") or []) >= 1


def test_trigger_record_stream_rule() -> None:
    import sys
    from pathlib import Path

    scripts = Path(__file__).resolve().parents[2] / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))

    from workflow_build.trigger_from_canvas import read_start_trigger_config

    canvas = {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {
                    "config": {
                        "trigger_type": "recordStream",
                        "stream_external_id": "my-stream",
                        "batch_size": 100,
                        "batch_timeout": 60,
                    }
                },
            }
        ]
    }
    cfg = read_start_trigger_config(canvas)
    rule = cfg["trigger_rule"]
    assert rule["triggerType"] == "recordStream"
    assert rule["streamExternalId"] == "my-stream"
    assert rule["batchSize"] == 100
    assert rule["batchTimeout"] == 60
