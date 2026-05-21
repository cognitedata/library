"""Incremental hash must survive transform/validate RAW overwrites."""

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

from cdf_fn_common.discovery_query_shared import (  # noqa: E402
    EXTRACTION_INPUTS_HASH_COLUMN,
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    WORKFLOW_STATUS_COLUMN,
)
from cdf_fn_common.incremental_scope import (  # noqa: E402
    build_latest_hash_index_for_table,
)


class _FakeInstance:
    def __init__(self, *, external_id: str, instance_id: str, properties: Dict[str, Any]) -> None:
        self.external_id = external_id
        self.space = "sp1"
        self.instance_id = instance_id
        self.last_updated_time = 1_700_000_000_000
        self._properties = properties

    def dump(self) -> Dict[str, Any]:
        return {
            "externalId": self.external_id,
            "space": self.space,
            "lastUpdatedTime": self.last_updated_time,
            "properties": {"cdf_cdm": {"CogniteFile/v1": self._properties}},
        }


def test_transform_preserves_hash_for_incremental_index(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate view query then transform on the same RAW key; hash index must retain view-query hash."""
    stored: Dict[str, Dict[str, Any]] = {}

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            del db_name, table_name
            for key, cols in row.items():
                stored[key] = dict(cols)

        def __call__(self, *_a, **_k):
            for key in sorted(stored.keys()):
                yield type("Row", (), {"key": key, "columns": stored[key]})()

    client = MagicMock()
    client.raw.rows = _Rows()

    inst = _FakeInstance(
        external_id="FILE-1",
        instance_id="uuid-file-1",
        properties={"name": "doc.pdf", "externalId": "FILE-1", "mimeType": "application/pdf"},
    )

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        lambda *_a, **_k: iter([inst]),
    )

    scope_key = "test_scope_file"
    run_id = "20260101T120000.000000Z-abc123"
    vq_data: Dict[str, Any] = {
        "task_id": "kea__vq_file",
        "run_id": run_id,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "incremental_change_processing": True,
            "include_properties": ["name", "externalId", "mimeType"],
            "filters": [],
        },
        "configuration": {},
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler

    ViewQueryHandler.run("fn_dm_view_query", vq_data, client, None)

    row_key = next(iter(stored))
    assert stored[row_key].get(EXTRACTION_INPUTS_HASH_COLUMN)

    tr_data: Dict[str, Any] = {
        "task_id": "kea__tr_file",
        "run_id": run_id,
        "config": {
            "description": "trim",
            "handler_id": "trim_whitespace",
            "enabled": True,
            "steps": [{"handler_id": "trim_whitespace", "enabled": True, "fields": [{"field_name": "name"}]}],
        },
        "compiled_workflow": {
            "tasks": [
                {"id": "kea__vq_file", "depends_on": [], "persistence": {}},
                {
                    "id": "kea__tr_file",
                    "depends_on": ["kea__vq_file"],
                    "persistence": {},
                },
            ]
        },
        "discovery_predecessor_outputs": {
            "kea__vq_file": {
                "raw_db": "db_discovery",
                "raw_table": "discovery_state",
            }
        },
    }
    from fn_dm_transform.engine.orchestration import discovery_handle_transform

    discovery_handle_transform("fn_dm_transform", tr_data, client, None)

    assert stored[row_key].get(EXTRACTION_INPUTS_HASH_COLUMN)
    index = build_latest_hash_index_for_table(client, "db_discovery", "discovery_state")
    assert scope_key not in index or not index
    # scope_key comes from view config; resolve from stored row
    sk = str(stored[row_key].get(SCOPE_KEY_COLUMN) or "")
    assert sk
    assert index[sk]["sp1:uuid-file-1"] == stored[row_key][EXTRACTION_INPUTS_HASH_COLUMN]


def test_second_view_query_skips_unchanged_after_transform_wrote_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Second incremental listing should skip cohort emit when hash survived transform overwrite."""
    stored: Dict[str, Dict[str, Any]] = {}

    class _Rows:
        def retrieve(self, *_a, **_k):
            return None

        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            del db_name, table_name
            for key, cols in row.items():
                stored[key] = dict(cols)

        def __call__(self, *_a, **_k):
            for key in sorted(stored.keys()):
                yield type("Row", (), {"key": key, "columns": stored[key]})()

    client = MagicMock()
    client.raw.rows = _Rows()

    inst = _FakeInstance(
        external_id="FILE-1",
        instance_id="uuid-file-1",
        properties={"name": "doc.pdf", "externalId": "FILE-1", "mimeType": "application/pdf"},
    )

    monkeypatch.setattr(
        "fn_dm_view_query.engine.handlers.view_query.list_all_instances",
        lambda *_a, **_k: iter([inst]),
    )

    run_id = "20260101T120000.000000Z-abc123"
    vq_data: Dict[str, Any] = {
        "task_id": "kea__vq_file",
        "run_id": run_id,
        "config": {
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
            "incremental_change_processing": True,
            "include_properties": ["name", "externalId", "mimeType"],
            "filters": [],
        },
        "configuration": {},
    }
    tr_data: Dict[str, Any] = {
        "task_id": "kea__tr_file",
        "run_id": run_id,
        "config": {
            "description": "trim",
            "handler_id": "trim_whitespace",
            "enabled": True,
            "steps": [{"handler_id": "trim_whitespace", "enabled": True, "fields": [{"field_name": "name"}]}],
        },
        "compiled_workflow": {
            "tasks": [
                {"id": "kea__vq_file", "canvas_node_id": "vq_file", "depends_on": []},
                {"id": "kea__tr_file", "canvas_node_id": "tr_file", "depends_on": ["kea__vq_file"]},
            ]
        },
        "discovery_predecessor_outputs": {
            "kea__vq_file": {"raw_db": "db_discovery", "raw_table": "discovery_state"}
        },
    }
    from fn_dm_view_query.engine.handlers.view_query import ViewQueryHandler
    from fn_dm_transform.engine.orchestration import discovery_handle_transform

    summary1 = ViewQueryHandler.run("fn_dm_view_query", dict(vq_data), client, None)
    discovery_handle_transform("fn_dm_transform", tr_data, client, None)
    assert summary1["instances_written"] == 1

    vq_data["run_id"] = "20260102T120000.000000Z-def456"
    summary2 = ViewQueryHandler.run("fn_dm_view_query", dict(vq_data), client, None)
    assert summary2["instances_skipped_unchanged_hash"] == 1
    assert summary2["instances_written"] == 0
