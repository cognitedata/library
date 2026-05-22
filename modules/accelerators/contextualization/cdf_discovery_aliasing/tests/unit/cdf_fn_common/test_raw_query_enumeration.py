"""RAW query enumeration and truncation."""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

from fn_dm_raw_query.engine.orchestration import discovery_handle_raw_query


def test_raw_query_unlimited_when_read_limit_zero() -> None:
    inserted: List[Dict[str, Any]] = []

    class _Row:
        def __init__(self, key: str) -> None:
            self.key = key
            self.columns = {
                "NODE_INSTANCE_ID": key,
                "EXTERNAL_ID": key,
                "SCOPE_KEY": "s",
            }

    class _Rows:
        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            inserted.append(row)

    rows = [_Row("1"), _Row("2"), _Row("3")]

    client = MagicMock()
    client.raw.rows = _Rows()

    def _iter(*_a: Any, **_k: Any):
        for r in rows:
            yield r

    import fn_dm_raw_query.engine.handlers.raw_query as raw_mod

    raw_mod.iter_raw_table_rows_chunked = lambda *_a, **_k: _iter()
    try:
        data = {
            "task_id": "kea__rq",
            "run_id": "run_x",
            "config": {
                "source_raw_db": "db",
                "source_raw_table": "tbl",
            },
        }
        summary = discovery_handle_raw_query("fn_dm_raw_query", data, client, None)
    finally:
        from cdf_fn_common.incremental_scope import iter_raw_table_rows_chunked

        raw_mod.iter_raw_table_rows_chunked = iter_raw_table_rows_chunked

    assert summary["instances_written"] == 3
    assert summary["rows_truncated"] is False
    assert summary["list_complete"] is True


def test_raw_query_truncates_when_read_limit_set() -> None:
    inserted: List[Dict[str, Any]] = []

    class _Row:
        def __init__(self, key: str) -> None:
            self.key = key
            self.columns = {"NODE_INSTANCE_ID": key, "EXTERNAL_ID": key}

    class _Rows:
        def insert(self, *, db_name: str, table_name: str, row: Dict[str, Any]) -> None:
            inserted.append(row)

    rows = [_Row(str(i)) for i in range(10)]

    client = MagicMock()
    client.raw.rows = _Rows()

    def _iter(*_a: Any, **_k: Any):
        for r in rows:
            yield r

    import fn_dm_raw_query.engine.handlers.raw_query as raw_mod

    raw_mod.iter_raw_table_rows_chunked = lambda *_a, **_k: _iter()
    try:
        data = {
            "task_id": "kea__rq",
            "run_id": "run_x",
            "config": {
                "source_raw_db": "db",
                "source_raw_table": "tbl",
                "read_limit": 5,
            },
        }
        summary = discovery_handle_raw_query("fn_dm_raw_query", data, client, None)
    finally:
        from cdf_fn_common.incremental_scope import iter_raw_table_rows_chunked

        raw_mod.iter_raw_table_rows_chunked = iter_raw_table_rows_chunked

    assert summary["instances_written"] == 5
    assert summary["rows_truncated"] is True
    assert summary["truncation_reason"] == "read_limit"
