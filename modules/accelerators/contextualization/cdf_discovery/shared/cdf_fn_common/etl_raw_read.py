"""RAW table row iteration and property parsing for ETL query handlers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any, Dict

RECORD_KIND_COLUMN = "RECORD_KIND"
RECORD_KIND_ENTITY = "entity"
RUN_ID_COLUMN = "RUN_ID"
PROPERTIES_JSON_COLUMN = "PROPERTIES_JSON"
EXTERNAL_ID_COLUMN = "EXTERNAL_ID"
NODE_INSTANCE_ID_COLUMN = "NODE_INSTANCE_ID"


def raw_row_columns(row: Any) -> Dict[str, Any]:
    cols = getattr(row, "columns", None) or {}
    return dict(cols) if isinstance(cols, dict) else {}


def iter_raw_table_rows_chunked(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    chunk_size: int = 2500,
) -> Iterator[Any]:
    """Iterate all RAW rows (chunked iterator API when available)."""
    rows_api = client.raw.rows
    if callable(rows_api):
        for item in rows_api(raw_db, raw_table, chunk_size=chunk_size):
            if hasattr(item, "columns"):
                yield item
                continue
            if isinstance(item, Iterable) and not isinstance(item, (str, bytes, dict)):
                for row in item:
                    if hasattr(row, "columns"):
                        yield row
        return
    listed = rows_api.list(raw_db, raw_table, limit=-1)
    for row in listed:
        yield row


def parse_raw_row_properties(cols: Dict[str, Any]) -> Dict[str, Any]:
    """Parse PROPERTIES_JSON for filter eval; fall back to raw column map."""
    props_raw = cols.get(PROPERTIES_JSON_COLUMN)
    if isinstance(props_raw, str) and props_raw.strip():
        try:
            parsed = json.loads(props_raw)
            if isinstance(parsed, dict):
                return dict(parsed)
        except json.JSONDecodeError:
            pass
    return {"raw_columns": dict(cols)}
