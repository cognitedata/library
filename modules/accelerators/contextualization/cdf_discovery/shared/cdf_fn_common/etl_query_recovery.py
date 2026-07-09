"""Query handler checkpoint helpers for retry/resume semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
from cdf_fn_common.etl_cohort_storage import resolve_incremental_state_sink
from cdf_fn_common.etl_incremental_scope import (
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    WORKFLOW_SCOPE_COLUMN,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    raw_row_columns,
)
from cdf_fn_common.etl_run_scope import resolve_workflow_scope

RECORD_KIND_QUERY_CHECKPOINT = "query_checkpoint"
_CHECKPOINT_COUNT_COLUMN = "CHECKPOINT_COUNT"
_CHECKPOINT_COMPLETE_COLUMN = "CHECKPOINT_COMPLETE"
_TASK_ID_COLUMN = "TASK_ID"


@dataclass(frozen=True)
class QueryCheckpointState:
    rows_completed: int = 0
    is_complete: bool = False
    continuation_token: str = ""


_CHECKPOINT_TOKEN_COLUMN = "CHECKPOINT_TOKEN"


def _checkpoint_row_key(task_id: str, workflow_scope: str) -> str:
    tid = str(task_id or "").strip() or "task"
    ws = str(workflow_scope or "").strip() or "default"
    return f"qcp:{ws}:{tid}"[:256]


def load_query_checkpoint_state(
    client: Any,
    data: Mapping[str, Any],
    *,
    task_id: str,
) -> QueryCheckpointState:
    if client is None:
        return QueryCheckpointState()
    raw_db, raw_table = resolve_incremental_state_sink(data)
    create_table_if_not_exists(client, raw_db, raw_table)
    workflow_scope = resolve_workflow_scope(data)
    row_key = _checkpoint_row_key(task_id, workflow_scope)
    try:
        row = client.raw.rows.retrieve(raw_db, raw_table, row_key)
    except Exception:
        return QueryCheckpointState()
    if not row:
        return QueryCheckpointState()
    cols = raw_row_columns(row)
    if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_QUERY_CHECKPOINT:
        return QueryCheckpointState()
    try:
        n = int(cols.get(_CHECKPOINT_COUNT_COLUMN) or 0)
    except (TypeError, ValueError):
        n = 0
    done_raw = cols.get(_CHECKPOINT_COMPLETE_COLUMN)
    done = bool(done_raw) if isinstance(done_raw, bool) else str(done_raw).lower() == "true"
    token = str(cols.get(_CHECKPOINT_TOKEN_COLUMN) or "").strip()
    return QueryCheckpointState(rows_completed=max(0, n), is_complete=done, continuation_token=token)


def save_query_checkpoint_state(
    client: Any,
    data: Mapping[str, Any],
    *,
    task_id: str,
    run_id: str,
    rows_completed: int,
    is_complete: bool,
    continuation_token: str = "",
) -> None:
    if client is None:
        return
    raw_db, raw_table = resolve_incremental_state_sink(data)
    create_table_if_not_exists(client, raw_db, raw_table)
    workflow_scope = resolve_workflow_scope(data)
    row_key = _checkpoint_row_key(task_id, workflow_scope)
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_QUERY_CHECKPOINT,
        WORKFLOW_SCOPE_COLUMN: workflow_scope,
        RUN_ID_COLUMN: run_id,
        _TASK_ID_COLUMN: task_id,
        _CHECKPOINT_COUNT_COLUMN: max(0, int(rows_completed)),
        _CHECKPOINT_COMPLETE_COLUMN: bool(is_complete),
        _CHECKPOINT_TOKEN_COLUMN: str(continuation_token or ""),
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
    }
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={row_key: cols})
