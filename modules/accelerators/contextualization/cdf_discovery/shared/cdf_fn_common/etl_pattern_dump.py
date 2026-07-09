"""RAW dump of full diagram-detect job payloads (``db_discovery.pattern_dump``)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, MutableSequence, Optional, Sequence

from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
from cdf_fn_common.etl_incremental_scope import (
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    WORKFLOW_SCOPE_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_DETECTED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
)

PATTERN_DUMP_RAW_DB = "db_discovery"
PATTERN_DUMP_RAW_TABLE = "pattern_dump"

RECORD_KIND_PATTERN_DUMP = "pattern_detect_dump"
RESULT_JSON_COLUMN = "RESULT_JSON"
JOB_ID_COLUMN = "JOB_ID"
TASK_ID_COLUMN = "TASK_ID"
PACK_INDEX_COLUMN = "PACK_INDEX"
PACK_TOTAL_COLUMN = "PACK_TOTAL"
FILE_IDS_JSON_COLUMN = "FILE_IDS_JSON"
ITEMS_COUNT_COLUMN = "ITEMS_COUNT"
DETECT_STATUS_COLUMN = "DETECT_STATUS"

DiagramDetectCompleteHook = Callable[["DiagramDetectCompleteContext"], None]
_hooks: MutableSequence[DiagramDetectCompleteHook] = []


@dataclass(frozen=True)
class DiagramDetectCompleteContext:
    client: Any
    job_id: int
    results: Mapping[str, Any]
    run_id: str
    workflow_scope: str
    task_id: str
    pack_index: int
    pack_total: int
    file_ids: Sequence[int]
    log: Any = None


def register_diagram_detect_complete_hook(hook: DiagramDetectCompleteHook) -> None:
    _hooks.append(hook)


def invoke_diagram_detect_complete_hooks(ctx: DiagramDetectCompleteContext) -> None:
    for hook in _hooks:
        hook(ctx)


def pattern_dump_row_key(*, run_id: str, job_id: int, workflow_scope: str = "") -> str:
    ws = str(workflow_scope or "").strip().replace(":", "_")
    base = f"pattern_dump_{run_id}_job_{int(job_id)}"
    if ws:
        return f"{base}_{ws}"[:240]
    return base[:240]


def persist_diagram_detect_pattern_dump(
    ctx: DiagramDetectCompleteContext,
    *,
    raw_db: str = PATTERN_DUMP_RAW_DB,
    raw_table: str = PATTERN_DUMP_RAW_TABLE,
    log: Any = None,
) -> str:
    """Write one completed diagram-detect job body to RAW ``pattern_dump``."""
    if ctx.client is None:
        raise ValueError("pattern_dump requires a CDF client")

    create_table_if_not_exists(ctx.client, raw_db, raw_table)
    items = ctx.results.get("items") if isinstance(ctx.results, Mapping) else None
    items_count = len(items) if isinstance(items, list) else 0
    row_key = pattern_dump_row_key(
        run_id=ctx.run_id,
        job_id=ctx.job_id,
        workflow_scope=ctx.workflow_scope,
    )
    cols: Dict[str, Any] = {
        RECORD_KIND_COLUMN: RECORD_KIND_PATTERN_DUMP,
        WORKFLOW_SCOPE_COLUMN: ctx.workflow_scope,
        RUN_ID_COLUMN: ctx.run_id,
        TASK_ID_COLUMN: ctx.task_id,
        JOB_ID_COLUMN: int(ctx.job_id),
        PACK_INDEX_COLUMN: int(ctx.pack_index),
        PACK_TOTAL_COLUMN: int(ctx.pack_total),
        FILE_IDS_JSON_COLUMN: json.dumps([int(fid) for fid in ctx.file_ids], default=str),
        ITEMS_COUNT_COLUMN: items_count,
        DETECT_STATUS_COLUMN: str(ctx.results.get("status") or "Completed"),
        WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_DETECTED,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        RESULT_JSON_COLUMN: json.dumps(dict(ctx.results), default=str),
    }
    ctx.client.raw.rows.insert(
        db_name=raw_db,
        table_name=raw_table,
        row={row_key: cols},
    )
    if log and hasattr(log, "info"):
        log.info(
            "pattern_dump job_id=%s items=%s row_key=%s sink=%s/%s",
            ctx.job_id,
            items_count,
            row_key,
            raw_db,
            raw_table,
        )
    return row_key


def _default_pattern_dump_hook(ctx: DiagramDetectCompleteContext) -> None:
    persist_diagram_detect_pattern_dump(ctx, log=ctx.log)


register_diagram_detect_complete_hook(_default_pattern_dump_hook)
