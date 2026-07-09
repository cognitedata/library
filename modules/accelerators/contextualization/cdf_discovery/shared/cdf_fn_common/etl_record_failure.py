"""Per-entity processing failure recording for cohort RAW (ETL transform path)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
from cdf_fn_common.etl_discovery_query_shared import _as_dict
from cdf_fn_common.etl_incremental_scope import (
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_FAILED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
)


@dataclass
class EntityFailureRecorder:
    client: Any
    raw_db: str
    raw_table: str
    max_processing_attempts: Optional[int] = None
    entities_failed: int = 0

    def flush_fdm(self, *, log: Any = None) -> None:
        return


def max_processing_attempts_from_data(data: Mapping[str, Any]) -> Optional[int]:
    cfg = _as_dict(data.get("configuration"))
    params = _as_dict(cfg.get("parameters"))
    raw = params.get("max_processing_attempts")
    if raw is None:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def build_entity_failure_recorder(
    client: Any,
    data: Mapping[str, Any],
    *,
    raw_db: str,
    raw_table: str,
    log: Any = None,
) -> EntityFailureRecorder:
    return EntityFailureRecorder(
        client=client,
        raw_db=raw_db,
        raw_table=raw_table,
        max_processing_attempts=max_processing_attempts_from_data(data),
    )


def mark_cohort_entity_failed(
    client: Any,
    raw_db: str,
    raw_table: str,
    row_key: str,
    cols: Mapping[str, Any],
) -> None:
    rk = str(row_key or "").strip()
    if not rk:
        return
    new_cols = dict(cols)
    new_cols[WORKFLOW_STATUS_COLUMN] = WORKFLOW_STATUS_FAILED
    new_cols[WORKFLOW_STATUS_UPDATED_AT_COLUMN] = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    )
    create_table_if_not_exists(client, raw_db, raw_table)
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={rk: new_cols})


def record_entity_processing_failure(
    recorder: EntityFailureRecorder,
    *,
    row_key: str,
    cols: Mapping[str, Any],
    error_message: str,
    log: Any = None,
) -> bool:
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
    try:
        mark_cohort_entity_failed(recorder.client, recorder.raw_db, recorder.raw_table, row_key, cols)
    except Exception as write_ex:
        if log and hasattr(log, "warning"):
            log.warning(
                "Failed to persist processing failure node=%s row_key=%s table=%s/%s: %s",
                nid or "?",
                row_key,
                recorder.raw_db,
                recorder.raw_table,
                (str(write_ex) or "")[:500],
            )
        return False
    recorder.entities_failed += 1
    if log and hasattr(log, "warning"):
        log.warning(
            "Entity processing failed node=%s row_key=%s: %s",
            nid or "?",
            row_key,
            (error_message or "")[:500],
        )
    return True
