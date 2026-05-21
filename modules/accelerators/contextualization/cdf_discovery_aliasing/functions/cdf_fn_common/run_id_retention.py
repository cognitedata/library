"""Pipeline run_id parsing and retention cutoff helpers for RAW cohort cleanup."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from .incremental_scope import (
    EXTRACTION_INPUTS_HASH_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_WATERMARK,
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_CHECKPOINT,
    raw_row_columns,
)

DEFAULT_RETENTION_HOURS = 72.0

# ``YYYYMMDDTHHMMSS.ffffffZ`` or ``…Z-{12 hex}`` (see new_pipeline_run_id).
_PIPELINE_RUN_ID_TS_RE = re.compile(
    r"^(\d{8}T\d{6}\.\d+)Z(?:-[0-9a-f]{12})?$",
    re.IGNORECASE,
)


def parse_pipeline_run_id_utc(run_id: str) -> Optional[datetime]:
    """Return UTC datetime from a generated pipeline ``run_id``, or ``None`` if not parseable."""
    rid = str(run_id or "").strip()
    if not rid:
        return None
    m = _PIPELINE_RUN_ID_TS_RE.match(rid)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S.%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def is_run_id_older_than(
    run_id: str,
    *,
    max_age_hours: float = DEFAULT_RETENTION_HOURS,
    now: Optional[datetime] = None,
) -> bool:
    """True when ``run_id`` encodes a pipeline timestamp strictly before ``now - max_age_hours``."""
    ts = parse_pipeline_run_id_utc(run_id)
    if ts is None:
        return False
    ref = now if now is not None else datetime.now(timezone.utc)
    cutoff = ref - timedelta(hours=float(max_age_hours))
    return ts < cutoff


def run_id_from_row(key: str, columns: Mapping[str, Any]) -> str:
    """Pipeline ``run_id`` from the ``RUN_ID`` column (row keys are canvas-node scoped)."""
    _ = key
    return str(columns.get(RUN_ID_COLUMN) or "").strip()


def should_purge_cohort_row(
    key: str,
    columns: Mapping[str, Any],
    *,
    current_run_id: str,
    cutoff_utc: datetime,
) -> bool:
    """
    True when an inter-node cohort row should be deleted (current run or older than cutoff).

    Watermarks and checkpoint rows are never purged. Rows with ``EXTRACTION_INPUTS_HASH`` are
    retained for incremental skip logic (except when older than *cutoff_utc*). Ephemeral cohort
    rows without a digest are still purged for the current ``run_id``.
    """
    k = str(key or "").strip()
    if k.startswith("scope_wm_"):
        return False
    rk = str(columns.get(RECORD_KIND_COLUMN) or "").strip().lower()
    if rk in (RECORD_KIND_WATERMARK, WORKFLOW_STATUS_CHECKPOINT, "checkpoint"):
        return False
    rid = run_id_from_row(k, columns)
    if not rid:
        return False
    digest = columns.get(EXTRACTION_INPUTS_HASH_COLUMN)
    if digest is not None and str(digest).strip():
        ts = parse_pipeline_run_id_utc(rid)
        if ts is not None and ts < cutoff_utc:
            return True
        return False
    cur = str(current_run_id or "").strip()
    if cur and rid == cur:
        return True
    ts = parse_pipeline_run_id_utc(rid)
    if ts is None:
        return False
    return ts < cutoff_utc


def should_purge_cohort_raw_row(row: Any, *, current_run_id: str, cutoff_utc: datetime) -> bool:
    """Convenience wrapper for Cognite RAW row objects."""
    key = str(getattr(row, "key", "") or "").strip()
    return should_purge_cohort_row(
        key, raw_row_columns(row), current_run_id=current_run_id, cutoff_utc=cutoff_utc
    )
