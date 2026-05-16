"""Attach RAW table row samples to local discovery run JSON (from task summary messages)."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from cdf_fn_common.incremental_scope import (
    RUN_ID_COLUMN,
    iter_raw_table_rows_chunked,
)

RAW_RESULTS_SCHEMA_VERSION = 1


def default_max_raw_rows_scanned() -> int:
    """Upper bound on RAW rows read per table when sampling for ``raw_results`` (env override)."""
    try:
        return max(1000, int(os.environ.get("KEA_RAW_RESULTS_MAX_RAW_ROWS_SCANNED", "100000")))
    except ValueError:
        return 100_000


def _first_nonempty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _raw_db_table_from_summary(obj: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    db = _first_nonempty(obj.get("raw_db"), obj.get("sink_raw_db"))
    tbl = _first_nonempty(
        obj.get("raw_table"),
        obj.get("sink_raw_table"),
        obj.get("raw_table_key"),
    )
    if db and tbl:
        return db, tbl
    return None


def _parse_task_message(message: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(message, str) or not message.strip():
        return None
    try:
        parsed = json.loads(message)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def collect_raw_locations_from_task_outputs(
    task_outputs: Mapping[str, Any],
) -> Dict[Tuple[str, str], Set[str]]:
    """
    Map (raw_db, raw_table) -> task ids whose ``message`` JSON references that sink.

    Uses ``discovery_task_outputs`` entries (``status`` / ``message`` per task).
    """
    out: Dict[Tuple[str, str], Set[str]] = {}
    for tid, snap in task_outputs.items():
        if not isinstance(snap, dict):
            continue
        summary = _parse_task_message(snap.get("message"))
        if not summary:
            continue
        loc = _raw_db_table_from_summary(summary)
        if not loc:
            continue
        out.setdefault(loc, set()).add(str(tid))
    return out


def _row_matches_run_id_filter(columns: Mapping[str, Any], run_id: Optional[str]) -> bool:
    """When *run_id* is set, keep only rows that declare the same ``RUN_ID`` (cohort tables)."""
    rid = (run_id or "").strip()
    if not rid:
        return True
    if RUN_ID_COLUMN not in columns:
        return True
    return str(columns.get(RUN_ID_COLUMN) or "").strip() == rid


def _fetch_rows_sample(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    row_limit: int,
    logger: Any,
    run_id: Optional[str] = None,
    max_raw_rows_scanned: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], bool, Optional[str], int, bool]:
    """
    Return (rows, truncated, error, rows_examined, scan_truncated).

    *truncated* is True when at least ``row_limit`` matching rows were collected.
    *scan_truncated* is True when the walk stopped because ``max_raw_rows_scanned`` was exceeded
    before collecting ``row_limit`` rows (sparse ``RUN_ID`` filter on large tables).
    """
    rows: List[Dict[str, Any]] = []
    err: Optional[str] = None
    truncated = False
    scan_truncated = False
    examined = 0
    cap = max_raw_rows_scanned if max_raw_rows_scanned and max_raw_rows_scanned > 0 else default_max_raw_rows_scanned()
    try:
        for row in iter_raw_table_rows_chunked(client, raw_db, raw_table):
            examined += 1
            if examined > cap:
                scan_truncated = True
                break
            cols = getattr(row, "columns", None) or {}
            cdict = dict(cols) if isinstance(cols, dict) else {}
            if not _row_matches_run_id_filter(cdict, run_id):
                continue
            rows.append(
                {
                    "key": str(getattr(row, "key", "") or ""),
                    "columns": cdict,
                }
            )
            if len(rows) >= row_limit:
                truncated = True
                break
    except Exception as ex:
        err = f"{type(ex).__name__}: {ex}"
        if logger and hasattr(logger, "warning"):
            logger.warning(
                "raw_results_attachment: failed to read %s/%s: %s",
                raw_db,
                raw_table,
                err,
            )
    return rows, truncated, err, examined, scan_truncated


def build_raw_results_bundle(
    client: Any,
    task_outputs: Mapping[str, Any],
    *,
    row_limit: int,
    max_tables: int,
    logger: Any,
    run_id: Optional[str] = None,
    max_raw_rows_scanned: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build a JSON-serializable bundle of RAW row samples for tables referenced in *task_outputs*.

    *row_limit* — max rows per table (must be > 0).
    *max_tables* — max distinct (db, table) pairs to fetch.
    *run_id* — when set, cohort-style rows are filtered to ``RUN_ID`` matching this pipeline run
    (so incremental re-runs do not surface unrelated historical rows in the report).
    *max_raw_rows_scanned* — max RAW rows examined per table (default: env ``KEA_RAW_RESULTS_MAX_RAW_ROWS_SCANNED``
    or 100000); stops early with ``raw_scan_truncated`` on the table entry when exceeded.
    """
    if client is None or row_limit <= 0 or max_tables <= 0:
        return {
            "schema_version": RAW_RESULTS_SCHEMA_VERSION,
            "row_limit_per_table": max(0, row_limit),
            "max_tables": max(0, max_tables),
            "tables": [],
            "skipped": "disabled_or_invalid_limits",
        }

    locs = collect_raw_locations_from_task_outputs(task_outputs)
    ordered: List[Tuple[str, str]] = list(locs.keys())
    ordered.sort(key=lambda x: (x[0], x[1]))

    tables_out: List[Dict[str, Any]] = []
    for raw_db, raw_table in ordered[:max_tables]:
        task_ids = sorted(locs.get((raw_db, raw_table), set()))
        rows, truncated, err, examined, scan_truncated = _fetch_rows_sample(
            client,
            raw_db,
            raw_table,
            row_limit=row_limit,
            logger=logger,
            run_id=run_id,
            max_raw_rows_scanned=max_raw_rows_scanned,
        )
        entry: Dict[str, Any] = {
            "raw_db": raw_db,
            "raw_table": raw_table,
            "source_task_ids": task_ids,
            "row_count": len(rows),
            "row_limit": row_limit,
            "truncated": truncated,
            "rows_examined": examined,
            "raw_scan_truncated": scan_truncated,
            "rows": rows,
        }
        if err:
            entry["error"] = err
        tables_out.append(entry)

    out: Dict[str, Any] = {
        "schema_version": RAW_RESULTS_SCHEMA_VERSION,
        "row_limit_per_table": row_limit,
        "max_tables": max_tables,
        "tables_fetched": len(tables_out),
        "tables_known": len(ordered),
        "tables": tables_out,
    }
    if (run_id or "").strip():
        out["run_id"] = str(run_id).strip()
    return out
