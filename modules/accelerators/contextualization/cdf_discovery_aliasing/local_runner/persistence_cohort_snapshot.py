"""Capture cohort RAW rows (and inverted-index sink) at persistence tasks for run results."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.cohort_storage import (
    predecessor_canvas_node_ids,
    predecessor_node_table_locations,
    require_run_id,
)
from cdf_fn_common.discovery_cohort import iter_predecessor_cohort_rows
from cdf_fn_common.discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    resolve_inverted_index_sink,
    resolve_task_config,
)
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .raw_results_attachment import _fetch_rows_sample

PERSISTENCE_COHORT_SCHEMA_VERSION = 1

PERSISTENCE_SNAPSHOT_FUNCTIONS: frozenset[str] = frozenset(
    {
        "fn_dm_view_save",
        "fn_dm_raw_save",
        "fn_dm_classic_save",
        "fn_dm_inverted_index",
    }
)


def parse_handler_summary_message(message: Any) -> Dict[str, Any]:
    if not isinstance(message, str) or not message.strip():
        return {}
    try:
        parsed = json.loads(message)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _serialize_cohort_row(row: Any) -> Dict[str, Any]:
    cols = dict(getattr(row, "columns", None) or {})
    return {
        "key": str(getattr(row, "key", "") or ""),
        "columns": cols,
    }


def _collect_predecessor_cohort_rows(
    client: Any,
    data: Mapping[str, Any],
    *,
    task_id: str,
    row_limit: int,
) -> Dict[str, Any]:
    pred_locations = predecessor_node_table_locations(data, task_id)
    pred_canvas_nodes = predecessor_canvas_node_ids(data, task_id)
    sources = [{"raw_db": d, "raw_table": t} for d, t in pred_locations]
    rows: List[Dict[str, Any]] = []
    truncated = False
    for row in iter_predecessor_cohort_rows(client, data, task_id):
        cols = dict(getattr(row, "columns", None) or {})
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        rows.append(_serialize_cohort_row(row))
        if len(rows) >= row_limit:
            truncated = True
            break
    return {
        "predecessor_sources": sources,
        "predecessor_canvas_node_ids": pred_canvas_nodes,
        "cohort_rows": rows,
        "cohort_row_count": len(rows),
        "truncated": truncated,
        "row_limit": row_limit,
    }


def _sample_inverted_index_sink(
    client: Any,
    data: Mapping[str, Any],
    *,
    row_limit: int,
    logger: Any,
    max_raw_rows_scanned: Optional[int],
) -> Dict[str, Any]:
    raw_db, raw_table = resolve_inverted_index_sink(data)
    rows, truncated, err, examined, scan_truncated = _fetch_rows_sample(
        client,
        raw_db,
        raw_table,
        row_limit=row_limit,
        logger=logger,
        run_id=None,
        max_raw_rows_scanned=max_raw_rows_scanned,
    )
    out: Dict[str, Any] = {
        "raw_db": raw_db,
        "raw_table": raw_table,
        "row_count": len(rows),
        "row_limit": row_limit,
        "truncated": truncated,
        "rows_examined": examined,
        "raw_scan_truncated": scan_truncated,
        "index_rows": rows,
    }
    if err:
        out["error"] = err
    return out


def build_persistence_cohort_snapshot(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    task_id: str,
    function_external_id: str,
    row_limit: int = 500,
    logger: Any = None,
    max_raw_rows_scanned: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Snapshot cohort entity rows read by save / inverted-index handlers, plus inverted-index sink rows.

    Reads predecessor **node tables** for the current pipeline run.
    """
    merge_compiled_task_into_data(data)
    run_id = require_run_id(data)
    data["run_id"] = run_id
    limit = max(1, int(row_limit or 500))

    out: Dict[str, Any] = {
        "schema_version": PERSISTENCE_COHORT_SCHEMA_VERSION,
        "task_id": task_id,
        "function_external_id": function_external_id,
        "run_id": run_id,
    }

    if client is None:
        out["predecessor_cohort"] = {
            "predecessor_sources": [],
            "predecessor_canvas_node_ids": [],
            "cohort_rows": [],
            "cohort_row_count": 0,
            "truncated": False,
            "row_limit": limit,
            "error": "no_client",
        }
    else:
        out["predecessor_cohort"] = _collect_predecessor_cohort_rows(
            client,
            data,
            task_id=task_id,
            row_limit=limit,
        )

    if function_external_id == "fn_dm_inverted_index":
        if client is None:
            raw_db, raw_table = resolve_inverted_index_sink(data)
            out["inverted_index_persistence"] = {
                "raw_db": raw_db,
                "raw_table": raw_table,
                "row_count": 0,
                "row_limit": limit,
                "truncated": False,
                "index_rows": [],
                "error": "no_client",
            }
        else:
            out["inverted_index_persistence"] = _sample_inverted_index_sink(
                client,
                data,
                row_limit=limit,
                logger=logger,
                max_raw_rows_scanned=max_raw_rows_scanned,
            )

    return out
