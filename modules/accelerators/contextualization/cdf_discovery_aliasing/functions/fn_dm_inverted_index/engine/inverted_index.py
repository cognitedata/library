"""Build / maintain inverted index rows from discovery predecessor cohort RAW."""

from __future__ import annotations

import json
from typing import Any, Dict

from cdf_fn_common.discovery_inverted_index import run_discovery_inverted_index
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def run_inverted_index(data: Dict[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    """
    Index lookup keys from configured ``index_kinds`` on predecessor cohort payloads.

    Property names and kinds come only from task ``config`` (canvas node ``data.config``).
    """
    merge_compiled_task_into_data(data)
    summary = run_discovery_inverted_index(
        "fn_dm_inverted_index",
        data,
        client,
        log,
    )
    if summary.get("status") == "skipped":
        inv = {
            "status": "skipped",
            "reason": summary.get("reason", "no_index_kinds_configured"),
            "inverted_writes": 0,
            "entities": 0,
            "postings": 0,
            "index_kinds_configured": summary.get("index_kinds_configured"),
        }
    else:
        inv = {
            "status": "ok",
            "inverted_writes": int(summary.get("inverted_writes") or 0),
            "entities": int(summary.get("entities") or 0),
            "postings": int(summary.get("postings") or 0),
            "rows_read": int(summary.get("rows_read") or 0),
            "raw_db": summary.get("raw_db"),
            "raw_table": summary.get("raw_table"),
            "run_id": summary.get("run_id"),
            "index_kinds_configured": summary.get("index_kinds_configured"),
            "predecessor_raw_sources": summary.get("predecessor_raw_sources"),
        }
    msg = json.dumps(
        {
            "function_external_id": "fn_dm_inverted_index",
            "task_id": data.get("task_id"),
            "inverted_index": inv,
        },
        default=str,
    )
    data["status"] = "succeeded"
    data["message"] = msg
    if log:
        log.info(
            "fn_dm_inverted_index writes=%s postings=%s",
            inv.get("inverted_writes"),
            inv.get("postings"),
        )
    return {"status": "succeeded", "message": msg}
