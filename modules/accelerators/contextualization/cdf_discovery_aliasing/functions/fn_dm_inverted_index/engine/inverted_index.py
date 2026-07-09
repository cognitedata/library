"""Build / maintain inverted index rows from discovery predecessor cohort RAW."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from cdf_fn_common.discovery_inverted_index import run_discovery_inverted_index
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def run_inverted_index(
    data: MutableMapping[str, Any], client: Any, log: Any
) -> Dict[str, Any]:
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
            "entries_created": 0,
            "entries_updated": 0,
            "entities": 0,
            "postings": 0,
            "index_kinds_configured": summary.get("index_kinds_configured"),
        }
    else:
        inv = {
            "status": "ok",
            "entries_created": int(summary.get("entries_created") or 0),
            "entries_updated": int(summary.get("entries_updated") or 0),
            "entities": int(summary.get("entities") or 0),
            "postings": int(summary.get("postings") or 0),
            "rows_read": int(summary.get("rows_read") or 0),
            "storage_backend": summary.get("storage_backend"),
            "match_scope_key": summary.get("match_scope_key"),
            "raw_database": summary.get("raw_database"),
            "run_id": summary.get("run_id"),
            "index_kinds_configured": summary.get("index_kinds_configured"),
            "predecessor_raw_sources": summary.get("predecessor_raw_sources"),
        }
    if log and hasattr(log, "info"):
        log.info(
            "fn_dm_inverted_index created=%s updated=%s postings=%s backend=%s",
            inv.get("entries_created"),
            inv.get("entries_updated"),
            inv.get("postings"),
            inv.get("storage_backend"),
        )
    return {
        "function_external_id": "fn_dm_inverted_index",
        "task_id": data.get("task_id"),
        "inverted_index": inv,
    }
