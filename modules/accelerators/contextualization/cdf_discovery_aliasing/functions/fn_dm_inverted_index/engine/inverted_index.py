"""Inverted index build (discovery stub — predecessor introspection only)."""

from __future__ import annotations

import json
from typing import Any, Dict

from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def run_inverted_index_stub(data: Dict[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    """
    Build / maintain inverted index rows (stub).

    **Discovery path:** ``data`` includes ``discovery_predecessor_outputs`` (task_id → last snapshot)
    from the local runner, plus IR ``payload`` keys ``inverted_index_input_source`` and
    ``upstream_compiled_task_ids``. Full RAW indexing from FK/document JSON replaces this stub.
    """
    del client
    merge_compiled_task_into_data(data)
    preds = data.get("discovery_predecessor_outputs")
    n_pred = len(preds) if isinstance(preds, dict) else 0
    upstream = data.get("upstream_compiled_task_ids")
    if not isinstance(upstream, list):
        upstream = []
    inv = {
        "status": "ok" if n_pred else "skipped",
        "inverted_writes": 0,
        "entities": 0,
        "postings": 0,
        "fk_postings": 0,
        "doc_postings": 0,
        "source": str(data.get("inverted_index_input_source") or "discovery_predecessor_payloads"),
        "predecessor_tasks": n_pred,
        "upstream_compiled_task_ids": upstream,
    }
    if inv["status"] == "skipped":
        inv["reason"] = "no_discovery_predecessor_outputs"
    msg = json.dumps(
        {
            "stub": True,
            "function_external_id": "fn_dm_inverted_index",
            "task_id": data.get("task_id"),
            "inverted_index": inv,
        }
    )
    data["status"] = "succeeded"
    data["message"] = msg
    if log:
        log.info("fn_dm_inverted_index (discovery stub) predecessors=%s", n_pred)
    return {"status": "succeeded", "message": msg}
