"""Minimal shared helpers for ETL pipeline handlers."""

from __future__ import annotations

import json
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple


def _first_nonempty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def new_pipeline_run_id() -> str:
    return str(uuid.uuid4())


def _pipeline_run_seed_payload(data: Mapping[str, Any]) -> Dict[str, Any]:
    configuration = _as_dict(data.get("configuration"))
    params = _as_dict(configuration.get("parameters"))
    payload: Dict[str, Any] = {
        "configuration_id": _first_nonempty(configuration.get("id")),
        "configuration_scope": configuration.get("scope"),
        "workflow_scope": _first_nonempty(params.get("workflow_scope")),
        "schema_version": int(configuration.get("schemaVersion") or 1),
    }
    correlation_id = _first_nonempty(params.get("correlation_id"))
    if correlation_id:
        payload["correlation_id"] = correlation_id
    return payload


def resolve_pipeline_run_key(data: Mapping[str, Any]) -> str:
    explicit = _first_nonempty(data.get("run_id"))
    if explicit:
        return explicit
    configuration = data.get("configuration")
    if not isinstance(configuration, dict):
        return ""
    seed = _pipeline_run_seed_payload(data)
    canonical = json.dumps(seed, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def require_pipeline_run_key(data: Mapping[str, Any]) -> str:
    run_key = resolve_pipeline_run_key(data)
    if run_key:
        if isinstance(data, MutableMapping):
            data["run_id"] = run_key
        return run_key
    keys_sample = sorted([str(k) for k in data.keys()])[:40]
    task_id = str(data.get("task_id") or "")
    compact_probe = {
        "task_id": task_id,
        "keys_sample": keys_sample,
        "configuration_id": _first_nonempty(_as_dict(data.get("configuration")).get("id")),
        "seed_payload": _pipeline_run_seed_payload(data),
    }
    raise ValueError(
        "pipeline_run_key is required. Expected configuration input and optional "
        f"configuration.parameters.correlation_id. probe={json.dumps(compact_probe, default=str)}"
    )


def resolve_task_config(data: Mapping[str, Any]) -> Dict[str, Any]:
    return _as_dict(data.get("config"))


def merge_compiled_task_into_data(data: MutableMapping[str, Any]) -> None:
    """Merge compiled IR task payload into handler ``data`` when present."""
    compiled = data.get("compiled_task")
    if not isinstance(compiled, dict):
        return
    payload = compiled.get("payload")
    if isinstance(payload, dict):
        cfg = payload.get("config")
        if isinstance(cfg, dict):
            data["config"] = {**_as_dict(data.get("config")), **cfg}
    for key in ("persistence", "predecessors"):
        if key in compiled and key not in data:
            data[key] = compiled[key]


def resolve_sink_table(data: Mapping[str, Any]) -> Tuple[str, str]:
    persistence = _as_dict(data.get("persistence"))
    cfg = resolve_task_config(data)
    raw_db = _first_nonempty(
        persistence.get("raw_db"),
        cfg.get("raw_db"),
        "db_discovery_etl",
    )
    raw_table = _first_nonempty(
        persistence.get("raw_table"),
        persistence.get("raw_table_key"),
        cfg.get("raw_table"),
        cfg.get("raw_table_key"),
        data.get("task_id"),
        "etl_state",
    )
    return raw_db, raw_table


def node_instance_id_str(inst: Any) -> str:
    space = _first_nonempty(getattr(inst, "space", None))
    ext = _first_nonempty(getattr(inst, "external_id", None))
    if space and ext:
        return f"{space}:{ext}"
    return ext


def extract_view_properties(instance: Any, view_id: Any) -> Dict[str, Any]:
    dumped = instance.dump() if hasattr(instance, "dump") else {}
    if not isinstance(dumped, dict):
        return {}
    props = (
        dumped.get("properties", {})
        .get(view_id.space, {})
        .get(f"{view_id.external_id}/{view_id.version}", {})
        or {}
    )
    return dict(props) if isinstance(props, dict) else {}


def serialize_row(properties: Mapping[str, Any]) -> str:
    return json.dumps(dict(properties), default=str, sort_keys=True)


def iter_rows_from_task_buffer(
    data: Mapping[str, Any],
    buffer_task_id: str,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Return rows buffered for a completed task (local in-memory DAG handoff)."""
    buffers = data.get("etl_task_row_buffers")
    if not isinstance(buffers, dict):
        return []
    raw = buffers.get(str(buffer_task_id).strip())
    if not isinstance(raw, list):
        return []
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for item in raw:
        if isinstance(item, dict) and isinstance(item.get("properties"), dict):
            out.append((dict(item.get("columns") or {}), dict(item["properties"])))
    return out


def iter_predecessor_rows(data: Mapping[str, Any]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Return ``(columns, properties)`` rows from in-memory predecessor buffer or RAW stub."""
    buf = data.get("_predecessor_rows")
    if isinstance(buf, list):
        out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for item in buf:
            if isinstance(item, dict) and isinstance(item.get("properties"), dict):
                cols = dict(item.get("columns") or {})
                out.append((cols, dict(item["properties"])))
        return out
    return []


def iter_predecessor_rows_for_task(
    data: Mapping[str, Any],
    task_id: str,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Rows from direct compiled-workflow predecessors (``etl_task_row_buffers``),
    else the task-local ``_predecessor_rows`` buffer.
    """
    from cdf_fn_common.etl_cohort_storage import (
        predecessor_canvas_node_ids,
        predecessor_task_ids,
    )

    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for pred_tid in predecessor_task_ids(data, task_id):
        out.extend(iter_rows_from_task_buffer(data, pred_tid))
    if not out:
        for pred_cn in predecessor_canvas_node_ids(data, task_id):
            out.extend(iter_rows_from_task_buffer(data, pred_cn))
    if out:
        return out
    return iter_predecessor_rows(data)


def emit_agent_debug_log(
    *,
    run_id: str,
    hypothesis_id: str,
    location: str,
    message: str,
    data: Mapping[str, Any],
) -> None:
    log_path = Path(
        "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-d31d35.log"
    )
    payload = {
        "sessionId": "d31d35",
        "runId": run_id or "",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": dict(data),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    try:
        with log_path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        return
