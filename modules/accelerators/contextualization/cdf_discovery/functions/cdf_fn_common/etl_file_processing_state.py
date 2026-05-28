"""Per-file RAW state for diagram / pattern-extract workflows."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

from cdf_fn_common.etl_incremental_scope import (
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    WORKFLOW_SCOPE_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
)

RECORD_KIND_FILE = "file"
RECORD_KIND_CHECKPOINT = "checkpoint"
FILE_STATE_TABLE_SUFFIX = "__file_state"

FILE_STATUS_PENDING = "pending"
FILE_STATUS_PROCESSING = "processing"
FILE_STATUS_DETECTED = "detected"
FILE_STATUS_INDEXED = "indexed"
FILE_STATUS_FAILED = "failed"

FILE_ID_COLUMN = "FILE_ID"
UPLOADED_TIME_COLUMN = "UPLOADED_TIME"
ATTEMPTS_COLUMN = "ATTEMPTS"
LAST_ERROR_COLUMN = "LAST_ERROR"
STATE_JSON_COLUMN = "STATE_JSON"
CHUNKS_DONE_COLUMN = "CHUNKS_DONE"
CHUNKS_TOTAL_COLUMN = "CHUNKS_TOTAL"


def file_state_table_name(base_table: str) -> str:
    base = str(base_table or "discovery_state").strip() or "discovery_state"
    name = f"{base}{FILE_STATE_TABLE_SUFFIX}"
    if len(name) <= 64:
        return name
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:8]
    budget = 64 - len(FILE_STATE_TABLE_SUFFIX) - len(digest) - 2
    head = base[: max(4, budget)].rstrip("_")
    return f"{head}_{digest}{FILE_STATE_TABLE_SUFFIX}"


def file_state_row_key(file_id: int, workflow_scope: str = "") -> str:
    ws = str(workflow_scope or "").strip()
    if ws:
        return f"file_{ws}_{int(file_id)}"
    return f"file_{int(file_id)}"


def _cols(row: Any) -> Dict[str, Any]:
    cols = getattr(row, "columns", None) or {}
    return dict(cols) if isinstance(cols, dict) else {}


def load_file_processing_state(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    workflow_scope: str = "",
) -> Dict[int, Dict[str, Any]]:
    """Load per-file state keyed by numeric file id."""
    from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
    from cdf_fn_common.etl_incremental_scope import iter_raw_table_rows_chunked

    create_table_if_not_exists(client, raw_db, raw_table)
    out: Dict[int, Dict[str, Any]] = {}
    for row in iter_raw_table_rows_chunked(client, raw_db, raw_table):
        cols = _cols(row)
        if str(cols.get(RECORD_KIND_COLUMN) or "") != RECORD_KIND_FILE:
            continue
        row_ws = str(cols.get(WORKFLOW_SCOPE_COLUMN) or "").strip()
        if workflow_scope and row_ws and row_ws != workflow_scope:
            continue
        fid_raw = cols.get(FILE_ID_COLUMN) or getattr(row, "key", None)
        try:
            file_id = int(fid_raw)
        except (TypeError, ValueError):
            continue
        state_json = cols.get(STATE_JSON_COLUMN)
        if isinstance(state_json, str) and state_json.strip():
            try:
                state_data = json.loads(state_json)
            except json.JSONDecodeError:
                state_data = {}
        elif isinstance(state_json, dict):
            state_data = dict(state_json)
        else:
            state_data = {}
        state_data.setdefault("file_id", file_id)
        state_data["status"] = cols.get(WORKFLOW_STATUS_COLUMN) or state_data.get("status")
        state_data["attempts"] = cols.get(ATTEMPTS_COLUMN) or state_data.get("attempts", 0)
        if cols.get(LAST_ERROR_COLUMN):
            state_data["last_error"] = cols.get(LAST_ERROR_COLUMN)
        if cols.get(CHUNKS_TOTAL_COLUMN) is not None:
            try:
                state_data["chunks_total"] = int(cols.get(CHUNKS_TOTAL_COLUMN))
            except (TypeError, ValueError):
                pass
        if cols.get(CHUNKS_DONE_COLUMN) is not None:
            try:
                state_data["chunks_done"] = int(cols.get(CHUNKS_DONE_COLUMN))
            except (TypeError, ValueError):
                pass
        out[file_id] = state_data
    return out


def upsert_file_state_raw(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    file_id: int,
    workflow_scope: str,
    run_id: str,
    state_data: Mapping[str, Any],
) -> None:
    from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists

    create_table_if_not_exists(client, raw_db, raw_table)
    status = str(state_data.get("status") or FILE_STATUS_PENDING)
    try:
        attempts = int(state_data.get("attempts") or 0)
    except (TypeError, ValueError):
        attempts = 0
    cols: Dict[str, Any] = {
        RECORD_KIND_COLUMN: RECORD_KIND_FILE,
        WORKFLOW_SCOPE_COLUMN: workflow_scope,
        FILE_ID_COLUMN: int(file_id),
        WORKFLOW_STATUS_COLUMN: status,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        RUN_ID_COLUMN: run_id,
        ATTEMPTS_COLUMN: attempts,
        STATE_JSON_COLUMN: json.dumps(dict(state_data), default=str),
    }
    if state_data.get("last_error"):
        cols[LAST_ERROR_COLUMN] = str(state_data.get("last_error"))
    uploaded = state_data.get("uploaded_time") or (state_data.get("file_info") or {}).get(
        "uploadedTime"
    )
    if uploaded:
        cols[UPLOADED_TIME_COLUMN] = str(uploaded)
    if state_data.get("chunks_total") is not None:
        try:
            cols[CHUNKS_TOTAL_COLUMN] = int(state_data.get("chunks_total"))
        except (TypeError, ValueError):
            pass
    if state_data.get("chunks_done") is not None:
        try:
            cols[CHUNKS_DONE_COLUMN] = int(state_data.get("chunks_done"))
        except (TypeError, ValueError):
            pass
    client.raw.rows.insert(
        db_name=raw_db,
        table_name=raw_table,
        row={file_state_row_key(file_id, workflow_scope): cols},
    )


def _normalize_uploaded_time(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def resolve_incremental_change_processing(data: Mapping[str, Any]) -> bool:
    """Whether file-pattern workflows should skip already-detected files."""
    raw = data.get("incremental_change_processing")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() not in {"false", "0", "no", "off"}
    cfg = data.get("config")
    if isinstance(cfg, dict) and "incremental_change_processing" in cfg:
        return resolve_incremental_change_processing({"incremental_change_processing": cfg["incremental_change_processing"]})
    return True


def select_files_for_processing(
    files: List[Dict[str, Any]],
    state_store: Mapping[int, Mapping[str, Any]],
    *,
    max_attempts: int = 3,
) -> List[Dict[str, Any]]:
    """Return files that need diagram detect (new, re-uploaded, or retryable failure)."""
    out: List[Dict[str, Any]] = []
    for file_info in files:
        file_id = int(file_info["id"])
        uploaded = _normalize_uploaded_time(file_info.get("uploadedTime"))
        if file_id not in state_store:
            out.append(file_info)
            continue
        stored = dict(state_store[file_id])
        stored_info = stored.get("file_info") if isinstance(stored.get("file_info"), dict) else {}
        stored_uploaded = _normalize_uploaded_time(stored_info.get("uploadedTime"))
        if uploaded and stored_uploaded and uploaded > stored_uploaded:
            out.append(file_info)
            continue
        status = str(stored.get("status") or "").strip().lower()
        try:
            attempts = int(stored.get("attempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        if status == FILE_STATUS_DETECTED or status == FILE_STATUS_INDEXED:
            continue
        if status == FILE_STATUS_FAILED and attempts >= max_attempts:
            continue
        if status in (FILE_STATUS_PENDING, FILE_STATUS_PROCESSING, FILE_STATUS_FAILED, ""):
            out.append(file_info)
    return out


def optional_positive_int(val: Any) -> Optional[int]:
    """Parse a positive int cap, or None when unset/invalid."""
    if val is None:
        return None
    try:
        n = int(val)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def cap_files_for_run(
    files: List[Dict[str, Any]],
    max_files_per_run: Any,
) -> List[Dict[str, Any]]:
    """Return at most *max_files_per_run* files (stable order); no-op when unset."""
    cap = optional_positive_int(max_files_per_run)
    if cap is None:
        return list(files)
    return list(files)[:cap]


def detect_child_config_from_fanout_cfg(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Settings passed to each dynamic detect child task (pages per call, tokens, etc.)."""
    out: Dict[str, Any] = {}
    for key in (
        "max_pages_per_file_reference",
        "max_pages_per_detect_request",
        "min_tokens",
        "partial_match",
        "diagram_poll_timeout_sec",
        "diagram_detect_config",
        "max_detect_jobs_per_invocation",
    ):
        if key not in cfg:
            continue
        val = cfg.get(key)
        if val is None or val == "":
            continue
        out[key] = val
    return out


def resolve_file_workflow_params(data: Mapping[str, Any]) -> Dict[str, Any]:
    from cdf_fn_common.etl_discovery_query_shared import _as_dict

    cfg = _as_dict(data.get("configuration"))
    params = _as_dict(cfg.get("parameters"))
    if not params:
        params = _as_dict(data.get("parameters"))
    persistence = _as_dict(data.get("persistence"))
    return {
        "raw_db": str(
            params.get("raw_db")
            or persistence.get("raw_db")
            or data.get("raw_db")
            or "db_discovery"
        ).strip(),
        "raw_table_key": str(
            params.get("raw_table_key")
            or persistence.get("raw_table_key")
            or data.get("raw_table_key")
            or "discovery_state"
        ).strip(),
        "workflow_scope": str(
            params.get("workflow_scope") or data.get("workflow_scope") or "file_pattern_extract"
        ).strip(),
        "batch_size": int(params.get("batch_size") or 1),
        "max_attempts": int(params.get("max_attempts") or 3),
        "max_files_per_run": optional_positive_int(params.get("max_files_per_run")),
        "mime_type": params.get("mime_type"),
        "instance_space": params.get("instance_space"),
        "max_pages_per_file_reference": int(params.get("max_pages_per_file_reference") or 15),
        "max_pages_per_detect_request": int(params.get("max_pages_per_detect_request") or 15),
        "max_pattern_samples": int(params.get("max_pattern_samples") or 100),
        "min_tokens": int(params.get("min_tokens") or 2),
        "diagram_poll_timeout_sec": int(params.get("diagram_poll_timeout_sec") or 840),
        "max_detect_jobs_per_invocation": int(params.get("max_detect_jobs_per_invocation") or 1),
        "pattern_normalization": str(params.get("pattern_normalization") or "file_annotation"),
        "child_function_external_id": str(
            params.get("child_function_external_id") or "fn_etl_file_annotation"
        ),
        "child_timeout": int(params.get("child_timeout") or 3600),
        "child_retries": int(params.get("child_retries") or 2),
    }


def file_state_sink_from_data(data: Mapping[str, Any]) -> tuple[str, str]:
    p = resolve_file_workflow_params(data)
    return p["raw_db"], file_state_table_name(p["raw_table_key"])


def write_fanout_checkpoint_raw(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    workflow_scope: str,
    run_id: str,
    checkpoint: Mapping[str, Any],
) -> None:
    from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists

    create_table_if_not_exists(client, raw_db, raw_table)
    key = f"checkpoint_{workflow_scope}_{run_id}"[:256]
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_CHECKPOINT,
        WORKFLOW_SCOPE_COLUMN: workflow_scope,
        RUN_ID_COLUMN: run_id,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        "CHECKPOINT_JSON": json.dumps(dict(checkpoint), default=str),
    }
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={key: cols})


def plan_detect_packs_for_files(
    pending_files: List[Dict[str, Any]],
    *,
    max_pages_per_file_reference: int,
    max_pages_per_detect_request: int,
) -> List[Dict[str, Any]]:
    """Plan one diagrams.detect page-pack per dynamic child task."""
    from cdf_fn_common.etl_diagram_detect import (
        chunk_file_into_page_blocks,
        pack_file_refs_into_detect_requests,
        serialize_file_ref,
    )

    file_by_id = {int(f["id"]): dict(f) for f in pending_files}
    packs_out: List[Dict[str, Any]] = []
    pack_index = 0
    for file_info in pending_files:
        fid = int(file_info["id"])
        refs = chunk_file_into_page_blocks(
            file_info, max_pages_per_file_reference=max_pages_per_file_reference
        )
        for pack in pack_file_refs_into_detect_requests(
            refs, max_pages_per_detect_request=max_pages_per_detect_request
        ):
            serial = [serialize_file_ref(ref) for ref in pack]
            file_ids = sorted({int(r["file_id"]) for r in serial if r.get("file_id")})
            packs_out.append(
                {
                    "pack_index": pack_index,
                    "file_ids": file_ids,
                    "files": [file_by_id[i] for i in file_ids if i in file_by_id],
                    "detect_pack": serial,
                }
            )
            pack_index += 1
    return packs_out


def count_detect_packs_per_file(pack_specs: Sequence[Mapping[str, Any]]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for spec in pack_specs:
        for fid in spec.get("file_ids") or []:
            try:
                counts[int(fid)] = counts.get(int(fid), 0) + 1
            except (TypeError, ValueError):
                continue
    return counts


def record_detect_pack_completion(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    file_id: int,
    workflow_scope: str,
    run_id: str,
    file_info: Mapping[str, Any],
    state_store: Mapping[int, Mapping[str, Any]],
) -> bool:
    """
    Increment packs completed for a file. Mark ``detected`` when all packs are done.

    Returns True when the file transitions to detected.
    """
    prev = dict(state_store.get(file_id) or {})
    try:
        chunks_total = int(prev.get("chunks_total") or 0)
    except (TypeError, ValueError):
        chunks_total = 0
    try:
        chunks_done = int(prev.get("chunks_done") or 0) + 1
    except (TypeError, ValueError):
        chunks_done = 1
    try:
        attempts = int(prev.get("attempts") or 0)
    except (TypeError, ValueError):
        attempts = 0

    fully_done = chunks_total > 0 and chunks_done >= chunks_total
    status = FILE_STATUS_DETECTED if fully_done else FILE_STATUS_PROCESSING
    state_data: Dict[str, Any] = {
        "status": status,
        "file_info": dict(file_info),
        "attempts": attempts,
        "chunks_total": chunks_total,
        "chunks_done": chunks_done,
    }
    upsert_file_state_raw(
        client,
        raw_db=raw_db,
        raw_table=raw_table,
        file_id=file_id,
        workflow_scope=workflow_scope,
        run_id=run_id,
        state_data=state_data,
    )
    return fully_done


def build_dynamic_detect_pack_tasks(
    pack_specs: List[Dict[str, Any]],
    *,
    entities: List[Dict[str, Any]],
    run_id: str,
    workflow_scope: str,
    child_function_external_id: str,
    child_timeout: int = 3600,
    child_retries: int = 2,
    depends_on: Optional[List[str]] = None,
    child_detect_config: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build CDF dynamic workflow tasks — one function invocation per detect page-pack."""
    deps = depends_on or []
    tasks_out: List[Dict[str, Any]] = []
    total = len(pack_specs)
    for spec in pack_specs:
        pack_index = int(spec.get("pack_index") or len(tasks_out))
        file_ids = list(spec.get("file_ids") or [])
        files = list(spec.get("files") or [])
        detect_pack = list(spec.get("detect_pack") or [])
        task_ext = f"detect_pack_{pack_index}"
        child_data: Dict[str, Any] = {
            "task_id": task_ext,
            "file_ids": file_ids,
            "files": files,
            "detect_pack": detect_pack,
            "pack_index": pack_index,
            "pack_total": total,
            "entities": entities,
            "run_id": run_id,
            "workflow_scope": workflow_scope,
            "incremental_change_processing": "${workflow.input.incremental_change_processing}",
            "configuration": "${workflow.input.configuration}",
        }
        if child_detect_config:
            child_data["config"] = dict(child_detect_config)
        page_span = sum(
            max(1, int(r.get("last_page") or 1) - int(r.get("first_page") or 1) + 1)
            for r in detect_pack
            if isinstance(r, dict)
        )
        tasks_out.append(
            {
                "externalId": task_ext,
                "type": "function",
                "dependsOn": [{"externalId": d} for d in deps],
                "parameters": {
                    "function": {
                        "externalId": child_function_external_id,
                        "data": child_data,
                        "isAsyncComplete": True,
                    }
                },
                "name": f"Pattern detect pack {pack_index + 1}/{total}",
                "description": (
                    f"Diagram pattern detect ({len(file_ids)} file(s), ~{page_span} pages)"
                ),
                "retries": child_retries,
                "timeout": child_timeout,
                "onFailure": "skipTask",
            }
        )
    return tasks_out


def build_dynamic_detect_tasks(
    pending_files: List[Dict[str, Any]],
    *,
    entities: List[Dict[str, Any]],
    batch_size: int,
    run_id: str,
    workflow_scope: str,
    child_function_external_id: str,
    child_timeout: int = 3600,
    child_retries: int = 2,
    depends_on: Optional[List[str]] = None,
    child_detect_config: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build CDF dynamic workflow task specs for diagram detect batches."""
    deps = depends_on or []
    tasks_out: List[Dict[str, Any]] = []
    if not pending_files:
        return tasks_out
    n_batches = (len(pending_files) + batch_size - 1) // batch_size
    for batch_num in range(n_batches):
        start = batch_num * batch_size
        chunk = pending_files[start : start + batch_size]
        file_ids = [int(f["id"]) for f in chunk]
        task_ext = f"detect_batch_{batch_num}"
        child_data: Dict[str, Any] = {
            "task_id": task_ext,
            "file_ids": file_ids,
            "files": chunk,
            "entities": entities,
            "run_id": run_id,
            "workflow_scope": workflow_scope,
            "incremental_change_processing": "${workflow.input.incremental_change_processing}",
            "configuration": "${workflow.input.configuration}",
        }
        if child_detect_config:
            child_data["config"] = dict(child_detect_config)
        tasks_out.append(
            {
                "externalId": task_ext,
                "type": "function",
                "dependsOn": [{"externalId": d} for d in deps],
                "parameters": {
                    "function": {
                        "externalId": child_function_external_id,
                        "data": child_data,
                        "isAsyncComplete": True,
                    }
                },
                "name": f"Pattern detect batch {batch_num + 1}/{n_batches}",
                "description": f"Diagram pattern detect for {len(chunk)} file(s)",
                "retries": child_retries,
                "timeout": child_timeout,
                "onFailure": "skipTask",
            }
        )
    return tasks_out
