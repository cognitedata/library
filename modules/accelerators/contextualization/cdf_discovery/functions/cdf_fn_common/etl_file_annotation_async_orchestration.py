"""Split-phase file annotation orchestration with 7-minute pre-emptive budget."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_cohort_storage import require_pipeline_run_key
from cdf_fn_common.etl_common import _first_nonempty
from cdf_fn_common.etl_diagram_detect import (
    fetch_diagram_job_once,
    flatten_detect_items_to_cohort_rows,
    run_diagram_detect,
)
from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_file_annotation import (
    resolve_detect_packs_for_invocation,
    resolve_file_annotation_entities,
    resolve_file_annotation_files,
    write_file_annotation_cohort_rows,
)
from cdf_fn_common.etl_file_processing_state import (
    file_state_sink_from_data,
    load_detect_job_state,
    load_file_processing_state,
    resolve_file_workflow_params,
    upsert_detect_job_state_raw,
)
from cdf_fn_common.etl_file_annotation.state import record_annotation_pack_completion
from cdf_fn_common.etl_task_inputs import resolve_two_task_ids
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_workflow_task_complete import (
    complete_workflow_task,
    fail_workflow_task,
    orchestration_task_id_from_data,
)

_MAX_RUNTIME = timedelta(minutes=7)
_TERMINAL_JOB_STATUSES = {"completed_processed", "failed", "timed_out"}
_FANOUT_MODE_VALUES = {"annotation", "pattern", "both"}


def _deadline(started: datetime) -> datetime:
    return started + _MAX_RUNTIME


def _is_uploaded(file_obj: Any, file_info: Mapping[str, Any]) -> bool:
    uploaded_flag = getattr(file_obj, "uploaded", None)
    if isinstance(uploaded_flag, bool):
        return uploaded_flag
    uploaded_time = getattr(file_obj, "uploaded_time", None) or getattr(file_obj, "uploadedTime", None)
    if uploaded_time:
        return True
    return bool(file_info.get("uploadedTime") or file_info.get("uploaded_time"))


def _uploaded_files(client: Any, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    files_api = getattr(client, "files", None)
    for file_info in files:
        fid = int(file_info.get("id") or 0)
        if not fid:
            continue
        if files_api is None:
            out.append(file_info)
            continue
        try:
            obj = files_api.retrieve(id=fid) if hasattr(files_api, "retrieve") else None
        except Exception:
            obj = None
        if _is_uploaded(obj, file_info):
            out.append(file_info)
    return out


def _job_status(job_body: Mapping[str, Any]) -> str:
    return str(job_body.get("status") or "").strip().lower()


def _resolve_fanout_mode(data: Mapping[str, Any], cfg: Mapping[str, Any]) -> str:
    configuration = data.get("configuration")
    params = (
        configuration.get("parameters")
        if isinstance(configuration, dict) and isinstance(configuration.get("parameters"), dict)
        else {}
    )
    mode = str(cfg.get("fanout_mode") or params.get("fanout_mode") or "both").strip().lower()
    if mode not in _FANOUT_MODE_VALUES:
        raise ValueError(
            "file_annotation requires config.fanout_mode to be one of "
            "'annotation', 'pattern', 'both'"
        )
    return mode


def _resolve_queue_task_ids(
    data: Mapping[str, Any], cfg: Mapping[str, Any], *, task_id: str, error_context: str
) -> List[str]:
    fanout_mode = _resolve_fanout_mode(data, cfg)
    if fanout_mode == "both":
        left, right = resolve_two_task_ids(
            data,
            left_key="source_task_id_pattern",
            right_key="source_task_id_annotation",
            error_context=error_context,
        )
        return [left, right]
    preferred_key = (
        "source_task_id_pattern" if fanout_mode == "pattern" else "source_task_id_annotation"
    )
    queue_task_id = _first_nonempty(
        data.get(preferred_key),
        cfg.get(preferred_key),
        data.get("source_task_id"),
        cfg.get("source_task_id"),
        data.get("queue_task_id"),
        cfg.get("queue_task_id"),
        task_id,
    )
    if not queue_task_id:
        raise ValueError(f"{error_context} requires a queue task id")
    return [queue_task_id]


def etl_handle_file_annotation_launch(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("file_annotation launch requires a CDF client")

    started = datetime.now(timezone.utc)
    merge_compiled_task_into_data(data)
    params = resolve_file_workflow_params(data)
    cfg = resolve_task_config(data)
    task_id = str(data.get("task_id") or fn_external_id)
    orch_task_id = orchestration_task_id_from_data(data)
    run_id = require_pipeline_run_key(data)
    workflow_scope = params["workflow_scope"]
    raw_db, state_table = file_state_sink_from_data(data)

    entities = resolve_file_annotation_entities(data, cfg, client=client, params=params)
    files = resolve_file_annotation_files(data, cfg, client)
    files = _uploaded_files(client, files)
    if not files:
        raise ValueError("file_annotation launch: no uploaded files available to scan.")

    packs = resolve_detect_packs_for_invocation(data, files, cfg, params=params)
    existing = load_detect_job_state(
        client,
        raw_db,
        state_table,
        workflow_scope=workflow_scope,
        run_id=run_id,
        task_id=task_id,
    )
    existing_by_pack = {int(r.get("pack_index") or -1): r for r in existing}

    submitted = 0
    skipped_existing = 0
    for pack_index, pack in enumerate(packs):
        if datetime.now(timezone.utc) >= _deadline(started):
            break
        if pack_index in existing_by_pack:
            skipped_existing += 1
            continue
        serial_pack: List[Dict[str, Any]] = []
        file_ids: List[int] = []
        for ref in pack:
            fid = int(getattr(ref, "file_id", None) or 0)
            first = int(getattr(ref, "first_page", None) or 1)
            last = int(getattr(ref, "last_page", None) or first)
            serial_pack.append({"file_id": fid, "first_page": first, "last_page": last})
            if fid and fid not in file_ids:
                file_ids.append(fid)

        job_id = run_diagram_detect(
            client,
            pack,
            entities,
            partial_match=bool(cfg.get("partial_match", params.get("partial_match", True))),
            min_tokens=int(cfg.get("min_tokens") or params.get("min_tokens") or 2),
            pattern_mode=bool(cfg.get("pattern_mode", params.get("pattern_mode", False))),
            search_field=str(
                cfg.get("search_field")
                or params.get("search_field")
                or ("sample" if bool(cfg.get("pattern_mode", params.get("pattern_mode", False))) else "aliases")
            ),
            diagram_detect_config=cfg.get("diagram_detect_config")
            if isinstance(cfg.get("diagram_detect_config"), dict)
            else None,
        )
        upsert_detect_job_state_raw(
            client,
            raw_db=raw_db,
            raw_table=state_table,
            workflow_scope=workflow_scope,
            run_id=run_id,
            task_id=task_id,
            pack_index=pack_index,
            state_data={
                "task_id": task_id,
                "pack_index": pack_index,
                "pack_total": len(packs),
                "job_id": int(job_id),
                "job_status": "submitted",
                "file_ids": file_ids,
                "files": [dict(f) for f in files if int(f.get("id") or 0) in set(file_ids)],
                "detect_pack": serial_pack,
                "submitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "entities": entities,
            },
        )
        submitted += 1

    output = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "workflow_scope": workflow_scope,
        "packs_total": len(packs),
        "packs_submitted": submitted,
        "packs_existing": skipped_existing,
        "time_budget_exhausted": datetime.now(timezone.utc) >= _deadline(started),
    }
    if orch_task_id is not None:
        complete_workflow_task(client, orch_task_id, output=output)
    return output


def etl_handle_file_annotation_finalize(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("file_annotation finalize requires a CDF client")

    started = datetime.now(timezone.utc)
    merge_compiled_task_into_data(data)
    params = resolve_file_workflow_params(data)
    cfg = resolve_task_config(data)
    task_id = str(data.get("task_id") or fn_external_id)
    fanout_mode = _resolve_fanout_mode(data, cfg)
    queue_task_ids = _resolve_queue_task_ids(
        data,
        cfg,
        task_id=task_id,
        error_context="file_annotation finalize",
    )
    orch_task_id = orchestration_task_id_from_data(data)
    run_id = require_pipeline_run_key(data)
    workflow_scope = params["workflow_scope"]
    raw_db, state_table = file_state_sink_from_data(data)

    jobs: List[Dict[str, Any]] = []
    for queue_task_id in queue_task_ids:
        task_filter = None if queue_task_id in {"", "fanout"} else queue_task_id
        jobs.extend(
            load_detect_job_state(
                client,
                raw_db,
                state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                task_id=task_filter,
            )
        )
    dedup: Dict[tuple[Any, Any, Any], Dict[str, Any]] = {}
    for job in jobs:
        key = (job.get("task_id"), job.get("pack_index"), job.get("job_id"))
        dedup[key] = job
    jobs = list(dedup.values())
    jobs = [j for j in jobs if str(j.get("job_status") or "").strip().lower() not in _TERMINAL_JOB_STATUSES]

    state_store = load_file_processing_state(client, raw_db, state_table, workflow_scope=workflow_scope)
    rows_to_write: List[Dict[str, Any]] = []
    completed_now = 0
    pending = 0

    for job in jobs:
        if datetime.now(timezone.utc) >= _deadline(started):
            break
        pack_index = int(job.get("pack_index") or 0)
        file_list = [dict(f) for f in (job.get("files") or []) if isinstance(f, dict)]
        file_info_map: Dict[int, Dict[str, Any]] = {
            int(f.get("id") or 0): dict(f) for f in file_list if int(f.get("id") or 0)
        }
        ref_meta_by_file: Dict[int, Dict[str, Any]] = {}
        for ref in job.get("detect_pack") or []:
            if not isinstance(ref, dict):
                continue
            fid = int(ref.get("file_id") or 0)
            if not fid:
                continue
            first = int(ref.get("first_page") or 1)
            last = int(ref.get("last_page") or first)
            ref_meta_by_file[fid] = {"first_page": first, "last_page": last}

        job_task_id = str(job.get("task_id") or queue_task_ids[0]).strip()
        job_id = int(job.get("job_id") or 0)
        body = fetch_diagram_job_once(client, job_id)
        status = _job_status(body)
        if status == "completed":
            rows = flatten_detect_items_to_cohort_rows(
                body,
                file_info_map,
                run_id=run_id,
                scope_key=workflow_scope,
                file_ref_meta=ref_meta_by_file,
            )
            rows_to_write.extend(rows)
            for file_info in file_list:
                fid = int(file_info.get("id") or 0)
                if not fid:
                    continue
                record_annotation_pack_completion(
                    client,
                    raw_db=raw_db,
                    raw_table=state_table,
                    file_id=fid,
                    workflow_scope=workflow_scope,
                    run_id=run_id,
                    file_info=file_info,
                    state_store=state_store,
                )
            upsert_detect_job_state_raw(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                task_id=job_task_id,
                pack_index=pack_index,
                state_data={
                    **dict(job),
                    "job_status": "completed_processed",
                    "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                },
            )
            completed_now += 1
        elif status in {"failed"}:
            upsert_detect_job_state_raw(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                task_id=job_task_id,
                pack_index=pack_index,
                state_data={**dict(job), "job_status": "failed", "last_error": body.get("error")},
            )
        else:
            pending += 1
            upsert_detect_job_state_raw(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                task_id=job_task_id,
                pack_index=pack_index,
                state_data={**dict(job), "job_status": status or "running"},
            )

    if rows_to_write:
        write_file_annotation_cohort_rows(
            client,
            data,
            rows_to_write,
            task_id=task_id,
            run_id=run_id,
            scope=workflow_scope,
            log=log,
        )

    output = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "workflow_scope": workflow_scope,
        "fanout_mode": fanout_mode,
        "queue_task_ids": queue_task_ids,
        "jobs_total": len(jobs),
        "jobs_completed_now": completed_now,
        "jobs_pending": pending,
        "annotation_rows": len(rows_to_write),
        "time_budget_exhausted": datetime.now(timezone.utc) >= _deadline(started),
    }
    if orch_task_id is not None:
        complete_workflow_task(client, orch_task_id, output=output)
    return output


def etl_handle_file_annotation_barrier(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("file_annotation barrier requires a CDF client")

    merge_compiled_task_into_data(data)
    task_id = str(data.get("task_id") or fn_external_id)
    cfg = resolve_task_config(data)
    fanout_mode = _resolve_fanout_mode(data, cfg)
    queue_task_ids = _resolve_queue_task_ids(
        data,
        cfg,
        task_id=task_id,
        error_context="file_annotation barrier",
    )
    run_id = require_pipeline_run_key(data)
    params = resolve_file_workflow_params(data)
    workflow_scope = params["workflow_scope"]
    raw_db, state_table = file_state_sink_from_data(data)
    orch_task_id = orchestration_task_id_from_data(data)

    jobs: List[Dict[str, Any]] = []
    for queue_task_id in queue_task_ids:
        task_filter = None if queue_task_id in {"", "fanout"} else queue_task_id
        jobs.extend(
            load_detect_job_state(
                client,
                raw_db,
                state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                task_id=task_filter,
            )
        )
    dedup: Dict[tuple[Any, Any, Any], Dict[str, Any]] = {}
    for job in jobs:
        key = (job.get("task_id"), job.get("pack_index"), job.get("job_id"))
        dedup[key] = job
    jobs = list(dedup.values())
    pending = [
        j
        for j in jobs
        if str(j.get("job_status") or "").strip().lower() not in _TERMINAL_JOB_STATUSES
    ]
    if pending:
        msg = (
            f"file_annotation barrier: {len(pending)} detect jobs still pending for "
            f"queue_task_ids={queue_task_ids}"
        )
        if orch_task_id is not None:
            fail_workflow_task(client, orch_task_id, error_message=msg)
        raise ValueError(msg)

    output = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "workflow_scope": workflow_scope,
        "fanout_mode": fanout_mode,
        "queue_task_ids": queue_task_ids,
        "jobs_terminal": len(jobs),
    }
    if orch_task_id is not None:
        complete_workflow_task(client, orch_task_id, output=output)
    return output
