"""Thin orchestration for fn_etl_file_annotation."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.etl_cohort_storage import require_run_id
from cdf_fn_common.etl_diagram_detect import chunk_file_into_page_blocks
from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_file_annotation import (
    mark_files_failed_on_error,
    record_annotation_pack_completion,
    resolve_detect_packs_for_invocation,
    resolve_file_annotation_entities,
    resolve_file_annotation_files,
    run_one_annotation_pack,
    write_file_annotation_cohort_rows,
)
from cdf_fn_common.etl_file_annotation.run_pack import AnnotationPackContext
from cdf_fn_common.etl_file_processing_state import (
    file_state_sink_from_data,
    load_file_processing_state,
    resolve_file_workflow_params,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_workflow_task_complete import (
    complete_workflow_task,
    fail_workflow_task,
    orchestration_task_id_from_data,
)


def etl_handle_file_annotation(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("file_annotation requires a CDF client")

    merge_compiled_task_into_data(data)
    params = resolve_file_workflow_params(data)
    cfg = resolve_task_config(data)
    task_id = str(data.get("task_id") or fn_external_id)
    orch_task_id = orchestration_task_id_from_data(data)

    run_id = require_run_id(data)
    workflow_scope = params["workflow_scope"]
    raw_db, state_table = file_state_sink_from_data(data)

    entities = resolve_file_annotation_entities(data, cfg, client=client, params=params)
    files = resolve_file_annotation_files(data, cfg, client)

    max_ref = int(
        cfg.get("max_pages_per_file_reference") or params["max_pages_per_file_reference"]
    )

    ref_meta_by_file: Dict[int, Dict[str, Any]] = {}
    file_info_map: Dict[int, Dict[str, Any]] = {int(f["id"]): dict(f) for f in files}
    for file_info in files:
        fid = int(file_info["id"])
        for ref in chunk_file_into_page_blocks(
            file_info, max_pages_per_file_reference=max_ref
        ):
            first = int(getattr(ref, "first_page", None) or 1)
            last = int(getattr(ref, "last_page", None) or first)
            ref_meta_by_file[fid] = {"first_page": first, "last_page": last}

    packs = resolve_detect_packs_for_invocation(data, files, cfg, params=params)
    pack_total = len(packs)
    state_store = load_file_processing_state(
        client, raw_db, state_table, workflow_scope=workflow_scope
    )

    ctx = AnnotationPackContext(
        run_id=run_id,
        workflow_scope=workflow_scope,
        task_id=task_id,
        file_info_map=file_info_map,
        ref_meta_by_file=ref_meta_by_file,
    )

    cohort_rows: List[Dict[str, Any]] = []
    detect_jobs = 0
    pattern_dump_rows = 0

    try:
        for pack_index, pack in enumerate(packs, start=1):
            _job_id, rows, dumps = run_one_annotation_pack(
                client,
                pack,
                entities,
                cfg,
                ctx,
                pack_index=pack_index,
                pack_total=pack_total,
                params=params,
                log=log,
            )
            detect_jobs += 1
            pattern_dump_rows += dumps
            cohort_rows.extend(rows)

        write_file_annotation_cohort_rows(
            client,
            data,
            cohort_rows,
            task_id=task_id,
            run_id=run_id,
            scope=workflow_scope,
            log=log,
        )

        files_fully_detected = 0
        for file_info in files:
            fid = int(file_info["id"])
            if record_annotation_pack_completion(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                file_id=fid,
                workflow_scope=workflow_scope,
                run_id=run_id,
                file_info=file_info,
                state_store=state_store,
            ):
                files_fully_detected += 1

        output = {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "status": "ok",
            "detect_jobs": detect_jobs,
            "annotation_rows": len(cohort_rows),
            "pattern_dump_rows": pattern_dump_rows,
            "files_processed": len(files),
            "files_fully_detected": files_fully_detected,
            "packs_run": pack_total,
            "run_id": run_id,
        }
        if orch_task_id is not None:
            complete_workflow_task(client, orch_task_id, output=output)
        return output

    except Exception as e:
        mark_files_failed_on_error(
            client,
            files=files,
            raw_db=raw_db,
            raw_table=state_table,
            workflow_scope=workflow_scope,
            run_id=run_id,
            state_store=state_store,
            error_message=str(e),
        )
        if orch_task_id is not None:
            fail_workflow_task(client, orch_task_id, error_message=str(e))
        raise
