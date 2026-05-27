"""Per-file processing state after annotation packs."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from cdf_fn_common.etl_file_processing_state import (
    FILE_STATUS_FAILED,
    record_detect_pack_completion,
    upsert_file_state_raw,
)


def record_annotation_pack_completion(
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
    return record_detect_pack_completion(
        client,
        raw_db=raw_db,
        raw_table=raw_table,
        file_id=file_id,
        workflow_scope=workflow_scope,
        run_id=run_id,
        file_info=file_info,
        state_store=state_store,
    )


def mark_files_failed_on_error(
    client: Any,
    *,
    files: list[Dict[str, Any]],
    raw_db: str,
    raw_table: str,
    workflow_scope: str,
    run_id: str,
    state_store: Mapping[int, Mapping[str, Any]],
    error_message: str,
) -> None:
    err = str(error_message)
    for file_info in files:
        fid = int(file_info["id"])
        prev = state_store.get(fid, {})
        attempts = int(prev.get("attempts") or 0) + 1
        upsert_file_state_raw(
            client,
            raw_db=raw_db,
            raw_table=raw_table,
            file_id=fid,
            workflow_scope=workflow_scope,
            run_id=run_id,
            state_data={
                "status": FILE_STATUS_FAILED,
                "file_info": file_info,
                "attempts": attempts,
                "last_error": err,
                "chunks_total": prev.get("chunks_total"),
                "chunks_done": prev.get("chunks_done"),
            },
        )
