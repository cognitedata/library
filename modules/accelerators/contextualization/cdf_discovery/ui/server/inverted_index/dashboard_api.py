"""Read-only dashboard endpoints for index health and monitoring."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/inverted-index/dashboard", tags=["dashboard"])


def _api_error(e: Exception) -> HTTPException:
    from cognite.client.exceptions import CogniteAPIError

    if isinstance(e, ValueError):
        return HTTPException(status_code=400, detail=str(e))
    if isinstance(e, CogniteAPIError):
        return HTTPException(status_code=502, detail=str(e))
    return HTTPException(status_code=500, detail=str(e))


def _load_env() -> None:
    from local_runner.env import load_env
    from ui.server.inverted_index.paths import MODULE_ROOT

    load_env(MODULE_ROOT)


class BatchFileDeltasBody(BaseModel):
    file_external_ids: list[str] = Field(min_length=1)
    file_space: str = "cdf_cdm"
    match_scope_key: str | None = None
    detail_limit: int = 20


@router.get("/summary")
def dashboard_summary() -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_dashboard_summary

    try:
        return cmd_dashboard_summary()
    except Exception as e:
        raise _api_error(e) from e


@router.post("/tag-reuse")
def dashboard_tag_reuse() -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_tag_reuse_audit

    try:
        return cmd_tag_reuse_audit(all_scopes=True, min_scope_count=2, limit=5000)
    except Exception as e:
        raise _api_error(e) from e


@router.post("/tag-reuse/stream")
def dashboard_tag_reuse_stream(request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_tag_reuse_audit
    from ui.server.inverted_index.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_tag_reuse_audit(
            all_scopes=True,
            min_scope_count=2,
            limit=5000,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/file-deltas/stream")
def dashboard_file_deltas_stream(body: BatchFileDeltasBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_batch_file_deltas
    from ui.server.inverted_index.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_batch_file_deltas(
            body.file_external_ids,
            file_space=body.file_space,
            match_scope_key=body.match_scope_key,
            detail_limit=body.detail_limit,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )
