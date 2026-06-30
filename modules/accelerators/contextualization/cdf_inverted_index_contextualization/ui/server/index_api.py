"""Operational endpoints wrapping local_runner.commands."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/inverted-index", tags=["operations"])


def _api_error(e: Exception) -> HTTPException:
    from cognite.client.exceptions import CogniteAPIError

    if isinstance(e, ValueError):
        return HTTPException(status_code=400, detail=str(e))
    if isinstance(e, CogniteAPIError):
        return HTTPException(status_code=502, detail=str(e))
    return HTTPException(status_code=500, detail=str(e))


def _load_env() -> None:
    from local_runner.env import load_env
    from ui.server.paths import MODULE_ROOT

    load_env(MODULE_ROOT)


class DryRunBody(BaseModel):
    dry_run: bool = False


class BuildMetadataBody(DryRunBody):
    filter_updated_after: str | None = None
    batch_size: int | None = None
    progress_interval: int = 100


class BuildAnnotationsBody(DryRunBody):
    file_external_id: str | None = None
    detection_mode: Literal["all", "pattern", "standard"] = "all"


class QueryBody(BaseModel):
    terms: list[str] = Field(min_length=1)
    all_scopes: bool = False
    match_scope_keys: list[str] = Field(default_factory=list)
    source_types: list[str] | None = None
    min_confidence: float = 0.0
    reuse_only: bool = False
    hits_only: bool = False


class TagReuseBody(BaseModel):
    all_scopes: bool = False
    match_scope_keys: list[str] = Field(default_factory=list)
    min_scope_count: int = 2
    limit: int = 5000


class TargetDrivenBody(DryRunBody):
    instance_external_id: str | None = None
    instance_external_ids: list[str] = Field(default_factory=list)
    incoming_view_key: str | None = None
    view_external_id: str | None = None
    instance_space: str = "cdf_cdm"
    min_confidence: float = 0.6
    match_scope_keys: list[str] = Field(default_factory=list)
    scope_lookup_override: bool = False
    max_assets: int | None = None
    progress_interval: int = 100
    query_property: str | None = None
    force: bool = False


class FileBody(BaseModel):
    file_external_id: str = Field(min_length=1)
    file_space: str = "cdf_cdm"
    match_scope_key: str | None = None


class ListByFileBody(FileBody):
    source_types: list[str] | None = None
    limit: int = 5000


@router.post("/build/metadata")
def build_metadata(body: BuildMetadataBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_build_metadata

    try:
        return cmd_build_metadata(
            dry_run=body.dry_run,
            filter_updated_after=body.filter_updated_after,
            batch_size=body.batch_size,
            progress_interval=body.progress_interval,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/build/metadata/stream")
def build_metadata_stream(body: BuildMetadataBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_build_metadata
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_build_metadata(
            dry_run=body.dry_run,
            filter_updated_after=body.filter_updated_after,
            batch_size=body.batch_size,
            progress_interval=body.progress_interval,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/build/annotations")
def build_annotations(body: BuildAnnotationsBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_build_annotations

    try:
        return cmd_build_annotations(
            file_external_id=body.file_external_id,
            detection_mode=body.detection_mode,
            dry_run=body.dry_run,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/build/annotations/stream")
def build_annotations_stream(body: BuildAnnotationsBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_build_annotations
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_build_annotations(
            file_external_id=body.file_external_id,
            detection_mode=body.detection_mode,
            dry_run=body.dry_run,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/query")
def query_index(body: QueryBody) -> Any:
    _load_env()
    from local_runner.commands import cmd_query

    try:
        return cmd_query(
            body.terms,
            all_scopes=body.all_scopes,
            match_scope_keys=body.match_scope_keys or None,
            source_types=body.source_types,
            min_confidence=body.min_confidence,
            reuse_only=body.reuse_only,
            hits_only=body.hits_only,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/tag-reuse-audit")
def tag_reuse_audit(body: TagReuseBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_tag_reuse_audit

    try:
        return cmd_tag_reuse_audit(
            all_scopes=body.all_scopes,
            match_scope_keys=body.match_scope_keys or None,
            min_scope_count=body.min_scope_count,
            limit=body.limit,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/target-driven")
def target_driven(body: TargetDrivenBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_target_driven

    try:
        return cmd_target_driven(
            instance_external_id=body.instance_external_id,
            instance_external_ids=body.instance_external_ids or None,
            incoming_view_key=body.incoming_view_key,
            view_external_id=body.view_external_id,
            instance_space=body.instance_space,
            dry_run=body.dry_run,
            min_confidence=body.min_confidence,
            match_scope_keys=body.match_scope_keys or None,
            scope_lookup_override=body.scope_lookup_override,
            max_assets=body.max_assets,
            progress_interval=body.progress_interval,
            query_property=body.query_property,
            force=body.force,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/target-driven/stream")
def target_driven_stream(body: TargetDrivenBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_target_driven
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_target_driven(
            instance_external_id=body.instance_external_id,
            instance_external_ids=body.instance_external_ids or None,
            incoming_view_key=body.incoming_view_key,
            view_external_id=body.view_external_id,
            instance_space=body.instance_space,
            dry_run=body.dry_run,
            min_confidence=body.min_confidence,
            match_scope_keys=body.match_scope_keys or None,
            scope_lookup_override=body.scope_lookup_override,
            max_assets=body.max_assets,
            progress_interval=body.progress_interval,
            query_property=body.query_property,
            force=body.force,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/query/stream")
def query_stream(body: QueryBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_query
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_query(
            body.terms,
            all_scopes=body.all_scopes,
            match_scope_keys=body.match_scope_keys or None,
            source_types=body.source_types,
            min_confidence=body.min_confidence,
            reuse_only=body.reuse_only,
            hits_only=body.hits_only,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/tag-reuse-audit/stream")
def tag_reuse_audit_stream(body: TagReuseBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_tag_reuse_audit
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_tag_reuse_audit(
            all_scopes=body.all_scopes,
            match_scope_keys=body.match_scope_keys or None,
            min_scope_count=body.min_scope_count,
            limit=body.limit,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/score/stream")
def score_stream(body: FileBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_score
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_score(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/deltas/stream")
def file_deltas_stream(body: FileBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_deltas
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_deltas(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/list-by-file/stream")
def list_by_file_stream(body: ListByFileBody, request: Request) -> StreamingResponse:
    _load_env()
    from local_runner.commands import cmd_list_by_file
    from ui.server.operation_stream import stream_operation

    return stream_operation(
        lambda on_log, should_cancel: cmd_list_by_file(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
            source_types=body.source_types,
            limit=body.limit,
            on_log=on_log,
            should_cancel=should_cancel,
        ),
        request,
    )


@router.post("/score")
def score_file(body: FileBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_score

    try:
        return cmd_score(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/deltas")
def file_deltas(body: FileBody) -> dict[str, Any]:
    _load_env()
    from local_runner.commands import cmd_deltas

    try:
        return cmd_deltas(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
        )
    except Exception as e:
        raise _api_error(e) from e


@router.post("/list-by-file")
def list_by_file(body: ListByFileBody) -> list[dict[str, Any]]:
    _load_env()
    from local_runner.commands import cmd_list_by_file

    try:
        return cmd_list_by_file(
            body.file_external_id,
            match_scope_key=body.match_scope_key,
            file_space=body.file_space,
            source_types=body.source_types,
            limit=body.limit,
        )
    except Exception as e:
        raise _api_error(e) from e
