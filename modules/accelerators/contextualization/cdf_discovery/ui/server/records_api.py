"""Streams and Records API routes for Discovery operator UI."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

_TRANSFORM_FUNCTIONS = (
    Path(__file__).resolve().parent.parent.parent / "transform" / "functions"
)
if str(_TRANSFORM_FUNCTIONS) not in sys.path:
    sys.path.insert(0, str(_TRANSFORM_FUNCTIONS))

router = APIRouter(prefix="/api/cdf", tags=["records"])


def _client():
    from local_runner.client import get_client

    return get_client()


class StreamCreateBody(BaseModel):
    config: Dict[str, Any] = Field(default_factory=dict)


class RecordsSyncBody(BaseModel):
    limit: int = Field(100, ge=1, le=1000)
    cursor: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None
    sources: Optional[List[Dict[str, Any]]] = None
    include_tombstones: bool = False


class RecordsWriteBody(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)
    write_mode: str = "ingest"


@router.get("/streams")
def list_streams_route(
    limit: int = 1000,
    cursor: Optional[str] = None,
) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, list_streams

    try:
        client = _client()
        return list_streams(client, limit=limit, cursor=cursor)
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/streams/{stream_id}")
def get_stream_route(stream_id: str) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, retrieve_stream

    try:
        client = _client()
        return retrieve_stream(client, stream_id)
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/streams")
def create_stream_route(body: StreamCreateBody) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import (
        StreamsRecordsAPIError,
        build_stream_create_body,
        create_stream,
    )

    try:
        client = _client()
        payload = build_stream_create_body(body.config)
        return create_stream(client, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/streams/{stream_id}/records/sync")
def sync_records_route(stream_id: str, body: RecordsSyncBody) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, sync_records

    req: Dict[str, Any] = {"limit": body.limit}
    if body.cursor:
        req["cursor"] = body.cursor
    if body.filter:
        req["filter"] = body.filter
    if body.sources:
        req["sources"] = body.sources
    if body.include_tombstones:
        req["includeTombstones"] = True
    try:
        client = _client()
        return sync_records(client, stream_id, req)
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/streams/{stream_id}/records/ingest")
def ingest_records_route(stream_id: str, body: RecordsWriteBody) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, ingest_records

    try:
        client = _client()
        return ingest_records(client, stream_id, {"items": body.items})
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/streams/{stream_id}/records/upsert")
def upsert_records_route(stream_id: str, body: RecordsWriteBody) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, upsert_records

    try:
        client = _client()
        return upsert_records(client, stream_id, {"items": body.items})
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/streams/{stream_id}/records/delete")
def delete_records_route(stream_id: str, body: RecordsWriteBody) -> Dict[str, Any]:
    from cdf_fn_common.etl_streams_records_api import StreamsRecordsAPIError, delete_records

    try:
        client = _client()
        items = [
            {"space": i.get("space"), "externalId": i.get("externalId") or i.get("external_id")}
            for i in body.items
            if isinstance(i, dict)
        ]
        return delete_records(client, stream_id, {"items": items})
    except StreamsRecordsAPIError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
