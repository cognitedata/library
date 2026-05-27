"""CDF Streams and Records REST API (GA) — thin client via ``CogniteClient.get/post``."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional
from urllib.parse import quote

from cdf_fn_common.etl_common import _first_nonempty


class StreamsRecordsAPIError(RuntimeError):
    """Raised when Streams/Records API returns a non-success response."""


def _project_path(client: Any, suffix: str) -> str:
    project = str(getattr(getattr(client, "config", None), "project", None) or "").strip()
    if not project:
        raise ValueError("CDF client project is required for Streams/Records API calls")
    seg = suffix if suffix.startswith("/") else f"/{suffix}"
    return f"/api/v1/projects/{quote(project, safe='')}{seg}"


def _encode_stream_id(stream_external_id: str) -> str:
    return quote(str(stream_external_id or "").strip(), safe="")


def _response_json(response: Any) -> Dict[str, Any]:
    try:
        body = response.json()
    except Exception as e:
        raise StreamsRecordsAPIError(
            f"Streams/Records API returned non-JSON (status {getattr(response, 'status_code', '?')})"
        ) from e
    if not isinstance(body, dict):
        return {"items": body} if body is not None else {}
    return body


def _request(
    client: Any,
    method: str,
    path_suffix: str,
    *,
    json_body: Optional[Mapping[str, Any]] = None,
    params: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    url = _project_path(client, path_suffix)
    if method.upper() == "GET":
        response = client.get(url, params=dict(params) if params else None)
    elif method.upper() == "POST":
        response = client.post(url, json=dict(json_body or {}), params=dict(params) if params else None)
    else:
        raise ValueError(f"unsupported HTTP method: {method}")
    status = int(getattr(response, "status_code", 0) or 0)
    if status < 200 or status >= 300:
        detail = ""
        try:
            detail = (response.text or "")[:4000]
        except Exception:
            pass
        raise StreamsRecordsAPIError(
            f"Streams/Records API {method} {path_suffix} failed: HTTP {status} {detail}".strip()
        )
    return _response_json(response)


def list_streams(
    client: Any,
    *,
    limit: int = 1000,
    cursor: Optional[str] = None,
) -> Dict[str, Any]:
    """GET /streams — returns API body with ``items`` and optional ``nextCursor``."""
    params: Dict[str, Any] = {"limit": max(1, min(int(limit), 1000))}
    if cursor:
        params["cursor"] = cursor
    return _request(client, "GET", "/streams", params=params)


def retrieve_stream(client: Any, stream_external_id: str) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "GET", f"/streams/{sid}")


def create_stream(client: Any, body: Mapping[str, Any]) -> Dict[str, Any]:
    return _request(client, "POST", "/streams", json_body=body)


def sync_records(
    client: Any,
    stream_external_id: str,
    body: Mapping[str, Any],
) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "POST", f"/streams/{sid}/records/sync", json_body=body)


def filter_records(
    client: Any,
    stream_external_id: str,
    body: Mapping[str, Any],
) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "POST", f"/streams/{sid}/records/filter", json_body=body)


def ingest_records(
    client: Any,
    stream_external_id: str,
    body: Mapping[str, Any],
) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "POST", f"/streams/{sid}/records", json_body=body)


def upsert_records(
    client: Any,
    stream_external_id: str,
    body: Mapping[str, Any],
) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "POST", f"/streams/{sid}/records/upsert", json_body=body)


def delete_records(
    client: Any,
    stream_external_id: str,
    body: Mapping[str, Any],
) -> Dict[str, Any]:
    sid = _encode_stream_id(stream_external_id)
    return _request(client, "POST", f"/streams/{sid}/records/delete", json_body=body)


def _record_items(page: Mapping[str, Any]) -> List[Dict[str, Any]]:
    items = page.get("items")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _next_cursor(page: Mapping[str, Any]) -> Optional[str]:
    for key in ("nextCursor", "next_cursor"):
        cur = page.get(key)
        if cur is not None and str(cur).strip():
            return str(cur).strip()
    return None


def iter_record_pages(
    client: Any,
    stream_external_id: str,
    *,
    read_mode: str = "sync",
    body_base: Optional[MutableMapping[str, Any]] = None,
) -> Iterator[Dict[str, Any]]:
    """Yield one API page at a time (sync or filter)."""
    mode = str(read_mode or "sync").strip().lower() or "sync"
    body: Dict[str, Any] = dict(body_base or {})
    cursor: Optional[str] = body.get("cursor") if isinstance(body.get("cursor"), str) else None
    while True:
        req = dict(body)
        if cursor:
            req["cursor"] = cursor
        if mode == "filter":
            page = filter_records(client, stream_external_id, req)
        else:
            page = sync_records(client, stream_external_id, req)
        yield page
        cursor = _next_cursor(page)
        if not cursor:
            break


def flatten_record_properties(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Flatten record ``sources`` container props into a single property bag for cohort/filter."""
    out: Dict[str, Any] = {}
    for key in ("externalId", "external_id", "space", "lastUpdatedTime", "status"):
        if key in record and record[key] is not None:
            out[key] = record[key]
    sources = record.get("sources")
    if isinstance(sources, list):
        for src in sources:
            if not isinstance(src, dict):
                continue
            props = src.get("properties")
            if isinstance(props, dict):
                for k, v in props.items():
                    out[str(k)] = v
            container = src.get("container")
            prefix = ""
            if isinstance(container, dict):
                prefix = _first_nonempty(container.get("externalId"), container.get("external_id"))
            elif isinstance(container, str):
                prefix = container.strip()
            if prefix and isinstance(props, dict):
                for k, v in props.items():
                    out[f"{prefix}.{k}"] = v
    return out


def record_to_predecessor_row(
    record: Mapping[str, Any],
    *,
    stream_external_id: str,
) -> Dict[str, Any]:
    """Map API record item to in-memory predecessor row (columns + properties)."""
    space = _first_nonempty(record.get("space"))
    ext_id = _first_nonempty(record.get("externalId"), record.get("external_id"))
    nid = f"{space}:{ext_id}" if space and ext_id else ext_id or space or ""
    props = flatten_record_properties(record)
    sources = record.get("sources")
    if isinstance(sources, list):
        props["record_sources"] = sources
    return {
        "columns": {
            "node_instance_id": nid,
            "external_id": ext_id,
            "record_space": space,
            "stream_external_id": stream_external_id,
        },
        "properties": props,
        "record": dict(record),
    }


def build_records_request_body(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Build sync/filter request body from task config."""
    body: Dict[str, Any] = {}
    limit = cfg.get("batch_size") or cfg.get("limit")
    if limit is not None:
        try:
            body["limit"] = max(1, min(int(limit), 1000))
        except (TypeError, ValueError):
            pass
    filt = cfg.get("filter")
    if isinstance(filt, dict):
        body["filter"] = filt
    elif isinstance(filt, str) and filt.strip():
        try:
            parsed = json.loads(filt)
            if isinstance(parsed, dict):
                body["filter"] = parsed
        except json.JSONDecodeError:
            pass
    sources = cfg.get("sources")
    if isinstance(sources, list) and sources:
        body["sources"] = sources
    if cfg.get("include_tombstones") is True:
        body["includeTombstones"] = True
    cur = _first_nonempty(cfg.get("cursor"))
    if cur:
        body["cursor"] = cur
    return body


def build_stream_create_body(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Structured + optional ``stream_definition`` override for POST /streams."""
    override = cfg.get("stream_definition")
    if isinstance(override, dict):
        return dict(override)
    if isinstance(override, str) and override.strip():
        parsed = json.loads(override)
        if isinstance(parsed, dict):
            return parsed
    ext = _first_nonempty(cfg.get("stream_external_id"), cfg.get("externalId"), cfg.get("external_id"))
    space = _first_nonempty(cfg.get("stream_space"), cfg.get("space"))
    if not ext:
        raise ValueError("save_stream requires stream_external_id or stream_definition")
    body: Dict[str, Any] = {"externalId": ext}
    if space:
        body["space"] = space
    name = _first_nonempty(cfg.get("name"))
    if name:
        body["name"] = name
    desc = _first_nonempty(cfg.get("stream_description"), cfg.get("description"))
    if desc:
        body["description"] = desc
    template = _first_nonempty(cfg.get("template"))
    if template:
        body["template"] = template
    if "mutable" in cfg:
        body["mutable"] = bool(cfg.get("mutable"))
    sources = cfg.get("sources")
    if isinstance(sources, list) and sources:
        body["sources"] = sources
    return body


def cohort_row_to_record_item(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
    *,
    record_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Build Records API record object from cohort columns/properties."""
    from cdf_fn_common.etl_records_cohort import parse_record_sources_json

    from cdf_fn_common.etl_discovery_query_shared import EXTERNAL_ID_COLUMN, NODE_INSTANCE_ID_COLUMN
    from cdf_fn_common.etl_records_cohort import RECORD_SPACE_COLUMN

    space = _first_nonempty(
        cols.get(RECORD_SPACE_COLUMN),
        cols.get("RECORD_SPACE"),
        cols.get("record_space"),
        props.get("record_space"),
        props.get("space"),
    )
    ext_id = _first_nonempty(
        cols.get(EXTERNAL_ID_COLUMN),
        cols.get("EXTERNAL_ID"),
        cols.get("external_id"),
        props.get("external_id"),
        props.get("externalId"),
    )
    if not ext_id:
        nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "")
        if ":" in nid:
            ext_id = nid.split(":", 1)[1].strip()
    sources_raw = cols.get("RECORD_SOURCES_JSON") or props.get("record_sources")
    sources = parse_record_sources_json(sources_raw)
    if not sources and isinstance(props.get("record_sources"), list):
        sources = props.get("record_sources")
    item: Dict[str, Any] = {}
    if space:
        item["space"] = space
    if ext_id:
        item["externalId"] = ext_id
    if sources:
        item["sources"] = sources
    if record_type:
        item["type"] = record_type
    return item
