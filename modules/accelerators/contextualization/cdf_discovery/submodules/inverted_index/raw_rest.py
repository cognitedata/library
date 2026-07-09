"""CDF RAW rows REST API — thin client via ``CogniteClient.get``."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any
from urllib.parse import quote


class RawRowsAPIError(RuntimeError):
    """Raised when RAW rows API returns a non-success response."""


def _project_path(client: Any, suffix: str) -> str:
    project = str(getattr(getattr(client, "config", None), "project", None) or "").strip()
    if not project:
        raise ValueError("CDF client project is required for RAW rows API calls")
    seg = suffix if suffix.startswith("/") else f"/{suffix}"
    return f"/api/v1/projects/{quote(project, safe='')}{seg}"


def _encode_segment(name: str) -> str:
    return quote(str(name or "").strip(), safe="")


def _response_json(response: Any) -> dict[str, Any]:
    try:
        body = response.json()
    except Exception as e:
        raise RawRowsAPIError(
            f"RAW rows API returned non-JSON (status {getattr(response, 'status_code', '?')})"
        ) from e
    if not isinstance(body, dict):
        return {"items": body} if body is not None else {}
    return body


def _request_get(
    client: Any,
    path_suffix: str,
    *,
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    url = _project_path(client, path_suffix)
    response = client.get(url, params=dict(params) if params else None)
    status = int(getattr(response, "status_code", 0) or 0)
    if status < 200 or status >= 300:
        detail = ""
        try:
            detail = (response.text or "")[:4000]
        except Exception:
            pass
        raise RawRowsAPIError(
            f"RAW rows API GET {path_suffix} failed: HTTP {status} {detail}".strip()
        )
    return _response_json(response)


def _next_cursor(page: Mapping[str, Any]) -> str | None:
    for key in ("nextCursor", "next_cursor"):
        cur = page.get(key)
        if cur is not None and str(cur).strip():
            return str(cur).strip()
    return None


def _row_items(page: Mapping[str, Any]) -> list[dict[str, Any]]:
    items = page.get("items")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def list_raw_rows_page(
    client: Any,
    db_name: str,
    table_name: str,
    *,
    limit: int = 1000,
    cursor: str | None = None,
    columns: list[str] | None = None,
) -> dict[str, Any]:
    """GET /raw/dbs/{db}/tables/{table}/rows — one page with optional nextCursor."""
    db = _encode_segment(db_name)
    table = _encode_segment(table_name)
    path = f"/raw/dbs/{db}/tables/{table}/rows"
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 10_000))}
    if cursor:
        params["cursor"] = cursor
    if columns is not None:
        params["columns"] = ",".join(columns)
    return _request_get(client, path, params=params)


def iter_raw_rows(
    client: Any,
    db_name: str,
    table_name: str,
    *,
    page_size: int = 1000,
    columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield each RAW row item dict across all cursor pages."""
    cursor: str | None = None
    while True:
        page = list_raw_rows_page(
            client,
            db_name,
            table_name,
            limit=page_size,
            cursor=cursor,
            columns=columns,
        )
        for item in _row_items(page):
            yield item
        cursor = _next_cursor(page)
        if not cursor:
            break
