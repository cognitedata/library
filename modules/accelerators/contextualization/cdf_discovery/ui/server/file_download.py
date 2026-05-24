"""CDF File download and HEAD size probe for the operator UI."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from ui.server.file_content_query import (
    _ensure_uploaded,
    download_bytes_for_file,
    identifier_dict_for_file,
    resolve_cdf_file,
)

_CONTENT_RANGE_TOTAL_RE = re.compile(r"/(\d+)\s*$")


def _file_name(file_obj: Any) -> str:
    return str(getattr(file_obj, "name", None) or "download").strip() or "download"


def _file_mime(file_obj: Any) -> str:
    return str(getattr(file_obj, "mime_type", None) or getattr(file_obj, "mimeType", None) or "application/octet-stream")


def _parse_content_length(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        n = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def _parse_content_range_total(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    m = _CONTENT_RANGE_TOTAL_RE.search(str(raw).strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _download_link(client: Any, file_obj: Any) -> str:
    identifier = identifier_dict_for_file(file_obj)
    return str(client.files._get_download_link(identifier))


def _http_client(client: Any) -> Any:
    return client.files._http_client_with_retry


def probe_download_size_bytes(client: Any, file_obj: Any) -> Tuple[Optional[int], str, str]:
    """Return (size_bytes|None, filename, mime_type) without downloading the full file."""
    download_link = _download_link(client, file_obj)
    http = _http_client(client)
    timeout = client.config.file_transfer_timeout

    head = http.request("HEAD", download_link, headers={"accept": "*/*"}, timeout=timeout)
    size = _parse_content_length(head.headers.get("Content-Length"))
    if size is not None:
        return size, _file_name(file_obj), _file_mime(file_obj)

    ranged = http.request(
        "GET",
        download_link,
        headers={"accept": "*/*", "Range": "bytes=0-0"},
        timeout=timeout,
    )
    size = _parse_content_length(ranged.headers.get("Content-Length"))
    if size is None:
        size = _parse_content_range_total(ranged.headers.get("Content-Range"))
    return size, _file_name(file_obj), _file_mime(file_obj)


def resolve_uploaded_file(
    client: Any,
    *,
    file_id: Optional[int] = None,
    file_external_id: Optional[str] = None,
    file_instance_space: Optional[str] = None,
) -> Any:
    if (
        file_id is None
        and not (file_external_id or "").strip()
        and not (file_instance_space or "").strip()
    ):
        raise ValueError("file_id or file_external_id is required")
    file_obj = resolve_cdf_file(
        client,
        file_id=file_id,
        file_external_id=file_external_id,
        file_instance_space=file_instance_space,
    )
    _ensure_uploaded(file_obj)
    return file_obj


def download_file_bytes(
    client: Any,
    *,
    file_id: Optional[int] = None,
    file_external_id: Optional[str] = None,
    file_instance_space: Optional[str] = None,
) -> Tuple[bytes, str, str]:
    file_obj = resolve_uploaded_file(
        client,
        file_id=file_id,
        file_external_id=file_external_id,
        file_instance_space=file_instance_space,
    )
    data = download_bytes_for_file(client, file_obj)
    return data, _file_name(file_obj), _file_mime(file_obj)


def content_disposition_attachment(filename: str) -> str:
    safe = filename.replace('"', "'").replace("\r", "").replace("\n", "")
    return f'attachment; filename="{safe}"; filename*=UTF-8\'\'{quote(filename)}'
