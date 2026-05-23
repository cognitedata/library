"""Query tabular CDF File content locally via DuckDB (parquet, CSV, JSON)."""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from ui.server import cdf_browse

FileContentFormat = Literal["parquet", "csv", "json"]

FORMAT_EXTENSIONS: Dict[FileContentFormat, str] = {
    "parquet": "parquet",
    "csv": "csv",
    "json": "json",
}

_DDL_DML_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|COPY|ATTACH|DETACH|"
    r"GRANT|REVOKE|CALL|EXECUTE|PRAGMA|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)

_DEFAULT_MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


def _module_cache_dir() -> Path:
    root = Path(os.environ.get("CDF_EXPLORER_ROOT") or Path(__file__).resolve().parent.parent.parent)
    cache = root / ".cache" / "file_content"
    cache.mkdir(parents=True, exist_ok=True)
    return cache


def _max_download_bytes() -> int:
    raw = os.environ.get("CDF_EXPLORER_FILE_MAX_BYTES", "").strip()
    if raw.isdigit():
        return max(1, int(raw))
    return _DEFAULT_MAX_DOWNLOAD_BYTES


def _file_mime(file_obj: Any) -> str:
    return str(getattr(file_obj, "mime_type", None) or getattr(file_obj, "mimeType", None) or "").lower()


def _file_name(file_obj: Any) -> str:
    return str(getattr(file_obj, "name", None) or "").lower()


def _uploaded_time_key(file_obj: Any) -> str:
    ts = getattr(file_obj, "uploaded_time", None) or getattr(file_obj, "uploadedTime", None)
    if isinstance(ts, datetime):
        return ts.isoformat()
    if ts is not None:
        return str(ts)
    return "unknown"


def detect_format_from_metadata(file_obj: Any) -> Optional[FileContentFormat]:
    mime = _file_mime(file_obj)
    name = _file_name(file_obj)

    if "parquet" in mime or name.endswith(".parquet"):
        return "parquet"
    if mime in {"text/csv", "application/csv", "text/comma-separated-values"} or name.endswith(".csv"):
        return "csv"
    if (
        "json" in mime
        or "ndjson" in mime
        or name.endswith((".json", ".jsonl", ".ndjson"))
    ):
        return "json"
    return None


def _strip_sql_comments(query: str) -> str:
    q = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    q = re.sub(r"--[^\n\r]*", " ", q)
    return q


def validate_select_only_query(query: str) -> None:
    q = query.strip()
    if not q:
        raise ValueError("query is required")

    cleaned = _strip_sql_comments(q).strip()
    if ";" in cleaned.rstrip(";"):
        raise ValueError("Only a single SELECT statement is allowed")

    if _DDL_DML_PATTERN.search(cleaned):
        raise ValueError("Only read-only SELECT queries are allowed")

    upper = cleaned.lstrip("(").upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Only SELECT queries are allowed")


def resolve_cdf_file(
    client: Any,
    *,
    file_id: Optional[int] = None,
    file_external_id: Optional[str] = None,
) -> Any:
    if file_id is not None:
        return client.files.retrieve(id=int(file_id))
    ext = (file_external_id or "").strip()
    if ext:
        try:
            return client.files.retrieve(external_id=ext)
        except Exception:
            pass
        try:
            from cognite.client.data_modeling import NodeId

            # Best-effort DM instance lookup when external id is an instance key.
            matches = list(client.files.retrieve_multiple(instance_ids=[NodeId("unknown", ext)]))
            if matches:
                return matches[0]
        except Exception:
            pass
        return client.files.retrieve(external_id=ext)
    raise ValueError("file_id or file_external_id is required")


def _ensure_uploaded(file_obj: Any) -> None:
    uploaded = getattr(file_obj, "uploaded", None)
    if uploaded is None:
        uploaded = getattr(file_obj, "is_uploaded", None)
    if uploaded is False:
        raise ValueError("File content has not been uploaded to CDF")


def _cache_path(file_obj: Any, fmt: FileContentFormat) -> Path:
    file_id = getattr(file_obj, "id", None)
    if file_id is None:
        raise ValueError("Resolved file has no id")
    ext = FORMAT_EXTENSIONS[fmt]
    stamp = _uploaded_time_key(file_obj).replace(":", "-")
    return _module_cache_dir() / f"{file_id}_{stamp}.{ext}"


def _download_to_cache(client: Any, file_obj: Any, fmt: FileContentFormat) -> Path:
    path = _cache_path(file_obj, fmt)
    if path.is_file() and path.stat().st_size > 0:
        return path

    file_id = getattr(file_obj, "id", None)
    if file_id is None:
        raise ValueError("Resolved file has no id")

    data = client.files.download_bytes(id=int(file_id))
    size = len(data)
    max_bytes = _max_download_bytes()
    if size > max_bytes:
        raise ValueError(
            f"File size ({size} bytes) exceeds maximum allowed download size ({max_bytes} bytes)"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _duckdb_view_sql(path: Path, fmt: FileContentFormat) -> str:
    escaped = str(path).replace("'", "''")
    if fmt == "parquet":
        return f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{escaped}')"
    if fmt == "csv":
        return f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_csv_auto('{escaped}')"
    return (
        f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_json_auto('{escaped}', format='auto')"
    )


def _serialize_cell(value: Any, *, convert_to_string: bool) -> Any:
    if value is None:
        return None
    if convert_to_string:
        if isinstance(value, (dict, list)):
            import json

            try:
                return json.dumps(value)
            except TypeError:
                return str(value)
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, (dict, list)):
        import json

        try:
            return json.dumps(value)
        except TypeError:
            return str(value)
    return value


def _rows_to_items(
    columns: List[str],
    rows: List[tuple],
    *,
    convert_to_string: bool,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        item = {
            col: cdf_browse._truncate(_serialize_cell(val, convert_to_string=convert_to_string))
            for col, val in zip(columns, row)
        }
        items.append(item)
    return items


def run_file_content_sql(
    client: Any,
    *,
    query: str,
    limit: int = 100,
    file_id: Optional[int] = None,
    file_external_id: Optional[str] = None,
    fmt: Optional[FileContentFormat] = None,
    convert_to_string: bool = True,
) -> Dict[str, Any]:
    validate_select_only_query(query)

    file_obj = resolve_cdf_file(
        client, file_id=file_id, file_external_id=file_external_id
    )
    _ensure_uploaded(file_obj)

    detected = detect_format_from_metadata(file_obj)
    if fmt is None:
        if detected is None:
            raise ValueError("Could not detect a supported tabular file format (parquet, csv, json)")
        fmt = detected
    elif detected is not None and detected != fmt:
        raise ValueError(f"File format mismatch: metadata suggests {detected}, request specified {fmt}")

    path = _download_to_cache(client, file_obj, fmt)

    import duckdb

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(_duckdb_view_sql(path, fmt))
        wrapped = f"SELECT * FROM ({query.strip().rstrip(';')}) AS _explorer_q LIMIT {int(limit)}"
        result = con.execute(wrapped)
        columns = [str(d[0]) for d in (result.description or [])]
        rows = result.fetchall()
    finally:
        con.close()

    items = _rows_to_items(columns, rows, convert_to_string=convert_to_string)
    schema = [{"name": c, "type": None} for c in columns]

    return {
        "columns": columns,
        "items": items,
        "schema": schema,
        "row_count": len(items),
    }
