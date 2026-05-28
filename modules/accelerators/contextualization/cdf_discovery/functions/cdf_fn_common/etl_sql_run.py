"""CDF transformations SQL preview without UI server dependencies."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

RAW_VALUE_PREVIEW_LEN = 2000


def _truncate(value: Any, max_len: int = RAW_VALUE_PREVIEW_LEN) -> Any:
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + "…"
    if isinstance(value, dict):
        return {k: _truncate(v, max_len) for k, v in list(value.items())[:40]}
    if isinstance(value, list):
        return [_truncate(v, max_len) for v in value[:20]]
    return value


def _serialize_preview_schema_col(col: Any) -> Dict[str, Any]:
    if hasattr(col, "dump"):
        dumped = col.dump(camel_case=False)
        if isinstance(dumped, dict):
            return dumped
    col_type = getattr(col, "type", None)
    if hasattr(col_type, "dump"):
        type_val: Any = col_type.dump(camel_case=False)
    elif col_type is not None:
        type_val = str(col_type)
    else:
        type_val = None
    return {
        "name": getattr(col, "name", None),
        "sql_type": getattr(col, "sql_type", None),
        "nullable": getattr(col, "nullable", None),
        "type": type_val,
    }


def run_sql_preview(
    client: Any,
    *,
    query: str,
    limit: int = 100,
    source_limit: Optional[int] = None,
    convert_to_string: bool = True,
    infer_schema_limit: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Dict[str, Any]:
    """Preview SQL using CDF transformations ``preview``."""
    q = query.strip()
    if not q:
        raise ValueError("query is required")

    kwargs: Dict[str, Any] = {
        "query": q,
        "convert_to_string": convert_to_string,
        "limit": limit,
        "source_limit": source_limit,
    }
    if infer_schema_limit is not None:
        kwargs["infer_schema_limit"] = infer_schema_limit
    if timeout is not None:
        kwargs["timeout"] = timeout

    preview = client.transformations.preview(**kwargs)
    schema_cols: List[Dict[str, Any]] = []
    if preview.schema:
        for col in preview.schema:
            schema_cols.append(_serialize_preview_schema_col(col))

    items = preview.results or []
    columns: List[str] = []
    if items:
        columns = list(items[0].keys())
    elif schema_cols:
        columns = [str(c["name"]) for c in schema_cols if c.get("name")]

    return {
        "columns": columns,
        "items": [_truncate(row) for row in items],
        "schema": schema_cols,
        "row_count": len(items),
    }


def _normalized_col_key(key: str) -> str:
    return str(key).lower().replace("_", "")


_EXTERNAL_ID_NORM = frozenset(
    {"externalid", "id", "name", "key", "nodeexternalid", "external_id"}
)


def resolve_sql_row_external_id(row: Mapping[str, Any], column: str = "") -> str:
    """Pick a stable external id from a SQL result row."""
    col = str(column or "").strip()
    if col:
        for k, v in row.items():
            if k == col or _normalized_col_key(k) == _normalized_col_key(col):
                if v is not None and str(v).strip():
                    return str(v).strip()
    for k, v in row.items():
        if _normalized_col_key(k) in _EXTERNAL_ID_NORM:
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""
