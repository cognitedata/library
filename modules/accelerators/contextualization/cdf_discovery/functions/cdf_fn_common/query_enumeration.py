"""
Shared limit / pagination semantics for discovery query handlers.

``batch_size`` and ``limit`` on view/classic tasks are API **page sizes**, not total row caps.
``read_limit`` on RAW and explicit ``limit`` on SQL are optional **total** caps (0 = unlimited).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional
from cdf_fn_common.etl_run_scope import is_lookup_full_scan, pipeline_parameters

# Cognite DM / classic list APIs typically allow up to 1000 per request.
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000

# SQL transformations.preview ceiling used when limit is unset or 0.
SQL_PREVIEW_MAX_ROWS = 10_000


@dataclass
class QueryEnumerationStats:
    """Aggregate read/write metrics returned in handler summaries."""

    rows_read: int = 0
    rows_written: int = 0
    pages: int = 0
    rows_truncated: bool = False
    truncation_reason: Optional[str] = None
    list_complete: bool = True

    def to_summary_fields(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "enumeration_pages": self.pages,
            "rows_truncated": self.rows_truncated,
            "list_complete": self.list_complete,
        }
        if self.truncation_reason:
            out["truncation_reason"] = self.truncation_reason
        return out


def resolve_page_size(
    cfg: Mapping[str, Any],
    *,
    default: int = DEFAULT_PAGE_SIZE,
    max_page: int = MAX_PAGE_SIZE,
) -> int:
    """
    Page size for cursor-paginated list APIs (view query, classic query).

    ``batch_size`` and ``limit`` are aliases. Values <= 0 fall back to *default*.
    """
    raw = cfg.get("batch_size")
    if raw is None:
        raw = cfg.get("limit")
    try:
        n = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        n = default
    if n <= 0:
        n = default
    return min(max_page, n)


def resolve_read_limit(cfg: Mapping[str, Any]) -> int:
    """
    Optional total row cap for RAW query (and similar).

    0 or unset means scan the full source (no cap).
    """
    raw = cfg.get("read_limit")
    if raw is None:
        raw = cfg.get("limit")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def resolve_run_record_cap(data: Mapping[str, Any], cfg: Mapping[str, Any]) -> int:
    """
    Total records budget for a single workflow run.

    ``max_records_per_run`` <= 0 (or unset) means unlimited.
    Task config takes precedence over workflow parameters.
    """
    if is_lookup_full_scan(cfg):
        return 0
    raw = cfg.get("max_records_per_run")
    if raw is None:
        raw = pipeline_parameters(data).get("max_records_per_run")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def resolve_sql_row_limit(cfg: Mapping[str, Any]) -> int:
    """
    Row limit for ``transformations.preview``.

    0 or unset → ``SQL_PREVIEW_MAX_ROWS``; explicit positive values are capped at that max.
    """
    raw = cfg.get("limit")
    if raw is None:
        raw = cfg.get("batch_size")
    if raw is None:
        return SQL_PREVIEW_MAX_ROWS
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return SQL_PREVIEW_MAX_ROWS
    if n <= 0:
        return SQL_PREVIEW_MAX_ROWS
    return min(n, SQL_PREVIEW_MAX_ROWS)


def mark_truncated(
    stats: QueryEnumerationStats,
    *,
    reason: str,
    list_complete: bool = False,
) -> None:
    stats.rows_truncated = True
    stats.truncation_reason = reason
    stats.list_complete = list_complete


def enumeration_summary(
    stats: QueryEnumerationStats,
    *,
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Merge enumeration fields into a handler summary dict."""
    merged: Dict[str, Any] = dict(stats.to_summary_fields())
    if extra:
        merged.update(extra)
    return merged


def resolve_classic_list_limit(cfg: Mapping[str, Any]) -> int:
    """
    Total row cap for classic ``*.list``.

    ``read_limit`` / ``limit`` > 0 caps rows; otherwise ``-1`` (SDK unlimited pagination).
    """
    cap = resolve_read_limit(cfg)
    return cap if cap > 0 else -1


def list_all_classic_resources(
    client: Any,
    resource_type: str,
    *,
    limit: int = -1,
    stats_out: Optional[QueryEnumerationStats] = None,
) -> Iterable[Any]:
    """
    Yield all classic resources of *resource_type*.

    *limit* ``-1`` or ``None``: Cognite SDK fetches all pages internally.
    Positive *limit*: at most that many items (single request cap).
    """
    rt = resource_type.strip().lower()
    if rt in ("asset", "assets"):
        list_fn = client.assets.list
    elif rt in ("file", "files"):
        list_fn = client.files.list
    elif rt in ("event", "events"):
        list_fn = client.events.list
    elif rt in ("timeseries", "time_series", "time-series"):
        list_fn = client.time_series.list
    else:
        raise ValueError(f"Unsupported classic resource_type: {resource_type!r}")

    batch = list_fn(limit=limit)
    n = 0
    if isinstance(batch, list):
        items = batch
    else:
        items = list(batch) if batch is not None else []

    for item in items:
        n += 1
        yield item

    if stats_out is not None:
        stats_out.rows_read = n
        stats_out.pages = 1 if limit == -1 or limit is None else 1
        stats_out.list_complete = True
