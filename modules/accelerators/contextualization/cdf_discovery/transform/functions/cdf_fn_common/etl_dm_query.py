"""Data modeling view enumeration via ``instances.query`` cursor pagination.

Incremental listing uses a watermark ``Range`` on ``node.lastUpdatedTime``; ensure that
property and every filter field used in scope queries are indexed (BTree, cursorable).
Operator audit script (aliasing module):
``cdf_discovery_aliasing/scripts/audit_view_query_dm_indexes.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, MutableMapping, Optional

from .query_enumeration import QueryEnumerationStats, resolve_page_size
from .etl_ui_progress import emit_handler_progress

RESULT_SET_KEY = "rows"


@dataclass
class ViewQueryStats:
    page_count: int = 0
    instances_yielded: int = 0
    list_duration_sec: float = 0.0
    limit_per_page: int = 1000
    sort_property: Optional[str] = None
    api: str = "instances.query"


_INSTANCE_META_PROPERTY_NAMES = frozenset(
    {"externalid", "external_id", "space", "instanceid", "instance_id"}
)
_DEFAULT_PROPERTY_CANDIDATES = ("name", "description")


def view_cache_key(view_id: Any) -> str:
    """Stable cache key for view property name sets."""
    space = getattr(view_id, "space", None) or ""
    external_id = getattr(view_id, "external_id", None) or ""
    version = getattr(view_id, "version", None) or ""
    return f"{space}/{external_id}/{version}"


def _view_property_name_set(client: Any, view_id: Any) -> set[str]:
    try:
        batch = client.data_modeling.views.retrieve([view_id])
    except Exception:
        return set()
    if not batch:
        return set()
    view = batch[0]
    raw_props = getattr(view, "properties", None) or {}
    if hasattr(raw_props, "keys"):
        return {str(k) for k in raw_props.keys()}
    if hasattr(view, "dump"):
        dumped = view.dump(camel_case=False)
        if isinstance(dumped, dict):
            props = dumped.get("properties") or {}
            if isinstance(props, dict):
                return {str(k) for k in props.keys()}
    return set()


def _strip_instance_meta_property_names(names: List[str]) -> List[str]:
    out: List[str] = []
    for name in names:
        norm = str(name).strip().lower().replace("_", "")
        if not norm or norm in {m.replace("_", "") for m in _INSTANCE_META_PROPERTY_NAMES}:
            continue
        out.append(str(name).strip())
    return out


def _resolve_property_names(
    cfg: Dict[str, Any],
    *,
    client: Any = None,
    view_id: Any = None,
    property_names_cache: Optional[MutableMapping[str, set[str]]] = None,
) -> List[str]:
    raw = cfg.get("include_properties")
    requested: List[str] = []
    if isinstance(raw, list) and raw:
        requested = _strip_instance_meta_property_names([str(x) for x in raw])

    available: set[str] = set()
    if client is not None and view_id is not None:
        cache_key = view_cache_key(view_id)
        if property_names_cache is not None and cache_key in property_names_cache:
            available = property_names_cache[cache_key]
        else:
            available = _view_property_name_set(client, view_id)
            if property_names_cache is not None:
                property_names_cache[cache_key] = available

    if requested:
        if available:
            return [name for name in requested if name in available]
        return requested

    if available:
        return sorted(available)

    return list(_DEFAULT_PROPERTY_CANDIDATES)


def combine_view_query_filter(
    view_id: Any,
    *,
    user_filters: Optional[List[Any]] = None,
    instance_space: Optional[str] = None,
    watermark_filter: Any = None,
) -> Any:
    from cognite.client import data_modeling as dm

    from .source_view_filter_build import build_source_view_query_filter

    parts: List[Any] = [build_source_view_query_filter(view_id, user_filters or [])]
    space = str(instance_space or "").strip()
    if space and space.lower() != "all_spaces":
        parts.append(dm.filters.Equals(["node", "space"], space))
    if watermark_filter is not None:
        parts.append(watermark_filter)
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)


def build_view_query_document(
    *,
    view_id: Any,
    combined_filter: Any,
    page_size: int,
    property_names: List[str],
) -> Any:
    from cognite.client.data_classes.data_modeling.query import (
        NodeResultSetExpression,
        Query,
        Select,
        SourceSelector,
    )

    return Query(
        with_={
            RESULT_SET_KEY: NodeResultSetExpression(
                filter=combined_filter,
                limit=max(1, int(page_size)),
            ),
        },
        select={
            RESULT_SET_KEY: Select([SourceSelector(view_id, property_names)]),
        },
        cursors={},
    )


def _nodes_from_query_result(result: Any, key: str = RESULT_SET_KEY) -> List[Any]:
    if result is None:
        return []
    if isinstance(result, dict):
        raw = result.get(key) or result.get("nodes")
        return list(raw or [])
    nodes_attr = getattr(result, key, None)
    if nodes_attr is not None:
        return list(nodes_attr or [])
    nodes = getattr(result, "nodes", None)
    if nodes is not None:
        return list(nodes or [])
    if hasattr(result, "data"):
        data = result.data
        if isinstance(data, dict):
            raw = data.get(key) or data.get("nodes")
            return list(raw or [])
    return []


def _cursors_from_query_result(result: Any) -> Dict[str, Any]:
    if result is None:
        return {}
    if isinstance(result, dict):
        cursors = result.get("cursors")
        return dict(cursors) if isinstance(cursors, dict) else {}
    cursors = getattr(result, "cursors", None)
    if isinstance(cursors, dict):
        return dict(cursors)
    return {}


def query_all_view_instances(
    client: Any,
    *,
    view_id: Any,
    instance_space: Optional[str] = None,
    dm_filter: Any = None,
    cfg: Optional[Dict[str, Any]] = None,
    logger: Optional[Any] = None,
    progress_context: str = "",
    stats_out: Optional[ViewQueryStats] = None,
    property_names_cache: Optional[MutableMapping[str, set[str]]] = None,
) -> Iterable[Any]:
    """Page through DM nodes using ``client.data_modeling.instances.query``."""
    task_cfg = dict(cfg or {})
    page_size = resolve_page_size(task_cfg)
    combined = dm_filter
    if combined is None:
        watermark_filter = task_cfg.pop("_watermark_filter", None)
        combined = combine_view_query_filter(
            view_id,
            user_filters=task_cfg.get("filters") or [],
            instance_space=instance_space,
            watermark_filter=watermark_filter,
        )
    property_names = _resolve_property_names(
        task_cfg,
        client=client,
        view_id=view_id,
        property_names_cache=property_names_cache,
    )
    query = build_view_query_document(
        view_id=view_id,
        combined_filter=combined,
        page_size=page_size,
        property_names=property_names,
    )

    instances_api = client.data_modeling.instances
    query_fn = getattr(instances_api, "query", None)
    if not callable(query_fn):
        raise TypeError("client.data_modeling.instances.query is required")

    t0 = time.perf_counter()
    batch_no = 0
    total = 0

    while True:
        result = query_fn(query)
        nodes = _nodes_from_query_result(result, RESULT_SET_KEY)
        if not nodes:
            if batch_no == 0:
                break
            cursors = _cursors_from_query_result(result)
            if not cursors.get(RESULT_SET_KEY):
                break
        batch_no += 1
        n_in_page = 0
        for node in nodes:
            n_in_page += 1
            total += 1
            yield node
        if logger is not None and hasattr(logger, "info"):
            ctx = f" {progress_context}" if progress_context else ""
            logger.info(
                "instances.query batch %s complete%s: %s instance(s) this page, %s cumulative",
                batch_no,
                ctx,
                n_in_page,
                total,
            )
        emit_handler_progress(total, label="instances")
        cursors = _cursors_from_query_result(result)
        next_cursor = cursors.get(RESULT_SET_KEY)
        if not next_cursor:
            break
        query.cursors = {RESULT_SET_KEY: next_cursor}

    duration = round(time.perf_counter() - t0, 6)
    if stats_out is not None:
        stats_out.page_count = batch_no
        stats_out.instances_yielded = total
        stats_out.list_duration_sec = duration
        stats_out.limit_per_page = page_size
        stats_out.api = "instances.query"
    elif logger is not None and hasattr(logger, "info") and batch_no:
        ctx = f" {progress_context}" if progress_context else ""
        logger.info(
            "instances.query finished%s: %s page(s), %s instance(s), %.3fs",
            ctx,
            batch_no,
            total,
            duration,
        )


def query_stats_to_enumeration(stats: ViewQueryStats) -> QueryEnumerationStats:
    return QueryEnumerationStats(
        rows_read=stats.instances_yielded,
        rows_written=stats.instances_yielded,
        pages=stats.page_count,
        rows_truncated=False,
        list_complete=True,
    )
