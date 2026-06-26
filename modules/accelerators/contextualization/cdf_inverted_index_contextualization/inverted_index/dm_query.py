"""DM enumeration and index lookup via ``instances.query`` cursor pagination."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator, List, Optional

from inverted_index.scope import normalize_resolve_candidate

NODE_RESULT_KEY = "nodes"
EDGE_RESULT_KEY = "edges"
INDEX_RESULT_KEY = "index_rows"

_INSTANCE_META = frozenset(
    {"externalid", "external_id", "space", "instanceid", "instance_id", "lastupdatedtime"}
)


@dataclass
class QueryStats:
    page_count: int = 0
    instances_yielded: int = 0
    list_duration_sec: float = 0.0
    limit_per_page: int = 1000
    api: str = "instances.query"
    next_cursor: str = ""


def _require_query_fn(client: Any) -> Any:
    query_fn = getattr(client.data_modeling.instances, "query", None)
    if not callable(query_fn):
        raise TypeError("client.data_modeling.instances.query is required")
    return query_fn


def top_level_property_names(paths: Iterable[str]) -> List[str]:
    """Map dot-paths to unique top-level view property names for ``Select``."""
    names: List[str] = []
    seen: set[str] = set()
    for raw in paths:
        root = str(raw).split(".")[0].strip()
        norm = root.lower().replace("_", "")
        if not root or norm in _INSTANCE_META or root in seen:
            continue
        seen.add(root)
        names.append(root)
    return names


def _resolve_path_from_candidate(raw: Any) -> str | None:
    candidate = normalize_resolve_candidate(raw)
    if not candidate:
        return None
    return candidate["path"]


def collect_view_property_paths(
    *,
    view_external_id: str,
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
    extra_paths: list[str] | None = None,
) -> List[str]:
    """Gather property paths needed for index build / scope resolution on a view."""
    paths: list[str] = list(extra_paths or [])
    for view_cfg in index_field_config or []:
        if view_cfg.get("view") != view_external_id:
            continue
        for prop in view_cfg.get("properties") or []:
            if isinstance(prop, dict) and prop.get("path"):
                paths.append(str(prop["path"]))
    resolve_from = (scope_config or {}).get("resolve_from") or {}
    view_resolve = resolve_from.get(view_external_id) or {}
    if isinstance(view_resolve, dict):
        for candidates in view_resolve.values():
            if isinstance(candidates, list):
                for item in candidates:
                    path = _resolve_path_from_candidate(item)
                    if path:
                        paths.append(path)
            else:
                path = _resolve_path_from_candidate(candidates)
                if path:
                    paths.append(path)
    elif isinstance(view_resolve, list):
        for item in view_resolve:
            path = _resolve_path_from_candidate(item)
            if path:
                paths.append(path)
    defaults = (scope_config or {}).get("resolve_from_default") or {}
    if isinstance(defaults, dict):
        for candidates in defaults.values():
            if isinstance(candidates, list):
                for item in candidates:
                    path = _resolve_path_from_candidate(item)
                    if path:
                        paths.append(path)
            else:
                path = _resolve_path_from_candidate(candidates)
                if path:
                    paths.append(path)
    return paths


def annotation_select_property_names(cfg: dict) -> List[str]:
    """Property names to select for diagram annotation edge queries."""
    names = [
        cfg.get("text_property", "startNodeText"),
        cfg.get("confidence_property", "confidence"),
        cfg.get("status_property", "status"),
        cfg.get("page_property", "startNodePageNumber"),
        "tags",
    ]
    for key in cfg.get("bbox_properties") or [
        "startNodeXMin",
        "startNodeYMin",
        "startNodeXMax",
        "startNodeYMax",
    ]:
        names.append(str(key))
    mode_prop = cfg.get("detection_mode_property")
    if mode_prop:
        names.append(str(mode_prop))
    return top_level_property_names(names)


def watermark_filter(instance_kind: str, updated_after: datetime) -> Any:
    from cognite.client import data_modeling as dm

    ms = int(updated_after.timestamp() * 1000)
    return dm.filters.Range((instance_kind, "lastUpdatedTime"), gt=ms)


def combine_node_filter(
    view_id: Any,
    *,
    instance_space: str | None = None,
    user_filters: list[Any] | None = None,
    watermark_filter_obj: Any = None,
) -> Any:
    from cognite.client import data_modeling as dm

    parts: list[Any] = [dm.filters.HasData(views=[view_id])]
    space = str(instance_space or "").strip()
    if space:
        parts.append(dm.filters.Equals(["node", "space"], space))
    if user_filters:
        parts.extend(user_filters)
    if watermark_filter_obj is not None:
        parts.append(watermark_filter_obj)
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)


def combine_edge_filter(
    view_id: Any,
    *,
    edge_space: str | None = None,
    file_space: str | None = None,
    file_external_id: str | None = None,
    detection_mode_property: str | None = None,
    detection_mode: str | None = None,
    text_property: str | None = None,
    watermark_filter_obj: Any = None,
    user_filters: list[Any] | None = None,
) -> Any:
    from cognite.client import data_modeling as dm

    parts: list[Any] = [dm.filters.HasData(views=[view_id])]
    space = str(edge_space or "").strip()
    if space:
        parts.append(dm.filters.Equals(["edge", "space"], space))
    file_ext = str(file_external_id or "").strip()
    if file_ext:
        f_space = str(file_space or "cdf_cdm").strip()
        parts.append(
            dm.filters.Equals(
                ["edge", "startNode"],
                {"space": f_space, "externalId": file_ext},
            )
        )
    if detection_mode_property and detection_mode in ("standard", "pattern"):
        parts.append(
            dm.filters.Equals(
                view_id.as_property_ref(detection_mode_property),
                detection_mode,
            )
        )
    text_prop = str(text_property or "").strip()
    if text_prop:
        parts.append(dm.filters.Exists(view_id.as_property_ref(text_prop)))
    if watermark_filter_obj is not None:
        parts.append(watermark_filter_obj)
    if user_filters:
        parts.extend(user_filters)
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)


def _instances_from_result(result: Any, key: str) -> List[Any]:
    if result is None:
        return []
    if isinstance(result, dict):
        raw = result.get(key)
        if raw is None and key == NODE_RESULT_KEY:
            raw = result.get("nodes")
        if raw is None and key == EDGE_RESULT_KEY:
            raw = result.get("edges")
        return list(raw or [])
    nodes_attr = getattr(result, key, None)
    if nodes_attr is not None:
        return list(nodes_attr or [])
    if key == NODE_RESULT_KEY:
        nodes = getattr(result, "nodes", None)
        if nodes is not None:
            return list(nodes or [])
    if key == EDGE_RESULT_KEY:
        edges = getattr(result, "edges", None)
        if edges is not None:
            return list(edges or [])
    if hasattr(result, "data"):
        data = result.data
        if isinstance(data, dict):
            raw = data.get(key) or data.get("nodes") or data.get("edges")
            return list(raw or [])
    return []


def _cursors_from_result(result: Any) -> dict[str, Any]:
    if result is None:
        return {}
    if isinstance(result, dict):
        cursors = result.get("cursors")
        return dict(cursors) if isinstance(cursors, dict) else {}
    cursors = getattr(result, "cursors", None)
    if isinstance(cursors, dict):
        return dict(cursors)
    return {}


def _build_node_query(
    *,
    view_id: Any,
    combined_filter: Any,
    property_names: List[str],
    page_size: int,
    result_key: str = NODE_RESULT_KEY,
) -> Any:
    from cognite.client.data_classes.data_modeling.query import (
        NodeResultSetExpression,
        Query,
        Select,
        SourceSelector,
    )

    return Query(
        with_={
            result_key: NodeResultSetExpression(
                filter=combined_filter,
                limit=max(1, int(page_size)),
            ),
        },
        select={
            result_key: Select([SourceSelector(view_id, property_names)]),
        },
        cursors={},
    )


def _build_edge_query(
    *,
    view_id: Any,
    combined_filter: Any,
    property_names: List[str],
    page_size: int,
    result_key: str = EDGE_RESULT_KEY,
) -> Any:
    from cognite.client.data_classes.data_modeling.query import (
        EdgeResultSetExpression,
        Query,
        Select,
        SourceSelector,
    )

    return Query(
        with_={
            result_key: EdgeResultSetExpression(
                filter=combined_filter,
                limit=max(1, int(page_size)),
            ),
        },
        select={
            result_key: Select([SourceSelector(view_id, property_names)]),
        },
        cursors={},
    )


def _query_pages(
    client: Any,
    query: Any,
    *,
    result_key: str,
    page_size: int,
    max_items: int = 0,
    stats_out: QueryStats | None = None,
) -> Iterator[Any]:
    query_fn = _require_query_fn(client)
    t0 = time.perf_counter()
    batch_no = 0
    total = 0

    while True:
        if max_items > 0:
            remaining = max_items - total
            if remaining <= 0:
                break
            try:
                query.with_[result_key].limit = max(1, min(page_size, remaining))
            except Exception:
                pass

        result = query_fn(query)
        items = _instances_from_result(result, result_key)
        if not items:
            if batch_no == 0:
                break
            if not _cursors_from_result(result).get(result_key):
                break

        batch_no += 1
        for item in items:
            total += 1
            yield item

        cursors = _cursors_from_result(result)
        next_cursor = cursors.get(result_key)
        if max_items > 0 and total >= max_items:
            if stats_out is not None:
                stats_out.next_cursor = str(next_cursor or "")
            break
        if not next_cursor:
            if stats_out is not None:
                stats_out.next_cursor = ""
            break
        if stats_out is not None:
            stats_out.next_cursor = str(next_cursor)
        query.cursors = {result_key: next_cursor}

    duration = round(time.perf_counter() - t0, 6)
    if stats_out is not None:
        stats_out.page_count = batch_no
        stats_out.instances_yielded = total
        stats_out.list_duration_sec = duration
        stats_out.limit_per_page = page_size
        stats_out.api = "instances.query"


def query_all_nodes(
    client: Any,
    *,
    view_id: Any,
    property_names: List[str],
    instance_space: str | None = None,
    user_filters: list[Any] | None = None,
    filter_updated_after: datetime | None = None,
    page_size: int = 1000,
    max_items: int = 0,
    stats_out: QueryStats | None = None,
) -> Iterator[Any]:
    """Page DM nodes with server-side filters via ``instances.query``."""
    wm = (
        watermark_filter("node", filter_updated_after)
        if filter_updated_after is not None
        else None
    )
    combined = combine_node_filter(
        view_id,
        instance_space=instance_space,
        user_filters=user_filters,
        watermark_filter_obj=wm,
    )
    if not property_names:
        property_names = ["name"]
    query = _build_node_query(
        view_id=view_id,
        combined_filter=combined,
        property_names=property_names,
        page_size=page_size,
    )
    yield from _query_pages(
        client,
        query,
        result_key=NODE_RESULT_KEY,
        page_size=page_size,
        max_items=max_items,
        stats_out=stats_out,
    )


def query_all_edges(
    client: Any,
    *,
    view_id: Any,
    property_names: List[str],
    edge_space: str | None = None,
    file_space: str | None = None,
    file_external_id: str | None = None,
    detection_mode_property: str | None = None,
    detection_mode: str | None = None,
    text_property: str | None = None,
    filter_updated_after: datetime | None = None,
    user_filters: list[Any] | None = None,
    page_size: int = 1000,
    max_items: int = 0,
    stats_out: QueryStats | None = None,
) -> Iterator[Any]:
    """Page DM edges with server-side filters via ``instances.query``."""
    wm = (
        watermark_filter("edge", filter_updated_after)
        if filter_updated_after is not None
        else None
    )
    combined = combine_edge_filter(
        view_id,
        edge_space=edge_space,
        file_space=file_space,
        file_external_id=file_external_id,
        detection_mode_property=detection_mode_property,
        detection_mode=detection_mode if detection_mode in ("standard", "pattern") else None,
        text_property=text_property,
        watermark_filter_obj=wm,
        user_filters=user_filters,
    )
    if not property_names:
        property_names = ["startNodeText"]
    query = _build_edge_query(
        view_id=view_id,
        combined_filter=combined,
        property_names=property_names,
        page_size=page_size,
    )
    yield from _query_pages(
        client,
        query,
        result_key=EDGE_RESULT_KEY,
        page_size=page_size,
        max_items=max_items,
        stats_out=stats_out,
    )


def build_file_reference_filter(
    view_id: Any,
    *,
    file_external_id: str,
    reference_type: str = "CogniteFile",
    file_space: str | None = None,
    match_scope_key: str | None = None,
    source_types: list[str] | None = None,
) -> Any:
    from cognite.client import data_modeling as dm

    container = view_id.external_id
    parts: list[Any] = [
        dm.filters.HasData(views=[view_id]),
        dm.filters.Equals([container, "referenceExternalId"], file_external_id),
        dm.filters.Equals([container, "referenceType"], reference_type),
    ]
    if file_space:
        parts.append(dm.filters.Equals([container, "referenceSpace"], file_space))
    if match_scope_key:
        parts.append(dm.filters.Equals([container, "matchScopeKey"], match_scope_key))
    if source_types:
        parts.append(dm.filters.In([container, "sourceType"], source_types))
    return dm.filters.And(*parts)


def query_index_entries_by_file(
    client: Any,
    *,
    view_id: Any,
    index_space: str,
    file_external_id: str,
    file_space: str = "cdf_cdm",
    reference_type: str = "CogniteFile",
    match_scope_key: str | None = None,
    source_types: list[str] | None = None,
    property_names: list[str] | None = None,
    page_size: int = 1000,
    limit: int = 5000,
    stats_out: QueryStats | None = None,
) -> list[dict]:
    """Lookup inverted index rows for a containing CogniteFile reference."""
    from cognite.client import data_modeling as dm

    combined = build_file_reference_filter(
        view_id,
        file_external_id=file_external_id,
        reference_type=reference_type,
        file_space=file_space,
        match_scope_key=match_scope_key,
        source_types=source_types,
    )
    combined = dm.filters.And(
        combined,
        dm.filters.Equals(["node", "space"], index_space),
    )
    props = property_names or [
        "term",
        "normalizedTerm",
        "originalValue",
        "sourceType",
        "sourceProperty",
        "referenceExternalId",
        "referenceSpace",
        "referenceType",
        "matchScopeKey",
        "matchScope",
        "additionalMetadata",
        "buildJobId",
    ]
    query = _build_node_query(
        view_id=view_id,
        combined_filter=combined,
        property_names=props,
        page_size=min(page_size, 1000),
        result_key=INDEX_RESULT_KEY,
    )
    results: list[dict] = []
    for inst in _query_pages(
        client,
        query,
        result_key=INDEX_RESULT_KEY,
        page_size=page_size,
        max_items=limit,
        stats_out=stats_out,
    ):
        props_raw = dict(getattr(inst, "properties", None) or {})
        flat = props_raw
        if hasattr(props_raw, "get"):
            view_props = props_raw.get(view_id)
            if isinstance(view_props, dict):
                flat = dict(view_props)
        results.append(
            {
                "external_id": inst.external_id,
                "term": flat.get("term"),
                "normalized_term": flat.get("normalizedTerm") or flat.get("normalized_term"),
                "original_value": flat.get("originalValue") or flat.get("original_value"),
                "source_type": flat.get("sourceType") or flat.get("source_type"),
                "source_property": flat.get("sourceProperty") or flat.get("source_property"),
                "reference_external_id": flat.get("referenceExternalId")
                or flat.get("reference_external_id"),
                "reference_space": flat.get("referenceSpace") or flat.get("reference_space"),
                "reference_type": flat.get("referenceType") or flat.get("reference_type"),
                "match_scope_key": flat.get("matchScopeKey") or flat.get("match_scope_key"),
                "match_scope": flat.get("matchScope") or flat.get("match_scope"),
                "additional_metadata": flat.get("additionalMetadata")
                or flat.get("additional_metadata"),
                "build_job_id": flat.get("buildJobId") or flat.get("build_job_id"),
            }
        )
        if len(results) >= limit:
            break
    return results


def build_index_entry_filter(
    view_id: Any,
    *,
    normalized_terms: list[str],
    match_scope_key: str | None = None,
    match_scope_keys: list[str] | None = None,
    source_types: list[str] | None = None,
) -> Any:
    from cognite.client import data_modeling as dm

    container = view_id.external_id
    parts: list[Any] = [
        dm.filters.HasData(views=[view_id]),
        dm.filters.In([container, "normalizedTerm"], normalized_terms),
    ]
    if match_scope_keys:
        parts.append(dm.filters.In([container, "matchScopeKey"], match_scope_keys))
    elif match_scope_key:
        parts.append(dm.filters.Equals([container, "matchScopeKey"], match_scope_key))
    if source_types:
        parts.append(dm.filters.In([container, "sourceType"], source_types))
    if len(parts) == 1:
        return parts[0]
    return dm.filters.And(*parts)


def query_index_entries(
    client: Any,
    *,
    view_id: Any,
    index_space: str,
    normalized_terms: list[str],
    match_scope_key: str | None = None,
    match_scope_keys: list[str] | None = None,
    source_types: list[str] | None = None,
    property_names: list[str] | None = None,
    page_size: int = 1000,
    limit: int = 5000,
    stats_out: QueryStats | None = None,
) -> list[dict]:
    """Lookup inverted index rows via ``instances.query`` with server-side filters."""
    from cognite.client import data_modeling as dm

    if not normalized_terms:
        return []

    combined = build_index_entry_filter(
        view_id,
        normalized_terms=normalized_terms,
        match_scope_key=match_scope_key,
        match_scope_keys=match_scope_keys,
        source_types=source_types,
    )
    combined = dm.filters.And(
        combined,
        dm.filters.Equals(["node", "space"], index_space),
    )
    props = property_names or [
        "term",
        "normalizedTerm",
        "originalValue",
        "sourceType",
        "sourceProperty",
        "referenceExternalId",
        "referenceSpace",
        "referenceType",
        "matchScopeKey",
        "matchScope",
        "additionalMetadata",
        "buildJobId",
    ]
    query = _build_node_query(
        view_id=view_id,
        combined_filter=combined,
        property_names=props,
        page_size=min(page_size, 1000),
        result_key=INDEX_RESULT_KEY,
    )
    results: list[dict] = []
    for inst in _query_pages(
        client,
        query,
        result_key=INDEX_RESULT_KEY,
        page_size=page_size,
        max_items=limit,
        stats_out=stats_out,
    ):
        props_raw = dict(getattr(inst, "properties", None) or {})
        flat = props_raw
        if hasattr(props_raw, "get"):
            view_props = props_raw.get(view_id)
            if isinstance(view_props, dict):
                flat = dict(view_props)
        results.append(
            {
                "external_id": inst.external_id,
                "term": flat.get("term"),
                "normalized_term": flat.get("normalizedTerm") or flat.get("normalized_term"),
                "original_value": flat.get("originalValue") or flat.get("original_value"),
                "source_type": flat.get("sourceType") or flat.get("source_type"),
                "source_property": flat.get("sourceProperty") or flat.get("source_property"),
                "reference_external_id": flat.get("referenceExternalId")
                or flat.get("reference_external_id"),
                "reference_space": flat.get("referenceSpace") or flat.get("reference_space"),
                "reference_type": flat.get("referenceType") or flat.get("reference_type"),
                "match_scope_key": flat.get("matchScopeKey") or flat.get("match_scope_key"),
                "match_scope": flat.get("matchScope") or flat.get("match_scope"),
                "additional_metadata": flat.get("additionalMetadata")
                or flat.get("additional_metadata"),
                "build_job_id": flat.get("buildJobId") or flat.get("build_job_id"),
            }
        )
        if len(results) >= limit:
            break
    return results
