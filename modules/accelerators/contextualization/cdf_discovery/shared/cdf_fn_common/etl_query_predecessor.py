"""Helpers for query stages wired downstream of other canvas tasks."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from cdf_fn_common.etl_common import _first_nonempty, iter_predecessor_rows_for_task


def predecessor_external_ids(data: Mapping[str, Any], task_id: str) -> List[str]:
    """Distinct ``external_id`` values from direct predecessor cohort rows."""
    seen: set[str] = set()
    out: List[str] = []
    for cols, _props in iter_predecessor_rows_for_task(data, task_id):
        ext_id = _first_nonempty(cols.get("external_id"), cols.get("externalId"))
        if not ext_id or ext_id in seen:
            continue
        seen.add(ext_id)
        out.append(ext_id)
    return out


def should_restrict_view_query_to_predecessors(cfg: Mapping[str, Any]) -> bool:
    """When false, view query ignores upstream rows even if the canvas has predecessors."""
    raw = cfg.get("restrict_to_predecessors")
    if raw is None:
        return True
    return bool(raw)


def build_predecessor_instance_dm_filter(view_id: Any, external_ids: Sequence[str]) -> Any | None:
    """DM ``In`` filter on node ``externalId`` for upstream cohort instances."""
    ids = [str(x).strip() for x in external_ids if str(x).strip()]
    if not ids:
        return None
    from cognite.client import data_modeling as dm

    from cdf_fn_common.dm_filter_utils import property_reference_for_filter

    prop = property_reference_for_filter(view_id, "externalId", property_scope="node")
    return dm.filters.In(property=prop, values=ids)


def resolve_raw_query_source(
    data: Mapping[str, Any],
    task_id: str,
    cfg: Mapping[str, Any],
) -> Tuple[str, str, str] | None:
    """
    When RAW query has no explicit source table, use a single predecessor cohort table.

    Returns ``(raw_db, raw_table, source_run_id)`` or ``None`` when explicit config is set.
    """
    source_db = _first_nonempty(cfg.get("source_raw_db"))
    source_table = _first_nonempty(
        cfg.get("source_raw_table"),
        cfg.get("source_raw_table_key"),
    )
    if source_db and source_table:
        return None

    from cdf_fn_common.etl_cohort_storage import predecessor_node_table_locations

    locs = predecessor_node_table_locations(data, task_id)
    if not locs:
        return None
    if len(locs) > 1:
        raise ValueError(
            f"query_raw task {task_id!r}: multiple predecessor cohort tables; "
            "set config.source_raw_db and source_raw_table_key explicitly"
        )
    raw_db, raw_table = locs[0]
    source_run = _first_nonempty(cfg.get("source_run_id"), data.get("run_id"))
    if not source_run:
        raise ValueError(f"query_raw task {task_id!r}: run_id is required when reading predecessor RAW")
    return raw_db, raw_table, source_run


def raw_query_rows_from_predecessor_buffer(
    data: Mapping[str, Any],
    task_id: str,
    *,
    filters: list[Any],
    read_limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Build cohort rows from in-memory predecessor buffers (no RAW scan)."""
    from cdf_fn_common.etl_filter_eval import row_passes_filter
    from cdf_fn_common.etl_incremental_scope import EXTERNAL_ID_COLUMN, NODE_INSTANCE_ID_COLUMN

    rows: list[dict[str, Any]] = []
    n_read = 0
    for cols, props in iter_predecessor_rows_for_task(data, task_id):
        if not row_passes_filter(props, filters):
            continue
        n_read += 1
        if read_limit > 0 and n_read > read_limit:
            break
        nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), cols.get("node_instance_id"))
        ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), cols.get("external_id"), nid, str(n_read))
        rows.append(
            {
                "columns": {"node_instance_id": str(nid or ext_id), "external_id": ext_id},
                "properties": dict(props),
            }
        )
    return rows, n_read
