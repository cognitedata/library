"""Cumulative transform input: merge predecessor + sink cohort state before applying handlers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .cohort_storage import (
    CohortRowIndex,
    TableLocation,
    cohort_row_indexes_for_tables,
    get_or_build_cohort_row_index,
    instance_cohort_row_key,
    instance_identity_from_columns,
    node_cohort_table_name,
)
from .discovery_cohort import _props_from_row_columns
from .discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    _first_nonempty,
)
from .pipeline_steps import materialize_transform_steps
from .property_merge import (
    FieldPolicy,
    MergeListOptions,
    STRATEGY_MERGE_LIST,
    merge_list_property_values_list,
    merge_property_dicts,
    parse_field_policies,
)

INPUT_MODE_CUMULATIVE = "cumulative"
INPUT_MODE_REPLACE = "replace"
INPUT_MODES = frozenset({INPUT_MODE_CUMULATIVE, INPUT_MODE_REPLACE})


def parse_input_mode(cfg: Mapping[str, Any]) -> str:
    mode = _first_nonempty(cfg.get("input_mode")) or INPUT_MODE_CUMULATIVE
    if mode not in INPUT_MODES:
        raise ValueError(
            f"transform config input_mode must be one of {sorted(INPUT_MODES)}; got {mode!r}"
        )
    return mode


def cumulative_field_policies(cfg: Mapping[str, Any]) -> Dict[str, FieldPolicy]:
    """Field policies for merging cohort branches; explicit field_policies win per property."""
    out: Dict[str, FieldPolicy] = dict(parse_field_policies(cfg))
    _mode, steps = materialize_transform_steps(cfg)
    for step in steps:
        prop = _first_nonempty(step.get("output_field"))
        if not prop or prop in out:
            continue
        out[prop] = _default_merge_list_policy(prop)
    for prop in _DEFAULT_CUMULATIVE_MERGE_LIST_FIELDS:
        if prop not in out:
            out[prop] = _default_merge_list_policy(prop)
    return out


_DEFAULT_CUMULATIVE_MERGE_LIST_FIELDS = ("aliases", "indexKey")


def _default_merge_list_policy(prop: str) -> FieldPolicy:
    return FieldPolicy(
        property_name=prop,
        strategy=STRATEGY_MERGE_LIST,
        merge_list=MergeListOptions(unique=True, branch_order="by_dependency"),
    )


def load_cohort_row_by_key(
    client: Any,
    raw_db: str,
    raw_table: str,
    row_key: str,
) -> Any:
    """Return RAW row object for *row_key*, or None if missing."""
    key = str(row_key or "").strip()
    if not key or client is None:
        return None
    try:
        return client.raw.rows.retrieve(raw_db, raw_table, key)
    except Exception:
        return None


def _props_for_cohort_row(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    cols = dict(getattr(row, "columns", None) or {})
    if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
        return None
    return _props_from_row_columns(cols)


def props_for_key_at_location(
    client: Any,
    raw_db: str,
    raw_table: str,
    row_key: str,
) -> Optional[Dict[str, Any]]:
    """Fallback path: point retrieve when no in-memory index is available."""
    row = load_cohort_row_by_key(client, raw_db, raw_table, row_key)
    return _props_for_cohort_row(row)


def _props_from_table_index(
    table_indexes: Mapping[TableLocation, CohortRowIndex],
    raw_db: str,
    raw_table: str,
    row_key: str,
) -> Optional[Dict[str, Any]]:
    idx = table_indexes.get((raw_db, raw_table))
    if not idx:
        return None
    snap = idx.get(row_key)
    if snap is None:
        return None
    return dict(snap.properties)


def resolve_cumulative_input_props(
    client: Any,
    anchor_cols: Mapping[str, Any],
    *,
    writer_canvas_node_id: str,
    predecessor_canvas_node_ids: Sequence[str],
    raw_db: str,
    base_table: str,
    run_id: str,
    cfg: Mapping[str, Any],
    table_indexes: Optional[Mapping[TableLocation, CohortRowIndex]] = None,
) -> Dict[str, Any]:
    """
    Merge property dicts from predecessor node tables and (by default) the writer node table.

    *anchor_cols* supplies fallback when no branch exists (first write for an instance).
    When *table_indexes* is set, branch props are dict lookups (no ``raw.rows.retrieve``).
    """
    input_mode = parse_input_mode(cfg)
    policies = cumulative_field_policies(cfg)
    scope_key, nid = instance_identity_from_columns(anchor_cols)
    if not nid:
        return _props_from_row_columns(anchor_cols)

    row_key = instance_cohort_row_key(nid, scope_key)

    branches: List[Dict[str, Any]] = []
    seen_branch_sigs: set[str] = set()

    def _add_branch(props: Optional[Mapping[str, Any]]) -> None:
        if not props:
            return
        body = dict(props)
        sig = repr(sorted(body.items()))
        if sig in seen_branch_sigs:
            return
        seen_branch_sigs.add(sig)
        branches.append(body)

    def _branch_props(pred_table: str) -> Optional[Dict[str, Any]]:
        if table_indexes is not None:
            return _props_from_table_index(table_indexes, raw_db, pred_table, row_key)
        return props_for_key_at_location(client, raw_db, pred_table, row_key)

    for pred_canvas_id in predecessor_canvas_node_ids:
        pred_table = node_cohort_table_name(base_table, run_id, pred_canvas_id)
        _add_branch(_branch_props(pred_table))

    if input_mode == INPUT_MODE_CUMULATIVE:
        writer_table = node_cohort_table_name(base_table, run_id, writer_canvas_node_id)
        _add_branch(_branch_props(writer_table))

    if not branches:
        return _props_from_row_columns(anchor_cols)

    if len(branches) == 1:
        props = dict(branches[0])
        for prop, pol in policies.items():
            if pol.strategy != STRATEGY_MERGE_LIST:
                continue
            merged = merge_list_property_values_list(
                [props],
                prop,
                unique=pol.merge_list.unique,
            )
            if merged is not None:
                props[prop] = merged
        return props

    base = dict(branches[-1])
    policy_merged = merge_property_dicts(branches, policies)
    base.update(policy_merged)
    return base


def build_transform_table_indexes(
    client: Any,
    *,
    raw_db: str,
    base_table: str,
    run_id: str,
    writer_canvas_node_id: str,
    predecessor_canvas_node_ids: Sequence[str],
    cfg: Mapping[str, Any],
    index_cache: Any = None,
) -> Dict[TableLocation, CohortRowIndex]:
    """Indexes for all predecessor (+ writer when cumulative) node tables for one transform task."""
    locs: List[TableLocation] = []
    for pred_canvas_id in predecessor_canvas_node_ids:
        locs.append(
            (raw_db, node_cohort_table_name(base_table, run_id, pred_canvas_id))
        )
    if parse_input_mode(cfg) == INPUT_MODE_CUMULATIVE:
        locs.append(
            (
                raw_db,
                node_cohort_table_name(base_table, run_id, writer_canvas_node_id),
            )
        )
    return cohort_row_indexes_for_tables(client, locs, index_cache=index_cache)


def iter_unique_predecessor_entity_rows(
    client: Any,
    raw_db: str,
    base_table: str,
    run_id: str,
    *,
    predecessor_canvas_node_ids: Sequence[str],
    table_indexes: Optional[Mapping[TableLocation, CohortRowIndex]] = None,
    index_cache: Any = None,
) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    One cohort entity row per ``(scope_key, node_instance_id)`` across predecessor node tables.

    Yields ``(instance_row_key, columns)`` for each distinct instance.
    """
    seen: set[tuple[str, str]] = set()
    pred_list = list(predecessor_canvas_node_ids)
    for pred_canvas_id in pred_list:
        tbl = node_cohort_table_name(base_table, run_id, pred_canvas_id)
        loc: TableLocation = (raw_db, tbl)
        row_index: Optional[CohortRowIndex] = None
        if table_indexes is not None:
            row_index = table_indexes.get(loc)
        elif callable(index_cache):
            row_index = get_or_build_cohort_row_index(
                client, raw_db, tbl, index_cache=index_cache
            )
        if row_index is not None:
            for row_key, snap in row_index.items():
                cols = dict(snap.columns)
                scope_key, nid = instance_identity_from_columns(cols)
                if not nid:
                    continue
                inst = (scope_key, nid)
                if inst in seen:
                    continue
                seen.add(inst)
                yield row_key, cols
            continue
        from .cohort_storage import iter_cohort_entity_rows

        for row in iter_cohort_entity_rows(client, raw_db, tbl):
            cols = dict(getattr(row, "columns", None) or {})
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            scope_key, nid = instance_identity_from_columns(cols)
            if not nid:
                continue
            inst = (scope_key, nid)
            if inst in seen:
                continue
            seen.add(inst)
            yield instance_cohort_row_key(nid, scope_key), cols


def iter_unique_predecessor_entity_rows_list(
    client: Any,
    raw_db: str,
    base_table: str,
    run_id: str,
    *,
    predecessor_canvas_node_ids: Sequence[str],
    table_indexes: Optional[Mapping[TableLocation, CohortRowIndex]] = None,
    index_cache: Any = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Backward-compatible list materialization of :func:`iter_unique_predecessor_entity_rows`."""
    return list(
        iter_unique_predecessor_entity_rows(
            client,
            raw_db,
            base_table,
            run_id,
            predecessor_canvas_node_ids=predecessor_canvas_node_ids,
            table_indexes=table_indexes,
            index_cache=index_cache,
        )
    )
