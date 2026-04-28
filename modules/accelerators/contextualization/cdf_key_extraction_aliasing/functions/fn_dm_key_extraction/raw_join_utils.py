"""
Shared helpers for joining Cognite RAW tables to data model instances.

Used by key extraction when ``source_tables`` is configured: one lookup map per
distinct RAW table (cached per run), prefixed columns ``{table_id}__{column}``,
duplicate join keys resolved with last-row-wins (with a warning).

This module is the single place for that behavior so pipeline code does not
duplicate join semantics.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from cognite.client import CogniteClient

from cdf_fn_common.property_path import get_value_by_property_path

__all__ = [
    "normalize_cell_value",
    "list_raw_rows",
    "build_raw_lookup",
    "preload_raw_lookups",
    "entity_props_for_view",
    "merged_join_columns_for_instance",
]


def normalize_cell_value(v: Any) -> Optional[str]:
    """Stringify RAW/DM cell values; treat empty as missing."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    return s if s else None


def list_raw_rows(client: CogniteClient, st: Any) -> List[Any]:
    """Fetch all rows from a RAW table (optional column projection)."""
    kwargs: Dict[str, Any] = {
        "db_name": st.database_name,
        "table_name": st.table_name,
        "limit": -1,
    }
    if getattr(st, "columns", None):
        kwargs["columns"] = list(st.columns)
    try:
        rows = client.raw.rows.list(**kwargs)
    except TypeError:
        rows = client.raw.rows.list(st.database_name, st.table_name, limit=-1)
    return list(rows)


def build_raw_lookup(
    client: CogniteClient,
    st: Any,
    logger: Any,
) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Build map: join_key (str) -> { 'table_id__col': value, ... } for one RAW table.

    Last row wins if duplicate join keys exist.
    """
    tf = (st.join_fields or {}).get("table_field")
    if not tf:
        logger.error(f"source_table {st.table_id}: join_fields.table_field is required")
        return None
    try:
        rows = list_raw_rows(client, st)
    except Exception as e:
        logger.error(f"Failed to list RAW {st.database_name}/{st.table_name}: {e}")
        return None
    if not rows:
        return {}
    allowed: Optional[set] = None
    if getattr(st, "columns", None):
        allowed = set(st.columns)
        allowed.add(tf)
    lookup: Dict[str, Dict[str, Any]] = {}
    dup = 0
    for row in rows:
        cols = getattr(row, "columns", None) or {}
        if not isinstance(cols, dict):
            continue
        jv = cols.get(tf)
        nk = normalize_cell_value(jv)
        if not nk:
            continue
        prefixed: Dict[str, Any] = {}
        for k, v in cols.items():
            if k == tf:
                continue
            if allowed is not None and k not in allowed:
                continue
            pv = normalize_cell_value(v)
            if pv is None:
                continue
            prefixed[f"{st.table_id}__{k}"] = pv
        if nk in lookup:
            dup += 1
        lookup[nk] = prefixed
    if dup:
        logger.warning(
            f"RAW {st.table_name}: {dup} duplicate join key(s) on {tf!r}; using last row per key."
        )
    return lookup


def preload_raw_lookups(
    client: CogniteClient,
    source_tables: List[Any],
    logger: Any,
) -> Dict[Tuple[str, str], Optional[Dict[str, Dict[str, Any]]]]:
    """Load each distinct RAW table once per pipeline run."""
    out: Dict[Tuple[str, str], Optional[Dict[str, Dict[str, Any]]]] = {}
    for st in source_tables:
        key = (st.database_name, st.table_name)
        if key in out:
            continue
        out[key] = build_raw_lookup(client, st, logger)
    return out


def entity_props_for_view(instance: Any, entity_view_id: Any) -> Dict[str, Any]:
    """Property bag for one instance under the configured view identifier."""
    return (
        instance.dump()
        .get("properties", {})
        .get(entity_view_id.space, {})
        .get(f"{entity_view_id.external_id}/{entity_view_id.version}", {})
    )


def merged_join_columns_for_instance(
    entity_props: Dict[str, Any],
    source_tables: List[Any],
    lookups: Dict[Tuple[str, str], Optional[Dict[str, Dict[str, Any]]]],
) -> Dict[str, Any]:
    """Left-join semantics: accumulate prefixed columns from each configured RAW table."""
    merged: Dict[str, Any] = {}
    for st in source_tables:
        lu = lookups.get((st.database_name, st.table_name)) or {}
        if not lu:
            continue
        jf = (st.join_fields or {}).get("view_field")
        tf = (st.join_fields or {}).get("table_field")
        if not jf or not tf:
            continue
        vk = get_value_by_property_path(entity_props, str(jf))
        nk = normalize_cell_value(vk)
        if not nk:
            continue
        row = lu.get(nk)
        if row:
            merged.update(row)
    return merged
