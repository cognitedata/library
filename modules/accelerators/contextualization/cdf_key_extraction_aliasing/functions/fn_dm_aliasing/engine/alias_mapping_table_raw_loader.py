"""
Load alias mapping table rows from Cognite RAW for AliasMappingTableHandler.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Pattern, Tuple

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore


def _cell_str(val: Any) -> Optional[str]:
    if val is None or (pd is not None and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def _normalize_scope(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return "global"
    s = str(raw).strip().lower()
    if s in ("global", "space", "view_external_id", "instance"):
        return s
    return None


def parse_dataframe_to_rows(
    df: Any,
    raw_table: Dict[str, Any],
    rule_default_source_match: str = "exact",
    case_insensitive: bool = False,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Build resolved row dicts from a pandas DataFrame.

    Each row dict: source, aliases (list), scope, scope_value, source_match, regex_pattern (optional).
    """
    if pd is None:
        return [], ["pandas is required to parse alias mapping table RAW data"]

    errors: List[str] = []
    key_column = raw_table.get("key_column") or raw_table.get("source_tag")
    if not key_column:
        return [], ["raw_table requires key_column"]

    alias_columns: List[str] = list(raw_table.get("alias_columns") or [])
    scope_column = raw_table.get("scope_column", "scope")
    scope_value_column = raw_table.get("scope_value_column", "scope_value")
    source_match_column = raw_table.get("source_match_column")

    if key_column not in df.columns:
        return [], [f"key_column {key_column!r} not in RAW table columns"]

    resolved: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        source = _cell_str(row.get(key_column))
        if not source:
            continue

        aliases: List[str] = []
        for col in alias_columns:
            if col not in df.columns:
                errors.append(f"alias column {col!r} missing (row index {idx})")
                continue
            v = _cell_str(row.get(col))
            if v:
                aliases.append(v)

        scope_raw = _cell_str(row.get(scope_column)) if scope_column in df.columns else None
        scope = _normalize_scope(scope_raw)
        if scope is None:
            errors.append(
                f"Invalid scope {scope_raw!r} at row index {idx}; skipping row"
            )
            continue

        scope_value: Optional[str] = None
        if scope != "global" and scope_value_column in df.columns:
            scope_value = _cell_str(row.get(scope_value_column))
        if scope != "global" and not scope_value:
            errors.append(
                f"scope {scope!r} requires scope_value at row index {idx}; skipping row"
            )
            continue

        row_source_match: Optional[str] = None
        if source_match_column and source_match_column in df.columns:
            row_source_match = _cell_str(row.get(source_match_column))
            if row_source_match:
                row_source_match = row_source_match.strip().lower()

        effective = row_source_match or rule_default_source_match or "exact"
        if effective not in ("exact", "glob", "regex"):
            errors.append(
                f"Invalid source_match {effective!r} at row index {idx}; skipping row"
            )
            continue

        regex_pattern: Optional[Pattern[str]] = None
        if effective == "regex":
            flags = re.IGNORECASE if case_insensitive else 0
            try:
                regex_pattern = re.compile(source, flags)
            except re.error as e:
                errors.append(f"Invalid regex in source {source!r} at row {idx}: {e}")
                continue

        resolved.append(
            {
                "source": source,
                "aliases": aliases,
                "scope": scope,
                "scope_value": scope_value,
                "source_match": effective,
                "regex_pattern": regex_pattern,
            }
        )

    return resolved, errors


def load_alias_mapping_table_from_client(
    client: Any,
    raw_table: Dict[str, Any],
    rule_default_source_match: str = "exact",
    case_insensitive: bool = False,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Retrieve RAW table via Cognite client and parse rows."""
    if client is None:
        return [], ["Cognite client is required to load alias_mapping_table raw_table"]

    if pd is None:
        return [], ["pandas is required for RAW alias mapping table"]

    db = raw_table.get("database_name")
    table = raw_table.get("table_name")
    if not db or not table:
        return [], ["raw_table requires database_name and table_name"]

    columns = raw_table.get("columns")
    try:
        df = client.raw.rows.retrieve_dataframe(
            db_name=db,
            table_name=table,
            limit=None,
            columns=columns,
        )
    except Exception as e:
        return [], [f"Failed to read RAW {db}/{table}: {e}"]

    if df is None or len(df) == 0:
        return [], []

    return parse_dataframe_to_rows(
        df, raw_table, rule_default_source_match, case_insensitive=case_insensitive
    )
