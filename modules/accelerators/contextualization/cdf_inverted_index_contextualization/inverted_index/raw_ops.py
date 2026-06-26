"""CDF RAW operations for scoped postings index."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from inverted_index.config import RAW_POSTINGS_POLICY
from inverted_index.storage.raw_keys import (
    build_raw_postings_row_key,
    merge_postings,
    posting_from_index_entry,
    resolve_raw_partition_table,
)

logger = logging.getLogger(__name__)


def create_table_if_not_exists(client: Any, raw_db: str, table: str) -> None:
    if not client:
        return
    try:
        if raw_db not in client.raw.databases.list(limit=-1).as_names():
            client.raw.databases.create(raw_db)
    except Exception as exc:
        logger.debug("RAW database create skipped: %s", exc)
    try:
        if table not in client.raw.tables.list(raw_db, limit=-1).as_names():
            client.raw.tables.create(raw_db, table)
    except Exception as exc:
        logger.debug("RAW table create skipped: %s", exc)


def _row_columns(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    columns = getattr(row, "columns", None)
    if isinstance(columns, dict):
        return dict(columns)
    if isinstance(row, dict):
        return dict(row.get("columns") or row)
    return {}


def parse_postings_json(raw_value: Any) -> list[dict]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [dict(x) for x in raw_value if isinstance(x, dict)]
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [dict(x) for x in parsed if isinstance(x, dict)]
        except json.JSONDecodeError:
            return []
    return []


def load_postings_row(
    client: Any | None,
    raw_db: str,
    table: str,
    row_key: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    columns: dict[str, Any] = {}
    if client is not None:
        try:
            row = client.raw.rows.retrieve(raw_db, table, row_key)
            columns = _row_columns(row)
        except Exception:
            columns = {}
    elif local_cache is not None:
        columns = dict(local_cache.get(table, {}).get(row_key, {}))

    postings = parse_postings_json(columns.get("POSTINGS_JSON"))
    overflow_keys: list[Any] = []
    raw_overflow = columns.get("OVERFLOW_KEYS")
    if isinstance(raw_overflow, str) and raw_overflow.strip():
        try:
            overflow_keys = json.loads(raw_overflow)
        except json.JSONDecodeError:
            overflow_keys = []
    elif isinstance(raw_overflow, list):
        overflow_keys = raw_overflow

    spill_sources = []
    if client is not None:
        spill_sources = overflow_keys
    elif local_cache is not None:
        spill_sources = overflow_keys

    for spill_key in spill_sources:
        if not isinstance(spill_key, str):
            continue
        if client is not None:
            try:
                spill_row = client.raw.rows.retrieve(raw_db, table, spill_key)
                spill_cols = _row_columns(spill_row)
                postings.extend(parse_postings_json(spill_cols.get("POSTINGS_JSON")))
            except Exception:
                continue
        elif local_cache is not None:
            spill_cols = local_cache.get(table, {}).get(spill_key, {})
            postings.extend(parse_postings_json(spill_cols.get("POSTINGS_JSON")))

    return postings, columns


def _split_postings_with_overflow(
    postings: list[dict],
    row_key: str,
    policy: dict,
) -> tuple[list[dict], list[str]]:
    max_per_row = int(policy.get("max_postings_per_row", 500))
    if len(postings) <= max_per_row:
        return postings, []
    primary = postings[:max_per_row]
    rest = postings[max_per_row:]
    chunk_size = max_per_row
    overflow_keys = [
        f"{row_key}::__overflow_{idx // chunk_size}"
        for idx in range(0, len(rest), chunk_size)
    ]
    return primary, overflow_keys


def build_row_columns(
    *,
    row_key: str,
    match_scope_key: str,
    normalized_term: str,
    postings: list[dict],
    build_job_id: str | None = None,
    policy: dict | None = None,
) -> tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]:
    pol = policy or RAW_POSTINGS_POLICY
    primary_postings, overflow_keys = _split_postings_with_overflow(
        postings, row_key, pol
    )
    now = datetime.now(timezone.utc).isoformat()
    primary_columns = {
        "RECORD_KIND": "index_postings",
        "LOOKUP_KEY": row_key,
        "NORMALIZED_TERM": normalized_term,
        "MATCH_SCOPE_KEY": match_scope_key,
        "POSTINGS_JSON": json.dumps(primary_postings, default=str),
        "OVERFLOW_KEYS": json.dumps(overflow_keys) if overflow_keys else "",
        "UPDATED_AT": now,
        "BUILD_JOB_ID": build_job_id or "",
    }
    overflow_rows: list[tuple[str, dict[str, Any]]] = []
    if overflow_keys:
        rest = postings[len(primary_postings) :]
        chunk_size = int(pol.get("max_postings_per_row", 500))
        for idx, spill_key in enumerate(overflow_keys):
            chunk = rest[idx * chunk_size : (idx + 1) * chunk_size]
            overflow_rows.append(
                (
                    spill_key,
                    {
                        "RECORD_KIND": "index_postings_overflow",
                        "LOOKUP_KEY": spill_key,
                        "NORMALIZED_TERM": normalized_term,
                        "MATCH_SCOPE_KEY": match_scope_key,
                        "POSTINGS_JSON": json.dumps(chunk, default=str),
                        "UPDATED_AT": now,
                        "BUILD_JOB_ID": build_job_id or "",
                    },
                )
            )
    return primary_columns, overflow_rows


def upsert_postings_rows(
    client: Any | None,
    raw_db: str,
    table: str,
    rows: list[dict[str, Any]],
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> None:
    if not rows:
        return
    if client is not None:
        row_map = {
            row["key"]: row["columns"]
            for row in rows
            if row.get("key") and isinstance(row.get("columns"), dict)
        }
        if row_map:
            client.raw.rows.insert(db_name=raw_db, table_name=table, row=row_map)
    if local_cache is not None:
        partition = local_cache.setdefault(table, {})
        for row in rows:
            key = row.get("key")
            cols = row.get("columns") or {}
            if key:
                partition[key] = dict(cols)


def _row_key_from_raw_row(row: Any) -> str:
    key = getattr(row, "key", None)
    if key is not None and str(key).strip():
        return str(key).strip()
    if isinstance(row, dict):
        raw_key = row.get("key")
        if raw_key is not None and str(raw_key).strip():
            return str(raw_key).strip()
    return ""


def _normalized_term_from_lookup_key(row_key: str, match_scope_key: str) -> str | None:
    """Parse normalized_term from a postings row key; skip overflow spill keys."""
    key = str(row_key or "").strip()
    if not key or "::__overflow_" in key:
        return None
    prefix = f"{match_scope_key}::"
    if not key.startswith(prefix):
        return None
    term = key[len(prefix) :].strip()
    return term or None


def list_registered_scope_keys(
    client: Any | None,
    storage_config: dict,
    *,
    local_registry: dict[str, dict] | None = None,
) -> list[str]:
    """Return sorted distinct match_scope_key values from the partition registry."""
    if local_registry is not None:
        return sorted(local_registry.keys())

    raw_cfg = storage_config.get("raw", {})
    raw_db = raw_cfg.get("database")
    registry_table = raw_cfg.get("registry_table", "inverted_index__registry")
    if client is None or not raw_db:
        return []

    scopes: set[str] = set()
    cursor: str | None = None
    while True:
        result = client.raw.rows.list(
            db_name=raw_db,
            table_name=registry_table,
            limit=1000,
            cursor=cursor,
        )
        for row in result:
            cols = _row_columns(row)
            if cols.get("RECORD_KIND") != "partition_registry":
                continue
            scope = str(cols.get("MATCH_SCOPE_KEY") or "").strip()
            if scope:
                scopes.add(scope)
        if not result.has_next:
            break
        cursor = result.cursor
    return sorted(scopes)


def iter_partition_terms(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> Iterator[str]:
    """Yield normalized terms present in one scope partition (admin scan; O(partition rows))."""
    raw_db = storage_config.get("raw", {}).get("database")
    table = resolve_raw_partition_table(match_scope_key, storage_config)

    if local_cache is not None:
        for row_key in sorted((local_cache.get(table) or {}).keys()):
            term = _normalized_term_from_lookup_key(row_key, match_scope_key)
            if term:
                yield term
        return

    if client is None or not raw_db:
        return

    cursor: str | None = None
    while True:
        result = client.raw.rows.list(
            db_name=raw_db,
            table_name=table,
            limit=1000,
            cursor=cursor,
        )
        for row in result:
            row_key = _row_key_from_raw_row(row)
            term = _normalized_term_from_lookup_key(row_key, match_scope_key)
            if term:
                yield term
        if not result.has_next:
            break
        cursor = result.cursor


def upsert_partition_registry(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    partition_table: str,
    *,
    local_registry: dict[str, dict] | None = None,
) -> None:
    raw_cfg = storage_config.get("raw", {})
    raw_db = raw_cfg.get("database")
    registry_table = raw_cfg.get("registry_table", "inverted_index__registry")
    now = datetime.now(timezone.utc).isoformat()
    columns = {
        "RECORD_KIND": "partition_registry",
        "MATCH_SCOPE_KEY": match_scope_key,
        "PARTITION_TABLE": partition_table,
        "CREATED_AT": now,
        "LAST_BUILD_AT": now,
    }
    if client is not None:
        create_table_if_not_exists(client, raw_db, registry_table)
        client.raw.rows.insert(
            db_name=raw_db,
            table_name=registry_table,
            row={match_scope_key: columns},
        )
    if local_registry is not None:
        local_registry[match_scope_key] = dict(columns)


def merge_and_upsert_lookup_key(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    normalized_term: str,
    new_entries: list[dict],
    *,
    build_job_id: str | None = None,
    local_cache: dict[str, dict[str, dict]] | None = None,
    local_registry: dict[str, dict] | None = None,
) -> bool:
    raw_db = storage_config.get("raw", {}).get("database")
    table = resolve_raw_partition_table(match_scope_key, storage_config)
    row_key = build_raw_postings_row_key(match_scope_key, normalized_term)

    if client is not None:
        create_table_if_not_exists(client, raw_db, table)
        upsert_partition_registry(
            client,
            storage_config,
            match_scope_key,
            table,
            local_registry=local_registry,
        )

    existing_postings, _cols = load_postings_row(
        client, raw_db, table, row_key, local_cache=local_cache
    )
    is_new = not existing_postings and (
        local_cache is None or row_key not in (local_cache.get(table) or {})
    )
    merged = merge_postings(
        existing_postings,
        [posting_from_index_entry(e) for e in new_entries],
    )
    primary_columns, overflow_rows = build_row_columns(
        row_key=row_key,
        match_scope_key=match_scope_key,
        normalized_term=normalized_term,
        postings=merged,
        build_job_id=build_job_id,
    )
    rows = [{"key": row_key, "columns": primary_columns}]
    for spill_key, spill_cols in overflow_rows:
        rows.append({"key": spill_key, "columns": spill_cols})
    upsert_postings_rows(client, raw_db, table, rows, local_cache=local_cache)
    return is_new
