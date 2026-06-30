"""CDF RAW operations for scoped postings index."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from inverted_index.config import (
    PARTITION_STRATEGY_TERM_FIRST_CHAR,
    PARTITION_STRATEGY_UNIFIED,
    RAW_POSTINGS_POLICY,
)
from inverted_index.storage.raw_keys import (
    all_term_bucket_slugs,
    build_raw_postings_row_key,
    list_scope_partition_tables,
    merge_postings,
    posting_from_index_entry,
    resolve_raw_partition_table,
    term_bucket,
)

logger = logging.getLogger(__name__)


class RawSchemaEnsurer:
    """Cache RAW database/table existence checks to avoid repeated catalog listings."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._databases: set[str] = set()
        self._tables: set[tuple[str, str]] = set()

    def ensure_table(self, raw_db: str, table: str) -> None:
        if not self._client:
            return
        if raw_db not in self._databases:
            try:
                if raw_db not in self._client.raw.databases.list(limit=-1).as_names():
                    self._client.raw.databases.create(raw_db)
            except Exception as exc:
                logger.debug("RAW database create skipped: %s", exc)
            self._databases.add(raw_db)
        table_key = (raw_db, table)
        if table_key in self._tables:
            return
        try:
            if table not in self._client.raw.tables.list(raw_db, limit=-1).as_names():
                self._client.raw.tables.create(raw_db, table)
        except Exception as exc:
            logger.debug("RAW table create skipped: %s", exc)
        self._tables.add(table_key)


def create_table_if_not_exists(
    client: Any,
    raw_db: str,
    table: str,
    *,
    ensurer: RawSchemaEnsurer | None = None,
) -> None:
    if not client:
        return
    if ensurer is not None:
        ensurer.ensure_table(raw_db, table)
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


def _parse_overflow_keys(columns: dict[str, Any]) -> list[str]:
    raw_overflow = columns.get("OVERFLOW_KEYS")
    if isinstance(raw_overflow, str) and raw_overflow.strip():
        try:
            parsed = json.loads(raw_overflow)
            if isinstance(parsed, list):
                return [str(k) for k in parsed if k]
        except json.JSONDecodeError:
            return []
    if isinstance(raw_overflow, list):
        return [str(k) for k in raw_overflow if k]
    return []


def strip_reference_postings_in_scope(
    client: Any | None,
    storage_config: dict,
    *,
    match_scope_key: str,
    reference_external_id: str,
    reference_space: str,
    source_types: list[str],
    local_cache: dict[str, dict[str, dict]] | None = None,
    local_registry: dict[str, dict] | None = None,
) -> dict[str, int]:
    """Remove postings for a reference within one scope partition (all term buckets)."""
    from inverted_index.storage.raw_keys import (
        list_scope_partition_tables,
        posting_matches_reference,
    )

    raw_db = storage_config.get("raw", {}).get("database")
    strategy = get_scope_partition_strategy(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    tables = list_scope_partition_tables(
        match_scope_key,
        storage_config,
        partition_strategy=strategy,
    )
    rows_scanned = 0
    rows_updated = 0
    postings_removed = 0

    for table in tables:
        keys_to_delete: list[str] = []
        rows_to_upsert: list[dict[str, Any]] = []

        for row_key, columns in _iter_all_raw_rows(
            client, raw_db, table, local_cache=local_cache
        ):
            if "::__overflow_" in row_key:
                continue
            rows_scanned += 1
            term = _normalized_term_from_lookup_key(row_key, match_scope_key) or ""
            if not term:
                continue

            existing_postings, _cols = load_postings_row(
                client, raw_db, table, row_key, local_cache=local_cache
            )
            old_overflow = _parse_overflow_keys(columns)
            before = len(existing_postings)
            kept = [
                posting
                for posting in existing_postings
                if not posting_matches_reference(
                    posting,
                    reference_external_id=reference_external_id,
                    reference_space=reference_space,
                    source_types=source_types,
                )
            ]
            removed = before - len(kept)
            if removed <= 0:
                continue
            postings_removed += removed

            if not kept:
                keys_to_delete.append(row_key)
                keys_to_delete.extend(old_overflow)
            else:
                primary_columns, overflow_rows = build_row_columns(
                    row_key=row_key,
                    match_scope_key=match_scope_key,
                    normalized_term=term,
                    postings=kept,
                )
                rows_to_upsert.append({"key": row_key, "columns": primary_columns})
                new_overflow_keys = {k for k, _ in overflow_rows}
                for spill_key, spill_cols in overflow_rows:
                    rows_to_upsert.append({"key": spill_key, "columns": spill_cols})
                for old_key in old_overflow:
                    if old_key not in new_overflow_keys:
                        keys_to_delete.append(old_key)
                rows_updated += 1

        if keys_to_delete:
            if client is not None:
                try:
                    client.raw.rows.delete(raw_db, table, keys_to_delete)
                except Exception as exc:
                    logger.warning(
                        "RAW remove_postings delete failed for %s: %s", table, exc
                    )
            elif local_cache is not None:
                partition = local_cache.get(table, {})
                for key in keys_to_delete:
                    partition.pop(key, None)
        if rows_to_upsert:
            upsert_postings_rows(
                client, raw_db, table, rows_to_upsert, local_cache=local_cache
            )

    return {
        "rows_scanned": rows_scanned,
        "rows_updated": rows_updated,
        "postings_removed": postings_removed,
    }


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


def _bucket_for_row_key(
    row_key: str,
    match_scope_key: str,
    *,
    bucket_mode: str,
) -> str | None:
    key = str(row_key or "").strip()
    if not key:
        return None
    if "::__overflow_" in key:
        primary_key = key.rsplit("::__overflow_", 1)[0]
        term = _normalized_term_from_lookup_key(primary_key, match_scope_key)
    else:
        term = _normalized_term_from_lookup_key(key, match_scope_key)
    if not term:
        return None
    return term_bucket(term, bucket_mode=bucket_mode)


def get_scope_registry_entry(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    *,
    local_registry: dict[str, dict] | None = None,
) -> dict[str, Any]:
    if local_registry is not None:
        return dict(local_registry.get(match_scope_key) or {})

    raw_cfg = storage_config.get("raw", {})
    raw_db = raw_cfg.get("database")
    registry_table = raw_cfg.get("registry_table", "inverted_index__registry")
    if client is None or not raw_db:
        return {}

    try:
        row = client.raw.rows.retrieve(raw_db, registry_table, match_scope_key)
        return _row_columns(row)
    except Exception:
        return {}


def get_scope_partition_strategy(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    *,
    local_registry: dict[str, dict] | None = None,
) -> str:
    entry = get_scope_registry_entry(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    strategy = str(entry.get("PARTITION_STRATEGY") or PARTITION_STRATEGY_UNIFIED).strip()
    if strategy not in (PARTITION_STRATEGY_UNIFIED, PARTITION_STRATEGY_TERM_FIRST_CHAR):
        return PARTITION_STRATEGY_UNIFIED
    return strategy


def resolve_scope_partition_table(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    *,
    normalized_term: str | None = None,
    local_registry: dict[str, dict] | None = None,
) -> str:
    strategy = get_scope_partition_strategy(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    return resolve_raw_partition_table(
        match_scope_key,
        storage_config,
        normalized_term=normalized_term,
        partition_strategy=strategy,
    )


def count_partition_table_rows(
    client: Any | None,
    raw_db: str,
    table: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> int:
    if local_cache is not None:
        return len(local_cache.get(table) or {})
    if client is None or not raw_db:
        return 0
    total = 0
    cursor: str | None = None
    while True:
        result = client.raw.rows.list(
            db_name=raw_db,
            table_name=table,
            limit=1000,
            cursor=cursor,
        )
        total += len(list(result))
        if not result.has_next:
            break
        cursor = result.cursor
    return total


def check_partition_row_counts(
    client: Any | None,
    storage_config: dict,
    *,
    local_registry: dict[str, dict] | None = None,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> dict[str, Any]:
    """Admin health check: row counts per scope; recommend reshard when over threshold."""
    term_cfg = storage_config.get("term_partition") or {}
    enabled = bool(term_cfg.get("enabled", False))
    threshold = int(term_cfg.get("activate_above_rows", 400_000))
    raw_db = storage_config.get("raw", {}).get("database")
    scopes = list_registered_scope_keys(
        client, storage_config, local_registry=local_registry
    )
    scope_reports: list[dict[str, Any]] = []
    reshard_recommended: list[str] = []

    for scope in scopes:
        strategy = get_scope_partition_strategy(
            client, storage_config, scope, local_registry=local_registry
        )
        if strategy == PARTITION_STRATEGY_TERM_FIRST_CHAR:
            tables = list_scope_partition_tables(
                scope, storage_config, partition_strategy=strategy
            )
            bucket_counts: dict[str, int] = {}
            total = 0
            for table in tables:
                count = count_partition_table_rows(
                    client, raw_db, table, local_cache=local_cache
                )
                if count:
                    bucket_counts[table] = count
                    total += count
            scope_reports.append(
                {
                    "match_scope_key": scope,
                    "partition_strategy": strategy,
                    "row_count": total,
                    "bucket_tables_with_data": len(bucket_counts),
                }
            )
            continue

        table = resolve_raw_partition_table(scope, storage_config)
        row_count = count_partition_table_rows(
            client, raw_db, table, local_cache=local_cache
        )
        report: dict[str, Any] = {
            "match_scope_key": scope,
            "partition_strategy": strategy,
            "partition_table": table,
            "row_count": row_count,
        }
        if enabled and row_count >= threshold:
            report["reshard_recommended"] = True
            reshard_recommended.append(scope)
            logger.warning(
                "term_partition_threshold_warning match_scope_key=%s row_count=%s threshold=%s",
                scope,
                row_count,
                threshold,
            )
        scope_reports.append(report)

    return {
        "term_partition_enabled": enabled,
        "activate_above_rows": threshold,
        "scopes": scope_reports,
        "reshard_recommended": reshard_recommended,
    }


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
    local_registry: dict[str, dict] | None = None,
) -> Iterator[str]:
    """Yield normalized terms present in one scope partition (admin scan)."""
    raw_db = storage_config.get("raw", {}).get("database")
    strategy = get_scope_partition_strategy(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    tables = list_scope_partition_tables(
        match_scope_key,
        storage_config,
        partition_strategy=strategy,
    )

    seen: set[str] = set()
    for table in tables:
        if local_cache is not None:
            for row_key in sorted((local_cache.get(table) or {}).keys()):
                term = _normalized_term_from_lookup_key(row_key, match_scope_key)
                if term and term not in seen:
                    seen.add(term)
                    yield term
            continue

        if client is None or not raw_db:
            continue

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
                if term and term not in seen:
                    seen.add(term)
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
    ensurer: RawSchemaEnsurer | None = None,
    partition_strategy: str | None = None,
    extra_columns: dict[str, Any] | None = None,
) -> None:
    raw_cfg = storage_config.get("raw", {})
    raw_db = raw_cfg.get("database")
    registry_table = raw_cfg.get("registry_table", "inverted_index__registry")
    now = datetime.now(timezone.utc).isoformat()
    existing = get_scope_registry_entry(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    columns = {
        "RECORD_KIND": "partition_registry",
        "MATCH_SCOPE_KEY": match_scope_key,
        "PARTITION_TABLE": partition_table,
        "PARTITION_STRATEGY": partition_strategy
        or existing.get("PARTITION_STRATEGY")
        or PARTITION_STRATEGY_UNIFIED,
        "CREATED_AT": existing.get("CREATED_AT") or now,
        "LAST_BUILD_AT": now,
    }
    if extra_columns:
        columns.update(extra_columns)
    if client is not None:
        create_table_if_not_exists(client, raw_db, registry_table, ensurer=ensurer)
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
    skip_partition_setup: bool = False,
    ensurer: RawSchemaEnsurer | None = None,
) -> bool:
    raw_db = storage_config.get("raw", {}).get("database")
    strategy = get_scope_partition_strategy(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    table = resolve_raw_partition_table(
        match_scope_key,
        storage_config,
        normalized_term=normalized_term,
        partition_strategy=strategy,
    )
    row_key = build_raw_postings_row_key(match_scope_key, normalized_term)

    if client is not None and not skip_partition_setup:
        create_table_if_not_exists(client, raw_db, table, ensurer=ensurer)
        upsert_partition_registry(
            client,
            storage_config,
            match_scope_key,
            resolve_raw_partition_table(match_scope_key, storage_config),
            local_registry=local_registry,
            ensurer=ensurer,
            partition_strategy=strategy,
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


def _iter_all_raw_rows(
    client: Any | None,
    raw_db: str,
    table: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
) -> Iterator[tuple[str, dict[str, Any]]]:
    if local_cache is not None:
        for key, cols in sorted((local_cache.get(table) or {}).items()):
            yield key, dict(cols)
        return
    if client is None:
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
            key = _row_key_from_raw_row(row)
            if key:
                yield key, _row_columns(row)
        if not result.has_next:
            break
        cursor = result.cursor


def reshard_scope_partition(
    client: Any | None,
    storage_config: dict,
    match_scope_key: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
    local_registry: dict[str, dict] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Copy unified partition rows into term-bucket tables; truncate unified on success.

    Physical relocate only — row keys and POSTINGS_JSON are unchanged.
    """
    term_cfg = storage_config.get("term_partition") or {}
    if not term_cfg.get("enabled", False):
        raise ValueError("term_partition.enabled must be true to reshard a scope")

    strategy = get_scope_partition_strategy(
        client,
        storage_config,
        match_scope_key,
        local_registry=local_registry,
    )
    if strategy == PARTITION_STRATEGY_TERM_FIRST_CHAR:
        return {
            "match_scope_key": match_scope_key,
            "skipped": True,
            "reason": "already_term_first_char",
        }

    raw_db = storage_config.get("raw", {}).get("database")
    unified_table = resolve_raw_partition_table(match_scope_key, storage_config)
    bucket_mode = str(term_cfg.get("bucket_mode") or "script_aware")

    if not dry_run:
        upsert_partition_registry(
            client,
            storage_config,
            match_scope_key,
            unified_table,
            local_registry=local_registry,
            partition_strategy=PARTITION_STRATEGY_UNIFIED,
            extra_columns={"RESHARD_IN_PROGRESS": "true"},
        )

    rows_by_bucket: dict[str, list[dict[str, Any]]] = {}
    source_row_count = 0
    for row_key, columns in _iter_all_raw_rows(
        client, raw_db, unified_table, local_cache=local_cache
    ):
        source_row_count += 1
        bucket_slug = _bucket_for_row_key(
            row_key, match_scope_key, bucket_mode=bucket_mode
        )
        if not bucket_slug:
            logger.warning("Skipping row without bucket during reshard: %s", row_key)
            continue
        target_table = resolve_raw_partition_table(
            match_scope_key,
            storage_config,
            term_bucket_slug=bucket_slug,
            partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
        )
        rows_by_bucket.setdefault(target_table, []).append(
            {"key": row_key, "columns": dict(columns)}
        )

    activated_buckets: list[str] = []
    copied_row_count = 0
    ensurer = RawSchemaEnsurer(client) if client is not None else None

    for target_table, row_batch in rows_by_bucket.items():
        if dry_run:
            copied_row_count += len(row_batch)
            continue
        create_table_if_not_exists(client, raw_db, target_table, ensurer=ensurer)
        upsert_postings_rows(
            client, raw_db, target_table, row_batch, local_cache=local_cache
        )
        copied_row_count += len(row_batch)
        for bucket in all_term_bucket_slugs(bucket_mode=bucket_mode):
            if resolve_raw_partition_table(
                match_scope_key,
                storage_config,
                term_bucket_slug=bucket,
                partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
            ) == target_table:
                activated_buckets.append(bucket)
                break

    if source_row_count != copied_row_count:
        if not dry_run:
            upsert_partition_registry(
                client,
                storage_config,
                match_scope_key,
                unified_table,
                local_registry=local_registry,
                partition_strategy=PARTITION_STRATEGY_UNIFIED,
                extra_columns={"RESHARD_IN_PROGRESS": "false"},
            )
        raise RuntimeError(
            f"Reshard verify failed for {match_scope_key}: "
            f"source_rows={source_row_count} copied_rows={copied_row_count}"
        )

    if dry_run:
        return {
            "match_scope_key": match_scope_key,
            "dry_run": True,
            "source_row_count": source_row_count,
            "copied_row_count": copied_row_count,
            "bucket_tables": len(rows_by_bucket),
        }

    if client is not None:
        try:
            client.raw.tables.delete(raw_db, [unified_table])
            create_table_if_not_exists(client, raw_db, unified_table, ensurer=ensurer)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to truncate unified table {unified_table} after reshard"
            ) from exc
    elif local_cache is not None:
        local_cache.pop(unified_table, None)

    activated_buckets = sorted(set(activated_buckets))
    upsert_partition_registry(
        client,
        storage_config,
        match_scope_key,
        unified_table,
        local_registry=local_registry,
        partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
        extra_columns={
            "RESHARD_IN_PROGRESS": "false",
            "ROW_COUNT_ESTIMATE": str(source_row_count),
            "ACTIVATED_BUCKETS": json.dumps(activated_buckets),
        },
    )

    return {
        "match_scope_key": match_scope_key,
        "partition_strategy": PARTITION_STRATEGY_TERM_FIRST_CHAR,
        "source_row_count": source_row_count,
        "copied_row_count": copied_row_count,
        "activated_buckets": activated_buckets,
        "unified_table_truncated": unified_table,
    }
