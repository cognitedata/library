"""RAW scoped postings storage backend."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from inverted_index.cancellation import raise_if_cancelled
from inverted_index.config import RAW_SCOPE_POLICY
from inverted_index.normalize import normalize_query_terms
from inverted_index.raw_ops import (
    RawSchemaEnsurer,
    create_table_if_not_exists,
    get_scope_partition_strategy,
    list_registered_scope_keys,
    load_postings_row,
    merge_and_upsert_lookup_key,
    parse_postings_json,
    resolve_scope_partition_table,
    upsert_partition_registry,
)
from inverted_index.storage.raw_keys import (
    build_raw_postings_row_key,
    flatten_postings_to_entries,
    list_scope_partition_tables,
    merge_postings,
    resolve_raw_partition_table,
)

logger = logging.getLogger(__name__)


def validate_raw_scope_config(scope_config: dict) -> None:
    if not scope_config.get("enabled", False):
        return
    levels = scope_config.get("levels") or []
    if not levels:
        return
    min_levels = RAW_SCOPE_POLICY.get("min_levels", ["site", "unit"])
    for level in min_levels:
        if level not in levels:
            raise ValueError(
                f"RAW backend requires scope levels {min_levels}; got {levels}"
            )


class RawStorageAdapter:
    """RAW postings backend — scoped partition tables + registry."""

    def __init__(self, storage_config: dict, client: Any = None) -> None:
        self._config = storage_config
        self._client = client
        self._local_partitions: dict[str, dict[str, dict]] = {}
        self._local_registry: dict[str, dict] = {}

    @property
    def raw_database(self) -> str:
        return self._config.get("raw", {}).get("database", "db_contextualization_idx")

    def _partition_strategy(self, match_scope_key: str) -> str:
        return get_scope_partition_strategy(
            self._client,
            self._config,
            match_scope_key,
            local_registry=self._local_registry if self._client is None else None,
        )

    def upsert_index_entries(
        self,
        entries: list[dict],
        *,
        dry_run: bool = False,
        on_progress: Callable[[str], None] | None = None,
        progress_interval: int = 100,
        should_cancel: Callable[[], bool] | None = None,
        log_prefix: str = "index-upsert",
        **kwargs: Any,
    ) -> dict:
        if dry_run:
            return {"entries_created": len(entries), "entries_updated": 0, "dry_run": True}
        if not entries:
            return {"entries_created": 0, "entries_updated": 0}

        by_key: dict[str, list[dict]] = {}
        for entry in entries:
            scope = entry.get("match_scope_key", "")
            term = entry.get("normalized_term", "")
            if not scope or not term:
                logger.warning("Skipping index entry without scope/term")
                continue
            row_key = build_raw_postings_row_key(scope, term)
            by_key.setdefault(row_key, []).append(entry)

        created = 0
        updated = 0
        build_job_id = entries[0].get("build_job_id") if entries else None
        local_cache = self._local_partitions if self._client is None else None
        local_registry = self._local_registry if self._client is None else None
        emit = on_progress if progress_interval > 0 else None
        started_at = time.monotonic()
        total_keys = len(by_key)

        ensurer = RawSchemaEnsurer(self._client) if self._client is not None else None
        tables_to_ensure: set[str] = set()
        scopes_seen: set[str] = set()
        for row_key in by_key:
            scope, term = row_key.rsplit("::", 1)
            scopes_seen.add(scope)
            table = resolve_scope_partition_table(
                self._client,
                self._config,
                scope,
                normalized_term=term,
                local_registry=local_registry,
            )
            tables_to_ensure.add(table)

        if self._client is not None:
            for table in tables_to_ensure:
                create_table_if_not_exists(
                    self._client, self.raw_database, table, ensurer=ensurer
                )

        for scope in scopes_seen:
            strategy = self._partition_strategy(scope)
            registry_table = resolve_raw_partition_table(scope, self._config)
            if self._client is not None:
                upsert_partition_registry(
                    self._client,
                    self._config,
                    scope,
                    registry_table,
                    local_registry=local_registry,
                    ensurer=ensurer,
                    partition_strategy=strategy,
                )
            elif local_registry is not None:
                upsert_partition_registry(
                    None,
                    self._config,
                    scope,
                    registry_table,
                    local_registry=local_registry,
                    partition_strategy=strategy,
                )

        for lookup_keys_done, (row_key, group) in enumerate(by_key.items(), start=1):
            raise_if_cancelled(should_cancel)
            scope, term = row_key.rsplit("::", 1)
            is_new = merge_and_upsert_lookup_key(
                self._client,
                self._config,
                scope,
                term,
                group,
                build_job_id=build_job_id,
                local_cache=local_cache,
                local_registry=local_registry,
                skip_partition_setup=True,
                ensurer=ensurer,
            )
            if is_new:
                created += 1
            else:
                updated += 1
            if emit and lookup_keys_done % progress_interval == 0:
                emit(
                    f"[{log_prefix}] upserting lookup_keys={lookup_keys_done}/{total_keys} "
                    f"created={created} updated={updated} "
                    f"elapsed={time.monotonic() - started_at:.1f}s"
                )

        return {"entries_created": created, "entries_updated": updated}

    def query_by_terms(
        self,
        normalized_terms: list[str],
        *,
        match_scope_key: str | None = None,
        match_scope_keys: list[str] | None = None,
        source_types: list[str] | None = None,
        min_confidence: float = 0.0,
        limit: int = 5000,
    ) -> list[dict]:
        scopes = match_scope_keys
        if scopes is None and match_scope_key:
            scopes = [match_scope_key]
        if not scopes:
            return []

        normalized_terms = normalize_query_terms(normalized_terms)
        if not normalized_terms:
            return []

        raw_db = self.raw_database
        local_cache = self._local_partitions if self._client is None else None
        local_registry = self._local_registry if self._client is None else None
        results: list[dict] = []

        for scope in scopes:
            for term in normalized_terms:
                table = resolve_scope_partition_table(
                    self._client,
                    self._config,
                    scope,
                    normalized_term=term,
                    local_registry=local_registry,
                )
                row_key = build_raw_postings_row_key(scope, term)
                postings, _cols = load_postings_row(
                    self._client,
                    raw_db,
                    table,
                    row_key,
                    local_cache=local_cache,
                )
                entries = flatten_postings_to_entries(
                    postings,
                    match_scope_key=scope,
                    normalized_term=term,
                )
                for entry in entries:
                    if source_types and entry.get("source_type") not in source_types:
                        continue
                    conf = (entry.get("additional_metadata") or {}).get("confidence")
                    if conf is not None and float(conf) < min_confidence:
                        continue
                    results.append(entry)
                    if len(results) >= limit:
                        return results
        return results

    def list_by_file(
        self,
        file_external_id: str,
        *,
        source_types: list[str] | None = None,
        file_space: str = "cdf_cdm",
        match_scope_key: str | None = None,
        limit: int = 5000,
    ) -> list[dict]:
        if self._client is None:
            return self._list_by_file_from_cache(
                file_external_id,
                source_types,
                file_space=file_space,
                match_scope_key=match_scope_key,
                limit=limit,
            )

        scopes = [match_scope_key] if match_scope_key else list_registered_scope_keys(
            self._client, self._config, local_registry=self._local_registry
        )
        if not scopes:
            return []

        raw_db = self.raw_database
        results: list[dict] = []
        for scope in scopes:
            strategy = self._partition_strategy(scope)
            tables = list_scope_partition_tables(
                scope, self._config, partition_strategy=strategy
            )
            for table in tables:
                partition: dict[str, dict] = {}
                try:
                    result = self._client.raw.rows.list(
                        db_name=raw_db,
                        table_name=table,
                        limit=10_000,
                    )
                    partition = {
                        getattr(row, "key", row.get("key")): dict(
                            getattr(row, "columns", row.get("columns", {}))
                        )
                        for row in result
                    }
                except Exception as exc:
                    logger.warning("RAW list_by_file scan failed for %s: %s", table, exc)
                    partition = {}

                for cols in partition.values():
                    postings = parse_postings_json(cols.get("POSTINGS_JSON"))
                    for posting in postings:
                        if self._posting_matches_file(
                            posting,
                            file_external_id,
                            file_space=file_space,
                            source_types=source_types,
                        ):
                            entry = dict(posting)
                            entry.setdefault("match_scope_key", scope)
                            results.append(entry)
                            if len(results) >= limit:
                                return merge_postings([], results)[:limit]
        return merge_postings([], results)[:limit]

    @staticmethod
    def _posting_matches_file(
        posting: dict,
        file_external_id: str,
        *,
        file_space: str,
        source_types: list[str] | None,
    ) -> bool:
        if source_types and posting.get("source_type") not in source_types:
            return False
        if (
            posting.get("reference_type") == "CogniteFile"
            and posting.get("reference_external_id") == file_external_id
            and (not file_space or posting.get("reference_space") == file_space)
        ):
            return True
        meta = posting.get("additional_metadata") or {}
        return meta.get("file_external_id") == file_external_id

    def remove_postings_for_reference(
        self,
        *,
        match_scope_key: str,
        reference_external_id: str,
        reference_space: str,
        source_types: list[str],
    ) -> dict:
        from inverted_index.raw_ops import strip_reference_postings_in_scope

        if not match_scope_key:
            return {"rows_scanned": 0, "rows_updated": 0, "postings_removed": 0}
        return strip_reference_postings_in_scope(
            self._client,
            self._config,
            match_scope_key=match_scope_key,
            reference_external_id=reference_external_id,
            reference_space=reference_space,
            source_types=source_types,
            local_cache=self._local_partitions if self._client is None else None,
            local_registry=self._local_registry if self._client is None else None,
        )

    def _list_by_file_from_cache(
        self,
        file_external_id: str,
        source_types: list[str] | None,
        *,
        file_space: str = "cdf_cdm",
        match_scope_key: str | None = None,
        limit: int = 5000,
    ) -> list[dict]:
        results: list[dict] = []
        for _table, partition in self._local_partitions.items():
            for row_key, cols in partition.items():
                scope = row_key.rsplit("::", 1)[0] if "::" in row_key else ""
                if match_scope_key and scope != match_scope_key:
                    continue
                postings = parse_postings_json(cols.get("POSTINGS_JSON"))
                for posting in postings:
                    if self._posting_matches_file(
                        posting,
                        file_external_id,
                        file_space=file_space,
                        source_types=source_types,
                    ):
                        entry = dict(posting)
                        if scope:
                            entry.setdefault("match_scope_key", scope)
                        results.append(entry)
                        if len(results) >= limit:
                            return merge_postings([], results)[:limit]
        return merge_postings([], results)[:limit]

    def delete_subset(
        self,
        *,
        match_scope_key: str | None = None,
        source_type: str | None = None,
        build_job_id: str | None = None,
        lookup_keys: list[str] | None = None,
    ) -> int:
        raw_db = self.raw_database
        deleted = 0
        local_registry = self._local_registry if self._client is None else None

        if lookup_keys:
            if not match_scope_key:
                return 0
            for key in lookup_keys:
                term = key.rsplit("::", 1)[-1] if "::" in key else ""
                table = resolve_scope_partition_table(
                    self._client,
                    self._config,
                    match_scope_key,
                    normalized_term=term or None,
                    local_registry=local_registry,
                )
                if self._client is not None:
                    try:
                        self._client.raw.rows.delete(raw_db, table, [key])
                        deleted += 1
                    except Exception as exc:
                        logger.warning("RAW delete failed for %s: %s", key, exc)
                elif self._local_partitions.get(table, {}).pop(key, None):
                    deleted += 1
            return deleted

        if match_scope_key and not source_type and not build_job_id:
            strategy = self._partition_strategy(match_scope_key)
            tables = list_scope_partition_tables(
                match_scope_key,
                self._config,
                partition_strategy=strategy,
            )
            if self._client is not None:
                for table in tables:
                    try:
                        self._client.raw.tables.delete(raw_db, [table])
                        create_table_if_not_exists(self._client, raw_db, table)
                        deleted += 1
                    except Exception as exc:
                        logger.warning("Partition truncate failed for %s: %s", table, exc)
            else:
                for table in tables:
                    if self._local_partitions.pop(table, None) is not None:
                        deleted += 1
            return deleted

        raise NotImplementedError(
            "RAW subset delete by source_type/build_job_id requires admin partition scan"
        )

    def ensure_registry(self, match_scope_key: str) -> str:
        strategy = self._partition_strategy(match_scope_key)
        table = resolve_raw_partition_table(match_scope_key, self._config)
        upsert_partition_registry(
            self._client,
            self._config,
            match_scope_key,
            table,
            local_registry=self._local_registry if self._client is None else None,
            partition_strategy=strategy,
        )
        return table
