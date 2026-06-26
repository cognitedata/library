"""In-memory storage adapter for local prototype runs."""

from __future__ import annotations

from typing import Any

from inverted_index.normalize import normalize_query_terms


class MemoryStorageAdapter:
    """Simple dict-backed index for unit tests and offline demos."""

    def __init__(self, storage_config: dict | None = None) -> None:
        self._config = storage_config or {}
        self._entries: dict[str, dict[str, Any]] = {}

    def upsert_index_entries(self, entries: list[dict], *, dry_run: bool = False) -> dict:
        created = 0
        updated = 0
        if dry_run:
            return {
                "entries_created": len(entries),
                "entries_updated": 0,
                "dry_run": True,
            }
        for entry in entries:
            ext_id = entry.get("external_id") or entry.get("reference_external_id")
            if ext_id in self._entries:
                updated += 1
            else:
                created += 1
            self._entries[ext_id] = dict(entry)
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
        normalized_terms = normalize_query_terms(normalized_terms)
        if not normalized_terms:
            return []

        scope_set: set[str] | None = None
        if match_scope_keys:
            scope_set = {s for s in match_scope_keys if s}
        elif match_scope_key:
            scope_set = {match_scope_key}

        results: list[dict] = []
        term_set = set(normalized_terms)
        for entry in self._entries.values():
            if entry.get("normalized_term") not in term_set:
                continue
            entry_scope = entry.get("match_scope_key")
            if scope_set is not None and entry_scope not in scope_set:
                continue
            if source_types and entry.get("source_type") not in source_types:
                continue
            conf = (entry.get("additional_metadata") or {}).get("confidence")
            if conf is not None and float(conf) < min_confidence:
                continue
            results.append(dict(entry))
            if len(results) >= limit:
                break
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
        """List entries where reference is CogniteFile (with legacy metadata fallback)."""
        results: list[dict] = []
        for entry in self._entries.values():
            if match_scope_key and entry.get("match_scope_key") != match_scope_key:
                continue
            if source_types and entry.get("source_type") not in source_types:
                continue
            if (
                entry.get("reference_type") == "CogniteFile"
                and entry.get("reference_external_id") == file_external_id
                and (not file_space or entry.get("reference_space") == file_space)
            ):
                results.append(dict(entry))
                continue
            meta = entry.get("additional_metadata") or {}
            file_ref = meta.get("file_external_id") or meta.get("linked_file_extid")
            if file_ref == file_external_id:
                results.append(dict(entry))
        return results[:limit]

    def delete_subset(
        self,
        *,
        source_type: str | None = None,
        build_job_id: str | None = None,
        match_scope_key: str | None = None,
    ) -> int:
        to_delete = []
        for ext_id, entry in self._entries.items():
            if source_type and entry.get("source_type") != source_type:
                continue
            if build_job_id and entry.get("build_job_id") != build_job_id:
                continue
            if match_scope_key and entry.get("match_scope_key") != match_scope_key:
                continue
            to_delete.append(ext_id)
        for ext_id in to_delete:
            del self._entries[ext_id]
        return len(to_delete)

    @property
    def entries(self) -> dict[str, dict]:
        return self._entries
