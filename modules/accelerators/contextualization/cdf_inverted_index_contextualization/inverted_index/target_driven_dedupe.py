"""Target-driven run deduplication via RAW state rows."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from inverted_index.config import TARGET_DRIVEN_DEDUPE_CONFIG

logger = logging.getLogger(__name__)


class TargetDrivenDedupeBuffer:
    """Buffer dedupe state rows and flush in multi-key RAW inserts."""

    def __init__(self, *, chunk_size: int = 500) -> None:
        self._chunk_size = max(1, int(chunk_size))
        self._records: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def append(
        self,
        instance_space: str,
        instance_external_id: str,
        query_terms: list[str],
        scope_key: str,
        summary: dict,
    ) -> None:
        with self._lock:
            self._records.append(
                {
                    "instance_space": instance_space,
                    "instance_external_id": instance_external_id,
                    "query_terms": list(query_terms),
                    "scope_key": scope_key,
                    "summary": summary,
                }
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def should_flush(self) -> bool:
        with self._lock:
            return len(self._records) >= self._chunk_size

    def flush(
        self,
        client: Any,
        *,
        cfg: dict | None = None,
        force: bool = False,
    ) -> int:
        with self._lock:
            if not self._records:
                return 0
            records = list(self._records)
            if not force and len(records) < self._chunk_size:
                return 0
            self._records.clear()
        return flush_target_driven_dedupe_records(client, records, cfg=cfg)


def terms_hash(query_terms: list[str]) -> str:
    payload = json.dumps(sorted(query_terms), separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def dedupe_key(
    instance_space: str,
    instance_external_id: str,
    query_terms: list[str],
    scope_key: str,
) -> str:
    return f"{instance_space}:{instance_external_id}:{terms_hash(query_terms)}:{scope_key}"


def should_skip_target_driven(
    client: Any,
    instance_space: str,
    instance_external_id: str,
    query_terms: list[str],
    scope_key: str,
    *,
    cfg: dict | None = None,
    force: bool = False,
) -> bool:
    """Return True when the same query terms ran within cooldown."""
    if force or not client:
        return False
    resolved = cfg or TARGET_DRIVEN_DEDUPE_CONFIG
    if not resolved.get("enabled", True):
        return False
    key = dedupe_key(instance_space, instance_external_id, query_terms, scope_key)
    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "target_driven_state")
    cooldown = int(resolved.get("cooldown_seconds", 300))
    try:
        row = client.raw.rows.retrieve(raw_db, table, key)
        cols = getattr(row, "columns", None) or {}
        last_run = cols.get("LAST_RUN_AT")
        if not last_run:
            return False
        last_dt = datetime.fromisoformat(str(last_run).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - last_dt).total_seconds()
        return age < cooldown
    except Exception:
        return False


def record_target_driven_run(
    client: Any,
    instance_space: str,
    instance_external_id: str,
    query_terms: list[str],
    scope_key: str,
    summary: dict,
    *,
    cfg: dict | None = None,
) -> None:
    if not client:
        return
    flush_target_driven_dedupe_records(
        client,
        [
            {
                "instance_space": instance_space,
                "instance_external_id": instance_external_id,
                "query_terms": query_terms,
                "scope_key": scope_key,
                "summary": summary,
            }
        ],
        cfg=cfg,
    )


def flush_target_driven_dedupe_records(
    client: Any,
    records: list[dict[str, Any]],
    *,
    cfg: dict | None = None,
) -> int:
    if not client or not records:
        return 0
    from inverted_index.raw_ops import create_table_if_not_exists

    resolved = cfg or TARGET_DRIVEN_DEDUPE_CONFIG
    if not resolved.get("enabled", True):
        return 0

    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "target_driven_state")
    create_table_if_not_exists(client, raw_db, table)
    now = datetime.now(timezone.utc).isoformat()
    row: dict[str, dict[str, Any]] = {}
    for record in records:
        instance_space = str(record["instance_space"])
        instance_external_id = str(record["instance_external_id"])
        query_terms = list(record.get("query_terms") or [])
        scope_key = str(record.get("scope_key") or "")
        summary = record.get("summary") or {}
        key = dedupe_key(instance_space, instance_external_id, query_terms, scope_key)
        row[key] = {
            "RECORD_KIND": "target_driven_state",
            "LAST_RUN_AT": now,
            "TERMS_HASH": terms_hash(query_terms),
            "ALIASES_HASH": terms_hash(query_terms),
            "LINKS_CREATED": int(summary.get("links_created") or 0),
            "REFERENCES_FOUND": int(summary.get("references_found") or 0),
        }
    client.raw.rows.insert(
        db_name=raw_db,
        table_name=table,
        row=row,
    )
    return len(records)
