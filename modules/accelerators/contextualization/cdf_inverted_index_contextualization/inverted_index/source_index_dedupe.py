"""Source metadata index deduplication via RAW state rows."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from inverted_index.config import SOURCE_INDEX_CONFIG

logger = logging.getLogger(__name__)


def source_content_hash(view_props: dict[str, Any], watch_properties: list[str]) -> str:
    payload: dict[str, Any] = {}
    for path in watch_properties:
        top = path.split(".")[0]
        if top in view_props:
            payload[path] = view_props[top]
        elif path in view_props:
            payload[path] = view_props[path]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def dedupe_key(instance_space: str, instance_external_id: str, content_hash: str) -> str:
    return f"{instance_space}:{instance_external_id}:{content_hash}"


def should_skip_source_index(
    client: Any,
    instance_space: str,
    instance_external_id: str,
    content_hash: str,
    *,
    cfg: dict | None = None,
    force: bool = False,
) -> bool:
    """Return True when the same content hash was indexed within cooldown."""
    if force or not client:
        return False
    resolved = cfg or SOURCE_INDEX_CONFIG.get("dedupe") or {}
    if not resolved.get("enabled", True):
        return False
    key = dedupe_key(instance_space, instance_external_id, content_hash)
    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "source_index_state")
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


def record_source_index_run(
    client: Any,
    instance_space: str,
    instance_external_id: str,
    content_hash: str,
    summary: dict,
    *,
    cfg: dict | None = None,
) -> None:
    if not client:
        return
    from inverted_index.raw_ops import create_table_if_not_exists

    resolved = cfg or SOURCE_INDEX_CONFIG.get("dedupe") or {}
    raw_db = resolved.get("raw_database", "db_contextualization_idx")
    table = resolved.get("state_table", "source_index_state")
    key = dedupe_key(instance_space, instance_external_id, content_hash)
    create_table_if_not_exists(client, raw_db, table)
    now = datetime.now(timezone.utc).isoformat()
    client.raw.rows.insert(
        db_name=raw_db,
        table_name=table,
        row={
            key: {
                "RECORD_KIND": "source_index_state",
                "LAST_RUN_AT": now,
                "CONTENT_HASH": content_hash,
                "CANDIDATE_ENTRIES": int(summary.get("candidate_entries") or 0),
                "ENTRIES_CREATED": int(summary.get("entries_created") or 0),
            }
        },
    )
