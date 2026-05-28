"""Content-hash skip helpers for incremental ETL (Phase 2)."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def row_content_hash(properties: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(properties), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def should_skip_unchanged(
    *,
    content_hash: str,
    previous_hash: str | None,
    incremental_skip_unchanged: bool,
) -> bool:
    if not incremental_skip_unchanged:
        return False
    if previous_hash is None:
        return False
    return content_hash == previous_hash
