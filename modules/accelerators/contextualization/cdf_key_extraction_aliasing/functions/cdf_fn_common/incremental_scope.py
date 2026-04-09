"""
Incremental key-extraction scope: watermarks, cohort row keys, WORKFLOW_STATUS helpers.

Used by fn_dm_incremental_state_update, fn_dm_key_extraction, fn_dm_aliasing, and
fn_dm_alias_persistence for RAW-backed handoff (single unified raw_table_key).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

# Column names (stable contract)
WORKFLOW_STATUS_COLUMN = "WORKFLOW_STATUS"
WORKFLOW_STATUS_UPDATED_AT_COLUMN = "WORKFLOW_STATUS_UPDATED_AT"
WORKFLOW_STATUS_DETECTED = "detected"
WORKFLOW_STATUS_EXTRACTED = "extracted"
WORKFLOW_STATUS_ALIASED = "aliased"
WORKFLOW_STATUS_PERSISTED = "persisted"
WORKFLOW_STATUS_FAILED = "failed"
# Watermark / checkpoint rows (not cohort)
WORKFLOW_STATUS_CHECKPOINT = "checkpoint"

CHANGE_KIND_COLUMN = "CHANGE_KIND"
CHANGE_KIND_ADD = "add"
CHANGE_KIND_UPDATE = "update"

NODE_INSTANCE_ID_COLUMN = "NODE_INSTANCE_ID"
SCOPE_KEY_COLUMN = "SCOPE_KEY"
RAW_ROW_KEY_COLUMN = "RAW_ROW_KEY"
EXTERNAL_ID_COLUMN = "EXTERNAL_ID"

RECORD_KIND_COLUMN = "RECORD_KIND"
RECORD_KIND_ENTITY = "entity"
RECORD_KIND_WATERMARK = "watermark"
RECORD_KIND_RUN = "run"

HIGH_WATERMARK_MS_COLUMN = "HIGH_WATERMARK_MS"
RUN_ID_COLUMN = "RUN_ID"

# Per-entity SHA-256 of extraction source inputs (see cdf_fn_common.extraction_input_hash).
EXTRACTION_INPUTS_HASH_COLUMN = "EXTRACTION_INPUTS_HASH"


def scope_key_from_view_dict(view: Dict[str, Any]) -> str:
    """Deterministic scope id from view config (filters JSON must be stable-ordered)."""
    canonical = {
        "view_space": view.get("view_space"),
        "view_external_id": view.get("view_external_id"),
        "view_version": view.get("view_version"),
        "instance_space": view.get("instance_space"),
        "filters": view.get("filters") or [],
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def scope_watermark_row_key(scope_key: str) -> str:
    return f"scope_wm_{scope_key}"


def cohort_row_key(
    run_id: str, node_instance_id: str, scope_key: Optional[str] = None
) -> str:
    """RAW row key for a per-run cohort instance row (scope disambiguates multi-view scopes)."""
    if scope_key:
        return f"{run_id}:{scope_key}:{node_instance_id}"
    return f"{run_id}:{node_instance_id}"


def node_instance_id_str(instance: Any) -> str:
    """
    Space-qualified node instance id for RAW keys and correlation.

    Prefer ``space`` + ``instance_id`` (UUID) when present; fall back to externalId-only
    only when instance id is unavailable (should be rare for DM nodes).
    """
    space = getattr(instance, "space", None) or ""
    iid = getattr(instance, "instance_id", None)
    if iid is not None:
        s = str(iid).strip()
        if space:
            return f"{space}:{s}"
        return s
    ext = getattr(instance, "external_id", None)
    if ext is not None:
        return f"{space}:{ext}" if space else str(ext)
    return ""


def node_last_updated_time_ms(instance: Any) -> Optional[int]:
    """Best-effort ms timestamp from DM node lastUpdatedTime."""
    raw = getattr(instance, "last_updated_time", None)
    if raw is None:
        dump = instance.dump() if hasattr(instance, "dump") else {}
        if isinstance(dump, dict):
            raw = dump.get("lastUpdatedTime")
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    try:
        # ISO string
        from datetime import datetime

        if isinstance(raw, str):
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    try:
        return int(raw)
    except Exception:
        return None


def norm_workflow_status(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, list) and val:
        val = val[0]
    return str(val).strip().lower()


def raw_row_columns(row: Any) -> Dict[str, Any]:
    cols = getattr(row, "columns", None) or {}
    return dict(cols) if isinstance(cols, dict) else {}


def iter_raw_table_rows_chunked(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    chunk_size: int = 2500,
) -> Iterator[Any]:
    """Iterate all RAW rows (chunked iterator API when available)."""
    rows_api = client.raw.rows
    if callable(rows_api):
        for item in rows_api(raw_db, raw_table, chunk_size=chunk_size):
            # SDK iterator may yield either single Row objects or per-page lists.
            if hasattr(item, "columns"):
                yield item
                continue
            if isinstance(item, Iterable) and not isinstance(
                item, (str, bytes, dict)
            ):
                for row in item:
                    if hasattr(row, "columns"):
                        yield row
        return
    listed = rows_api.list(raw_db, raw_table, limit=-1)
    for row in listed:
        yield row


def load_prior_node_ids_for_scope(
    client: Any,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    *,
    chunk_size: int = 2500,
) -> Set[str]:
    """
    Instance ids (NODE_INSTANCE_ID column) seen in prior entity rows for this scope.

    Used to classify add vs update. O(table scan) — acceptable for typical state tables;
    optimize later with an index side table if needed.
    """
    seen: Set[str] = set()
    try:
        for row in iter_raw_table_rows_chunked(
            client, raw_db, raw_table, chunk_size=chunk_size
        ):
            cols = raw_row_columns(row)
            if norm_workflow_status(cols.get(RECORD_KIND_COLUMN)) != RECORD_KIND_ENTITY:
                continue
            if str(cols.get(SCOPE_KEY_COLUMN) or "") != scope_key:
                continue
            nid = cols.get(NODE_INSTANCE_ID_COLUMN)
            if nid:
                seen.add(str(nid))
    except Exception:
        return seen
    return seen


def load_latest_hash_by_node_for_scope(
    client: Any,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    *,
    chunk_size: int = 2500,
) -> Dict[str, str]:
    """
    Latest EXTRACTION_INPUTS_HASH per NODE_INSTANCE_ID for this scope.

    Uses entity rows with WORKFLOW_STATUS in extracted / aliased / persisted and a
    non-empty EXTRACTION_INPUTS_HASH. Picks the row with greatest UPDATED_AT /
    WORKFLOW_STATUS_UPDATED_AT (epoch ms); tie-break by RUN_ID lexicographic order.
    """
    completed = {
        WORKFLOW_STATUS_EXTRACTED,
        WORKFLOW_STATUS_ALIASED,
        WORKFLOW_STATUS_PERSISTED,
    }
    best: Dict[str, Tuple[str, float, str]] = {}

    def _ts_ms(cols: Dict[str, Any]) -> float:
        for key in (WORKFLOW_STATUS_UPDATED_AT_COLUMN, "UPDATED_AT"):
            raw = cols.get(key)
            if raw is None:
                continue
            if isinstance(raw, (int, float)):
                return float(raw)
            s = str(raw).strip()
            if not s:
                continue
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
                return dt.timestamp() * 1000.0
            except Exception:
                continue
        return 0.0

    try:
        for row in iter_raw_table_rows_chunked(
            client, raw_db, raw_table, chunk_size=chunk_size
        ):
            cols = raw_row_columns(row)
            if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
                continue
            if str(cols.get(SCOPE_KEY_COLUMN) or "") != scope_key:
                continue
            nid = cols.get(NODE_INSTANCE_ID_COLUMN)
            if not nid:
                continue
            h = cols.get(EXTRACTION_INPUTS_HASH_COLUMN)
            if not h or not str(h).strip():
                continue
            st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
            if st not in completed:
                continue
            ts = _ts_ms(cols)
            rid = str(cols.get(RUN_ID_COLUMN) or "")
            nid_s = str(nid)
            prev = best.get(nid_s)
            if prev is None or (ts, rid) > (prev[1], prev[2]):
                best[nid_s] = (str(h).strip(), ts, rid)
    except Exception:
        return {}

    return {k: v[0] for k, v in best.items()}


def read_watermark_high_ms(
    client: Any, raw_db: str, raw_table: str, wm_key: str
) -> Optional[int]:
    try:
        row = client.raw.rows.retrieve(raw_db, raw_table, wm_key)
    except Exception:
        return None
    if not row:
        return None
    cols = raw_row_columns(row)
    raw = cols.get(HIGH_WATERMARK_MS_COLUMN)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def discover_single_run_id_for_status(
    client: Any,
    raw_db: str,
    raw_table: str,
    wanted_status: str,
    *,
    chunk_size: int = 2500,
) -> Optional[str]:
    """
    Return RUN_ID among entity rows with WORKFLOW_STATUS.
    - If exactly one distinct RUN_ID exists, return it.
    - If multiple RUN_IDs exist, return the latest lexicographic RUN_ID
      (RUN_ID format is timestamp-like, so lexicographic order is chronological).
    - If no RUN_ID can be determined, return None.
    """
    run_ids: Set[str] = set()
    try:
        for row in iter_raw_table_rows_chunked(
            client, raw_db, raw_table, chunk_size=chunk_size
        ):
            cols = raw_row_columns(row)
            if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
                continue
            st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
            if wanted_status == WORKFLOW_STATUS_EXTRACTED and not st:
                pass
            elif st != wanted_status:
                continue
            rid = cols.get(RUN_ID_COLUMN)
            if rid:
                run_ids.add(str(rid))
    except Exception:
        return None
    if len(run_ids) >= 1:
        return max(run_ids)
    return None


def iter_cohort_entity_rows(
    client: Any,
    raw_db: str,
    raw_table: str,
    run_id: str,
    wanted_status: str,
    *,
    chunk_size: int = 2500,
) -> List[Any]:
    """Return RAW rows for this run_id and workflow status (full scan filter)."""
    out: List[Any] = []
    for row in iter_raw_table_rows_chunked(
        client, raw_db, raw_table, chunk_size=chunk_size
    ):
        cols = raw_row_columns(row)
        if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
            continue
        if str(cols.get(RUN_ID_COLUMN) or "") != run_id:
            continue
        st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
        if st == wanted_status or (
            wanted_status == WORKFLOW_STATUS_EXTRACTED and not st
        ):
            out.append(row)
    return out


def transition_workflow_status_for_run(
    client: Any,
    raw_db: str,
    raw_table: str,
    run_id: str,
    from_status: str,
    to_status: str,
    *,
    chunk_size: int = 2500,
) -> int:
    """
    For all entity rows with RUN_ID and WORKFLOW_STATUS=from_status, set to_status.
    Returns number of rows updated.
    """
    n = 0
    row_map: Dict[str, Dict[str, Any]] = {}
    for row in iter_raw_table_rows_chunked(
        client, raw_db, raw_table, chunk_size=chunk_size
    ):
        cols = raw_row_columns(row)
        if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
            continue
        if str(cols.get(RUN_ID_COLUMN) or "") != run_id:
            continue
        if norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN)) != from_status:
            continue
        key = getattr(row, "key", None)
        if not key:
            continue
        new_cols = dict(cols)
        new_cols[WORKFLOW_STATUS_COLUMN] = to_status
        new_cols[WORKFLOW_STATUS_UPDATED_AT_COLUMN] = datetime.now(
            timezone.utc
        ).isoformat(timespec="milliseconds")
        row_map[str(key)] = new_cols
        n += 1
    if row_map:
        client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row=row_map)
    return n


def list_all_instances(
    client: Any,
    *,
    instance_type: str,
    space: Optional[str],
    sources: List[Any],
    filter: Any,
    limit_per_page: int = 1000,
) -> Iterable[Any]:
    """Page through instances.list until cursor exhausted."""
    cursor = None
    while True:
        kwargs: Dict[str, Any] = dict(
            instance_type=instance_type,
            space=space,
            sources=sources,
            filter=filter,
            limit=limit_per_page,
        )
        if cursor is not None:
            kwargs["cursor"] = cursor
        batch = client.data_modeling.instances.list(**kwargs)
        if not batch:
            break
        for node in batch:
            yield node
        cursor = getattr(batch, "cursor", None)
        if not cursor:
            break
