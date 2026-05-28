"""
Incremental key-extraction scope: watermarks, cohort row keys, WORKFLOW_STATUS helpers.

Used by discovery pipeline functions (view/raw/classic query and downstream stages)
for RAW-backed cohort handoff (single unified ``raw_table_key`` where applicable).
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists
from cdf_fn_common.etl_ui_progress import emit_handler_progress

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
RECORD_KIND_RECORD = "record"
RECORD_KIND_INDEX = "index_posting"
RECORD_KIND_WATERMARK = "watermark"
RECORD_KIND_RUN = "run"

HIGH_WATERMARK_MS_COLUMN = "HIGH_WATERMARK_MS"
RUN_ID_COLUMN = "RUN_ID"

# Per-entity SHA-256 of extraction source inputs (see cdf_fn_common.extraction_input_hash).
EXTRACTION_INPUTS_HASH_COLUMN = "EXTRACTION_INPUTS_HASH"

# Cross-run incremental partition (parallel workflows must not share state).
WORKFLOW_SCOPE_COLUMN = "WORKFLOW_SCOPE"
SOURCE_VIEW_FINGERPRINT_COLUMN = "SOURCE_VIEW_FINGERPRINT"

INCREMENTAL_STATE_TABLE_SUFFIX = "__incremental"
# RAW table name max length (CDF API).
_CDF_RAW_TABLE_NAME_MAX_LEN = 64


def _sanitize_key_segment(value: str, *, max_len: int = 40) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "default"
    cleaned = "".join(c if c.isalnum() or c in "-_" else "_" for c in raw)
    cleaned = cleaned.strip("_") or "default"
    if len(cleaned) > max_len:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
        head = cleaned[: max_len - 13].rstrip("_")
        cleaned = f"{head}_{digest}"
    return cleaned


def incremental_state_table_name(base_table: str) -> str:
    """
    Stable RAW table for cross-run watermarks and hash state (not deleted by run cleanup).

    Example: ``discovery_state`` → ``discovery_state__incremental``.
    """
    base = str(base_table or "discovery_state").strip() or "discovery_state"
    name = f"{base}{INCREMENTAL_STATE_TABLE_SUFFIX}"
    if len(name) <= _CDF_RAW_TABLE_NAME_MAX_LEN:
        return name
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:8]
    budget = _CDF_RAW_TABLE_NAME_MAX_LEN - len(INCREMENTAL_STATE_TABLE_SUFFIX) - len(digest) - 2
    head = base[: max(4, budget)].rstrip("_")
    return f"{head}_{digest}{INCREMENTAL_STATE_TABLE_SUFFIX}"


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


def scope_watermark_row_key(scope_key: str, workflow_scope: str = "") -> str:
    """RAW watermark row key; includes *workflow_scope* when set (multi-workflow safe)."""
    sk = str(scope_key or "").strip()
    ws = str(workflow_scope or "").strip()
    if ws:
        return f"scope_wm_{_sanitize_key_segment(ws, max_len=24)}_{_sanitize_key_segment(sk, max_len=32)}"
    return f"scope_wm_{sk}"


def incremental_entity_row_key(
    workflow_scope: str,
    scope_key: str,
    node_instance_id: str,
) -> str:
    """Stable RAW row key for per-instance hash state in the incremental table."""
    ws = _sanitize_key_segment(workflow_scope, max_len=24)
    sk = _sanitize_key_segment(scope_key, max_len=32)
    nid = _sanitize_key_segment(node_instance_id, max_len=48)
    key = f"inc_{ws}_{sk}_{nid}"
    if len(key) <= 256:
        return key
    digest = hashlib.sha256(f"{ws}\0{sk}\0{node_instance_id}".encode("utf-8")).hexdigest()[:16]
    return f"inc_{digest}"


def _row_matches_workflow_scope(cols: Dict[str, Any], workflow_scope: str) -> bool:
    if not workflow_scope:
        return True
    row_ws = str(cols.get(WORKFLOW_SCOPE_COLUMN) or "").strip()
    if not row_ws:
        # Legacy rows without WORKFLOW_SCOPE: ignore when a scope filter is required.
        return False
    return row_ws == workflow_scope


def build_incremental_entity_columns(
    *,
    workflow_scope: str,
    scope_key: str,
    node_instance_id: str,
    external_id: str,
    extraction_inputs_hash: str,
    run_id: str,
    source_view_fingerprint: str = "",
    last_updated_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Column map for upserting cross-run hash state on the stable incremental RAW table."""
    cols: Dict[str, Any] = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        WORKFLOW_SCOPE_COLUMN: workflow_scope,
        SCOPE_KEY_COLUMN: scope_key,
        NODE_INSTANCE_ID_COLUMN: node_instance_id,
        EXTERNAL_ID_COLUMN: external_id,
        EXTRACTION_INPUTS_HASH_COLUMN: extraction_inputs_hash,
        WORKFLOW_STATUS_COLUMN: WORKFLOW_STATUS_DETECTED,
        RUN_ID_COLUMN: run_id,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
    }
    if source_view_fingerprint:
        cols[SOURCE_VIEW_FINGERPRINT_COLUMN] = source_view_fingerprint
    if last_updated_ms is not None:
        cols["LAST_UPDATED_MS"] = int(last_updated_ms)
    return cols


def write_incremental_watermark_raw(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    workflow_scope: str,
    high_ms: int,
    run_id: str,
) -> None:
    """Persist listing watermark on the stable incremental RAW table."""
    create_table_if_not_exists(client, raw_db, raw_table)
    wm_key = scope_watermark_row_key(scope_key, workflow_scope)
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_WATERMARK,
        WORKFLOW_SCOPE_COLUMN: workflow_scope,
        SCOPE_KEY_COLUMN: scope_key,
        HIGH_WATERMARK_MS_COLUMN: int(high_ms),
        RUN_ID_COLUMN: run_id,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
    }
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={wm_key: cols})


def upsert_incremental_entity_hashes_raw(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    workflow_scope: str,
    scope_key: str,
    run_id: str,
    source_view_fingerprint: str,
    items: List[Dict[str, Any]],
) -> None:
    """
    Batch upsert hash rows on the stable incremental table.

    Each *item* must include ``node_instance_id``, ``external_id``, ``extraction_inputs_hash``;
    optional ``last_updated_ms``.
    """
    if not items:
        return
    create_table_if_not_exists(client, raw_db, raw_table)
    row_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        nid = str(it.get("node_instance_id") or "").strip()
        h = str(it.get("extraction_inputs_hash") or "").strip()
        if not nid or not h:
            continue
        key = incremental_entity_row_key(workflow_scope, scope_key, nid)
        row_map[key] = build_incremental_entity_columns(
            workflow_scope=workflow_scope,
            scope_key=scope_key,
            node_instance_id=nid,
            external_id=str(it.get("external_id") or ""),
            extraction_inputs_hash=h,
            run_id=run_id,
            source_view_fingerprint=source_view_fingerprint,
            last_updated_ms=it.get("last_updated_ms"),
        )
    if row_map:
        client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row=row_map)


def dm_node_instance_space(instance: Any) -> str:
    """Best-effort DM instance space from SDK node or ``dump()`` payload."""
    space = getattr(instance, "space", None)
    if space is not None and str(space).strip():
        return str(space).strip()
    dump = instance.dump() if hasattr(instance, "dump") else {}
    if isinstance(dump, dict):
        for key in ("space",):
            v = dump.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        node = dump.get("node")
        if isinstance(node, dict):
            v = node.get("space")
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def node_instance_id_str(instance: Any) -> str:
    """
    Space-qualified node instance id for RAW keys and correlation.

    Prefer ``space`` + ``instance_id`` (UUID) when present; fall back to externalId-only
    only when instance id is unavailable (should be rare for DM nodes).
    """
    space = dm_node_instance_space(instance)
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
                node = dump.get("node")
                if isinstance(node, dict):
                    raw = node.get("lastUpdatedTime")
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


def _row_ts_ms_for_hash_tiebreak(cols: Dict[str, Any]) -> float:
    """Epoch ms from WORKFLOW_STATUS_UPDATED_AT / UPDATED_AT for hash-index tie-break."""
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


_HASH_INDEX_STATUSES = frozenset(
    {
        WORKFLOW_STATUS_DETECTED,
        WORKFLOW_STATUS_EXTRACTED,
        WORKFLOW_STATUS_ALIASED,
        WORKFLOW_STATUS_PERSISTED,
    }
)


def build_latest_hash_index_for_table(
    client: Any,
    raw_db: str,
    raw_table: str,
    *,
    workflow_scope: str = "",
    chunk_size: int = 2500,
) -> Dict[str, Dict[str, str]]:
    """
    One RAW table scan: latest EXTRACTION_INPUTS_HASH per NODE_INSTANCE_ID per SCOPE_KEY.

    When *workflow_scope* is set, only rows with matching :data:`WORKFLOW_SCOPE_COLUMN` are indexed
    (multi-workflow parallel safety on the stable incremental table).

    Same row eligibility and (timestamp, RUN_ID) tie-break as :func:`load_latest_hash_by_node_for_scope`.
    Used by the local runner to avoid N full scans when multiple ``fn_dm_view_query`` tasks share a sink.
    """
    wf_scope = str(workflow_scope or "").strip()
    best: Dict[str, Dict[str, Tuple[str, float, str]]] = {}
    try:
        for row in iter_raw_table_rows_chunked(
            client, raw_db, raw_table, chunk_size=chunk_size
        ):
            cols = raw_row_columns(row)
            if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
                continue
            if not _row_matches_workflow_scope(cols, wf_scope):
                continue
            scope_key = str(cols.get(SCOPE_KEY_COLUMN) or "").strip()
            if not scope_key:
                continue
            nid = cols.get(NODE_INSTANCE_ID_COLUMN)
            if not nid:
                continue
            h = cols.get(EXTRACTION_INPUTS_HASH_COLUMN)
            if not h or not str(h).strip():
                continue
            st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
            if st not in _HASH_INDEX_STATUSES:
                continue
            ts = _row_ts_ms_for_hash_tiebreak(cols)
            rid = str(cols.get(RUN_ID_COLUMN) or "")
            nid_s = str(nid)
            inner = best.setdefault(scope_key, {})
            prev = inner.get(nid_s)
            if prev is None or (ts, rid) > (prev[1], prev[2]):
                inner[nid_s] = (str(h).strip(), ts, rid)
    except Exception:
        return {}

    return {sk: {nid: triple[0] for nid, triple in inner.items()} for sk, inner in best.items()}


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
    workflow_scope: str = "",
    chunk_size: int = 2500,
) -> Dict[str, str]:
    """
    Latest EXTRACTION_INPUTS_HASH per NODE_INSTANCE_ID for this scope.

    Uses entity rows with WORKFLOW_STATUS in detected / extracted / aliased / persisted and a
    non-empty EXTRACTION_INPUTS_HASH. Picks the row with greatest UPDATED_AT /
    WORKFLOW_STATUS_UPDATED_AT (epoch ms); tie-break by RUN_ID lexicographic order.

    Including **detected** lets discovery-only pipelines (view query without key extraction)
    reuse hashes written at cohort time for ``incremental_skip_unchanged_source_inputs``.

    Implemented as a slice of :func:`build_latest_hash_index_for_table` (one full table scan).
    """
    full = build_latest_hash_index_for_table(
        client,
        raw_db,
        raw_table,
        workflow_scope=workflow_scope,
        chunk_size=chunk_size,
    )
    return dict(full.get(scope_key, {}))


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


@dataclass
class ListInstancesStats:
    """Metrics from a paginated ``instances.list`` walk (view-query baseline / tuning)."""

    page_count: int = 0
    instances_yielded: int = 0
    list_duration_sec: float = 0.0
    sort_property: Optional[str] = None
    limit_per_page: int = 0


def view_query_list_sort(*, incremental: bool) -> Any:
    """
    Sort for ``instances.list`` during view query.

    Returns ``None`` for all modes. Incremental discovery uses a
    ``Range`` filter on ``("node", "lastUpdatedTime")`` (see ``_watermark_filter`` in
    ``fn_dm_view_query``), which the Instances API accepts. The same path is **not** valid
    for ``sort`` — requests with ``InstanceSort(("node", "lastUpdatedTime"), ...)`` fail with
    ``Invalid sort property '[node, lastUpdatedTime]'``. Listing therefore uses the API
    default sort (internal id), which is cursorable with ``HasData`` / space filters per
    Cognite DM performance guidance.
    """
    del incremental
    return None


def _sort_property_label(sort: Any) -> Optional[str]:
    if sort is None:
        return None
    prop = getattr(sort, "property", None)
    if prop is None and isinstance(sort, dict):
        prop = sort.get("property")
    if isinstance(prop, (list, tuple)):
        return ".".join(str(p) for p in prop)
    return str(prop) if prop is not None else repr(sort)


def list_all_instances(
    client: Any,
    *,
    instance_type: str,
    space: Optional[str],
    sources: List[Any],
    filter: Any,
    limit_per_page: int = 1000,
    sort: Any = None,
    logger: Optional[Any] = None,
    progress_context: str = "",
    stats_out: Optional[ListInstancesStats] = None,
) -> Iterable[Any]:
    """Page through instances using the SDK chunk iterator (``instances(chunk_size=…)``).

    ``instances.list(limit=N)`` treats *N* as a **total** cap (not per-page) and does not
    accept a ``cursor`` argument, so manual pagination capped at one page of 1000. The
    callable API uses ``_list_generator`` with ``limit=None`` and proper ``nextCursor`` handling.

    When ``logger`` is set, logs after each API page completes: batch index, instances
    in that page, and cumulative instance count. Optional ``stats_out`` receives
    aggregate timing and page counts for handler summaries.
    """
    t0 = time.perf_counter()
    batch_no = 0
    total = 0
    sort_label = _sort_property_label(sort)
    if stats_out is not None:
        stats_out.limit_per_page = limit_per_page
        stats_out.sort_property = sort_label
    page_size = max(1, int(limit_per_page or 1000))
    list_kwargs: Dict[str, Any] = dict(
        chunk_size=page_size,
        instance_type=instance_type,
        space=space,
        sources=sources,
        filter=filter,
        limit=None,
    )
    if sort is not None:
        list_kwargs["sort"] = sort
    from cdf_fn_common.etl_cognite_retry import call_with_transient_retry

    def _open_chunk_iterator():
        instances_api = client.data_modeling.instances
        if not callable(instances_api):
            raise TypeError("client.data_modeling.instances must support chunk iteration")
        return instances_api(**list_kwargs)

    page_iter = call_with_transient_retry(_open_chunk_iterator, logger=logger)
    for batch in page_iter:
        if not batch:
            continue
        batch_no += 1
        n_in_page = 0
        for node in batch:
            n_in_page += 1
            total += 1
            yield node
        if logger is not None and hasattr(logger, "info"):
            ctx = f" {progress_context}" if progress_context else ""
            sort_note = f" sort={sort_label}" if sort_label else ""
            logger.info(
                "instances.list batch %s complete%s: %s instance(s) this page, %s cumulative%s",
                batch_no,
                ctx,
                n_in_page,
                total,
                sort_note,
            )
        emit_handler_progress(total, label="instances")
    if stats_out is not None:
        stats_out.page_count = batch_no
        stats_out.instances_yielded = total
        stats_out.list_duration_sec = round(time.perf_counter() - t0, 6)
    elif logger is not None and hasattr(logger, "info") and batch_no:
        ctx = f" {progress_context}" if progress_context else ""
        logger.info(
            "instances.list finished%s: %s page(s), %s instance(s), %.3fs%s",
            ctx,
            batch_no,
            total,
            time.perf_counter() - t0,
            f" sort={sort_label}" if sort_label else "",
        )
