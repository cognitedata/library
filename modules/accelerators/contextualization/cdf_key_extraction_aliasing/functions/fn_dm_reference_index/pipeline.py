"""
CDF Pipeline: reference index (inverted index for foreign-key and document references).

Populates a RAW table keyed by normalized lookup tokens; postings list which source
instances reference each token. Candidate keys from extraction are never indexed.

Performance-related ``data`` keys: ``reference_index_prefetch_table`` (full index list once),
``reference_index_retrieve_concurrency`` (parallel cold ``retrieve``),
``source_raw_list_page_size`` / ``source_raw_read_limit`` (paged source reads),
``reference_index_insert_batch_size``, ``skip_reference_index_ddl``.
The handler no-ops unless ``enable_reference_index`` is true (see ``handler.py``).
"""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Row

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    CogniteClient = None  # type: ignore
    Row = None  # type: ignore

from ..cdf_fn_common.cdf_utils import create_table_if_not_exists
from ..cdf_fn_common.incremental_scope import (
    EXTERNAL_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RECORD_KIND_RUN,
    RECORD_KIND_WATERMARK,
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_EXTRACTED,
    discover_single_run_id_for_status,
    norm_workflow_status,
    raw_row_columns,
)

try:
    from ..fn_dm_key_extraction.pipeline import (
        DOCUMENT_REFERENCES_JSON_COLUMN,
        FOREIGN_KEY_REFERENCES_JSON_COLUMN,
    )
except ImportError:  # pragma: no cover
    FOREIGN_KEY_REFERENCES_JSON_COLUMN = "FOREIGN_KEY_REFERENCES_JSON"
    DOCUMENT_REFERENCES_JSON_COLUMN = "DOCUMENT_REFERENCES_JSON"

try:
    from ..fn_dm_aliasing.cdf_adapter import _convert_yaml_direct_to_aliasing_config
    from ..fn_dm_aliasing.engine.tag_aliasing_engine import AliasingEngine
except ImportError:  # pragma: no cover
    _convert_yaml_direct_to_aliasing_config = None
    AliasingEngine = None

REFERENCE_KIND_FOREIGN_KEY = "foreign_key"
REFERENCE_KIND_DOCUMENT = "document"

SNAPSHOT_KEY_PREFIX = "ssrc_"
INVERTED_KEY_PREFIX = "t_"

# Chunk size for final RAW insert; Cognite SDK also splits large insert calls internally.
# Wider rows (many posting_* columns) may need a lower batch size to stay under request limits.
DEFAULT_REFERENCE_INDEX_INSERT_BATCH_SIZE = 5000

# Sentinel: key present in disk cache but RAW row does not exist.
_DISK_ABSENT: Any = object()


class _IndexRowStore:
    """Loads reference-index RAW rows into memory or fetches on demand (with optional concurrency)."""

    def __init__(
        self,
        client: Any,
        db: str,
        table: str,
        logger: Any,
        *,
        full_prefetch: bool,
        retrieve_concurrency: int,
    ) -> None:
        self.client = client
        self.db = db
        self.table = table
        self.logger = logger
        self.full_prefetch = full_prefetch
        self.retrieve_concurrency = max(1, int(retrieve_concurrency or 1))
        # key -> columns dict (only existing rows in full_prefetch; lazy adds misses as _DISK_ABSENT)
        self._cache: Dict[str, Any] = {}

    def prefetch_entire_table(self) -> int:
        """List all rows from the index table into _cache. Returns row count."""
        n = 0
        for row_list in self.client.raw.rows(
            self.db, self.table, chunk_size=10000, limit=None
        ):
            for row in row_list:
                self._cache[row.key] = dict(getattr(row, "columns", {}) or {})
                n += 1
        self.full_prefetch = True
        self.logger.info("Reference index: prefetched %s index RAW row(s) into memory", n)
        return n

    def ensure_loaded(self, keys: Set[str]) -> None:
        if self.full_prefetch:
            return
        missing = [k for k in keys if k not in self._cache]
        if not missing:
            return
        if self.retrieve_concurrency <= 1:
            for k in missing:
                self._cache[k] = self._retrieve_columns(k)
            return

        def fetch_one(k: str) -> Tuple[str, Any]:
            return k, self._retrieve_columns(k)

        with ThreadPoolExecutor(max_workers=self.retrieve_concurrency) as pool:
            futures = [pool.submit(fetch_one, k) for k in missing]
            for fut in as_completed(futures):
                k, cols = fut.result()
                self._cache[k] = cols

    def _retrieve_columns(self, key: str) -> Union[Dict[str, Any], Any]:
        try:
            row = self.client.raw.rows.retrieve(self.db, self.table, key)
        except Exception:
            return _DISK_ABSENT
        if not row:
            return _DISK_ABSENT
        return dict(getattr(row, "columns", {}) or {})

    def get_row(self, key: str) -> Any:
        """Return a row-like object with .key and .columns, or None if missing."""
        if self.full_prefetch:
            if key not in self._cache:
                return None
            return SimpleNamespace(key=key, columns=dict(self._cache[key]))
        if key not in self._cache:
            self.ensure_loaded({key})
        val = self._cache.get(key)
        if val is _DISK_ABSENT:
            return None
        return SimpleNamespace(key=key, columns=dict(val))


def normalize_lookup_token(value: str) -> str:
    """Stable normalization for inverted row keys (trim + Unicode casefold)."""
    return str(value).strip().casefold()


def inverted_row_key(norm_token: str) -> str:
    h = hashlib.sha256(norm_token.encode("utf-8")).hexdigest()
    return f"{INVERTED_KEY_PREFIX}{h}"


def source_snapshot_row_key(instance_space: str, external_id: str) -> str:
    payload = f"{instance_space}\0{external_id}".encode("utf-8")
    return f"{SNAPSHOT_KEY_PREFIX}{hashlib.sha256(payload).hexdigest()}"


def _parse_reference_json(raw_json: Optional[str]) -> List[Dict[str, Any]]:
    if not isinstance(raw_json, str) or not raw_json.strip():
        return []
    try:
        parsed = json.loads(raw_json)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, dict) and item.get("value") is not None:
            out.append(item)
    return out


def _posting_identity(p: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(p.get("source_external_id") or ""),
        str(p.get("source_instance_space") or ""),
        str(p.get("canonical_value") or ""),
        str(p.get("reference_kind") or ""),
    )


def _merge_remove_entity_postings(
    postings: List[Dict[str, Any]],
    source_external_id: str,
    source_instance_space: str,
) -> List[Dict[str, Any]]:
    return [
        p
        for p in postings
        if not (
            str(p.get("source_external_id") or "") == source_external_id
            and str(p.get("source_instance_space") or "") == source_instance_space
        )
    ]


def _build_aliasing_engine(data: Dict[str, Any], logger: Any) -> Any:
    if AliasingEngine is None or _convert_yaml_direct_to_aliasing_config is None:
        raise ValueError("AliasingEngine is not available (import fn_dm_aliasing)")
    provided = data.get("config")
    if not isinstance(provided, dict):
        raise ValueError("reference index requires config with aliasing rules (data.config)")
    unwrapped = provided.get("config", provided)
    if (
        isinstance(unwrapped, dict)
        and "data" in unwrapped
        and isinstance(unwrapped.get("data"), dict)
        and "aliasing_rules" in unwrapped["data"]
    ):
        aliasing_config = _convert_yaml_direct_to_aliasing_config({"config": unwrapped})
    else:
        aliasing_config = provided
    return AliasingEngine(aliasing_config, logger)


def _alias_tokens_for_value(
    engine: Any,
    canonical: str,
    entity_type: str,
) -> List[str]:
    """Canonical value plus generated aliases (inline engine; not fn_dm_aliasing RAW)."""
    result = engine.generate_aliases(tag=canonical, entity_type=entity_type, context={})
    out: List[str] = [canonical]
    for a in result.aliases or []:
        s = str(a).strip()
        if s and s not in out:
            out.append(s)
    return out


def _load_source_raw_rows(
    client: CogniteClient,
    source_raw_db: str,
    source_raw_table_key: str,
    data: Dict[str, Any],
    logger: Any,
) -> Tuple[List[Any], int]:
    """
    Read key-extraction RAW using the SDK row iterator (paged list calls under the hood).

    ``source_raw_read_limit`` / ``raw_read_limit``: max rows total (default 10000).
    ``0`` or negative means unlimited (read until API exhaustion).

    ``source_raw_list_page_size``: rows per chunk (default 10000, max 10000).
    """
    raw_cap = data.get("source_raw_read_limit", data.get("raw_read_limit", 10000))
    if raw_cap is None:
        max_rows = None
    else:
        cap = int(raw_cap)
        max_rows = None if cap <= 0 else cap

    page = int(data.get("source_raw_list_page_size", 10000) or 10000)
    page = max(1, min(page, 10000))

    out: List[Any] = []
    chunks = 0
    for row_list in client.raw.rows(
        source_raw_db,
        source_raw_table_key,
        chunk_size=page,
        limit=max_rows,
    ):
        chunks += 1
        out.extend(row_list)
    logger.info(
        "Reference index: loaded %s source RAW row(s) in %s list chunk(s) "
        "(max_rows=%s page_size=%s)",
        len(out),
        chunks,
        max_rows if max_rows is not None else "unlimited",
        page,
    )
    return out, chunks


def _resolve_run_id_filter(
    client: CogniteClient,
    data: Dict[str, Any],
    source_raw_db: str,
    source_raw_table_key: str,
) -> Optional[str]:
    rid = data.get("source_run_id") or data.get("run_id")
    if rid:
        return str(rid)
    if data.get("incremental_auto_run_id"):
        rid2 = discover_single_run_id_for_status(
            client,
            source_raw_db,
            source_raw_table_key,
            WORKFLOW_STATUS_EXTRACTED,
        )
        if rid2:
            data["source_run_id"] = str(rid2)
            return str(rid2)
    return None


def persist_reference_index(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Read key-extraction RAW (FK + document JSON only), expand alias tokens per referenced
    value via inline AliasingEngine, upsert inverted index + per-source snapshots in RAW.
    """
    if not client:
        raise ValueError("CogniteClient is required for reference index persistence")
    if not CDF_AVAILABLE or Row is None:
        raise ValueError("CDF client/Row not available")

    engine = _build_aliasing_engine(data, logger)

    source_raw_db = str(data.get("source_raw_db") or "")
    source_raw_table_key = str(data.get("source_raw_table_key") or "")
    if not source_raw_db or not source_raw_table_key:
        raise ValueError("source_raw_db and source_raw_table_key are required")

    index_db = str(data.get("reference_index_raw_db") or data.get("index_raw_db") or source_raw_db)
    index_table = str(data.get("reference_index_raw_table") or data.get("index_raw_table") or "")
    if not index_table:
        raise ValueError(
            "reference_index_raw_table (or index_raw_table) is required — e.g. {site}_reference_index"
        )

    if not data.get("skip_reference_index_ddl"):
        create_table_if_not_exists(client, index_db, index_table, logger)
    logger.info(
        "Reference index: target RAW db=%s table=%s",
        index_db,
        index_table,
    )

    prefetch_table = bool(data.get("reference_index_prefetch_table", False))
    retrieve_concurrency = int(data.get("reference_index_retrieve_concurrency", 1) or 1)
    index_store = _IndexRowStore(
        client,
        index_db,
        index_table,
        logger,
        full_prefetch=prefetch_table,
        retrieve_concurrency=retrieve_concurrency,
    )
    if prefetch_table:
        index_store.prefetch_entire_table()

    progress_every = max(
        0,
        int(data.get("progress_every", data.get("reference_index_progress_every", 0)) or 0),
    )

    src_run = _resolve_run_id_filter(client, data, source_raw_db, source_raw_table_key)
    wf_filter = data.get("source_workflow_status") or (
        WORKFLOW_STATUS_EXTRACTED if src_run else None
    )
    wf_filter_n = norm_workflow_status(wf_filter) if wf_filter else None

    fk_entity_type = str(data.get("reference_index_fk_entity_type") or "asset")
    doc_entity_type = str(data.get("reference_index_document_entity_type") or "file")

    default_inst = data.get("source_instance_space")
    default_vs = data.get("source_view_space")
    default_ve = data.get("source_view_external_id")
    default_vv = data.get("source_view_version")

    run_tag = str(data.get("run_id") or src_run or "")

    logger.info(
        "Reference index: listing source RAW db=%s table=%s%s",
        source_raw_db,
        source_raw_table_key,
        f" run_id={src_run}" if src_run else "",
    )
    rows, source_list_chunks = _load_source_raw_rows(
        client, source_raw_db, source_raw_table_key, data, logger
    )
    data["reference_index_source_list_chunks"] = source_list_chunks
    if progress_every > 0:
        logger.info(
            "Reference index: progress log every %s entity/entities with references",
            progress_every,
        )

    entities_processed = 0
    inverted_writes = 0
    postings_written = 0
    fk_postings_written = 0
    doc_postings_written = 0

    now = datetime.now(timezone.utc).isoformat()

    insert_batch = max(
        1,
        int(
            data.get(
                "reference_index_insert_batch_size",
                DEFAULT_REFERENCE_INDEX_INSERT_BATCH_SIZE,
            )
            or DEFAULT_REFERENCE_INDEX_INSERT_BATCH_SIZE
        ),
    )

    # In-memory inverted rows + deferred snapshot writes; single batched flush at end.
    inv_cache: Dict[str, Dict[str, Any]] = {}
    dirty_inv: Set[str] = set()
    pending_snapshots: Dict[str, Dict[str, Any]] = {}
    alias_tokens_cache: Dict[Tuple[str, str], List[str]] = {}

    for row in rows or []:
        cols = raw_row_columns(row)
        rkind = cols.get(RECORD_KIND_COLUMN)
        if rkind in (RECORD_KIND_WATERMARK, RECORD_KIND_RUN):
            continue
        if rkind is not None and str(rkind).lower() != RECORD_KIND_ENTITY:
            continue
        if src_run:
            if str(cols.get(RUN_ID_COLUMN) or "") != str(src_run):
                continue
        if wf_filter_n:
            st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
            if (
                rkind is not None
                and str(rkind).lower() == RECORD_KIND_ENTITY
                and st
                and st != wf_filter_n
            ):
                continue

        fk_json = cols.get(FOREIGN_KEY_REFERENCES_JSON_COLUMN)
        doc_json = cols.get(DOCUMENT_REFERENCES_JSON_COLUMN)
        fk_list = _parse_reference_json(fk_json if isinstance(fk_json, str) else None)
        doc_list = _parse_reference_json(doc_json if isinstance(doc_json, str) else None)
        if not fk_list and not doc_list:
            continue

        external_id = str(cols.get(EXTERNAL_ID_COLUMN) or getattr(row, "key", "") or "")
        if not external_id:
            continue

        instance_space = str(cols.get("instance_space") or default_inst or "")
        view_space = str(cols.get("view_space") or default_vs or "")
        view_external_id = str(cols.get("view_external_id") or default_ve or "")
        view_version = str(cols.get("view_version") or default_vv or "")
        if not (instance_space and view_space and view_external_id and view_version):
            logger.warning(
                f"Skipping reference index for {external_id!r}: missing view/instance metadata "
                "(set source_instance_space, source_view_* on task data or ensure cohort columns)"
            )
            continue

        snap_key = source_snapshot_row_key(instance_space, external_id)
        entities_processed += 1
        if progress_every > 0 and entities_processed % progress_every == 0:
            logger.info(
                "Reference index progress: entities_with_refs=%s, inverted_row_writes=%s, "
                "posting_events=%s (foreign_key=%s, document=%s)",
                entities_processed,
                inverted_writes,
                postings_written,
                fk_postings_written,
                doc_postings_written,
            )

        # Build new postings grouped by inverted row key (needed for index key prefetch).
        new_inverted_keys: Set[str] = set()
        by_inverted: Dict[str, List[Dict[str, Any]]] = {}

        def add_refs(
            items: List[Dict[str, Any]],
            ref_kind: str,
            entity_type: str,
        ) -> None:
            nonlocal postings_written, fk_postings_written, doc_postings_written
            for item in items:
                cv = str(item.get("value") or "").strip()
                if not cv:
                    continue
                conf = item.get("confidence")
                cache_key = (entity_type, cv)
                if cache_key in alias_tokens_cache:
                    tokens = alias_tokens_cache[cache_key]
                else:
                    tokens = _alias_tokens_for_value(engine, cv, entity_type)
                    alias_tokens_cache[cache_key] = tokens
                for tok in tokens:
                    norm = normalize_lookup_token(tok)
                    if not norm:
                        continue
                    ik = inverted_row_key(norm)
                    new_inverted_keys.add(ik)
                    posting = {
                        "source_external_id": external_id,
                        "source_instance_space": instance_space,
                        "source_view_space": view_space,
                        "source_view_external_id": view_external_id,
                        "source_view_version": view_version,
                        "reference_kind": ref_kind,
                        "canonical_value": cv,
                        "confidence": conf,
                        "run_id": run_tag or None,
                    }
                    by_inverted.setdefault(ik, []).append(posting)
                    postings_written += 1
                    if ref_kind == REFERENCE_KIND_FOREIGN_KEY:
                        fk_postings_written += 1
                    else:
                        doc_postings_written += 1

        add_refs(fk_list, REFERENCE_KIND_FOREIGN_KEY, fk_entity_type)
        add_refs(doc_list, REFERENCE_KIND_DOCUMENT, doc_entity_type)

        index_store.ensure_loaded({snap_key})
        old_snap = index_store.get_row(snap_key)
        old_inverted_keys: List[str] = []
        if old_snap:
            sc = getattr(old_snap, "columns", {}) or {}
            raw_ik = sc.get("inverted_keys_json")
            if isinstance(raw_ik, str) and raw_ik.strip():
                try:
                    old_inverted_keys = list(json.loads(raw_ik))
                except Exception:
                    old_inverted_keys = []

        prefetch_keys: Set[str] = set(old_inverted_keys) | set(by_inverted.keys())
        prefetch_keys -= set(inv_cache.keys())
        index_store.ensure_loaded(prefetch_keys)

        for ik in old_inverted_keys:
            if ik in inv_cache:
                bucket = inv_cache[ik]
                postings = list(bucket["postings"])
                lookup_token = str(bucket.get("lookup_token") or "")
            else:
                inv_row = index_store.get_row(ik)
                if not inv_row:
                    continue
                icols = getattr(inv_row, "columns", {}) or {}
                pj = icols.get("postings_json")
                if not isinstance(pj, str):
                    continue
                try:
                    postings = json.loads(pj)
                except Exception:
                    postings = []
                if not isinstance(postings, list):
                    postings = []
                lt = icols.get("lookup_token")
                lookup_token = str(lt) if lt is not None else ""
                inv_cache[ik] = {"postings": postings, "lookup_token": lookup_token}
            postings = _merge_remove_entity_postings(
                postings, external_id, instance_space
            )
            inv_cache[ik] = {"postings": postings, "lookup_token": lookup_token}
            dirty_inv.add(ik)
            inverted_writes += 1

        # Merge new postings into inverted rows (append; dedupe by posting identity).
        for ik, new_posts in by_inverted.items():
            existing: List[Dict[str, Any]] = []
            lookup_display = ""
            if ik in inv_cache:
                bucket = inv_cache[ik]
                existing = list(bucket["postings"])
                lookup_display = str(bucket.get("lookup_token") or "")
            else:
                inv_row = index_store.get_row(ik)
                if inv_row:
                    icols = getattr(inv_row, "columns", {}) or {}
                    lookup_display = str(icols.get("lookup_token") or "")
                    pj = icols.get("postings_json")
                    if isinstance(pj, str) and pj.strip():
                        try:
                            existing = json.loads(pj)
                        except Exception:
                            existing = []
                if not isinstance(existing, list):
                    existing = []
                inv_cache[ik] = {
                    "postings": existing,
                    "lookup_token": lookup_display,
                }

            existing = _merge_remove_entity_postings(
                existing, external_id, instance_space
            )
            seen: Set[Tuple[str, str, str, str]] = set()
            merged: List[Dict[str, Any]] = []
            for p in existing + new_posts:
                pid = _posting_identity(p)
                if pid in seen:
                    continue
                seen.add(pid)
                merged.append(p)

            first_norm = next(
                (
                    normalize_lookup_token(p["canonical_value"])
                    for p in new_posts
                    if p.get("canonical_value")
                ),
                "",
            )
            if not lookup_display and first_norm:
                lookup_display = first_norm

            inv_cache[ik] = {
                "postings": merged,
                "lookup_token": lookup_display,
            }
            dirty_inv.add(ik)
            inverted_writes += 1

        pending_snapshots[snap_key] = {
            "record_kind": "reference_index_source",
            "source_external_id": external_id,
            "source_instance_space": instance_space,
            "inverted_keys_json": json.dumps(sorted(new_inverted_keys)),
            "updated_at": now,
        }

    flush_rows: List[Any] = []
    for ik in sorted(dirty_inv):
        bucket = inv_cache[ik]
        flush_rows.append(
            Row(
                key=ik,
                columns={
                    "lookup_token": str(bucket.get("lookup_token") or "") or ik,
                    "postings_json": json.dumps(bucket["postings"]),
                    "updated_at": now,
                },
            )
        )
    for snap_key in sorted(pending_snapshots.keys()):
        flush_rows.append(
            Row(key=snap_key, columns=pending_snapshots[snap_key])
        )
    insert_batches = 0
    if flush_rows:
        insert_batches = _raw_insert_batched(
            client, index_db, index_table, flush_rows, insert_batch
        )

    logger.info(
        "Reference index: entities_with_refs=%s, inverted_row_writes=%s, "
        "posting_events=%s (foreign_key=%s, document=%s), insert_batches=%s",
        entities_processed,
        inverted_writes,
        postings_written,
        fk_postings_written,
        doc_postings_written,
        insert_batches,
    )
    data["reference_index_entities_processed"] = entities_processed
    data["reference_index_inverted_writes"] = inverted_writes
    data["reference_index_posting_events"] = postings_written
    data["reference_index_fk_posting_events"] = fk_postings_written
    data["reference_index_document_posting_events"] = doc_postings_written
    data["reference_index_raw_db"] = index_db
    data["reference_index_raw_table"] = index_table
    data["reference_index_insert_batches"] = insert_batches


def _raw_insert_batched(
    client: CogniteClient,
    db: str,
    table: str,
    rows: List[Any],
    batch_size: int,
) -> int:
    n_batches = 0
    for i in range(0, len(rows), batch_size):
        client.raw.rows.insert(db, table, rows[i : i + batch_size])
        n_batches += 1
    return n_batches


def run_locally_stub() -> None:
    """Placeholder for local runs; use handler.run_locally if added."""
    raise NotImplementedError("Use handler local runner with CDF credentials")
