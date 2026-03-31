"""
CDF Pipeline: reference index (inverted index for foreign-key and document references).

Populates a RAW table keyed by normalized lookup tokens; postings list which source
instances reference each token. Candidate keys from extraction are never indexed.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Row

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    CogniteClient = None  # type: ignore
    Row = None  # type: ignore

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

    raw_limit = int(data.get("source_raw_read_limit", data.get("raw_read_limit", 10000)))
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

    rows = client.raw.rows.list(source_raw_db, source_raw_table_key, limit=raw_limit)

    entities_processed = 0
    inverted_writes = 0
    postings_written = 0

    now = datetime.now(timezone.utc).isoformat()

    # Collect per-entity work: remove old postings from inverted rows, then add new.
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

        # Remove previous postings for this source from inverted rows (snapshot).
        old_snap = _raw_retrieve(client, index_db, index_table, snap_key)
        old_inverted_keys: List[str] = []
        if old_snap:
            sc = getattr(old_snap, "columns", {}) or {}
            raw_ik = sc.get("inverted_keys_json")
            if isinstance(raw_ik, str) and raw_ik.strip():
                try:
                    old_inverted_keys = list(json.loads(raw_ik))
                except Exception:
                    old_inverted_keys = []

        for ik in old_inverted_keys:
            inv_row = _raw_retrieve(client, index_db, index_table, ik)
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
            postings = _merge_remove_entity_postings(
                postings, external_id, instance_space
            )
            out_cols: Dict[str, Any] = {
                "postings_json": json.dumps(postings),
                "updated_at": now,
            }
            lt = icols.get("lookup_token")
            if lt is not None:
                out_cols["lookup_token"] = lt
            _raw_upsert(client, index_db, index_table, ik, out_cols)
            inverted_writes += 1

        # Build new postings grouped by inverted row key.
        new_inverted_keys: Set[str] = set()
        by_inverted: Dict[str, List[Dict[str, Any]]] = {}

        def add_refs(
            items: List[Dict[str, Any]],
            ref_kind: str,
            entity_type: str,
        ) -> None:
            nonlocal postings_written
            for item in items:
                cv = str(item.get("value") or "").strip()
                if not cv:
                    continue
                conf = item.get("confidence")
                tokens = _alias_tokens_for_value(engine, cv, entity_type)
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

        add_refs(fk_list, REFERENCE_KIND_FOREIGN_KEY, fk_entity_type)
        add_refs(doc_list, REFERENCE_KIND_DOCUMENT, doc_entity_type)

        # Merge new postings into inverted rows (append; dedupe by posting identity).
        for ik, new_posts in by_inverted.items():
            inv_row = _raw_retrieve(client, index_db, index_table, ik)
            existing: List[Dict[str, Any]] = []
            lookup_display = ""
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

            # Re-apply remove for this entity (row may have been recreated).
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

            _raw_upsert(
                client,
                index_db,
                index_table,
                ik,
                {
                    "lookup_token": lookup_display or ik,
                    "postings_json": json.dumps(merged),
                    "updated_at": now,
                },
            )
            inverted_writes += 1

        # Save snapshot of inverted keys for future removals.
        _raw_upsert(
            client,
            index_db,
            index_table,
            snap_key,
            {
                "record_kind": "reference_index_source",
                "source_external_id": external_id,
                "source_instance_space": instance_space,
                "inverted_keys_json": json.dumps(sorted(new_inverted_keys)),
                "updated_at": now,
            },
        )

    logger.info(
        f"Reference index: entities_with_refs={entities_processed}, "
        f"inverted_row_writes={inverted_writes}, posting_events={postings_written}"
    )
    data["reference_index_entities_processed"] = entities_processed
    data["reference_index_inverted_writes"] = inverted_writes
    data["reference_index_posting_events"] = postings_written
    data["reference_index_raw_db"] = index_db
    data["reference_index_raw_table"] = index_table


def _raw_retrieve(client: CogniteClient, db: str, table: str, key: str) -> Any:
    try:
        return client.raw.rows.retrieve(db, table, key)
    except Exception:
        return None


def _raw_upsert(
    client: CogniteClient,
    db: str,
    table: str,
    key: str,
    columns: Dict[str, Any],
) -> None:
    client.raw.rows.insert(db, table, Row(key=key, columns=columns))


def run_locally_stub() -> None:
    """Placeholder for local runs; use handler.run_locally if added."""
    raise NotImplementedError("Use handler local runner with CDF credentials")
