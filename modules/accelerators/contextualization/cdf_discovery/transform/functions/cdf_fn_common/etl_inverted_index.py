"""Inverted-index row shape and helpers (legacy ``fn_dm_inverted_index`` contract)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from cdf_fn_common.etl_discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    QUERY_SOURCE_COLUMN,
    QUERY_TASK_ID_COLUMN,
    RECORD_KIND_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    _first_nonempty,
    _flush_rows,
)
from cdf_fn_common.etl_incremental_scope import RECORD_KIND_INDEX, RUN_ID_COLUMN, raw_row_columns
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue

INDEX_KIND_COLUMN = "INDEX_KIND"
LOOKUP_KEY_COLUMN = "LOOKUP_KEY"
POSTINGS_JSON_COLUMN = "POSTINGS_JSON"
UPDATED_AT_COLUMN = "UPDATED_AT"


def normalize_lookup_key(token: str) -> str:
    return str(token or "").strip().casefold()


def parse_index_kinds_config(cfg: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """Build ``(index_kind, property_name)`` pairs from ``cfg['index_kinds']``."""
    raw = cfg.get("index_kinds")
    if not isinstance(raw, dict) or not raw:
        return []
    out: List[Tuple[str, str]] = []
    for kind, props in raw.items():
        kind_s = str(kind or "").strip()
        if not kind_s or not isinstance(props, list):
            continue
        for prop in props:
            prop_s = str(prop or "").strip()
            if prop_s:
                out.append((kind_s, prop_s))
    return out


def _instance_identity_from_row(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Tuple[str, str, str]:
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN))
    inst_space = _first_nonempty(props.get("instance_space"))
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
    if not inst_space and nid and ":" in nid:
        inst_space = nid.split(":", 1)[0].strip()
    return inst_space, ext_id, nid


def build_index_posting(
    *,
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
    index_kind: str,
    source_property: str,
    token: str,
    confidence: Optional[float],
    run_id: str,
    default_view_version: str = "v1",
) -> Dict[str, Any]:
    inst_space, ext_id, nid = _instance_identity_from_row(cols, props)
    posting: Dict[str, Any] = {
        "instance_space": inst_space,
        "external_id": ext_id,
        "node_instance_id": nid,
        "view_space": _first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
        "view_external_id": _first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN)),
        "view_version": _first_nonempty(cols.get(VIEW_VERSION_COLUMN), default_view_version),
        "entity_type": _first_nonempty(cols.get(ENTITY_TYPE_COLUMN)),
        "source_property": source_property,
        "index_kind": index_kind,
        "run_id": run_id,
    }
    if confidence is not None:
        posting["confidence"] = confidence
    return posting


def posting_dedupe_key(posting: Mapping[str, Any]) -> Tuple[str, str, str]:
    return (
        str(posting.get("instance_space") or ""),
        str(posting.get("external_id") or ""),
        str(posting.get("source_property") or ""),
    )


def merge_postings(
    existing: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge by (instance_space, external_id, source_property); incoming replaces same run_id."""
    by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for p in existing:
        if isinstance(p, dict):
            by_key[posting_dedupe_key(p)] = dict(p)
    for p in incoming:
        if not isinstance(p, dict):
            continue
        key = posting_dedupe_key(p)
        prior = by_key.get(key)
        if prior and str(prior.get("run_id") or "") == str(p.get("run_id") or ""):
            by_key[key] = dict(p)
        elif prior is None:
            by_key[key] = dict(p)
        else:
            merged = dict(prior)
            if p.get("confidence") is not None:
                merged["confidence"] = p["confidence"]
            merged["run_id"] = p.get("run_id")
            by_key[key] = merged
    return list(by_key.values())


def build_inverted_index_rows(
    *,
    pending: Mapping[Tuple[str, str], List[Dict[str, Any]]],
    run_id: str,
    canvas_node_id: str,
    query_source: str = "build_index",
    row_key_template: str = "{index_kind}:{lookup_key}",
    row_key_formatter: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Materialize legacy inverted-index RAW rows (no load from persistent sink)."""
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    raw_rows: List[Dict[str, Any]] = []
    format_key = row_key_formatter or (lambda kind, key, _tpl: f"{kind}:{key}")
    for (index_kind, lookup_key), new_posts in pending.items():
        merged = merge_postings([], new_posts)
        row_key = format_key(index_kind, lookup_key, row_key_template)
        raw_rows.append(
            {
                "key": row_key,
                "columns": {
                    RECORD_KIND_COLUMN: RECORD_KIND_INDEX,
                    RUN_ID_COLUMN: run_id,
                    INDEX_KIND_COLUMN: index_kind,
                    LOOKUP_KEY_COLUMN: lookup_key,
                    POSTINGS_JSON_COLUMN: json.dumps(merged, default=str),
                    UPDATED_AT_COLUMN: now,
                    QUERY_SOURCE_COLUMN: query_source,
                    QUERY_TASK_ID_COLUMN: canvas_node_id,
                },
            }
        )
    return raw_rows


def load_existing_postings(client: Any, raw_db: str, raw_table: str, row_key: str) -> List[Dict[str, Any]]:
    try:
        row = client.raw.rows.retrieve(raw_db, raw_table, row_key)
    except Exception:
        return []
    if not row:
        return []
    cols = raw_row_columns(row)
    raw = cols.get(POSTINGS_JSON_COLUMN)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [dict(x) for x in parsed if isinstance(x, dict)]
        except json.JSONDecodeError:
            return []
    return []


def persist_inverted_index_rows(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    index_rows: List[Mapping[str, Any]],
    merge_with_existing: bool = True,
    log: Any = None,
) -> int:
    """Upsert inverted-index rows to a persistent RAW table."""
    from cdf_fn_common.etl_cdf_utils import create_table_if_not_exists

    if not index_rows:
        return 0
    if client is None:
        raise ValueError("CogniteClient is required")

    queue = RawRowsUploadQueue(client)
    pending_flush: List[Dict[str, Any]] = []
    writes = 0

    for item in index_rows:
        if not isinstance(item, Mapping):
            continue
        row_key = _first_nonempty(item.get("key"))
        cols_in = item.get("columns")
        if not row_key or not isinstance(cols_in, Mapping):
            continue
        cols = dict(cols_in)
        if merge_with_existing:
            existing = load_existing_postings(client, raw_db, raw_table, row_key)
            try:
                incoming = json.loads(str(cols.get(POSTINGS_JSON_COLUMN) or "[]"))
            except json.JSONDecodeError:
                incoming = []
            if not isinstance(incoming, list):
                incoming = []
            merged = merge_postings(
                existing,
                [dict(x) for x in incoming if isinstance(x, dict)],
            )
            cols[POSTINGS_JSON_COLUMN] = json.dumps(merged, default=str)
        pending_flush.append({"key": row_key, "columns": cols})
        writes += 1
        if len(pending_flush) >= 500:
            create_table_if_not_exists(client, raw_db, raw_table, log)
            _flush_rows(queue, raw_db, raw_table, pending_flush, client=client)
            pending_flush.clear()

    if pending_flush:
        create_table_if_not_exists(client, raw_db, raw_table, log)
        _flush_rows(queue, raw_db, raw_table, pending_flush, client=client)

    return writes
