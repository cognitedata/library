"""Discovery inverted index: config-driven lookup keys → contextualization index entries."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping, MutableMapping, Optional, Tuple

from .cohort_storage import require_run_id
from .discovery_cohort import (
    iter_predecessor_instance_props,
    iter_predecessor_raw_locations,
)
from .discovery_validate import _normalize_field_values
from .discovery_query_shared import (
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    resolve_task_config,
    _first_nonempty,
)
from .index_entry_bridge import (
    pending_groups_to_index_entries,
    persist_index_entries_via_adapter,
    resolve_index_storage_config,
    resolve_match_scope_key_from_workflow,
)
from .task_runtime import merge_compiled_task_into_data

KIND_METADATA = "metadata"
KIND_FILE_ANNOTATION = "file_annotation"
KIND_ASSET_ANNOTATION = "asset_annotation"


def normalize_lookup_key(token: str) -> str:
    return str(token or "").strip().casefold()


def parse_index_kinds_config(cfg: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """
    Build ``(index_kind, property_name)`` pairs from ``cfg['index_kinds']`` only.

    No hardcoded property list — empty config means the handler skips indexing.
    """
    raw = cfg.get("index_kinds")
    if not isinstance(raw, dict) or not raw:
        return []
    out: List[Tuple[str, str]] = []
    for kind, props in raw.items():
        kind_s = str(kind or "").strip()
        if not kind_s:
            continue
        if not isinstance(props, list):
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


def _build_posting(
    *,
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
    index_kind: str,
    source_property: str,
    token: str,
    confidence: Optional[float],
    run_id: str,
) -> Dict[str, Any]:
    inst_space, ext_id, nid = _instance_identity_from_row(cols, props)
    posting: Dict[str, Any] = {
        "term": token,
        "instance_space": inst_space,
        "external_id": ext_id,
        "node_instance_id": nid,
        "view_space": _first_nonempty(cols.get(VIEW_SPACE_COLUMN)),
        "view_external_id": _first_nonempty(cols.get(VIEW_EXTERNAL_ID_COLUMN)),
        "view_version": _first_nonempty(cols.get(VIEW_VERSION_COLUMN), "v1"),
        "source_property": source_property,
        "index_kind": index_kind,
        "run_id": run_id,
    }
    if confidence is not None:
        posting["confidence"] = confidence
    return posting


def _posting_dedupe_key(posting: Mapping[str, Any]) -> Tuple[str, str, str]:
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
            by_key[_posting_dedupe_key(p)] = dict(p)
    for p in incoming:
        if not isinstance(p, dict):
            continue
        key = _posting_dedupe_key(p)
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


def run_discovery_inverted_index(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    index_pairs = parse_index_kinds_config(cfg)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    if not index_pairs:
        return {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "status": "skipped",
            "reason": "no_index_kinds_configured",
            "entries_created": 0,
            "entries_updated": 0,
            "entities": 0,
            "postings": 0,
        }

    if not client:
        raise ValueError("CogniteClient is required")

    run_id = require_run_id(data)
    data["run_id"] = run_id
    storage_config, scope_config = resolve_index_storage_config(data, cfg)
    match_scope_key, match_scope = resolve_match_scope_key_from_workflow(data, scope_config)
    source_type_overrides = cfg.get("source_type_overrides")
    if not isinstance(source_type_overrides, dict):
        source_type_overrides = {}

    pending: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    entities_seen: set[Tuple[str, str]] = set()
    rows_read = 0
    tokens_indexed = 0

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    for cols, props in iter_predecessor_instance_props(client, data, task_id):
        rows_read += 1
        inst_space, ext_id, _nid = _instance_identity_from_row(cols, props)
        if inst_space and ext_id:
            entities_seen.add((inst_space, ext_id))

        for index_kind, property_name in index_pairs:
            filtered_tokens = [
                (v, c)
                for v, c in _normalize_field_values(
                    props.get(property_name),
                    initial=1.0,
                    field=property_name,
                    parallel_source=props,
                )
            ]
            tokens_indexed += len(filtered_tokens)
            for token, conf in filtered_tokens:
                norm = normalize_lookup_key(token)
                if not norm:
                    continue
                pending[(index_kind, norm)].append(
                    _build_posting(
                        cols=cols,
                        props=props,
                        index_kind=index_kind,
                        source_property=property_name,
                        token=token,
                        confidence=conf,
                        run_id=run_id,
                    )
                )

    entries = pending_groups_to_index_entries(
        pending,
        match_scope_key=match_scope_key,
        match_scope=match_scope,
        build_job_id=run_id,
        source_type_overrides=source_type_overrides,
    )
    upsert_result = persist_index_entries_via_adapter(
        client,
        entries,
        storage_config,
        log=log,
    )

    index_kinds_configured = {
        kind: sorted({p for k, p in index_pairs if k == kind})
        for kind in sorted({k for k, _ in index_pairs})
    }

    summary = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "entries_created": int(upsert_result.get("entries_created") or 0),
        "entries_updated": int(upsert_result.get("entries_updated") or 0),
        "entities": len(entities_seen),
        "postings": len(entries),
        "storage_backend": storage_config.get("backend"),
        "match_scope_key": match_scope_key,
        "run_id": run_id,
        "index_kinds_configured": index_kinds_configured,
        "predecessor_raw_sources": [
            {"raw_db": d, "raw_table": t} for d, t in pred_locations
        ],
    }
    if storage_config.get("backend") == "raw":
        summary["raw_database"] = storage_config.get("raw", {}).get("database")

    if log and hasattr(log, "info"):
        log.info(
            "%s inverted_index backend=%s created=%s updated=%s rows_read=%s scope=%s",
            fn_external_id,
            summary["storage_backend"],
            summary["entries_created"],
            summary["entries_updated"],
            rows_read,
            match_scope_key,
        )
    return summary
