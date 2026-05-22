"""Resolve incremental listing state backend (Key Discovery FDM vs RAW watermark)."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional

from cognite.client.data_classes.data_modeling.ids import ViewId

from .incremental_scope import (
    load_latest_hash_by_node_for_scope,
    read_watermark_high_ms,
    scope_watermark_row_key,
    write_incremental_watermark_raw,
)
from .key_discovery_state_fdm import (
    is_key_discovery_cdm_deployed,
    key_discovery_view_ids_from_parameters,
    load_key_discovery_scope_state_maps,
    read_key_discovery_high_watermark_ms,
    upsert_key_discovery_processing_state_success_batch,
    upsert_key_discovery_processing_state_failure_batch,
    upsert_scope_checkpoint,
)


@dataclass(frozen=True)
class KeyDiscoveryIncrementalBackend:
    """Key Discovery FDM checkpoint + processing-state (when views are deployed)."""

    instance_space: str
    workflow_scope: str
    source_view_fingerprint: str
    processing_view_id: ViewId
    checkpoint_view_id: ViewId


def try_resolve_key_discovery_backend(
    client: Any,
    ke_params: Mapping[str, Any],
    *,
    log: Any = None,
) -> Optional[KeyDiscoveryIncrementalBackend]:
    """
    Return a Key Discovery backend when ``key_discovery_instance_space`` and ``workflow_scope``
    are set and both Key Discovery views exist in the project; otherwise ``None`` (RAW fallback).
    """
    kd_space = str(ke_params.get("key_discovery_instance_space") or "").strip()
    wf_scope = str(ke_params.get("workflow_scope") or "").strip()
    if not kd_space or not wf_scope or client is None:
        return None
    proc_vid, ck_vid = key_discovery_view_ids_from_parameters(SimpleNamespace(**dict(ke_params)))
    if not is_key_discovery_cdm_deployed(client, proc_vid, ck_vid, logger=log):
        return None
    fp = str(ke_params.get("source_view_fingerprint") or "").strip()
    return KeyDiscoveryIncrementalBackend(
        instance_space=kd_space,
        workflow_scope=wf_scope,
        source_view_fingerprint=fp,
        processing_view_id=proc_vid,
        checkpoint_view_id=ck_vid,
    )


def read_listing_watermark_ms(
    client: Any,
    *,
    backend: Optional[KeyDiscoveryIncrementalBackend],
    raw_db: str,
    raw_table: str,
    scope_key: str,
    workflow_scope: str = "",
    source_view_fingerprint: str = "",
) -> Optional[int]:
    if backend is not None:
        return read_key_discovery_high_watermark_ms(
            client,
            backend.checkpoint_view_id,
            backend.instance_space,
            backend.workflow_scope,
            backend.source_view_fingerprint or source_view_fingerprint,
        )
    return read_watermark_high_ms(
        client,
        raw_db,
        raw_table,
        scope_watermark_row_key(scope_key, workflow_scope),
    )


def load_hash_by_node_for_scope(
    client: Any,
    *,
    backend: Optional[KeyDiscoveryIncrementalBackend],
    raw_db: str,
    raw_table: str,
    scope_key: str,
    workflow_scope: str = "",
    hash_index_cache: Any = None,
) -> Dict[str, str]:
    if backend is not None:
        hash_by_node, _prior, _attempts = load_key_discovery_scope_state_maps(
            client,
            backend.processing_view_id,
            backend.instance_space,
            backend.workflow_scope,
            backend.source_view_fingerprint,
        )
        return hash_by_node
    if callable(hash_index_cache):
        full_index = hash_index_cache(client, raw_db, raw_table, workflow_scope)
        return dict(full_index.get(scope_key, {}))
    return load_latest_hash_by_node_for_scope(
        client,
        raw_db,
        raw_table,
        scope_key,
        workflow_scope=workflow_scope,
    )


def write_listing_watermark_raw(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    workflow_scope: str,
    high_ms: int,
    run_id: str,
) -> None:
    """Persist watermark on the stable incremental RAW table (RAW backend only)."""
    write_incremental_watermark_raw(
        client,
        raw_db=raw_db,
        raw_table=raw_table,
        scope_key=scope_key,
        workflow_scope=workflow_scope,
        high_ms=high_ms,
        run_id=run_id,
    )


def write_listing_watermark_ms(
    client: Any,
    *,
    backend: KeyDiscoveryIncrementalBackend,
    high_ms: int,
    log: Any = None,
) -> None:
    upsert_scope_checkpoint(
        client,
        backend.checkpoint_view_id,
        backend.instance_space,
        backend.workflow_scope,
        backend.source_view_fingerprint,
        int(high_ms),
        logger=log,
    )


def flush_key_discovery_processing_states(
    client: Any,
    backend: Optional[KeyDiscoveryIncrementalBackend],
    pending: List[Dict[str, Any]],
    *,
    log: Any = None,
) -> None:
    if backend is None or not pending:
        return
    upsert_key_discovery_processing_state_success_batch(
        client,
        backend.processing_view_id,
        backend.instance_space,
        pending,
        logger=log,
    )


def flush_key_discovery_processing_failures(
    client: Any,
    backend: Optional[KeyDiscoveryIncrementalBackend],
    pending: List[Dict[str, Any]],
    *,
    log: Any = None,
) -> None:
    if backend is None or not pending:
        return
    upsert_key_discovery_processing_state_failure_batch(
        client,
        backend.processing_view_id,
        backend.instance_space,
        pending,
        logger=log,
    )
