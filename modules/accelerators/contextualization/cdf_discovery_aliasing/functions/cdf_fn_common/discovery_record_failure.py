"""Per-entity processing failure: cohort RAW status + optional Key Discovery FDM state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from .discovery_query_shared import _as_dict
from .incremental_listing import (
    KeyDiscoveryIncrementalBackend,
    flush_key_discovery_processing_failures,
    try_resolve_key_discovery_backend,
)
from .incremental_scope import (
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_FAILED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
)


@dataclass
class EntityFailureRecorder:
    """Accumulates per-entity failures for batch FDM flush at end of a task."""

    client: Any
    raw_db: str
    raw_table: str
    kd_backend: Optional[KeyDiscoveryIncrementalBackend] = None
    attempt_by_node: Dict[str, int] = field(default_factory=dict)
    max_processing_attempts: Optional[int] = None
    kd_failure_pending: List[Dict[str, Any]] = field(default_factory=list)
    entities_failed: int = 0

    def flush_fdm(self, *, log: Any = None) -> None:
        if self.kd_failure_pending:
            flush_key_discovery_processing_failures(
                self.client, self.kd_backend, self.kd_failure_pending, log=log
            )
            self.kd_failure_pending.clear()


def max_processing_attempts_from_data(data: Mapping[str, Any]) -> Optional[int]:
    """Read optional ``parameters.max_processing_attempts`` from scope configuration."""
    cfg = _as_dict(data.get("configuration"))
    params = _as_dict(cfg.get("parameters"))
    raw = params.get("max_processing_attempts")
    if raw is None:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def build_entity_failure_recorder(
    client: Any,
    data: Mapping[str, Any],
    *,
    raw_db: str,
    raw_table: str,
    log: Any = None,
) -> EntityFailureRecorder:
    cfg = _as_dict(data.get("configuration"))
    params = _as_dict(cfg.get("parameters"))
    kd_backend = try_resolve_key_discovery_backend(client, params, log=log)
    attempt_by_node: Dict[str, int] = {}
    if kd_backend is not None:
        from .key_discovery_state_fdm import load_key_discovery_scope_state_maps

        _hash, _prior, attempt_by_node = load_key_discovery_scope_state_maps(
            client,
            kd_backend.processing_view_id,
            kd_backend.instance_space,
            kd_backend.workflow_scope,
            kd_backend.source_view_fingerprint,
            logger=log,
        )
    return EntityFailureRecorder(
        client=client,
        raw_db=raw_db,
        raw_table=raw_table,
        kd_backend=kd_backend,
        attempt_by_node=attempt_by_node,
        max_processing_attempts=max_processing_attempts_from_data(data),
    )


def mark_cohort_entity_failed(
    client: Any,
    raw_db: str,
    raw_table: str,
    row_key: str,
    cols: Mapping[str, Any],
) -> None:
    """Upsert one cohort entity row with ``WORKFLOW_STATUS=failed``."""
    rk = str(row_key or "").strip()
    if not rk:
        return
    new_cols = dict(cols)
    new_cols[WORKFLOW_STATUS_COLUMN] = WORKFLOW_STATUS_FAILED
    new_cols[WORKFLOW_STATUS_UPDATED_AT_COLUMN] = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    )
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={rk: new_cols})


def record_entity_processing_failure(
    recorder: EntityFailureRecorder,
    *,
    row_key: str,
    cols: Mapping[str, Any],
    error_message: str,
    log: Any = None,
) -> bool:
    """
    Mark entity failed in RAW and queue Key Discovery FDM failure when backend is active.

    Returns False when ``max_processing_attempts`` is exceeded (entity skipped).
    """
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
    ext_id = str(cols.get(EXTERNAL_ID_COLUMN) or "").strip()
    prior_attempts = int(recorder.attempt_by_node.get(nid, 0)) if nid else 0
    attempt_count = prior_attempts + 1

    max_attempts = recorder.max_processing_attempts
    if max_attempts is not None and attempt_count > max_attempts:
        if log and hasattr(log, "warning"):
            log.warning(
                "Skipping entity %s: attempt_count=%s exceeds max_processing_attempts=%s",
                nid or row_key,
                attempt_count,
                max_attempts,
            )
        return False

    mark_cohort_entity_failed(
        recorder.client, recorder.raw_db, recorder.raw_table, row_key, cols
    )

    backend = recorder.kd_backend
    if backend is not None and nid:
        recorder.kd_failure_pending.append(
            {
                "workflow_scope": backend.workflow_scope,
                "source_view_fingerprint": backend.source_view_fingerprint,
                "record_instance_key": nid,
                "record_external_id": ext_id,
                "error_message": (error_message or "")[:8000],
                "attempt_count": attempt_count,
            }
        )
        recorder.attempt_by_node[nid] = attempt_count
        if len(recorder.kd_failure_pending) >= 500:
            recorder.flush_fdm(log=log)

    recorder.entities_failed += 1
    if log and hasattr(log, "warning"):
        log.warning(
            "Entity processing failed node=%s row_key=%s attempt=%s: %s",
            nid or "?",
            row_key,
            attempt_count,
            (error_message or "")[:500],
        )
    return True
