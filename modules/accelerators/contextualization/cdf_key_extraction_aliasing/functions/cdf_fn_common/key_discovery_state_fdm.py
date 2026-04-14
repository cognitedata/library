"""
Key Discovery incremental state in FDM (CDM-aligned views implementing CogniteDescribable).

Checkpoint: global listing cursor (highWatermarkMs). Processing state: per-source-record
hash and status for cohort skip logic.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

# Align with container property names (camelCase)
KEY_DISCOVERY_STATUS_PROCESSED = "processed"
KEY_DISCOVERY_STATUS_FAILED = "failed"

DESCRIBABLE_VIEW = ViewId(space="cdf_cdm", external_id="CogniteDescribable", version="v1")


def is_key_discovery_cdm_deployed(
    client: Any,
    processing_view_id: ViewId,
    checkpoint_view_id: ViewId,
    *,
    logger: Optional[Any] = None,
) -> bool:
    """
    Return True if both Key Discovery views exist in the project (deployed DMS views).

    When False, callers should use RAW watermark / EXTRACTION_INPUTS_HASH incremental state.
    """
    try:
        views = client.data_modeling.views.retrieve(
            [processing_view_id, checkpoint_view_id],
            all_versions=False,
        )
    except CogniteNotFoundError as ex:
        if logger and hasattr(logger, "warning"):
            logger.warning(
                "Key Discovery FDM views not found in project; using RAW incremental state: %s",
                ex,
            )
        return False
    except CogniteAPIError as ex:
        if logger and hasattr(logger, "warning"):
            logger.warning(
                "Key Discovery FDM views could not be retrieved (code=%s); using RAW incremental state: %s",
                getattr(ex, "code", None),
                ex,
            )
        return False
    got = {(v.space, v.external_id, v.version) for v in views}
    need = {
        (
            processing_view_id.space,
            processing_view_id.external_id,
            processing_view_id.version,
        ),
        (
            checkpoint_view_id.space,
            checkpoint_view_id.external_id,
            checkpoint_view_id.version,
        ),
    }
    if need <= got:
        return True
    missing = need - got
    if logger and hasattr(logger, "warning"):
        logger.warning(
            "Key Discovery FDM views missing from retrieve result %s; using RAW incremental state",
            missing,
        )
    return False


def _view_prop(view_id: ViewId, name: str) -> Tuple[str, str, str]:
    return (view_id.space, f"{view_id.external_id}/{view_id.version}", name)


def key_discovery_checkpoint_external_id(
    workflow_scope: str, source_view_fingerprint: str
) -> str:
    """Stable external id for a scope checkpoint node (kdsc prefix)."""
    raw = f"{workflow_scope}\0{source_view_fingerprint}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"kdsc_{h}"


def key_discovery_processing_external_id(
    workflow_scope: str,
    source_view_fingerprint: str,
    record_instance_key: str,
) -> str:
    """Stable external id for a per-record processing state node."""
    raw = f"{workflow_scope}\0{source_view_fingerprint}\0{record_instance_key}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
    return f"kdps_{h}"


def _fingerprint_filter(view_id: ViewId, fp: str) -> dm.filters.Filter:
    """Fingerprint is always stored as a string; use \"\" when no view disambiguation."""
    return dm.filters.Equals(
        property=_view_prop(view_id, "sourceViewFingerprint"),
        value=fp or "",
    )


def read_key_discovery_high_watermark_ms(
    client: Any,
    checkpoint_view_id: ViewId,
    instance_space: str,
    workflow_scope: str,
    source_view_fingerprint: str = "",
) -> Optional[int]:
    """Read highWatermarkMs from KeyDiscoveryScopeCheckpoint, or None if missing."""
    parts: List[dm.filters.Filter] = [
        dm.filters.HasData(views=[checkpoint_view_id]),
        dm.filters.Equals(
            property=_view_prop(checkpoint_view_id, "workflowScope"),
            value=workflow_scope,
        ),
    ]
    parts.append(_fingerprint_filter(checkpoint_view_id, source_view_fingerprint))
    filt = dm.filters.And(*parts)
    batch = client.data_modeling.instances.list(
        instance_type="node",
        space=instance_space,
        sources=[checkpoint_view_id],
        filter=filt,
        limit=5,
    )
    if not batch:
        return None
    node = batch[0]
    return _read_int_prop(node, checkpoint_view_id, "highWatermarkMs")


def _read_int_prop(node: Any, view_id: ViewId, prop: str) -> Optional[int]:
    view_block = _view_block(node, view_id)
    raw = view_block.get(prop)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _view_block(node: Any, view_id: ViewId) -> Dict[str, Any]:
    p = getattr(node, "properties", None)
    if isinstance(p, dict):
        block = p.get(view_id.space, {})
        if isinstance(block, dict):
            vb = block.get(f"{view_id.external_id}/{view_id.version}", {})
            if isinstance(vb, dict):
                return vb
    dump = node.dump() if hasattr(node, "dump") else {}
    props = dump.get("properties") or {}
    space_block = props.get(view_id.space) or {}
    view_block = space_block.get(f"{view_id.external_id}/{view_id.version}") or {}
    return dict(view_block) if isinstance(view_block, dict) else {}


def _read_str_prop(node: Any, view_id: ViewId, prop: str) -> Optional[str]:
    view_block = _view_block(node, view_id)
    raw = view_block.get(prop)
    if raw is None:
        return None
    return str(raw)


def upsert_scope_checkpoint(
    client: Any,
    checkpoint_view_id: ViewId,
    instance_space: str,
    workflow_scope: str,
    source_view_fingerprint: str,
    high_watermark_ms: int,
    *,
    logger: Optional[Any] = None,
) -> None:
    ext_id = key_discovery_checkpoint_external_id(
        workflow_scope, source_view_fingerprint
    )
    now = datetime.now(timezone.utc)
    ck_props = {
        "workflowScope": workflow_scope,
        "highWatermarkMs": int(high_watermark_ms),
        "updatedAt": now,
    }
    if source_view_fingerprint:
        ck_props["sourceViewFingerprint"] = source_view_fingerprint
    else:
        ck_props["sourceViewFingerprint"] = ""

    apply = NodeApply(
        space=instance_space,
        external_id=ext_id,
        sources=[
            NodeOrEdgeData(
                DESCRIBABLE_VIEW,
                {
                    "name": f"kd-checkpoint-{ext_id[:12]}",
                    "description": "Key Discovery scope checkpoint",
                },
            ),
            NodeOrEdgeData(checkpoint_view_id, ck_props),
        ],
    )
    client.data_modeling.instances.apply(nodes=[apply])
    if logger and hasattr(logger, "info"):
        logger.info(
            f"Upserted KeyDiscoveryScopeCheckpoint externalId={ext_id} highWatermarkMs={high_watermark_ms}"
        )


def load_key_discovery_scope_state_maps(
    client: Any,
    processing_view_id: ViewId,
    instance_space: str,
    workflow_scope: str,
    source_view_fingerprint: str = "",
    *,
    limit_per_page: int = 1000,
    logger: Optional[Any] = None,
) -> Tuple[Dict[str, str], Set[str]]:
    """
    Load lastSeenHash (for processed rows) and prior record instance keys from CDM.

    Returns (hash_by_node_id, prior_node_ids).
    """
    parts: List[dm.filters.Filter] = [
        dm.filters.HasData(views=[processing_view_id]),
        dm.filters.Equals(
            property=_view_prop(processing_view_id, "workflowScope"),
            value=workflow_scope,
        ),
    ]
    parts.append(_fingerprint_filter(processing_view_id, source_view_fingerprint))
    filt = dm.filters.And(*parts)

    hash_by_node: Dict[str, str] = {}
    prior_ids: Set[str] = set()

    cursor = None
    while True:
        kwargs: Dict[str, Any] = dict(
            instance_type="node",
            space=instance_space,
            sources=[processing_view_id],
            filter=filt,
            limit=limit_per_page,
        )
        if cursor is not None:
            kwargs["cursor"] = cursor
        batch = client.data_modeling.instances.list(**kwargs)
        if not batch:
            break
        for node in batch:
            nid = _read_str_prop(node, processing_view_id, "recordInstanceKey")
            if not nid:
                continue
            prior_ids.add(nid)
            st = (_read_str_prop(node, processing_view_id, "status") or "").lower()
            if st == KEY_DISCOVERY_STATUS_PROCESSED:
                h = _read_str_prop(node, processing_view_id, "lastSeenHash")
                if h:
                    hash_by_node[nid] = h
        cursor = getattr(batch, "cursor", None)
        if not cursor:
            break

    if logger and hasattr(logger, "debug"):
        logger.debug(
            f"load_key_discovery_scope_state_maps: {len(prior_ids)} prior, {len(hash_by_node)} hashes"
        )
    return hash_by_node, prior_ids


def upsert_key_discovery_processing_state_success(
    client: Any,
    processing_view_id: ViewId,
    instance_space: str,
    workflow_scope: str,
    source_view_fingerprint: str,
    record_instance_key: str,
    record_external_id: str,
    last_seen_hash: str,
    last_watermark_value_ms: Optional[int],
    hash_version: int = 2,
    *,
    logger: Optional[Any] = None,
) -> None:
    ext_id = key_discovery_processing_external_id(
        workflow_scope, source_view_fingerprint, record_instance_key
    )
    now = datetime.now(timezone.utc)
    props = {
        "workflowScope": workflow_scope,
        "sourceViewFingerprint": source_view_fingerprint or "",
        "recordExternalId": record_external_id or "",
        "recordInstanceKey": record_instance_key,
        "lastSeenHash": last_seen_hash,
        "processedAt": now,
        "status": KEY_DISCOVERY_STATUS_PROCESSED,
        "attemptCount": 0,
        "hashVersion": int(hash_version),
    }
    if last_watermark_value_ms is not None:
        props["lastWatermarkValue"] = int(last_watermark_value_ms)

    apply = NodeApply(
        space=instance_space,
        external_id=ext_id,
        sources=[
            NodeOrEdgeData(
                DESCRIBABLE_VIEW,
                {
                    "name": f"kd-state-{ext_id[:12]}",
                    "description": "Key Discovery processing state",
                },
            ),
            NodeOrEdgeData(processing_view_id, props),
        ],
    )
    client.data_modeling.instances.apply(nodes=[apply])
    if logger and hasattr(logger, "debug"):
        logger.debug(f"Upserted KeyDiscoveryProcessingState externalId={ext_id}")


def record_key_discovery_processing_failure(
    client: Any,
    processing_view_id: ViewId,
    instance_space: str,
    workflow_scope: str,
    source_view_fingerprint: str,
    record_instance_key: str,
    record_external_id: str,
    error_message: str,
    attempt_count: int = 1,
    *,
    logger: Optional[Any] = None,
) -> None:
    ext_id = key_discovery_processing_external_id(
        workflow_scope, source_view_fingerprint, record_instance_key
    )
    now = datetime.now(timezone.utc)
    props = {
        "workflowScope": workflow_scope,
        "sourceViewFingerprint": source_view_fingerprint or "",
        "recordExternalId": record_external_id or "",
        "recordInstanceKey": record_instance_key,
        "processedAt": now,
        "status": KEY_DISCOVERY_STATUS_FAILED,
        "attemptCount": int(attempt_count),
        "lastError": (error_message or "")[:8000],
    }
    apply = NodeApply(
        space=instance_space,
        external_id=ext_id,
        sources=[
            NodeOrEdgeData(
                DESCRIBABLE_VIEW,
                {
                    "name": f"kd-state-{ext_id[:12]}",
                    "description": "Key Discovery processing state",
                },
            ),
            NodeOrEdgeData(processing_view_id, props),
        ],
    )
    client.data_modeling.instances.apply(nodes=[apply])
    if logger and hasattr(logger, "warning"):
        logger.warning(f"Recorded KeyDiscoveryProcessingState failure externalId={ext_id}")


def view_id_for_key_discovery(
    schema_space: str,
    external_id: str,
    version: str,
) -> ViewId:
    return ViewId(space=schema_space, external_id=external_id, version=version)


def key_discovery_view_ids_from_parameters(params: Any) -> Tuple[ViewId, ViewId]:
    """Return (processing_state_view_id, checkpoint_view_id) from Pydantic parameters."""
    ss = str(getattr(params, "key_discovery_schema_space", None) or "dm_key_discovery")
    ver = str(getattr(params, "key_discovery_dm_version", None) or "v1")
    proc = str(
        getattr(params, "key_discovery_processing_state_view_external_id", None)
        or "KeyDiscoveryProcessingState"
    )
    ck = str(
        getattr(params, "key_discovery_checkpoint_view_external_id", None)
        or "KeyDiscoveryScopeCheckpoint"
    )
    return (
        ViewId(space=ss, external_id=proc, version=ver),
        ViewId(space=ss, external_id=ck, version=ver),
    )
