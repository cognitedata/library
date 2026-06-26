"""Full index rebuild migration (file-as-reference diagram entry shape)."""

from __future__ import annotations

import logging
from typing import Any

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import SCOPE_CONFIG
from inverted_index.raw_ops import list_registered_scope_keys
from inverted_index.sources.annotations import list_diagram_annotations
from inverted_index.storage import get_storage_adapter

logger = logging.getLogger(__name__)


def _resolve_purge_scopes(
    client: Any,
    storage_config: dict,
    scope_config: dict,
    *,
    match_scope_keys: list[str] | None,
    storage_adapter: Any,
) -> list[str]:
    if match_scope_keys:
        return list(match_scope_keys)
    local_registry = getattr(storage_adapter, "_local_registry", None)
    scopes = list_registered_scope_keys(
        client,
        storage_config,
        local_registry=local_registry,
    )
    if scopes:
        return scopes
    fallback = scope_config.get("fallback_scope_key") or SCOPE_CONFIG.get(
        "fallback_scope_key", "global"
    )
    return [str(fallback)] if fallback else []


def migrate_index(
    client: Any,
    *,
    storage_config: dict,
    scope_config: dict,
    index_field_config: list[dict],
    instance_spaces: list[str] | None = None,
    match_scope_keys: list[str] | None = None,
    purge: bool = True,
    dry_run: bool = False,
    storage_adapter: Any = None,
) -> dict:
    """
    Purge RAW index partitions and rebuild metadata + diagram annotation indexes.

    Required after the diagram entry shape change (reference_type CogniteFile).
    Legacy CogniteDiagramAnnotation reference rows are dropped on purge, not migrated in place.
    """
    backend = (storage_config or {}).get("backend", "raw")
    if backend not in ("raw", "memory"):
        raise ValueError(
            f"migrate_index supports RAW/memory backends only (got {backend!r}); "
            "purge DM rows manually then run build-metadata and build-annotations"
        )

    adapter = storage_adapter or get_storage_adapter(storage_config, client)
    scopes = _resolve_purge_scopes(
        client,
        storage_config,
        scope_config,
        match_scope_keys=match_scope_keys,
        storage_adapter=adapter,
    )

    purged_partitions = 0
    purge_errors: list[str] = []
    if purge:
        if dry_run:
            purged_partitions = len(scopes)
        else:
            for scope in scopes:
                try:
                    deleted = adapter.delete_subset(match_scope_key=scope)
                    if deleted:
                        purged_partitions += 1
                        logger.info("Purged partition for scope %s", scope)
                except Exception as exc:
                    msg = f"{scope}: {exc}"
                    logger.exception("Partition purge failed for %s", scope)
                    purge_errors.append(msg)

    metadata_result: dict = {}
    if not dry_run:
        metadata_result = build_metadata_index(
            client,
            index_field_config=index_field_config,
            scope_config=scope_config,
            storage_config=storage_config,
            instance_spaces=instance_spaces,
            storage_adapter=adapter,
        )
    else:
        metadata_result = {"dry_run": True, "entries_created": 0, "entries_updated": 0}

    annotations = None
    if client is not None and not dry_run:
        annotations = list_diagram_annotations(
            client,
            instance_spaces=instance_spaces,
            detection_mode="all",
        )

    annotation_result = build_diagram_annotation_index(
        client,
        scope_config=scope_config,
        storage_config=storage_config,
        annotations=annotations,
        instance_spaces=instance_spaces,
        dry_run=dry_run,
        storage_adapter=adapter,
    )

    return {
        "migration": "file_as_reference_diagram_entries",
        "backend": backend,
        "scopes_purged": scopes,
        "purged_partitions": purged_partitions,
        "purge_errors": purge_errors,
        "dry_run": dry_run,
        "metadata": metadata_result,
        "annotations": annotation_result,
    }
