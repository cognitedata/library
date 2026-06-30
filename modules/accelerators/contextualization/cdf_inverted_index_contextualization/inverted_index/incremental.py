"""Incremental index writes for caller-supplied detections and DM instances."""

from __future__ import annotations

import uuid
from typing import Any, Literal

from inverted_index.build import remove_postings_for_reference, upsert_index_entries
from inverted_index.config import (
    ANNOTATION_INDEX_CONFIG,
    INDEX_FIELD_CONFIG,
    INDEX_STORAGE_CONFIG,
    SCOPE_CONFIG,
)
from inverted_index.entries import (
    annotation_to_index_entry,
    build_entries_from_instance,
    pattern_detection_to_index_entry,
)
from inverted_index.sources.annotations import _view_properties
from inverted_index.storage import get_storage_adapter
from inverted_index.storage.raw_adapter import validate_raw_scope_config

WriteMode = Literal["upsert", "replace"]
METADATA_SOURCE_TYPES = ["asset_metadata", "file_metadata"]


def _resolve_view_config(
    index_field_config: list[dict],
    *,
    view_external_id: str | None = None,
    incoming_view_key: str | None = None,
    direct_relation_config: dict | None = None,
) -> dict:
    resolved_external_id = view_external_id
    if not resolved_external_id and incoming_view_key and direct_relation_config:
        views = direct_relation_config.get("views") or {}
        ref = views.get(incoming_view_key) or {}
        resolved_external_id = ref.get("external_id")

    if resolved_external_id:
        for view_cfg in index_field_config:
            if view_cfg.get("view") == resolved_external_id:
                return view_cfg
        raise ValueError(
            f"view_external_id {resolved_external_id!r} not in index_field_config"
        )

    raise ValueError("view_external_id or incoming_view_key is required")


def instance_dict_from_node(
    node: Any,
    *,
    view_space: str,
    view: str,
    version: str,
) -> dict[str, Any]:
    props = _view_properties(
        node,
        view_space=view_space,
        view=view,
        version=version,
    )
    return {
        "externalId": getattr(node, "external_id", None) or getattr(node, "externalId", ""),
        "space": getattr(node, "space", None) or view_space,
        "properties": props,
    }


def _normalize_write_mode(write_mode: str) -> WriteMode:
    mode = str(write_mode or "replace").lower()
    if mode not in ("upsert", "replace"):
        raise ValueError(f"write_mode must be upsert or replace; got {write_mode!r}")
    return mode  # type: ignore[return-value]


def _infer_shared_file_external_id(detections: list[dict]) -> str | None:
    file_ids = {
        str(d.get("file_external_id") or d.get("reference_external_id") or "").strip()
        for d in detections
    }
    file_ids.discard("")
    if len(file_ids) == 1:
        return next(iter(file_ids))
    if len(file_ids) > 1:
        raise ValueError(
            "All detections in one call must share the same file_external_id for replace mode"
        )
    return None


def upsert_diagram_detections(
    client: Any,
    detections: list[dict] | None = None,
    *,
    detection_mode: Literal["standard", "pattern"] | None = None,
    write_mode: WriteMode = "replace",
    file_external_id: str | None = None,
    file_space: str = "cdf_cdm",
    annotations: list[dict] | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    annotation_config: dict | None = None,
    dry_run: bool = False,
    storage_adapter: Any = None,
    build_job_id: str | None = None,
    virtual_tag_creation_config: dict | None = None,
) -> dict:
    """Write external diagram detection results to the inverted index."""
    scope_cfg = scope_config or SCOPE_CONFIG
    storage_cfg = storage_config or INDEX_STORAGE_CONFIG
    if storage_cfg.get("backend") == "raw":
        validate_raw_scope_config(scope_cfg)
    ann_cfg = annotation_config or ANNOTATION_INDEX_CONFIG
    mode = _normalize_write_mode(write_mode)
    job_id = build_job_id or str(uuid.uuid4())

    detection_rows = list(detections or [])
    annotation_rows = list(annotations or [])
    if not detection_rows and not annotation_rows:
        return {
            "write_mode": mode,
            "postings_removed": 0,
            "entries_created": 0,
            "entries_updated": 0,
            "candidate_entries": 0,
            "skipped": 0,
            "errors": [],
            "build_job_id": job_id,
        }

    adapter = storage_adapter
    if adapter is None and not dry_run:
        adapter = get_storage_adapter(storage_cfg, client)

    entries: list[dict] = []
    skipped = 0
    errors: list[str] = []
    modes_seen: set[str] = set()

    for row in detection_rows:
        row_mode = str(row.get("detection_mode") or detection_mode or "pattern")
        if row_mode not in ("standard", "pattern"):
            errors.append(f"invalid detection_mode on row: {row_mode}")
            skipped += 1
            continue
        if detection_mode is None and not row.get("detection_mode"):
            pass
        entry = pattern_detection_to_index_entry(
            row,
            detection_mode=row_mode,  # type: ignore[arg-type]
            scope_config=scope_cfg,
            build_job_id=job_id,
            annotation_config=ann_cfg,
        )
        if entry:
            entries.append(entry)
            modes_seen.add(row_mode)
        else:
            skipped += 1

    for ann in annotation_rows:
        row_mode = str(ann.get("detection_mode") or detection_mode or "pattern")
        if row_mode not in ("standard", "pattern"):
            errors.append(f"invalid detection_mode on annotation: {row_mode}")
            skipped += 1
            continue
        entry = annotation_to_index_entry(
            ann,
            detection_mode=row_mode,  # type: ignore[arg-type]
            scope_config=scope_cfg,
            build_job_id=job_id,
            annotation_config=ann_cfg,
        )
        if entry:
            entries.append(entry)
            modes_seen.add(row_mode)
        else:
            skipped += 1

    resolved_file_id = file_external_id or _infer_shared_file_external_id(detection_rows)
    if not resolved_file_id and entries:
        resolved_file_id = entries[0].get("reference_external_id")
    resolved_file_space = file_space
    if detection_rows and detection_rows[0].get("file_space"):
        resolved_file_space = str(detection_rows[0]["file_space"])

    postings_removed = 0
    if mode == "replace" and entries and adapter is not None and not dry_run:
        if not resolved_file_id:
            raise ValueError("file_external_id is required for replace mode")
        scope_key = entries[0].get("match_scope_key") or ""
        replace_modes = modes_seen or ({detection_mode} if detection_mode else {"pattern"})
        for det_mode in replace_modes:
            remove_result = remove_postings_for_reference(
                adapter,
                match_scope_key=scope_key,
                reference_external_id=str(resolved_file_id),
                reference_space=resolved_file_space,
                source_types=[f"diagram_annotation_{det_mode}"],
            )
            postings_removed += int(remove_result.get("postings_removed", 0))

    upsert_result = upsert_index_entries(
        client,
        entries,
        storage_cfg,
        dry_run=dry_run,
        storage_adapter=adapter,
        log_prefix="incremental-detections",
        virtual_tag_creation_config=virtual_tag_creation_config,
        scope_config=scope_cfg,
    )

    return {
        "write_mode": mode,
        "postings_removed": postings_removed,
        "entries_created": upsert_result.get("entries_created", 0),
        "entries_updated": upsert_result.get("entries_updated", 0),
        "candidate_entries": len(entries),
        "skipped": skipped,
        "errors": errors,
        "build_job_id": job_id,
        "dry_run": dry_run or upsert_result.get("dry_run", False),
    }


def build_metadata_index_for_instance(
    client: Any,
    instance_external_id: str,
    *,
    view_external_id: str | None = None,
    incoming_view_key: str | None = None,
    direct_relation_config: dict | None = None,
    instance_space: str = "cdf_cdm",
    write_mode: WriteMode = "replace",
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    dry_run: bool = False,
    storage_adapter: Any = None,
    build_job_id: str | None = None,
    virtual_tag_creation_config: dict | None = None,
) -> dict:
    """Index metadata terms for a single DM instance."""
    if client is None:
        raise RuntimeError("CogniteClient is required for build_metadata_index_for_instance")

    field_config = index_field_config or INDEX_FIELD_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    storage_cfg = storage_config or INDEX_STORAGE_CONFIG
    if storage_cfg.get("backend") == "raw":
        validate_raw_scope_config(scope_cfg)
    mode = _normalize_write_mode(write_mode)
    job_id = build_job_id or str(uuid.uuid4())

    view_cfg = _resolve_view_config(
        field_config,
        view_external_id=view_external_id,
        incoming_view_key=incoming_view_key,
        direct_relation_config=direct_relation_config,
    )
    view = str(view_cfg.get("view", ""))
    view_space = str(view_cfg.get("view_space", instance_space))
    version = str(view_cfg.get("version", "v1"))

    nodes = client.data_modeling.instances.retrieve_nodes(
        [(instance_space, instance_external_id)]
    )
    if not nodes:
        raise ValueError(
            f"Instance not found: space={instance_space} external_id={instance_external_id}"
        )
    instance = instance_dict_from_node(
        nodes[0],
        view_space=view_space,
        view=view,
        version=version,
    )
    entries = build_entries_from_instance(
        instance,
        view_cfg,
        scope_cfg,
        build_job_id=job_id,
    )

    adapter = storage_adapter
    if adapter is None and not dry_run:
        adapter = get_storage_adapter(storage_cfg, client)

    postings_removed = 0
    if mode == "replace" and adapter is not None and not dry_run:
        scope_key = entries[0].get("match_scope_key", "") if entries else ""
        if not scope_key:
            for _view_cfg in field_config:
                probe = build_entries_from_instance(instance, _view_cfg, scope_cfg)
                if probe:
                    scope_key = probe[0].get("match_scope_key", "")
                    break
        remove_result = remove_postings_for_reference(
            adapter,
            match_scope_key=scope_key or "",
            reference_external_id=instance_external_id,
            reference_space=instance_space,
            source_types=METADATA_SOURCE_TYPES,
        )
        postings_removed = int(remove_result.get("postings_removed", 0))

    upsert_result = upsert_index_entries(
        client,
        entries,
        storage_cfg,
        dry_run=dry_run,
        storage_adapter=adapter,
        log_prefix="incremental-metadata",
        virtual_tag_creation_config=virtual_tag_creation_config,
        scope_config=scope_cfg,
    )

    return {
        "write_mode": mode,
        "postings_removed": postings_removed,
        "entries_created": upsert_result.get("entries_created", 0),
        "entries_updated": upsert_result.get("entries_updated", 0),
        "candidate_entries": len(entries),
        "instance_external_id": instance_external_id,
        "view_external_id": view,
        "errors": [],
        "build_job_id": job_id,
        "dry_run": dry_run or upsert_result.get("dry_run", False),
    }


def build_metadata_index_for_instance_ids(
    client: Any,
    instance_external_ids: list[str],
    *,
    view_external_id: str | None = None,
    incoming_view_key: str | None = None,
    direct_relation_config: dict | None = None,
    instance_space: str = "cdf_cdm",
    write_mode: WriteMode = "replace",
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    dry_run: bool = False,
    storage_adapter: Any = None,
    build_job_id: str | None = None,
    virtual_tag_creation_config: dict | None = None,
) -> dict:
    """Index metadata terms for multiple DM instances."""
    field_config = index_field_config or INDEX_FIELD_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    storage_cfg = storage_config or INDEX_STORAGE_CONFIG
    if storage_cfg.get("backend") == "raw":
        validate_raw_scope_config(scope_cfg)
    mode = _normalize_write_mode(write_mode)
    job_id = build_job_id or str(uuid.uuid4())
    view_cfg = _resolve_view_config(
        field_config,
        view_external_id=view_external_id,
        incoming_view_key=incoming_view_key,
        direct_relation_config=direct_relation_config,
    )
    view = str(view_cfg.get("view", ""))

    adapter = storage_adapter
    if adapter is None and not dry_run:
        adapter = get_storage_adapter(storage_cfg, client)

    processed = 0
    postings_removed = 0
    entries_created = 0
    entries_updated = 0
    candidate_entries = 0
    errors: list[dict] = []
    results: list[dict] = []

    common_kwargs = {
        "view_external_id": view_external_id,
        "incoming_view_key": incoming_view_key,
        "direct_relation_config": direct_relation_config,
        "instance_space": instance_space,
        "write_mode": mode,
        "index_field_config": field_config,
        "scope_config": scope_cfg,
        "storage_config": storage_cfg,
        "dry_run": dry_run,
        "storage_adapter": adapter,
        "build_job_id": job_id,
        "virtual_tag_creation_config": virtual_tag_creation_config,
    }

    for external_id in instance_external_ids:
        try:
            result = build_metadata_index_for_instance(
                client,
                external_id,
                **common_kwargs,
            )
        except Exception as exc:
            errors.append(
                {
                    "instance_external_id": external_id,
                    "instance_space": instance_space,
                    "error": str(exc),
                }
            )
            continue

        processed += 1
        postings_removed += int(result.get("postings_removed") or 0)
        entries_created += int(result.get("entries_created") or 0)
        entries_updated += int(result.get("entries_updated") or 0)
        candidate_entries += int(result.get("candidate_entries") or 0)
        for err in result.get("errors") or []:
            errors.append(
                {
                    "instance_external_id": external_id,
                    "instance_space": instance_space,
                    "error": str(err),
                }
            )
        if dry_run:
            results.append(result)

    return {
        "write_mode": mode,
        "processed": processed,
        "postings_removed": postings_removed,
        "entries_created": entries_created,
        "entries_updated": entries_updated,
        "candidate_entries": candidate_entries,
        "instance_external_ids": instance_external_ids,
        "instance_space": instance_space,
        "view_external_id": view,
        "errors": errors,
        "build_job_id": job_id,
        "dry_run": dry_run,
        "results": results if dry_run else None,
    }
