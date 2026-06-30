"""Index build functions."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, Literal

from inverted_index.config import (
    ANNOTATION_INDEX_CONFIG,
    INDEX_FIELD_CONFIG,
    INDEX_STORAGE_CONFIG,
    SCOPE_CONFIG,
    VIRTUAL_TAG_CREATION_CONFIG,
)
from inverted_index.entries import (
    annotation_to_index_entry,
    build_entries_from_instance,
    pattern_detection_to_index_entry,
)
from inverted_index.cancellation import raise_if_cancelled
from inverted_index.sources.annotations import iter_view_instances, list_diagram_annotations
from inverted_index.storage import get_storage_adapter
from inverted_index.storage.raw_adapter import validate_raw_scope_config


def _format_metadata_progress(
    *,
    processed: int,
    candidate_entries: int,
    errors: int,
    elapsed_sec: float,
    view: str,
) -> str:
    return (
        f"[build-metadata] processed={processed} candidate_entries={candidate_entries} "
        f"errors={errors} elapsed={elapsed_sec:.1f}s view={view}"
    )


def upsert_index_entries(
    client: Any,
    entries: list[dict],
    storage_config: dict | None = None,
    *,
    dry_run: bool = False,
    storage_adapter: Any = None,
    on_progress: Callable[[str], None] | None = None,
    progress_interval: int = 100,
    should_cancel: Callable[[], bool] | None = None,
    log_prefix: str = "index-upsert",
    virtual_tag_creation_config: dict | None = None,
    scope_config: dict | None = None,
) -> dict:
    """Storage adapter entry point for DM or RAW backends."""
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    result = adapter.upsert_index_entries(
        entries,
        dry_run=dry_run,
        on_progress=on_progress,
        progress_interval=progress_interval,
        should_cancel=should_cancel,
        log_prefix=log_prefix,
    )
    vtc = virtual_tag_creation_config or VIRTUAL_TAG_CREATION_CONFIG
    if (
        client is not None
        and entries
        and not dry_run
        and vtc.get("enabled")
        and vtc.get("incremental_enabled")
    ):
        from inverted_index.virtual_tags import process_virtual_tags_for_index_entries

        vt_result = process_virtual_tags_for_index_entries(
            client,
            entries,
            virtual_tag_config=vtc,
            scope_config=scope_config or SCOPE_CONFIG,
            storage_config=cfg,
            storage_adapter=adapter,
            dry_run=False,
        )
        result["virtual_tags"] = vt_result
    return result


def remove_postings_for_reference(
    storage_adapter: Any,
    *,
    match_scope_key: str,
    reference_external_id: str,
    reference_space: str,
    source_types: list[str],
) -> dict:
    """Strip postings for a reference within a scope partition (replace mode)."""
    remover = getattr(storage_adapter, "remove_postings_for_reference", None)
    if not callable(remover):
        raise NotImplementedError(
            f"{type(storage_adapter).__name__} does not support remove_postings_for_reference"
        )
    return remover(
        match_scope_key=match_scope_key,
        reference_external_id=reference_external_id,
        reference_space=reference_space,
        source_types=source_types,
    )


def build_metadata_index(
    client: Any,
    space: str = "contextualization_idx",
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    instances_by_view: dict[str, list[dict]] | None = None,
    filter_updated_after: datetime | None = None,
    batch_size: int = 1000,
    dry_run: bool = False,
    storage_adapter: Any = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
    virtual_tag_creation_config: dict | None = None,
) -> dict:
    """
    Scan configured DM views and index asset tags / file references in metadata fields.

    ``instances_by_view`` allows local/demo injection: ``{view_external_id: [instance_dict, ...]}``.
    When omitted, queries CDF via ``client.data_modeling.instances.query``.
    """
    field_config = index_field_config or INDEX_FIELD_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    storage_cfg = dict(storage_config or INDEX_STORAGE_CONFIG)
    if storage_cfg.get("backend") == "raw":
        validate_raw_scope_config(scope_cfg)
    if space and storage_cfg.get("backend") == "dm":
        storage_cfg.setdefault("dm", {})["space"] = space

    build_job_id = str(uuid.uuid4())
    all_entries: list[dict] = []
    processed = 0
    errors: list[str] = []
    view_stats: dict[str, dict[str, int]] = {}
    started_at = time.monotonic()
    emit = on_progress if progress_interval > 0 else None
    view_names = [str(v.get("view", "")) for v in field_config if v.get("view")]
    if emit:
        emit(
            "[build-metadata] starting "
            f"views={','.join(view_names) or 'none'} dry_run={dry_run} "
            f"progress_interval={progress_interval}"
        )

    for view_cfg in field_config:
        raise_if_cancelled(should_cancel)
        view = view_cfg.get("view", "")
        view_space = view_cfg.get("view_space", "")
        configured_spaces = view_cfg.get("instance_spaces")
        spaces_label = (
            ",".join(str(s) for s in configured_spaces)
            if isinstance(configured_spaces, list) and configured_spaces
            else "all"
        )
        view_processed = 0
        view_entries = 0
        if emit:
            emit(
                f"[build-metadata] scanning view={view} view_space={view_space} "
                f"instance_spaces={spaces_label}"
            )

        if instances_by_view is not None:
            instance_iter: Iterable[dict] = instances_by_view.get(view, [])
        elif client is not None:
            try:
                instance_iter = iter_view_instances(
                    client,
                    view_config=view_cfg,
                    batch_size=batch_size,
                    filter_updated_after=filter_updated_after,
                    index_field_config=field_config,
                    scope_config=scope_cfg,
                )
            except Exception as exc:
                errors.append(f"{view}: {exc}")
                if emit:
                    emit(f"[build-metadata] view={view} error={exc}")
                continue
        else:
            instance_iter = []

        for instance in instance_iter:
            raise_if_cancelled(should_cancel)
            processed += 1
            view_processed += 1
            entries = build_entries_from_instance(
                instance,
                view_cfg,
                scope_cfg,
                build_job_id=build_job_id,
            )
            all_entries.extend(entries)
            view_entries += len(entries)
            if emit and processed % progress_interval == 0:
                emit(
                    _format_metadata_progress(
                        processed=processed,
                        candidate_entries=len(all_entries),
                        errors=len(errors),
                        elapsed_sec=time.monotonic() - started_at,
                        view=view,
                    )
                )

        if view:
            view_stats[view] = {
                "processed": view_processed,
                "candidate_entries": view_entries,
            }
            if emit:
                emit(
                    f"[build-metadata] view={view} complete "
                    f"processed={view_processed} candidate_entries={view_entries}"
                )

    raise_if_cancelled(should_cancel)

    candidate_entries = len(all_entries)
    if emit:
        if dry_run:
            emit(
                "[build-metadata] dry_run=true: RAW partition tables and "
                f"inverted_index__registry are not updated "
                f"(candidate_entries={candidate_entries})"
            )
        elif candidate_entries == 0:
            emit(
                "[build-metadata] no candidate entries; "
                "partition registry and RAW tables are unchanged"
            )
        emit(
            f"[build-metadata] upserting candidate_entries={candidate_entries} "
            f"dry_run={dry_run}"
        )

    upsert_result = upsert_index_entries(
        client,
        all_entries,
        storage_cfg,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        on_progress=on_progress,
        progress_interval=progress_interval,
        should_cancel=should_cancel,
        log_prefix="build-metadata",
        virtual_tag_creation_config=virtual_tag_creation_config,
        scope_config=scope_cfg,
    )

    registry_scopes: list[str] = []
    if (
        not dry_run
        and client is not None
        and storage_cfg.get("backend") == "raw"
        and candidate_entries > 0
    ):
        raise_if_cancelled(should_cancel)
        from inverted_index.raw_ops import list_registered_scope_keys

        registry_scopes = list_registered_scope_keys(client, storage_cfg)
        if emit and not registry_scopes:
            emit(
                "[build-metadata] warning: upsert completed but partition registry "
                "is still empty — check RAW write permissions and build errors"
            )
        elif emit:
            emit(
                "[build-metadata] registry scopes="
                f"{','.join(registry_scopes)}"
            )

    if emit:
        emit(
            "[build-metadata] complete "
            f"processed={processed} "
            f"candidate_entries={candidate_entries} "
            f"entries_created={upsert_result.get('entries_created', 0)} "
            f"entries_updated={upsert_result.get('entries_updated', 0)} "
            f"errors={len(errors)} elapsed={time.monotonic() - started_at:.1f}s"
        )
    return {
        "processed": processed,
        "candidate_entries": candidate_entries,
        "dry_run": dry_run,
        "entries_created": upsert_result.get("entries_created", 0),
        "entries_updated": upsert_result.get("entries_updated", 0),
        "registry_scopes": registry_scopes,
        "build_job_id": build_job_id,
        "errors": errors,
        "views": view_stats,
    }


def build_diagram_annotation_index(
    client: Any,
    space: str = "contextualization_idx",
    detection_mode: Literal["standard", "pattern", "all"] = "all",
    annotation_view: str = "CogniteDiagramAnnotation",
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    annotations: list[dict] | None = None,
    inline_detections: list[dict] | None = None,
    pattern_detections: list[dict] | None = None,
    filter_updated_after: datetime | None = None,
    batch_size: int = 500,
    instance_spaces: list[str] | None = None,
    dry_run: bool = False,
    storage_adapter: Any = None,
    should_cancel: Callable[[], bool] | None = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = None,
    virtual_tag_creation_config: dict | None = None,
) -> dict:
    """Index diagram annotations and index-only pattern/standard detections."""
    scope_cfg = scope_config or SCOPE_CONFIG
    storage_cfg = storage_config or INDEX_STORAGE_CONFIG
    if storage_cfg.get("backend") == "raw":
        validate_raw_scope_config(scope_cfg)
    build_job_id = str(uuid.uuid4())
    modes = (
        ["standard", "pattern"]
        if detection_mode == "all"
        else [detection_mode]
    )
    emit = on_progress if progress_interval > 0 else None
    started_at = time.monotonic()
    processed = 0

    if emit:
        emit(
            "[build-annotations] starting "
            f"detection_mode={detection_mode} dry_run={dry_run} "
            f"progress_interval={progress_interval}"
        )

    source_annotations = list(annotations or [])
    if not source_annotations and client is not None:
        if emit:
            emit("[build-annotations] fetching annotations from CDF")
        source_annotations = list_diagram_annotations(
            client,
            annotation_config=ANNOTATION_INDEX_CONFIG,
            instance_spaces=instance_spaces,
            filter_updated_after=filter_updated_after,
            detection_mode=detection_mode,
        )
        if emit:
            emit(f"[build-annotations] fetched annotations={len(source_annotations)}")
    all_entries: list[dict] = []
    for ann in source_annotations:
        raise_if_cancelled(should_cancel)
        processed += 1
        mode = ann.get("detection_mode") or ann.get("properties", {}).get(
            "detection_mode", "pattern"
        )
        if mode not in modes:
            continue
        linked_file = ann.get("linked_file")
        entry = annotation_to_index_entry(
            ann,
            detection_mode=mode,
            scope_config=scope_cfg,
            linked_file=linked_file,
            build_job_id=build_job_id,
        )
        if entry:
            all_entries.append(entry)
        if emit and processed % progress_interval == 0:
            emit(
                "[build-annotations] "
                f"processed={processed} candidate_entries={len(all_entries)} "
                f"elapsed={time.monotonic() - started_at:.1f}s"
            )

    index_only = list(inline_detections or pattern_detections or [])
    for detection in index_only:
        raise_if_cancelled(should_cancel)
        mode = str(detection.get("detection_mode") or "pattern")
        if mode not in modes:
            continue
        entry = pattern_detection_to_index_entry(
            detection,
            detection_mode=mode,
            scope_config=scope_cfg,
            build_job_id=build_job_id,
        )
        if entry:
            all_entries.append(entry)

    raise_if_cancelled(should_cancel)

    if emit:
        emit(
            f"[build-annotations] upserting candidate_entries={len(all_entries)} dry_run={dry_run}"
        )

    upsert_result = upsert_index_entries(
        client,
        all_entries,
        storage_cfg,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
        on_progress=on_progress,
        progress_interval=progress_interval,
        should_cancel=should_cancel,
        log_prefix="build-annotations",
        virtual_tag_creation_config=virtual_tag_creation_config,
        scope_config=scope_cfg,
    )
    if emit:
        emit(
            "[build-annotations] complete "
            f"processed={len(source_annotations) + len(index_only)} "
            f"entries_created={upsert_result.get('entries_created', 0)} "
            f"entries_updated={upsert_result.get('entries_updated', 0)} "
            f"elapsed={time.monotonic() - started_at:.1f}s"
        )
    return {
        "processed": len(source_annotations) + len(index_only),
        "entries_created": upsert_result.get("entries_created", 0),
        "entries_updated": upsert_result.get("entries_updated", 0),
        "build_job_id": build_job_id,
        "annotation_view": annotation_view,
        "errors": [],
    }
