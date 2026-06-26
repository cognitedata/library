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
) -> dict:
    """Storage adapter entry point for DM or RAW backends."""
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    return adapter.upsert_index_entries(entries, dry_run=dry_run)


def build_metadata_index(
    client: Any,
    space: str = "contextualization_idx",
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    instances_by_view: dict[str, list[dict]] | None = None,
    filter_updated_after: datetime | None = None,
    batch_size: int = 1000,
    instance_spaces: list[str] | None = None,
    dry_run: bool = False,
    storage_adapter: Any = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
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
        if emit:
            emit(f"[build-metadata] scanning view={view} view_space={view_space}")

        if instances_by_view is not None:
            instance_iter: Iterable[dict] = instances_by_view.get(view, [])
        elif client is not None:
            try:
                instance_iter = iter_view_instances(
                    client,
                    view=view,
                    view_space=view_space,
                    version=view_cfg.get("version", "v1"),
                    batch_size=batch_size,
                    instance_spaces=instance_spaces,
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
            entries = build_entries_from_instance(
                instance,
                view_cfg,
                scope_cfg,
                build_job_id=build_job_id,
            )
            all_entries.extend(entries)
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

    raise_if_cancelled(should_cancel)

    if emit:
        emit(
            f"[build-metadata] upserting candidate_entries={len(all_entries)} dry_run={dry_run}"
        )

    upsert_result = upsert_index_entries(
        client,
        all_entries,
        storage_cfg,
        dry_run=dry_run,
        storage_adapter=storage_adapter,
    )
    if emit:
        emit(
            "[build-metadata] complete "
            f"processed={processed} "
            f"entries_created={upsert_result.get('entries_created', 0)} "
            f"entries_updated={upsert_result.get('entries_updated', 0)} "
            f"errors={len(errors)} elapsed={time.monotonic() - started_at:.1f}s"
        )
    return {
        "processed": processed,
        "entries_created": upsert_result.get("entries_created", 0),
        "entries_updated": upsert_result.get("entries_updated", 0),
        "build_job_id": build_job_id,
        "errors": errors,
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
