"""Build inverted index entry payloads from DM instances and annotations."""

from __future__ import annotations

import hashlib
from typing import Any

from inverted_index.annotation_fields import (
    annotation_bbox,
    annotation_page,
    annotation_text,
    build_deterministic_annotation_external_id,
    build_detection_key,
)
from inverted_index.config import ANNOTATION_INDEX_CONFIG
from inverted_index.extract import (
    dedupe_extracted_terms,
    extract_terms_from_property,
    read_property_path,
)
from inverted_index.normalize import normalize_term
from inverted_index.scope import resolve_match_scope

FILE_VIEW = "CogniteFile"


def build_entry_external_id(entry: dict[str, Any]) -> str:
    parts = [
        entry.get("normalized_term", ""),
        entry.get("source_type", ""),
        entry.get("reference_external_id", ""),
        entry.get("source_property", ""),
    ]
    source_type = entry.get("source_type") or ""
    if source_type.startswith("diagram_annotation_"):
        meta = entry.get("additional_metadata") or {}
        parts.append(meta.get("detection_key") or meta.get("annotation_external_id") or "")
    key = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    ref = entry.get("reference_external_id", "unknown")[:40]
    return f"iie_{digest}_{ref}"


def build_entries_from_instance(
    instance: dict[str, Any],
    view_config: dict,
    scope_config: dict,
    *,
    linked_file: dict | None = None,
    build_job_id: str | None = None,
) -> list[dict[str, Any]]:
    view = view_config.get("view", "")
    view_space = view_config.get("view_space", "")
    external_id = (
        instance.get("externalId")
        or instance.get("external_id")
        or read_property_path(instance, "externalId")
        or ""
    )
    if not external_id:
        return []

    instance_space = str(
        instance.get("space") or instance.get("instance_space") or view_space
    )

    match_scope_key, match_scope = resolve_match_scope(
        instance,
        view,
        scope_config,
        linked_file=linked_file,
    )
    if scope_config.get("strict_scope", False) and not match_scope_key:
        return []

    entries: list[dict[str, Any]] = []
    for prop_cfg in view_config.get("properties", []):
        path = prop_cfg.get("path", "")
        prop_cfg = {**prop_cfg, "path": path}
        value = read_property_path(instance, path)
        source_type = prop_cfg.get("source_type", "asset_metadata")
        candidates = extract_terms_from_property(value, prop_cfg)
        for term, frag in dedupe_extracted_terms(candidates):
            normalized = normalize_term(term)
            if not normalized:
                continue
            additional = {
                "source_view": view,
                "source_view_space": view_space,
                "instance_space": instance_space,
                "match_scope": match_scope,
                "term_kind": (
                    "asset_tag"
                    if source_type == "asset_metadata"
                    else "file_ref"
                    if source_type == "file_metadata"
                    else source_type
                ),
                **{k: v for k, v in frag.items() if k not in ("source_type",)},
            }
            entry = {
                "term": term,
                "normalized_term": normalized,
                "original_value": frag.get("original_value", term),
                "source_type": source_type,
                "source_property": path,
                "reference_external_id": str(external_id),
                "reference_space": instance_space,
                "reference_type": view,
                "match_scope_key": match_scope_key or "",
                "match_scope": match_scope,
                "additional_metadata": additional,
                "build_job_id": build_job_id,
            }
            entry["external_id"] = build_entry_external_id(entry)
            entries.append(entry)
    return entries


def _diagram_entry_metadata(
    *,
    text: str,
    normalized: str,
    detection_mode: str,
    page: int | None,
    bbox: list[float] | None,
    props: dict[str, Any],
    cfg: dict,
    file_external_id: str,
    file_space: str,
    annotation_external_id: str,
    match_scope: dict,
    end_node_external_id: str | None = None,
    end_node_space: str | None = None,
    store_vertices: bool = False,
) -> dict[str, Any]:
    detection_key = build_detection_key(page=page, bbox=bbox, normalized_term=normalized)
    if not annotation_external_id:
        annotation_external_id = build_deterministic_annotation_external_id(
            file_external_id,
            page=page,
            normalized_term=normalized,
            bbox=bbox,
        )
    additional: dict[str, Any] = {
        "page": page,
        "bbox": bbox,
        "confidence": props.get(cfg.get("confidence_property", "confidence")),
        "detection_mode": detection_mode,
        "extracted_text": text,
        "status": props.get(cfg.get("status_property", "status")),
        "file_external_id": file_external_id,
        "file_space": file_space,
        "annotation_external_id": annotation_external_id,
        "detection_key": detection_key,
        "end_node_external_id": end_node_external_id,
        "end_node_space": end_node_space,
        "linked_asset_extid": end_node_external_id,
        "match_scope": match_scope,
    }
    if store_vertices:
        region = props.get("region") or {}
        additional["vertices"] = region.get("vertices") or []
    return additional


def annotation_to_index_entry(
    annotation: dict[str, Any],
    *,
    detection_mode: str,
    scope_config: dict,
    linked_file: dict | None = None,
    build_job_id: str | None = None,
    annotation_config: dict | None = None,
) -> dict[str, Any] | None:
    """Map a CogniteDiagramAnnotation edge to an index entry (file-as-reference)."""
    cfg = annotation_config or ANNOTATION_INDEX_CONFIG
    props = annotation.get("properties") or annotation
    text = annotation_text(props, cfg)
    if not text:
        return None

    file_external_id = annotation.get("file_external_id") or ""
    file_space = annotation.get("file_space") or cfg.get("view_space", "cdf_cdm")
    if not file_external_id:
        return None

    annotation_external_id = (
        annotation.get("externalId") or annotation.get("external_id") or ""
    )
    normalized = normalize_term(text)
    if not normalized:
        return None

    ann_view = cfg.get("view", "CogniteDiagramAnnotation")
    match_scope_key, match_scope = resolve_match_scope(
        annotation,
        ann_view,
        scope_config,
        linked_file=linked_file,
    )
    if scope_config.get("strict_scope", False) and not match_scope_key:
        return None

    bbox = annotation_bbox(props, cfg)
    page = annotation_page(props, cfg)
    store_vertices = not cfg.get("index_store_vertices", False)

    additional = _diagram_entry_metadata(
        text=text,
        normalized=normalized,
        detection_mode=detection_mode,
        page=page,
        bbox=bbox,
        props=props,
        cfg=cfg,
        file_external_id=str(file_external_id),
        file_space=str(file_space),
        annotation_external_id=str(annotation_external_id),
        match_scope=match_scope,
        end_node_external_id=annotation.get("end_node_external_id"),
        end_node_space=annotation.get("end_node_space"),
        store_vertices=store_vertices,
    )

    source_type = f"diagram_annotation_{detection_mode}"
    detection_key = additional["detection_key"]
    entry = {
        "term": text,
        "normalized_term": normalized,
        "original_value": text,
        "source_type": source_type,
        "source_property": f"detection:{detection_key}",
        "reference_external_id": str(file_external_id),
        "reference_space": str(file_space),
        "reference_type": FILE_VIEW,
        "match_scope_key": match_scope_key or "",
        "match_scope": match_scope,
        "additional_metadata": additional,
        "build_job_id": build_job_id,
    }
    entry["external_id"] = build_entry_external_id(entry)
    return entry


def pattern_detection_to_index_entry(
    detection: dict[str, Any],
    *,
    detection_mode: str = "pattern",
    scope_config: dict,
    build_job_id: str | None = None,
    annotation_config: dict | None = None,
) -> dict[str, Any] | None:
    """Map an index-only pattern/standard detection dict to an index entry."""
    cfg = annotation_config or ANNOTATION_INDEX_CONFIG
    props = detection.get("properties") or detection
    text = (
        detection.get("text")
        or detection.get("extracted_text")
        or annotation_text(props, cfg)
    )
    if not text:
        return None

    file_external_id = (
        detection.get("file_external_id")
        or detection.get("reference_external_id")
        or ""
    )
    file_space = detection.get("file_space") or "cdf_cdm"
    if not file_external_id:
        return None

    normalized = normalize_term(str(text))
    if not normalized:
        return None

    bbox = detection.get("bbox") or annotation_bbox(props, cfg)
    page = detection.get("page")
    if page is None:
        page = annotation_page(props, cfg)

    pseudo_annotation = {
        "externalId": detection.get("annotation_external_id") or "",
        "file_external_id": file_external_id,
        "file_space": file_space,
        "properties": props,
    }
    match_scope_key, match_scope = resolve_match_scope(
        pseudo_annotation,
        cfg.get("view", "CogniteDiagramAnnotation"),
        scope_config,
    )
    if scope_config.get("strict_scope", False) and not match_scope_key:
        return None

    annotation_external_id = str(
        detection.get("annotation_external_id")
        or build_deterministic_annotation_external_id(
            str(file_external_id),
            page=page,
            normalized_term=normalized,
            bbox=bbox if isinstance(bbox, list) else None,
        )
    )

    additional = _diagram_entry_metadata(
        text=str(text),
        normalized=normalized,
        detection_mode=detection_mode,
        page=page,
        bbox=bbox if isinstance(bbox, list) else None,
        props=props if isinstance(props, dict) else {},
        cfg=cfg,
        file_external_id=str(file_external_id),
        file_space=str(file_space),
        annotation_external_id=annotation_external_id,
        match_scope=match_scope,
        store_vertices=bool(detection.get("vertices")),
    )
    if detection.get("vertices"):
        additional["vertices"] = detection["vertices"]

    source_type = f"diagram_annotation_{detection_mode}"
    detection_key = additional["detection_key"]
    entry = {
        "term": str(text),
        "normalized_term": normalized,
        "original_value": str(text),
        "source_type": source_type,
        "source_property": f"detection:{detection_key}",
        "reference_external_id": str(file_external_id),
        "reference_space": str(file_space),
        "reference_type": FILE_VIEW,
        "match_scope_key": match_scope_key or "",
        "match_scope": match_scope,
        "additional_metadata": additional,
        "build_job_id": build_job_id,
    }
    entry["external_id"] = build_entry_external_id(entry)
    return entry
