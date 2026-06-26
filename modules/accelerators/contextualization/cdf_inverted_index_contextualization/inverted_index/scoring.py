"""Scoring and detection-mode delta functions."""

from __future__ import annotations

from typing import Any

from inverted_index.normalize import normalize_term
from inverted_index.query import query_index_by_terms


def calculate_metadata_match_score(
    client: Any,
    terms: list[str],
    match_scope_key: str | None = None,
    file_external_id: str | None = None,
    include_metadata_only_terms: bool = False,
    storage_adapter: Any = None,
) -> dict:
    """Score how detected diagram terms align with metadata index entries."""
    del file_external_id, include_metadata_only_terms
    unique_terms = list({normalize_term(t) for t in terms if normalize_term(t)})
    if not unique_terms:
        return {
            "unique_detected_terms": 0,
            "metadata_match_rate": 0.0,
            "metadata_hits": [],
            "terms_without_metadata_match": [],
        }

    asset_hits = query_index_by_terms(
        client,
        terms,
        match_scope_key=match_scope_key,
        source_types=["asset_metadata"],
        storage_adapter=storage_adapter,
    )
    file_hits = query_index_by_terms(
        client,
        terms,
        match_scope_key=match_scope_key,
        source_types=["file_metadata"],
        storage_adapter=storage_adapter,
    )

    terms_with_asset = {h["normalized_term"] for h in asset_hits}
    terms_with_file = {h["normalized_term"] for h in file_hits}
    terms_with_any = terms_with_asset | terms_with_file
    without = [t for t in unique_terms if t not in terms_with_any]

    containing = {h.get("reference_external_id") for h in asset_hits + file_hits}
    by_view: dict[str, int] = {}
    for h in asset_hits + file_hits:
        rt = h.get("reference_type", "unknown")
        by_view[rt] = by_view.get(rt, 0) + 1

    n = len(unique_terms)
    return {
        "unique_detected_terms": n,
        "terms_with_asset_metadata_match": len(terms_with_asset),
        "terms_with_file_metadata_match": len(terms_with_file),
        "terms_with_any_metadata_match": len(terms_with_any),
        "asset_metadata_match_rate": len(terms_with_asset) / n if n else 0.0,
        "file_metadata_match_rate": len(terms_with_file) / n if n else 0.0,
        "metadata_match_rate": len(terms_with_any) / n if n else 0.0,
        "unique_metadata_containing_instances": len(containing),
        "metadata_by_containing_view": by_view,
        "terms_without_metadata_match": without,
        "metadata_subscore": len(terms_with_any) / n if n else 0.0,
        "metadata_hits": [
            {
                "term": h.get("term"),
                "normalized_term": h.get("normalized_term"),
                "source_type": h.get("source_type"),
                "reference_external_id": h.get("reference_external_id"),
                "reference_type": h.get("reference_type"),
                "source_property": h.get("source_property"),
            }
            for h in asset_hits + file_hits
        ],
    }


def _terms_for_file(
    client: Any,
    file_external_id: str,
    source_type: str,
    storage_adapter: Any = None,
    annotations: list[dict] | None = None,
    file_space: str = "cdf_cdm",
) -> set[str]:
    if storage_adapter is not None and hasattr(storage_adapter, "list_by_file"):
        entries = storage_adapter.list_by_file(
            file_external_id,
            source_types=[source_type],
            file_space=file_space,
        )
        if entries:
            return {
                e.get("normalized_term", "")
                for e in entries
                if e.get("normalized_term")
            }
    if annotations:
        terms = set()
        for ann in annotations:
            mode = ann.get("detection_mode", "pattern")
            st = f"diagram_annotation_{mode}"
            if st != source_type:
                continue
            if ann.get("file_external_id") != file_external_id:
                continue
            text = (
                ann.get("properties", {}).get("startNodeText")
                or ann.get("properties", {}).get("detected_text")
                or ann.get("properties", {}).get("extracted_text")
                or ann.get("text")
            )
            if text:
                terms.add(normalize_term(str(text)))
        return terms
    return set()


def get_detection_mode_delta(
    client: Any,
    file_external_id: str,
    left_source_type: str,
    right_source_type: str,
    storage_adapter: Any = None,
    annotations: list[dict] | None = None,
) -> list[dict]:
    """Set difference on normalized terms with enrichment from left source."""
    left_terms = _terms_for_file(
        client, file_external_id, left_source_type, storage_adapter, annotations
    )
    right_terms = _terms_for_file(
        client, file_external_id, right_source_type, storage_adapter, annotations
    )
    delta_norm = left_terms - right_terms
    results: list[dict] = []
    if storage_adapter:
        entries = storage_adapter.list_by_file(
            file_external_id, source_types=[left_source_type]
        )
        for entry in entries:
            if entry.get("normalized_term") not in delta_norm:
                continue
            meta = entry.get("additional_metadata") or {}
            results.append(
                {
                    "term": entry.get("term"),
                    "normalized_term": entry.get("normalized_term"),
                    "pages": [meta.get("page")] if meta.get("page") else [],
                    "vertices": meta.get("vertices"),
                    "bbox": meta.get("bbox"),
                    "confidence": meta.get("confidence"),
                    "symbol_type": meta.get("symbol_type"),
                }
            )
    elif annotations:
        for ann in annotations:
            mode = ann.get("detection_mode", "pattern")
            st = f"diagram_annotation_{mode}"
            if st != left_source_type:
                continue
            text = (
                ann.get("properties", {}).get("startNodeText")
                or ann.get("properties", {}).get("detected_text", "")
            )
            norm = normalize_term(text)
            if norm not in delta_norm:
                continue
            props = ann.get("properties", {})
            results.append(
                {
                    "term": text,
                    "normalized_term": norm,
                    "confidence": props.get("confidence"),
                    "symbol_type": props.get("symbol_type"),
                }
            )
    return results


def get_pattern_not_in_standard_delta(
    client: Any,
    file_external_id: str,
    file_space: str = "cdf_cdm",
    include_metadata_gap: bool = True,
    storage_adapter: Any = None,
    annotations: list[dict] | None = None,
    match_scope_key: str | None = None,
) -> list[dict]:
    """Missing tags: pattern terms not in standard detection."""
    del file_space
    delta = get_detection_mode_delta(
        client,
        file_external_id,
        "diagram_annotation_pattern",
        "diagram_annotation_standard",
        storage_adapter,
        annotations,
    )
    if include_metadata_gap and delta:
        terms = [d["term"] for d in delta if d.get("term")]
        meta = calculate_metadata_match_score(
            client,
            terms,
            match_scope_key=match_scope_key,
            storage_adapter=storage_adapter,
        )
        meta_by_term = {h["term"]: h for h in meta.get("metadata_hits", [])}
        for row in delta:
            term = row.get("term")
            row["metadata_scores"] = {
                "asset_metadata": term in meta_by_term
                and meta_by_term[term].get("source_type") == "asset_metadata",
                "file_metadata": term in meta_by_term
                and meta_by_term[term].get("source_type") == "file_metadata",
            }
            if not any(row["metadata_scores"].values()):
                row["reason"] = (
                    "No matching standard annotation or metadata index match"
                )
    return delta


def get_standard_not_in_pattern_delta(
    client: Any,
    file_external_id: str,
    file_space: str = "cdf_cdm",
    min_confidence_standard: float = 0.0,
    include_pattern_library_hints: bool = True,
    storage_adapter: Any = None,
    annotations: list[dict] | None = None,
) -> list[dict]:
    """Pattern improvement candidates: standard terms not in pattern."""
    del file_space
    delta = get_detection_mode_delta(
        client,
        file_external_id,
        "diagram_annotation_standard",
        "diagram_annotation_pattern",
        storage_adapter,
        annotations,
    )
    filtered = [
        d
        for d in delta
        if d.get("confidence") is None
        or float(d["confidence"]) >= min_confidence_standard
    ]
    if include_pattern_library_hints:
        for row in filtered:
            row["pattern_library_hints"] = {
                "suggested_sample": row.get("term"),
                "symbol_type": row.get("symbol_type"),
                "pages": row.get("pages", []),
            }
            row["reason"] = "In standard detection only; absent from pattern extraction"
    return filtered


def calculate_contextualization_score(
    client: Any,
    file_external_id: str,
    file_space: str = "cdf_cdm",
    include_metadata_scoring: bool = True,
    annotation_weight: float = 0.6,
    metadata_weight: float = 0.4,
    include_cdm_direct_relation_scoring: bool = True,
    storage_adapter: Any = None,
    annotations: list[dict] | None = None,
    match_scope_key: str | None = None,
) -> dict:
    """Compute contextualization quality for a diagram/file."""
    del include_cdm_direct_relation_scoring
    pattern_terms = _terms_for_file(
        client,
        file_external_id,
        "diagram_annotation_pattern",
        storage_adapter,
        annotations,
        file_space,
    )
    standard_terms = _terms_for_file(
        client,
        file_external_id,
        "diagram_annotation_standard",
        storage_adapter,
        annotations,
        file_space,
    )
    union = pattern_terms | standard_terms
    overlap = pattern_terms & standard_terms
    mode_overlap = len(overlap) / len(union) if union else 0.0

    pattern_only = pattern_terms - standard_terms
    standard_only = standard_terms - pattern_terms

    annotation_subscore = mode_overlap
    metadata_scores: dict = {}
    metadata_subscore = 0.0
    if include_metadata_scoring and pattern_terms:
        raw_terms = list(pattern_terms)
        metadata_scores = calculate_metadata_match_score(
            client,
            raw_terms,
            match_scope_key=match_scope_key,
            storage_adapter=storage_adapter,
        )
        metadata_subscore = metadata_scores.get("metadata_subscore", 0.0)

    overall = (
        annotation_weight * annotation_subscore + metadata_weight * metadata_subscore
    )

    return {
        "overall_score": round(overall, 4),
        "annotation_scores": {
            "pattern_detections": len(pattern_terms),
            "standard_detections": len(standard_terms),
            "mode_overlap_rate": round(mode_overlap, 4),
            "missing_in_standard_but_in_pattern": len(pattern_only),
            "missing_in_pattern_but_in_standard": len(standard_only),
            "annotation_subscore": round(annotation_subscore, 4),
        },
        "metadata_scores": metadata_scores,
        "details": {
            "terms_pattern_not_standard": sorted(pattern_only),
            "terms_standard_not_pattern": sorted(standard_only),
        },
    }
