"""Unit tests for CDM annotation field mapping."""

from inverted_index.annotation_fields import (
    annotation_bbox,
    annotation_text,
    build_detection_key,
    build_deterministic_annotation_external_id,
    detection_mode_from_annotation,
)
from inverted_index.annotation_identity import compute_bbox_hash, compute_text_hash


def test_annotation_text_start_node_text() -> None:
    assert annotation_text({"startNodeText": "P-101A"}) == "P-101A"


def test_annotation_bbox_from_cdm_properties() -> None:
    bbox = annotation_bbox(
        {
            "startNodeXMin": 0.1,
            "startNodeYMin": 0.2,
            "startNodeXMax": 0.3,
            "startNodeYMax": 0.4,
        }
    )
    assert bbox == [0.1, 0.2, 0.3, 0.4]


def test_detection_mode_from_tags() -> None:
    mode = detection_mode_from_annotation(
        {"tags": ["pattern_mode"]},
        "ann-001",
    )
    assert mode == "pattern"


def test_default_identity_key_builders() -> None:
    bbox = [0.1, 0.2, 0.3, 0.4]
    bbox_hash = compute_bbox_hash(bbox, decimal_places=4, hex_length=8)
    text_hash = compute_text_hash("p101a", hex_length=8)
    assert build_detection_key(page=3, bbox=bbox, normalized_term="p101a") == (
        f"page3:bbox_{bbox_hash}:p101a"
    )
    assert build_deterministic_annotation_external_id(
        "FILE",
        page=3,
        normalized_term="p101a",
        bbox=bbox,
    ) == f"idx_ann_FILE_3_{text_hash}_{bbox_hash}"
