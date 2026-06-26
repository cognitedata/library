"""Unit tests for CDM annotation field mapping."""

from inverted_index.annotation_fields import (
    annotation_bbox,
    annotation_text,
    detection_mode_from_annotation,
)


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
