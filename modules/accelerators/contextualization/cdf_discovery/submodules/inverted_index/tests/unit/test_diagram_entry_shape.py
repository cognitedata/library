"""Unit tests for file-as-reference diagram index entry shape."""

from inverted_index.entries import annotation_to_index_entry, pattern_detection_to_index_entry
from inverted_index.config import SCOPE_CONFIG
from inverted_index.storage.raw_keys import merge_postings, posting_dedupe_key


def test_annotation_to_index_entry_uses_file_as_reference() -> None:
    ann = {
        "externalId": "ann_pat_001",
        "file_external_id": "FILE_PID_12",
        "file_space": "cdf_cdm",
        "properties": {
            "startNodeText": "P-101A",
            "confidence": 0.92,
            "status": "Suggested",
            "startNodePageNumber": 3,
            "startNodeXMin": 0.1,
            "startNodeYMin": 0.2,
            "startNodeXMax": 0.3,
            "startNodeYMax": 0.4,
        },
    }
    entry = annotation_to_index_entry(
        ann,
        detection_mode="pattern",
        scope_config=SCOPE_CONFIG,
    )
    assert entry is not None
    assert entry["reference_type"] == "CogniteFile"
    assert entry["reference_external_id"] == "FILE_PID_12"
    assert entry["source_property"].startswith("detection:")
    meta = entry["additional_metadata"]
    assert meta["annotation_external_id"] == "ann_pat_001"
    assert meta["detection_key"]


def test_multiple_detections_same_file_same_term_distinct_postings() -> None:
    base_meta = {
        "confidence": 0.9,
        "status": "Suggested",
        "page": 3,
    }
    postings = [
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_PID_12",
            "source_property": "detection:page3:bbox_aaa:p101a",
            "additional_metadata": {**base_meta, "detection_key": "page3:bbox_aaa:p101a"},
        },
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_PID_12",
            "source_property": "detection:page3:bbox_bbb:p101a",
            "additional_metadata": {**base_meta, "detection_key": "page3:bbox_bbb:p101a"},
        },
    ]
    merged = merge_postings([], postings)
    assert len(merged) == 2
    keys = {posting_dedupe_key(p) for p in merged}
    assert len(keys) == 2


def test_pattern_detection_to_index_entry_index_only() -> None:
    detection = {
        "file_external_id": "FILE_PID_12",
        "text": "P-101A",
        "page": 2,
        "bbox": [0.1, 0.2, 0.3, 0.4],
        "properties": {"confidence": 0.85, "status": "Suggested"},
    }
    entry = pattern_detection_to_index_entry(
        detection,
        detection_mode="pattern",
        scope_config=SCOPE_CONFIG,
    )
    assert entry is not None
    assert entry["reference_type"] == "CogniteFile"
    assert entry["additional_metadata"]["annotation_external_id"]
