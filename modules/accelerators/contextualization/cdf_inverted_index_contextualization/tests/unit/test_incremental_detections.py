"""Unit tests for incremental diagram detection writes."""

from inverted_index.incremental import upsert_diagram_detections
from inverted_index.storage.memory_adapter import MemoryStorageAdapter


def test_upsert_preserves_unrelated_postings() -> None:
    adapter = MemoryStorageAdapter()
    first = [
        {
            "file_external_id": "FILE_A",
            "text": "TAG-A",
            "page": 1,
            "bbox": [0.1, 0.2, 0.3, 0.4],
        }
    ]
    upsert_diagram_detections(
        None,
        first,
        detection_mode="pattern",
        write_mode="replace",
        file_external_id="FILE_A",
        storage_adapter=adapter,
    )
    second = [
        {
            "file_external_id": "FILE_B",
            "text": "TAG-B",
            "page": 1,
            "bbox": [0.2, 0.3, 0.4, 0.5],
        }
    ]
    upsert_diagram_detections(
        None,
        second,
        detection_mode="pattern",
        write_mode="upsert",
        file_external_id="FILE_B",
        storage_adapter=adapter,
    )
    terms = {e.get("term") for e in adapter.entries.values()}
    assert "TAG-A" in terms
    assert "TAG-B" in terms


def test_dry_run_does_not_write() -> None:
    adapter = MemoryStorageAdapter()
    result = upsert_diagram_detections(
        None,
        [
            {
                "file_external_id": "FILE_1",
                "text": "P-101A",
                "page": 1,
                "bbox": [0.1, 0.2, 0.3, 0.4],
            }
        ],
        detection_mode="standard",
        dry_run=True,
        storage_adapter=adapter,
    )
    assert result["dry_run"] is True
    assert adapter.entries == {}


def test_standard_detection_mode_source_type() -> None:
    adapter = MemoryStorageAdapter()
    upsert_diagram_detections(
        None,
        [
            {
                "file_external_id": "FILE_1",
                "text": "P-101A",
                "page": 1,
                "bbox": [0.1, 0.2, 0.3, 0.4],
            }
        ],
        detection_mode="standard",
        write_mode="replace",
        file_external_id="FILE_1",
        storage_adapter=adapter,
    )
    assert all(
        e.get("source_type") == "diagram_annotation_standard"
        for e in adapter.entries.values()
    )
