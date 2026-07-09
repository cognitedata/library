"""Unit tests for remove_postings_for_reference on storage adapters."""

from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.entries import pattern_detection_to_index_entry
from inverted_index.incremental import upsert_diagram_detections
from inverted_index.raw_ops import merge_and_upsert_lookup_key
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.storage.raw_adapter import RawStorageAdapter


def _diagram_entry(file_id: str, text: str, *, page: int, bbox: list[float]) -> dict:
    detection = {
        "file_external_id": file_id,
        "text": text,
        "page": page,
        "bbox": bbox,
        "properties": {"confidence": 0.9, "status": "Suggested"},
    }
    entry = pattern_detection_to_index_entry(
        detection,
        detection_mode="pattern",
        scope_config=SCOPE_CONFIG,
    )
    assert entry is not None
    return entry


def test_memory_remove_postings_for_reference() -> None:
    adapter = MemoryStorageAdapter()
    entries = [
        _diagram_entry("FILE_1", "P-101A", page=1, bbox=[0.1, 0.2, 0.3, 0.4]),
        _diagram_entry("FILE_1", "P-102B", page=2, bbox=[0.2, 0.3, 0.4, 0.5]),
        _diagram_entry("FILE_2", "P-201A", page=1, bbox=[0.1, 0.1, 0.2, 0.2]),
    ]
    adapter.upsert_index_entries(entries)
    result = adapter.remove_postings_for_reference(
        match_scope_key=entries[0]["match_scope_key"],
        reference_external_id="FILE_1",
        reference_space="cdf_cdm",
        source_types=["diagram_annotation_pattern"],
    )
    assert result["postings_removed"] == 2
    remaining = list(adapter.entries.values())
    assert len(remaining) == 1
    assert remaining[0]["reference_external_id"] == "FILE_2"


def test_raw_remove_postings_strips_matching_rows() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    scope = "global"
    postings = [
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_1",
            "reference_space": "cdf_cdm",
            "reference_type": "CogniteFile",
            "source_property": "detection:a",
            "term": "P-101A",
            "additional_metadata": {"detection_key": "a"},
        },
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_2",
            "reference_space": "cdf_cdm",
            "reference_type": "CogniteFile",
            "source_property": "detection:b",
            "term": "P-201A",
            "additional_metadata": {"detection_key": "b"},
        },
    ]
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "p101a",
        [
            {
                "term": "P-101A",
                "normalized_term": "p101a",
                "match_scope_key": scope,
                **postings[0],
            },
        ],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "p201a",
        [
            {
                "term": "P-201A",
                "normalized_term": "p201a",
                "match_scope_key": scope,
                **postings[1],
            },
        ],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )
    result = adapter.remove_postings_for_reference(
        match_scope_key=scope,
        reference_external_id="FILE_1",
        reference_space="cdf_cdm",
        source_types=["diagram_annotation_pattern"],
    )
    assert result["postings_removed"] >= 1
    hits = adapter.list_by_file(
        "FILE_1",
        source_types=["diagram_annotation_pattern"],
        match_scope_key=scope,
    )
    assert hits == []
    other = adapter.list_by_file(
        "FILE_2",
        source_types=["diagram_annotation_pattern"],
        match_scope_key=scope,
    )
    assert len(other) == 1


def test_incremental_replace_removes_stale_detection() -> None:
    adapter = MemoryStorageAdapter()
    old = [
        {
            "file_external_id": "FILE_PID_12",
            "text": "OLD-TAG",
            "page": 1,
            "bbox": [0.1, 0.2, 0.3, 0.4],
            "properties": {"confidence": 0.5},
        }
    ]
    upsert_diagram_detections(
        None,
        old,
        detection_mode="pattern",
        write_mode="replace",
        file_external_id="FILE_PID_12",
        storage_adapter=adapter,
    )
    assert len(adapter.entries) == 1

    new = [
        {
            "file_external_id": "FILE_PID_12",
            "text": "P-101A",
            "page": 2,
            "bbox": [0.2, 0.3, 0.4, 0.5],
            "properties": {"confidence": 0.9},
        }
    ]
    result = upsert_diagram_detections(
        None,
        new,
        detection_mode="pattern",
        write_mode="replace",
        file_external_id="FILE_PID_12",
        storage_adapter=adapter,
    )
    assert result["postings_removed"] == 1
    terms = {e.get("term") for e in adapter.entries.values()}
    assert "OLD-TAG" not in terms
    assert "P-101A" in terms
