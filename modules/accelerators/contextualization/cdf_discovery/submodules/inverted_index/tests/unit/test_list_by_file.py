"""Unit tests for list_index_entries_by_file and adapter list_by_file."""

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.query import list_index_entries_by_file
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from local_runner.demo import sample_annotations, sample_equipment_instances


def _indexed_adapter() -> MemoryStorageAdapter:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    build_diagram_annotation_index(
        client=None,
        annotations=sample_annotations(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    return adapter


def test_memory_adapter_list_by_file_reference_cognite_file() -> None:
    adapter = _indexed_adapter()
    entries = adapter.list_by_file(
        "FILE_PID_12",
        source_types=["diagram_annotation_pattern"],
    )
    assert entries
    assert all(e.get("reference_type") == "CogniteFile" for e in entries)
    assert all(e.get("reference_external_id") == "FILE_PID_12" for e in entries)


def test_list_index_entries_by_file_helper() -> None:
    adapter = _indexed_adapter()
    entries = list_index_entries_by_file(
        None,
        "FILE_PID_12",
        source_types=["diagram_annotation_standard"],
        storage_adapter=adapter,
    )
    assert entries
    assert any(e.get("normalized_term") == "p101a" for e in entries)
