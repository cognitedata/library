"""Unit tests for query and end-to-end demo flows."""

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.query import query_index_by_terms, resolve_query_scope_keys
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.storage.raw_adapter import RawStorageAdapter
from inverted_index.tag_reuse import summarize_tag_scope_reuse
from local_runner.demo import GLOBAL_SCOPE, run_demo, sample_annotations, sample_equipment_instances

SCOPE_KEY = GLOBAL_SCOPE


def test_query_scoped_lookup() -> None:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    hits = query_index_by_terms(
        None,
        ["P-101A"],
        match_scope_key=SCOPE_KEY,
        storage_adapter=adapter,
    )
    assert len(hits) >= 1
    assert hits[0]["reference_external_id"] == "EQ-1001"


def test_query_wrong_scope_returns_empty() -> None:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    hits = query_index_by_terms(
        None,
        ["P-101A"],
        match_scope_key="site:Rotterdam|unit:U200",
        storage_adapter=adapter,
    )
    # With global scope, wrong explicit scope returns empty
    assert hits == []


def test_demo_pipeline() -> None:
    report = run_demo(output_dir=None)
    assert report["index_entry_count"] > 0
    assert report["target_driven"]["references_found"] >= 1
    assert "overall_score" in report["contextualization_score"]


def test_diagram_index_and_deltas() -> None:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_diagram_annotation_index(
        client=None,
        annotations=sample_annotations(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    pattern = adapter.list_by_file(
        "FILE_PID_12", source_types=["diagram_annotation_pattern"]
    )
    assert len(pattern) >= 2


def test_multi_scope_query_and_reuse_summary() -> None:
    adapter = MemoryStorageAdapter()
    adapter.upsert_index_entries(
        [
            {
                "external_id": "eq-1",
                "term": "P-101A",
                "normalized_term": "p101a",
                "match_scope_key": "site:Test|unit:U100",
                "reference_external_id": "EQ-1",
                "source_type": "asset_metadata",
                "source_property": "name",
            },
            {
                "external_id": "eq-2",
                "term": "P-101A",
                "normalized_term": "p101a",
                "match_scope_key": "site:Test|unit:U200",
                "reference_external_id": "EQ-2",
                "source_type": "asset_metadata",
                "source_property": "name",
            },
        ]
    )
    hits = query_index_by_terms(
        None,
        ["P-101A"],
        match_scope_keys=["site:Test|unit:U100", "site:Test|unit:U200"],
        storage_adapter=adapter,
        strict_scope=False,
    )
    assert len(hits) == 2
    summary = summarize_tag_scope_reuse(
        hits,
        terms_queried=["p101a"],
        scopes_queried=["site:Test|unit:U100", "site:Test|unit:U200"],
    )
    assert summary["cross_scope_duplicate_count"] == 1


def test_resolve_all_scopes_falls_back_to_global_when_registry_empty(monkeypatch) -> None:
    from inverted_index import query as query_mod

    monkeypatch.setitem(query_mod.SCOPE_CONFIG, "enabled", False)
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    scopes = resolve_query_scope_keys(
        None,
        cfg,
        all_scopes=True,
        storage_adapter=adapter,
    )
    assert scopes == ["global"]
