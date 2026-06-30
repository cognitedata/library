"""Unit tests for target-driven scope filtering and lookup override."""

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.target_driven import process_target_driven_contextualization
from local_runner.demo import GLOBAL_SCOPE, sample_annotations, sample_equipment_instances

SCOPE_A = "site:Rotterdam|unit:U100"


def _adapter_with_index() -> MemoryStorageAdapter:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    return adapter


def test_scope_lookup_override_finds_hits() -> None:
    adapter = _adapter_with_index()
    instance = {
        "externalId": "ASSET_P101",
        "space": "cdf_cdm",
        "properties": {"aliases": ["P-101A"]},
    }
    result = process_target_driven_contextualization(
        None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance_space="cdf_cdm",
        instance=instance,
        storage_adapter=adapter,
        match_scope_keys=[GLOBAL_SCOPE],
        scope_lookup_override=True,
        dry_run=True,
    )
    assert result["references_found"] >= 1
    assert result["lookup_scope_keys"] == [GLOBAL_SCOPE]


def test_scope_filter_skips_non_matching_asset() -> None:
    adapter = _adapter_with_index()
    instance = {
        "externalId": "ASSET_P101",
        "space": "cdf_cdm",
        "properties": {"aliases": ["P-101A"]},
    }
    result = process_target_driven_contextualization(
        None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance_space="cdf_cdm",
        instance=instance,
        storage_adapter=adapter,
        match_scope_keys=[SCOPE_A],
        scope_lookup_override=False,
        dry_run=True,
    )
    assert result["skipped"] == 1
    assert result["reason"] == "scope_filtered"


def test_scope_filter_allows_matching_asset() -> None:
    adapter = _adapter_with_index()
    instance = {
        "externalId": "ASSET_P101",
        "space": "cdf_cdm",
        "properties": {"aliases": ["P-101A"]},
    }
    result = process_target_driven_contextualization(
        None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance_space="cdf_cdm",
        instance=instance,
        storage_adapter=adapter,
        match_scope_keys=[GLOBAL_SCOPE],
        scope_lookup_override=False,
        dry_run=True,
    )
    assert result.get("reason") != "scope_filtered"
    assert result["references_found"] >= 1


def test_target_driven_counts_query_filtered_by_confidence() -> None:
    adapter = MemoryStorageAdapter()
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        storage_adapter=adapter,
    )
    annotations = sample_annotations()
    annotations[0]["properties"]["confidence"] = 0.4
    build_diagram_annotation_index(
        client=None,
        annotations=annotations,
        storage_config=cfg,
        storage_adapter=adapter,
    )
    instance = {
        "externalId": "ASSET_P101",
        "space": "cdf_cdm",
        "properties": {"aliases": ["P-101A"]},
    }
    result = process_target_driven_contextualization(
        None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance_space="cdf_cdm",
        instance=instance,
        storage_adapter=adapter,
        match_scope_keys=[GLOBAL_SCOPE],
        scope_lookup_override=True,
        min_confidence=0.6,
        dry_run=True,
    )
    assert result["query_filtered_by_confidence"] >= 1
    assert all(
        (h.get("additional_metadata") or {}).get("confidence", 1.0) >= 0.6
        or (h.get("additional_metadata") or {}).get("confidence") is None
        for h in (result.get("hits") or [])
    )


def test_target_driven_uses_configured_query_property_name() -> None:
    adapter = _adapter_with_index()
    instance = {
        "externalId": "ASSET_P101",
        "space": "cdf_cdm",
        "properties": {"name": "P-101A"},
    }
    result = process_target_driven_contextualization(
        None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance_space="cdf_cdm",
        instance=instance,
        storage_adapter=adapter,
        match_scope_keys=[GLOBAL_SCOPE],
        scope_lookup_override=True,
        query_property="name",
        dry_run=True,
    )
    assert result["query_property"] == "name"
    assert result["query_terms"] == ["P-101A"]
    assert result["references_found"] >= 1
