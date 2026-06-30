"""Unit tests for target-driven query property defaults and fallbacks."""

from cognite.client import data_modeling as dm

from inverted_index.build import build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.target_driven import (
    batch_exists_filter,
    batch_scan_top_level_properties,
    effective_query_fallbacks,
    process_target_driven_contextualization,
)
from local_runner.demo import GLOBAL_SCOPE, sample_equipment_instances


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


def test_effective_query_fallbacks_default_includes_name() -> None:
    assert effective_query_fallbacks({"query_property_fallbacks": ["name"]}) == ("name",)


def test_effective_query_fallbacks_respects_exclude_empty_aliases() -> None:
    cfg = {"query_property_fallbacks": ["name"], "exclude_empty_aliases": True}
    assert effective_query_fallbacks(cfg) == ()


def test_batch_scan_top_level_properties_includes_name_fallback() -> None:
    props = batch_scan_top_level_properties(
        "aliases",
        {"query_property_fallbacks": ["name"]},
    )
    assert props == ["aliases", "name"]


def test_batch_scan_top_level_properties_aliases_only_when_excluding_empty() -> None:
    props = batch_scan_top_level_properties(
        "aliases",
        {"query_property_fallbacks": ["name"], "exclude_empty_aliases": True},
    )
    assert props == ["aliases"]


def test_batch_exists_filter_ors_primary_and_fallbacks() -> None:
    view_id = dm.ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
    filt = batch_exists_filter(
        view_id,
        "aliases",
        {"query_property_fallbacks": ["name"]},
    )
    assert isinstance(filt, dm.filters.Or)


def test_target_driven_uses_name_fallback_when_aliases_empty() -> None:
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
        target_driven_config={
            "query_property": "aliases",
            "query_property_fallbacks": ["name"],
            "exclude_empty_aliases": False,
        },
        dry_run=True,
    )
    assert result["query_terms"] == ["P-101A"]
    assert result["references_found"] >= 1


def test_target_driven_skips_name_only_when_exclude_empty_aliases() -> None:
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
        target_driven_config={
            "query_property": "aliases",
            "query_property_fallbacks": ["name"],
            "exclude_empty_aliases": True,
        },
        dry_run=True,
    )
    assert result["skipped"] == 1
    assert result["reason"] == "no_query_terms"
    assert result["query_terms"] == []
