"""Unit tests for RAW storage adapter (local cache, no CDF)."""

from inverted_index.build import build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.query import query_index_by_terms
from inverted_index.raw_ops import list_registered_scope_keys, merge_and_upsert_lookup_key
from inverted_index.storage.raw_adapter import RawStorageAdapter
from local_runner.demo import GLOBAL_SCOPE, sample_equipment_instances


def test_raw_adapter_upsert_and_query() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        scope_config=SCOPE_CONFIG,
        storage_adapter=adapter,
    )
    hits = query_index_by_terms(
        None,
        ["P-101A"],
        match_scope_key=GLOBAL_SCOPE,
        storage_adapter=adapter,
    )
    assert len(hits) >= 1
    assert hits[0]["reference_external_id"] == "EQ-1001"


def test_raw_adapter_query_normalizes_input_terms() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        scope_config=SCOPE_CONFIG,
        storage_adapter=adapter,
    )
    hits = adapter.query_by_terms(
        ["P-101A"],
        match_scope_key=GLOBAL_SCOPE,
    )
    assert len(hits) >= 1
    assert hits[0]["reference_external_id"] == "EQ-1001"


def test_raw_partition_table_created_in_cache() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    table = adapter.ensure_registry("site:Rotterdam|unit:U100")
    assert table.startswith("inverted_index__")


def test_list_registered_scope_keys_offline() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    adapter.ensure_registry("site:Rotterdam|unit:U100")
    adapter.ensure_registry("site:Rotterdam|unit:U200")
    keys = list_registered_scope_keys(None, cfg, local_registry=adapter._local_registry)
    assert keys == ["site:Rotterdam|unit:U100", "site:Rotterdam|unit:U200"]


def test_raw_adapter_multi_scope_query() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    for scope, ref in (
        ("site:Test|unit:U100", "EQ-1"),
        ("site:Test|unit:U200", "EQ-2"),
    ):
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
                    "reference_external_id": ref,
                    "source_type": "asset_metadata",
                    "source_property": "name",
                }
            ],
            local_cache=adapter._local_partitions,
            local_registry=adapter._local_registry,
        )
    hits = query_index_by_terms(
        None,
        ["P-101A"],
        match_scope_keys=["site:Test|unit:U100", "site:Test|unit:U200"],
        storage_adapter=adapter,
        strict_scope=False,
    )
    assert len(hits) == 2
    scopes = {h["match_scope_key"] for h in hits}
    assert scopes == {"site:Test|unit:U100", "site:Test|unit:U200"}
