"""Unit tests for RAW operations helpers."""

from unittest.mock import MagicMock

from inverted_index.config import INDEX_STORAGE_CONFIG
from inverted_index.raw_ops import (
    RawSchemaEnsurer,
    merge_and_upsert_lookup_key,
    upsert_partition_registry,
)
from inverted_index.storage.raw_adapter import RawStorageAdapter


def test_raw_schema_ensurer_caches_list_calls() -> None:
    client = MagicMock()
    client.raw.databases.list.return_value.as_names.return_value = ["db_test"]
    client.raw.tables.list.return_value.as_names.return_value = []

    ensurer = RawSchemaEnsurer(client)
    ensurer.ensure_table("db_test", "table_a")
    ensurer.ensure_table("db_test", "table_b")
    ensurer.ensure_table("db_test", "table_a")

    assert client.raw.databases.list.call_count == 1
    assert client.raw.tables.list.call_count == 2
    assert client.raw.tables.create.call_count == 2


def test_merge_and_upsert_skip_partition_setup() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    scope = "global"

    upsert_partition_registry(
        None,
        cfg,
        scope,
        adapter.ensure_registry(scope),
        local_registry=adapter._local_registry,
    )

    is_new = merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "p101a",
        [
            {
                "term": "P-101A",
                "normalized_term": "p101a",
                "match_scope_key": scope,
                "reference_external_id": "EQ-1",
                "source_type": "asset_metadata",
                "source_property": "name",
            }
        ],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
        skip_partition_setup=True,
    )

    assert is_new is True
    hits = adapter.query_by_terms(["P-101A"], match_scope_key=scope)
    assert len(hits) == 1
    assert hits[0]["reference_external_id"] == "EQ-1"
