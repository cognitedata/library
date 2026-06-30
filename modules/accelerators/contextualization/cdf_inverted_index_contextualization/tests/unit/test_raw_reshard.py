"""Unit tests for term sub-partition reshard and multi-table purge."""

from inverted_index.config import (
    PARTITION_STRATEGY_TERM_FIRST_CHAR,
    PARTITION_STRATEGY_UNIFIED,
    RAW_TERM_PARTITION_POLICY,
)
from inverted_index.raw_ops import (
    check_partition_row_counts,
    merge_and_upsert_lookup_key,
    reshard_scope_partition,
    upsert_partition_registry,
)
from inverted_index.storage.raw_adapter import RawStorageAdapter
from inverted_index.storage.raw_keys import resolve_raw_partition_table


def _cfg() -> dict:
    return {
        "backend": "raw",
        "raw": {
            "database": "db_test",
            "table_template": "inverted_index__{scope_slug}",
            "registry_table": "inverted_index__registry",
        },
        "term_partition": {**RAW_TERM_PARTITION_POLICY, "enabled": True},
    }


def _entry(scope: str, term: str, ref: str) -> dict:
    return {
        "term": term.upper(),
        "normalized_term": term,
        "match_scope_key": scope,
        "source_type": "asset_metadata",
        "source_property": "tag",
        "reference_external_id": ref,
        "reference_type": "CogniteEquipment",
    }


def test_reshard_scope_partition_moves_rows_to_buckets() -> None:
    scope = "site:Test|unit:U1"
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=None)

    upsert_partition_registry(
        None,
        cfg,
        scope,
        resolve_raw_partition_table(scope, cfg),
        local_registry=adapter._local_registry,
        partition_strategy=PARTITION_STRATEGY_UNIFIED,
    )

    for term, ref in [("p101a", "EQ-1"), ("x202b", "EQ-2"), ("ポンプ101", "EQ-3")]:
        merge_and_upsert_lookup_key(
            None,
            cfg,
            scope,
            term,
            [_entry(scope, term, ref)],
            local_cache=adapter._local_partitions,
            local_registry=adapter._local_registry,
        )

    unified_table = resolve_raw_partition_table(scope, cfg)
    assert len(adapter._local_partitions.get(unified_table) or {}) == 3

    result = reshard_scope_partition(
        None,
        cfg,
        scope,
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )
    assert result["partition_strategy"] == PARTITION_STRATEGY_TERM_FIRST_CHAR
    assert result["source_row_count"] == 3
    assert not adapter._local_partitions.get(unified_table)

    hits_p = adapter.query_by_terms(["p101a"], match_scope_key=scope)
    hits_x = adapter.query_by_terms(["x202b"], match_scope_key=scope)
    hits_jp = adapter.query_by_terms(["ポンプ101"], match_scope_key=scope)
    assert len(hits_p) == 1
    assert len(hits_x) == 1
    assert len(hits_jp) == 1


def test_delete_subset_truncates_all_bucket_tables() -> None:
    scope = "global"
    cfg = _cfg()
    adapter = RawStorageAdapter(cfg, client=None)

    upsert_partition_registry(
        None,
        cfg,
        scope,
        resolve_raw_partition_table(scope, cfg),
        local_registry=adapter._local_registry,
        partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
    )
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "p101a",
        [_entry(scope, "p101a", "EQ-1")],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "x202b",
        [_entry(scope, "x202b", "EQ-2")],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )
    assert adapter.query_by_terms(["p101a", "x202b"], match_scope_key=scope)

    deleted = adapter.delete_subset(match_scope_key=scope)
    assert deleted >= 2
    assert adapter.query_by_terms(["p101a", "x202b"], match_scope_key=scope) == []


def test_check_partition_row_counts_recommends_reshard() -> None:
    scope = "global"
    cfg = _cfg()
    cfg["term_partition"]["activate_above_rows"] = 1
    adapter = RawStorageAdapter(cfg, client=None)

    upsert_partition_registry(
        None,
        cfg,
        scope,
        resolve_raw_partition_table(scope, cfg),
        local_registry=adapter._local_registry,
        partition_strategy=PARTITION_STRATEGY_UNIFIED,
    )
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        "p101a",
        [_entry(scope, "p101a", "EQ-1")],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )

    report = check_partition_row_counts(
        None,
        cfg,
        local_registry=adapter._local_registry,
        local_cache=adapter._local_partitions,
    )
    assert scope in report["reshard_recommended"]
