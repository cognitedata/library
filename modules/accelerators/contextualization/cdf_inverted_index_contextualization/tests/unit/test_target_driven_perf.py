"""Unit tests for target-driven backfill performance helpers."""

from unittest.mock import MagicMock

from inverted_index.config import INDEX_STORAGE_CONFIG
from inverted_index.raw_ops import load_postings_rows_batch, merge_and_upsert_lookup_key
from inverted_index.storage.raw_adapter import RawStorageAdapter
from inverted_index.target_driven_dedupe import (
    TargetDrivenDedupeBuffer,
    flush_target_driven_dedupe_records,
)
from local_runner.demo import GLOBAL_SCOPE


def test_load_postings_rows_batch_offline() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    for term, ref in (("p101a", "EQ-1"), ("p101b", "EQ-2")):
        merge_and_upsert_lookup_key(
            None,
            cfg,
            GLOBAL_SCOPE,
            term,
            [
                {
                    "term": term.upper(),
                    "normalized_term": term,
                    "match_scope_key": GLOBAL_SCOPE,
                    "reference_external_id": ref,
                    "source_type": "asset_metadata",
                    "source_property": "name",
                }
            ],
            local_cache=adapter._local_partitions,
            local_registry=adapter._local_registry,
        )

    loaded = load_postings_rows_batch(
        None,
        cfg["raw"]["database"],
        adapter.ensure_registry(GLOBAL_SCOPE),
        [f"{GLOBAL_SCOPE}::p101a", f"{GLOBAL_SCOPE}::p101b"],
        local_cache=adapter._local_partitions,
    )
    assert len(loaded) == 2
    refs = {
        entry["reference_external_id"]
        for (_postings, _cols) in loaded.values()
        for entry in _postings
    }
    assert refs == {"EQ-1", "EQ-2"}


def test_raw_adapter_query_batches_terms_per_scope() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    for term, ref in (("taga", "EQ-A"), ("tagb", "EQ-B")):
        merge_and_upsert_lookup_key(
            None,
            cfg,
            GLOBAL_SCOPE,
            term,
            [
                {
                    "term": term.upper(),
                    "normalized_term": term,
                    "match_scope_key": GLOBAL_SCOPE,
                    "reference_external_id": ref,
                    "source_type": "asset_metadata",
                    "source_property": "name",
                }
            ],
            local_cache=adapter._local_partitions,
            local_registry=adapter._local_registry,
        )

    hits = adapter.query_by_terms(
        ["TAGA", "TAGB"],
        match_scope_key=GLOBAL_SCOPE,
    )
    refs = {hit["reference_external_id"] for hit in hits}
    assert refs == {"EQ-A", "EQ-B"}


def test_flush_target_driven_dedupe_records_batches_insert() -> None:
    client = MagicMock()
    count = flush_target_driven_dedupe_records(
        client,
        [
            {
                "instance_space": "cdf_cdm",
                "instance_external_id": "A1",
                "query_terms": ["P-101"],
                "scope_key": "global",
                "summary": {"links_created": 1, "references_found": 2},
            },
            {
                "instance_space": "cdf_cdm",
                "instance_external_id": "A2",
                "query_terms": ["P-102"],
                "scope_key": "global",
                "summary": {"links_created": 0, "references_found": 1},
            },
        ],
        cfg={"enabled": True},
    )
    assert count == 2
    client.raw.rows.insert.assert_called_once()
    row = client.raw.rows.insert.call_args.kwargs["row"]
    assert len(row) == 2
    assert all("TERMS_HASH" in cols for cols in row.values())


def test_target_driven_dedupe_buffer_flushes_in_chunks() -> None:
    client = MagicMock()
    buffer = TargetDrivenDedupeBuffer(chunk_size=2)
    for idx in range(3):
        buffer.append(
            "cdf_cdm",
            f"A{idx}",
            [f"P-{idx}"],
            "global",
            {"links_created": 1, "references_found": 1},
        )
    assert buffer.flush(client, force=True) == 3
    assert client.raw.rows.insert.call_count >= 1
