"""Unit tests for cross-scope tag reuse audit."""

from inverted_index.config import INDEX_STORAGE_CONFIG
from inverted_index.raw_ops import merge_and_upsert_lookup_key
from inverted_index.storage.raw_adapter import RawStorageAdapter
from inverted_index.tag_reuse import audit_cross_scope_tags

SCOPE_A = "site:Test|unit:U100"
SCOPE_B = "site:Test|unit:U200"
TERM = "p101a"


def _seed_scope_term(adapter: RawStorageAdapter, scope: str, ref: str) -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    merge_and_upsert_lookup_key(
        None,
        cfg,
        scope,
        TERM,
        [
            {
                "term": "P-101A",
                "normalized_term": TERM,
                "match_scope_key": scope,
                "reference_external_id": ref,
                "source_type": "asset_metadata",
                "source_property": "name",
            }
        ],
        local_cache=adapter._local_partitions,
        local_registry=adapter._local_registry,
    )


def test_audit_cross_scope_tags_finds_duplicate() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    _seed_scope_term(adapter, SCOPE_A, "EQ-1")
    _seed_scope_term(adapter, SCOPE_B, "EQ-2")

    report = audit_cross_scope_tags(
        None,
        cfg,
        scope_keys=[SCOPE_A, SCOPE_B],
        min_scope_count=2,
        storage_adapter=adapter,
    )
    assert report["scopes_scanned"] == 2
    assert report["lookup_keys_scanned"] >= 2
    by_term = report["reuse_metrics"]["by_term"]
    assert len(by_term) == 1
    assert by_term[0]["normalized_term"] == TERM
    assert by_term[0]["scope_count"] == 2


def test_audit_cross_scope_tags_emits_progress() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    _seed_scope_term(adapter, SCOPE_A, "EQ-1")
    _seed_scope_term(adapter, SCOPE_B, "EQ-2")

    messages: list[str] = []
    audit_cross_scope_tags(
        None,
        cfg,
        scope_keys=[SCOPE_A, SCOPE_B],
        min_scope_count=2,
        storage_adapter=adapter,
        on_progress=messages.append,
    )
    joined = "\n".join(messages)
    assert "[tag-reuse-audit] starting" in joined
    assert "scope 1/2" in joined
    assert "scope 1/2 complete" in joined
    assert "aggregating duplicates" in joined
    assert "complete" in joined


def test_audit_cross_scope_tags_min_scope_count() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    _seed_scope_term(adapter, SCOPE_A, "EQ-1")

    report = audit_cross_scope_tags(
        None,
        cfg,
        scope_keys=[SCOPE_A, SCOPE_B],
        min_scope_count=2,
        storage_adapter=adapter,
    )
    assert report["reuse_metrics"]["by_term"] == []
