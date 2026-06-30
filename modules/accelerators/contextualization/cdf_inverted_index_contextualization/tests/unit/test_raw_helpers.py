"""Unit tests for RAW postings helpers and term buckets."""

from inverted_index.config import (
    PARTITION_STRATEGY_TERM_FIRST_CHAR,
    PARTITION_STRATEGY_UNIFIED,
    RAW_TERM_PARTITION_POLICY,
)
from inverted_index.storage.raw_keys import (
    all_term_bucket_slugs,
    build_raw_postings_row_key,
    list_scope_partition_tables,
    merge_postings,
    resolve_raw_partition_table,
    scope_slug,
    term_bucket,
)


def _storage_config() -> dict:
    return {
        "raw": {"table_template": "inverted_index__{scope_slug}"},
        "term_partition": dict(RAW_TERM_PARTITION_POLICY),
    }


def test_scope_slug() -> None:
    assert scope_slug("site:Rotterdam|unit:U100") == "site_rotterdam_unit_u100"


def test_resolve_raw_partition_table_unified() -> None:
    cfg = _storage_config()
    name = resolve_raw_partition_table("site:Rotterdam|unit:U100", cfg)
    assert name.startswith("inverted_index__")
    assert len(name) <= 64


def test_resolve_raw_partition_table_sharded() -> None:
    cfg = _storage_config()
    name = resolve_raw_partition_table(
        "site:Rotterdam|unit:U100",
        cfg,
        normalized_term="p101a",
        partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR,
    )
    assert name.endswith("__p")
    assert len(name) <= 64


def test_term_bucket_latin_and_japanese() -> None:
    assert term_bucket("p101a") == "p"
    assert term_bucket("21pt1017") == "2"
    assert term_bucket("ポンプ101") == "kata"
    assert term_bucket("バルブ") == "kata"
    assert term_bucket("泵101").startswith("han_")


def test_all_term_bucket_slugs_includes_latin_and_han() -> None:
    buckets = all_term_bucket_slugs(bucket_mode="script_aware")
    assert "p" in buckets
    assert "kata" in buckets
    assert "han_4e" in buckets


def test_list_scope_partition_tables_unified_vs_sharded() -> None:
    cfg = _storage_config()
    scope = "site:Rotterdam|unit:U100"
    unified = list_scope_partition_tables(
        scope, cfg, partition_strategy=PARTITION_STRATEGY_UNIFIED
    )
    assert len(unified) == 1
    sharded = list_scope_partition_tables(
        scope, cfg, partition_strategy=PARTITION_STRATEGY_TERM_FIRST_CHAR
    )
    assert len(sharded) > 37


def test_build_raw_postings_row_key() -> None:
    key = build_raw_postings_row_key("site:Rotterdam|unit:U100", "p101a")
    assert key == "site:Rotterdam|unit:U100::p101a"


def test_merge_postings_dedupes_diagram_by_detection_key() -> None:
    a = [
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_1",
            "source_property": "detection:page1:bbox_aaa:x",
            "additional_metadata": {"detection_key": "page1:bbox_aaa:x"},
        }
    ]
    b = [
        {
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_1",
            "source_property": "detection:page1:bbox_bbb:x",
            "additional_metadata": {"detection_key": "page1:bbox_bbb:x"},
        }
    ]
    merged = merge_postings(a, b)
    assert len(merged) == 2


def test_merge_postings_dedupes() -> None:
    a = [
        {
            "source_type": "asset_metadata",
            "reference_external_id": "EQ-1",
            "source_property": "tag",
        }
    ]
    b = [
        {
            "source_type": "asset_metadata",
            "reference_external_id": "EQ-1",
            "source_property": "tag",
            "term": "X",
        }
    ]
    merged = merge_postings(a, b)
    assert len(merged) == 1
