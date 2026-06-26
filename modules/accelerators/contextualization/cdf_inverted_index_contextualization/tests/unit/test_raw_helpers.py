"""Unit tests for RAW postings helpers."""

from inverted_index.storage.raw_keys import (
    build_raw_postings_row_key,
    merge_postings,
    resolve_raw_partition_table,
    scope_slug,
)


def test_scope_slug() -> None:
    assert scope_slug("site:Rotterdam|unit:U100") == "site_rotterdam_unit_u100"


def test_resolve_raw_partition_table() -> None:
    cfg = {"raw": {"table_template": "inverted_index__{scope_slug}"}}
    name = resolve_raw_partition_table("site:Rotterdam|unit:U100", cfg)
    assert name.startswith("inverted_index__")
    assert len(name) <= 64


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
