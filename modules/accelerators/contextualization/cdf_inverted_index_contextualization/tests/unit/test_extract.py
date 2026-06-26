"""Unit tests for term extraction and deduplication."""

from inverted_index.extract import dedupe_extracted_terms, extract_terms_from_property


def test_whole_value_extraction() -> None:
    cfg = {"path": "tag", "source_type": "asset_metadata"}
    result = extract_terms_from_property("P-101A", cfg)
    assert len(result) == 1
    assert result[0][0] == "P-101A"


def test_regex_empty_pattern_passthrough() -> None:
    cfg = {
        "path": "tag",
        "source_type": "asset_metadata",
        "extract_mode": "regex",
        "extract_pattern": "",
    }
    result = extract_terms_from_property("P-101A", cfg)
    assert len(result) == 1
    assert result[0][0] == "P-101A"


def test_regex_multiple_terms() -> None:
    cfg = {
        "path": "notes",
        "source_type": "asset_metadata",
        "extract_mode": "regex",
        "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
    }
    result = extract_terms_from_property("See P-101A and P-102B", cfg)
    terms = {t for t, _ in result}
    assert terms == {"P-101A", "P-102B"}


def test_regex_excludes_instance_aliases() -> None:
    cfg = {
        "path": "notes",
        "source_type": "asset_metadata",
        "extract_mode": "regex",
        "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
    }
    result = extract_terms_from_property(
        "See P-101A and P-102B",
        cfg,
        exclude_normalized_aliases={"p101a"},
    )
    terms = {t for t, _ in result}
    assert terms == {"P-102B"}


def test_dedupe_occurrence_count() -> None:
    candidates = [
        ("P-101A", {"match_start": 0, "match_end": 6}),
        ("P-101A", {"match_start": 10, "match_end": 16}),
    ]
    deduped = dedupe_extracted_terms(candidates)
    assert len(deduped) == 1
    assert deduped[0][1]["occurrence_count"] == 2


def test_read_property_path_nested_dm_view() -> None:
    from inverted_index.extract import read_property_path

    instance = {
        "externalId": "FILE_1",
        "properties": {
            "cdf_cdm": {
                "CogniteFile/v1": {
                    "name": "P-101A.pdf",
                    "description": "Pump P-101A",
                }
            }
        },
    }
    assert read_property_path(instance, "name") == "P-101A.pdf"
    assert read_property_path(instance, "description") == "Pump P-101A"
