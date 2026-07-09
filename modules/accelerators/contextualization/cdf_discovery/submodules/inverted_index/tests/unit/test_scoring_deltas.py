"""Unit tests for scoped detection delta calculation."""

from inverted_index.scoring import (
    get_pattern_not_in_standard_delta,
    get_standard_not_in_pattern_delta,
)
from inverted_index.storage.memory_adapter import MemoryStorageAdapter

FILE_ID = "FILE_TEST_01"
SCOPE_A = "site:A|unit:U1"
SCOPE_B = "site:B|unit:U2"


def _file_entry(
    *,
    external_id: str,
    scope: str,
    source_type: str,
    term: str,
    normalized_term: str,
) -> dict:
    return {
        "external_id": external_id,
        "match_scope_key": scope,
        "source_type": source_type,
        "term": term,
        "normalized_term": normalized_term,
        "reference_type": "CogniteFile",
        "reference_external_id": FILE_ID,
        "reference_space": "cdf_cdm",
    }


def _adapter_with_two_scopes() -> MemoryStorageAdapter:
    adapter = MemoryStorageAdapter()
    adapter.upsert_index_entries(
        [
            _file_entry(
                external_id="a-pattern",
                scope=SCOPE_A,
                source_type="diagram_annotation_pattern",
                term="P-SCOPE-A",
                normalized_term="pscopea",
            ),
            _file_entry(
                external_id="b-pattern",
                scope=SCOPE_B,
                source_type="diagram_annotation_pattern",
                term="P-SCOPE-B",
                normalized_term="pscopeb",
            ),
            _file_entry(
                external_id="b-standard",
                scope=SCOPE_B,
                source_type="diagram_annotation_standard",
                term="P-SCOPE-B",
                normalized_term="pscopeb",
            ),
            _file_entry(
                external_id="a-standard-only",
                scope=SCOPE_A,
                source_type="diagram_annotation_standard",
                term="STD-ONLY-A",
                normalized_term="stdonlya",
            ),
        ]
    )
    return adapter


def test_missing_tags_scoped_to_single_partition() -> None:
    adapter = _adapter_with_two_scopes()
    missing = get_pattern_not_in_standard_delta(
        client=None,
        file_external_id=FILE_ID,
        include_metadata_gap=False,
        storage_adapter=adapter,
        match_scope_key=SCOPE_A,
    )
    norms = {row["normalized_term"] for row in missing}
    assert norms == {"pscopea"}


def test_missing_tags_merges_all_scopes_when_unscoped() -> None:
    adapter = _adapter_with_two_scopes()
    missing = get_pattern_not_in_standard_delta(
        client=None,
        file_external_id=FILE_ID,
        include_metadata_gap=False,
        storage_adapter=adapter,
        match_scope_key=None,
    )
    norms = {row["normalized_term"] for row in missing}
    assert norms == {"pscopea"}


def test_pattern_feedback_scoped_to_single_partition() -> None:
    adapter = _adapter_with_two_scopes()
    feedback = get_standard_not_in_pattern_delta(
        client=None,
        file_external_id=FILE_ID,
        include_pattern_library_hints=False,
        storage_adapter=adapter,
        match_scope_key=SCOPE_A,
    )
    norms = {row["normalized_term"] for row in feedback}
    assert norms == {"stdonlya"}


def test_pattern_feedback_merges_all_scopes_when_unscoped() -> None:
    adapter = _adapter_with_two_scopes()
    feedback = get_standard_not_in_pattern_delta(
        client=None,
        file_external_id=FILE_ID,
        include_pattern_library_hints=False,
        storage_adapter=adapter,
        match_scope_key=None,
    )
    norms = {row["normalized_term"] for row in feedback}
    assert norms == {"stdonlya"}
