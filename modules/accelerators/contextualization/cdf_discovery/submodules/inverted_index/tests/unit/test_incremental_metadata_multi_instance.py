"""Unit tests for multi-instance metadata indexing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.incremental import build_metadata_index_for_instance_ids


def test_build_metadata_index_for_instance_ids_aggregates() -> None:
    client = MagicMock()
    summaries = [
        {
            "postings_removed": 2,
            "entries_created": 3,
            "entries_updated": 1,
            "candidate_entries": 4,
            "errors": [],
        },
        {
            "postings_removed": 1,
            "entries_created": 2,
            "entries_updated": 0,
            "candidate_entries": 2,
            "errors": [],
        },
    ]

    with patch(
        "inverted_index.incremental.build_metadata_index_for_instance",
        side_effect=summaries,
    ) as single:
        result = build_metadata_index_for_instance_ids(
            client,
            ["EQ-1", "EQ-2"],
            view_external_id="CogniteEquipment",
            dry_run=True,
        )

    assert single.call_count == 2
    assert result["processed"] == 2
    assert result["postings_removed"] == 3
    assert result["entries_created"] == 5
    assert result["entries_updated"] == 1
    assert result["candidate_entries"] == 6
    assert result["instance_external_ids"] == ["EQ-1", "EQ-2"]
    assert len(result["results"]) == 2


def test_build_metadata_index_for_instance_ids_collects_errors() -> None:
    client = MagicMock()

    with patch(
        "inverted_index.incremental.build_metadata_index_for_instance",
        side_effect=[
            {
                "postings_removed": 0,
                "entries_created": 1,
                "entries_updated": 0,
                "candidate_entries": 1,
                "errors": [],
            },
            ValueError("Instance not found: space=cdf_cdm external_id=EQ-missing"),
        ],
    ):
        result = build_metadata_index_for_instance_ids(
            client,
            ["EQ-1", "EQ-missing"],
            view_external_id="CogniteEquipment",
            dry_run=True,
        )

    assert result["processed"] == 1
    assert len(result["errors"]) == 1
    assert result["errors"][0]["instance_external_id"] == "EQ-missing"
