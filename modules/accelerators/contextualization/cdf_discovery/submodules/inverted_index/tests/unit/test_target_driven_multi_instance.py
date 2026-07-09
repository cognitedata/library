"""Unit tests for multi-instance target-driven runs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.target_driven import (
    _merge_references_found_by_type,
    _references_found_by_type,
    run_target_driven_for_instance_ids,
)


def test_references_found_by_type_counts_hits() -> None:
    hits = [
        {"reference_type": "CogniteFile"},
        {"reference_type": "CogniteFile"},
        {"reference_type": "CogniteEquipment"},
        {},
    ]
    assert _references_found_by_type(hits) == {
        "CogniteFile": 2,
        "CogniteEquipment": 1,
        "unknown": 1,
    }
    assert _merge_references_found_by_type(
        {"CogniteFile": 1},
        {"CogniteFile": 2, "CogniteAsset": 1},
    ) == {"CogniteFile": 3, "CogniteAsset": 1}


def test_run_target_driven_for_instance_ids_aggregates() -> None:
    client = MagicMock()
    summaries = [
        {
            "references_found": 2,
            "references_found_by_type": {"CogniteFile": 1, "CogniteEquipment": 1},
            "links_created": 1,
        },
        {
            "references_found": 1,
            "references_found_by_type": {"CogniteFile": 1},
            "links_created": 0,
            "skipped": True,
        },
        {"error": "Instance not found"},
    ]

    with patch(
        "inverted_index.target_driven.process_target_driven_contextualization",
        side_effect=summaries,
    ) as proc:
        result = run_target_driven_for_instance_ids(
            client,
            ["A1", "A2", "A3"],
            incoming_view_key="asset",
            instance_space="cdf_cdm",
            direct_relation_config={
                "views": {
                    "asset": {
                        "space": "cdf_cdm",
                        "external_id": "CogniteAsset",
                        "version": "v1",
                    },
                },
                "links": {},
            },
            dry_run=True,
            progress_interval=0,
            on_progress=None,
        )

    assert proc.call_count == 3
    assert result["processed"] == 3
    assert result["skipped"] == 1
    assert result["references_found"] == 3
    assert result["references_found_by_type"] == {
        "CogniteFile": 2,
        "CogniteEquipment": 1,
    }
    assert result["links_created"] == 1
    assert len(result["errors"]) == 1
    assert result["instance_external_ids"] == ["A1", "A2", "A3"]


def test_parse_instance_id_args_dedupes() -> None:
    from local_runner.commands import parse_instance_id_args

    assert parse_instance_id_args("A1", ["A2", "A1,B3"]) == ["A1", "A2", "B3"]
