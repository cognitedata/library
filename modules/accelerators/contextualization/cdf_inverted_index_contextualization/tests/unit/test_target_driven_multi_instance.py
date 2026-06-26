"""Unit tests for multi-instance target-driven runs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.target_driven import run_target_driven_for_instance_ids


def test_run_target_driven_for_instance_ids_aggregates() -> None:
    client = MagicMock()
    summaries = [
        {"references_found": 2, "links_created": 1},
        {"references_found": 1, "links_created": 0, "skipped": True},
        {"error": "Instance not found"},
    ]

    with patch(
        "inverted_index.target_driven.process_target_driven_contextualization",
        side_effect=summaries,
    ) as proc:
        result = run_target_driven_for_instance_ids(
            client,
            ["A1", "A2", "A3"],
            instance_space="cdf_cdm",
            dry_run=True,
            progress_interval=0,
            on_progress=None,
        )

    assert proc.call_count == 3
    assert result["processed"] == 3
    assert result["skipped"] == 1
    assert result["references_found"] == 3
    assert result["links_created"] == 1
    assert len(result["errors"]) == 1
    assert result["instance_external_ids"] == ["A1", "A2", "A3"]


def test_parse_instance_id_args_dedupes() -> None:
    from local_runner.commands import parse_instance_id_args

    assert parse_instance_id_args("A1", ["A2", "A1,B3"]) == ["A1", "A2", "B3"]
