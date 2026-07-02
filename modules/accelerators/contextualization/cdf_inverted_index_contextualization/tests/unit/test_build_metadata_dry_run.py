"""Dry-run build commands must scan CDF without persisting."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.config import INDEX_FIELD_CONFIG, INDEX_STORAGE_CONFIG, SCOPE_CONFIG


def test_cmd_build_metadata_dry_run_queries_cdf() -> None:
    from local_runner.commands import cmd_build_metadata

    client = MagicMock()
    build_result = {
        "processed": 5,
        "entries_created": 3,
        "entries_updated": 0,
        "build_job_id": "job-1",
        "errors": [],
    }

    with patch(
        "local_runner.commands.create_cognite_client",
        return_value=client,
    ), patch(
        "local_runner.commands.build_metadata_index",
        return_value=build_result,
    ) as build_mock:
        result = cmd_build_metadata(dry_run=True)

    build_mock.assert_called_once()
    assert build_mock.call_args.args[0] is client
    call_kwargs = build_mock.call_args.kwargs
    assert call_kwargs["dry_run"] is True
    assert call_kwargs["storage_adapter"] is None
    assert result["processed"] == 5


def test_build_metadata_dry_run_counts_candidates_without_adapter() -> None:
    from inverted_index.build import build_metadata_index

    view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteEquipment")
    instances = {
        "CogniteEquipment": [
            {
                "externalId": "EQ-1",
                "properties": {"name": "P-101A", "description": "See P-102B"},
            }
        ],
    }

    result = build_metadata_index(
        MagicMock(),
        index_field_config=[view_cfg],
        scope_config=SCOPE_CONFIG,
        storage_config=INDEX_STORAGE_CONFIG,
        instances_by_view=instances,
        dry_run=True,
        storage_adapter=None,
    )

    assert result["processed"] == 1
    assert result["entries_created"] >= 1
    assert result.get("entries_updated", 0) == 0
    assert "scope_override_ambiguous" in result
    assert result["views"]["CogniteEquipment"]["scope_override_default"] == 1


def test_build_metadata_tracks_ambiguous_scope_override() -> None:
    from inverted_index.build import build_metadata_index

    scope_config = {
        "enabled": True,
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "strict_scope": False,
        "fallback_scope_key": "global",
        "resolve_from": {
            "CogniteEquipment": {
                "site": ["sourceContext"],
                "unit": ["sourceId"],
            }
        },
        "resolve_from_default": {},
    }
    view_cfg = {
        "view": "CogniteEquipment",
        "properties": [{"path": "description", "source_type": "asset_metadata"}],
        "properties_by_scope": {
            "site:Rotterdam|unit:*": {
                "mode": "merge",
                "properties": [
                    {"path": "description", "source_type": "asset_metadata", "extract_pattern": r"\bA\b"}
                ],
            },
            "site:*|unit:U100": {
                "mode": "merge",
                "properties": [
                    {"path": "description", "source_type": "asset_metadata", "extract_pattern": r"\bB\b"}
                ],
            },
        },
    }
    instances = {
        "CogniteEquipment": [
            {
                "externalId": "EQ-1",
                "properties": {
                    "sourceContext": "Rotterdam",
                    "sourceId": "U100",
                    "description": "Pump",
                },
            }
        ],
    }
    result = build_metadata_index(
        MagicMock(),
        index_field_config=[view_cfg],
        scope_config=scope_config,
        storage_config=INDEX_STORAGE_CONFIG,
        instances_by_view=instances,
        dry_run=True,
        storage_adapter=None,
    )
    assert result["scope_override_ambiguous"] == 1
    assert result["views"]["CogniteEquipment"]["scope_override_ambiguous"] == 1
    assert result["candidate_entries"] == 0
