"""Unit tests for source metadata index handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.source_index import handle_source_metadata_payload

VIEWS = {
    "file": {"space": "cdf_cdm", "external_id": "CogniteFile", "version": "v1"},
    "asset": {"space": "cdf_cdm", "external_id": "CogniteAsset", "version": "v1"},
}

SAMPLE_FILE_ITEM = {
    "externalId": "FILE_DOC_1",
    "space": "cdf_cdm",
    "properties": {
        "cdf_cdm": {
            "CogniteFile/v1": {
                "name": "PID-101",
                "description": "Drawing for P-101A",
            }
        }
    },
}


def _runtime() -> dict:
    return {
        "source_index_config": {
            "enabled": True,
            "watch_view_keys": ["file", "asset", "equipment", "timeseries"],
            "instance_spaces": [],
            "dedupe": {"enabled": False},
        },
        "direct_relation_config": {"views": VIEWS, "links": {}},
        "scope_config": {},
        "storage_config": {"backend": "raw", "raw": {"database": "db"}},
        "index_field_config": __import__(
            "inverted_index.config", fromlist=["INDEX_FIELD_CONFIG"]
        ).INDEX_FIELD_CONFIG,
    }


def test_handle_source_metadata_payload_items() -> None:
    client = MagicMock()
    with patch(
        "inverted_index.source_index.build_metadata_index_for_instance",
        return_value={"candidate_entries": 2, "entries_created": 2},
    ) as build:
        result = handle_source_metadata_payload(
            client,
            {"items": [SAMPLE_FILE_ITEM]},
            dry_run=True,
            runtime_config=_runtime(),
        )

    assert result.get("status") == "ok"
    assert result.get("trigger") == "workflow_data_modeling"
    assert result.get("processed") == 1
    assert result.get("ok_count") == 1
    build.assert_called_once()


def test_handle_source_metadata_payload_skips_empty_properties() -> None:
    client = MagicMock()
    item = {
        "externalId": "ASSET_EMPTY",
        "space": "cdf_cdm",
        "properties": {"cdf_cdm": {"CogniteAsset/v1": {"description": ""}}},
    }
    with patch("inverted_index.source_index.build_metadata_index_for_instance") as build:
        result = handle_source_metadata_payload(
            client,
            {"items": [item]},
            dry_run=True,
            runtime_config=_runtime(),
        )

    assert result.get("processed") == 1
    assert result["results"][0].get("reason") == "no_indexable_properties"
    build.assert_not_called()


def test_handle_source_metadata_payload_dedupe_skip() -> None:
    client = MagicMock()
    runtime = _runtime()
    runtime["source_index_config"]["dedupe"] = {
        "enabled": True,
        "cooldown_seconds": 300,
        "raw_database": "db",
        "state_table": "source_index_state",
    }
    with patch(
        "inverted_index.source_index.should_skip_source_index",
        return_value=True,
    ), patch("inverted_index.source_index.build_metadata_index_for_instance") as build:
        result = handle_source_metadata_payload(
            client,
            {"items": [SAMPLE_FILE_ITEM]},
            dry_run=False,
            runtime_config=runtime,
        )

    assert result["results"][0].get("reason") == "dedupe_cooldown"
    build.assert_not_called()


def test_handle_source_metadata_payload_force_bypasses_dedupe() -> None:
    client = MagicMock()
    runtime = _runtime()
    runtime["source_index_config"]["dedupe"] = {"enabled": True, "cooldown_seconds": 300}

    def _skip(*_args, **kwargs):
        return not kwargs.get("force", False)

    with patch(
        "inverted_index.source_index.should_skip_source_index",
        side_effect=_skip,
    ), patch(
        "inverted_index.source_index.build_metadata_index_for_instance",
        return_value={"candidate_entries": 1},
    ) as build:
        result = handle_source_metadata_payload(
            client,
            {"items": [SAMPLE_FILE_ITEM], "force": True},
            dry_run=True,
            runtime_config=runtime,
        )

    build.assert_called_once()
    assert result.get("ok_count") == 1
