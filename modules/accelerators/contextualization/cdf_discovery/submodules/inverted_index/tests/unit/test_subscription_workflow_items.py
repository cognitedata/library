"""Unit tests for workflow trigger items → subscription event mapping."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.subscription import (
    handle_aliases_subscription_payload,
    workflow_item_to_subscription_event,
    workflow_items_to_subscription_events,
)

VIEWS = {
    "asset": {"space": "cdf_cdm", "external_id": "CogniteAsset", "version": "v1"},
    "file": {"space": "cdf_cdm", "external_id": "CogniteFile", "version": "v1"},
}

SAMPLE_ASSET_ITEM = {
    "instanceType": "node",
    "externalId": "ASSET_P101",
    "space": "cdf_cdm",
    "properties": {
        "cdf_cdm": {
            "CogniteAsset/v1": {
                "aliases": ["P-101A", "p101a"],
                "name": "Pump 101",
            }
        }
    },
}

SAMPLE_FILE_ITEM = {
    "instanceType": "node",
    "externalId": "FILE_DOC_1",
    "space": "cdf_cdm",
    "properties": {
        "cdf_cdm": {
            "CogniteFile/v1": {
                "aliases": ["P-101A"],
                "name": "PID-101",
            }
        }
    },
}


def test_workflow_item_to_subscription_event_asset() -> None:
    event = workflow_item_to_subscription_event(
        SAMPLE_ASSET_ITEM,
        watch_property="aliases",
        views=VIEWS,
        watch_view_keys=["asset", "file"],
    )
    assert event is not None
    assert event["externalId"] == "ASSET_P101"
    assert event["view_external_id"] == "CogniteAsset"
    assert event["changed_properties"] == ["aliases"]
    assert event["after"]["properties"]["aliases"] == ["P-101A", "p101a"]


def test_workflow_item_to_subscription_event_file() -> None:
    event = workflow_item_to_subscription_event(
        SAMPLE_FILE_ITEM,
        watch_property="aliases",
        views=VIEWS,
        watch_view_keys=["asset", "file"],
    )
    assert event is not None
    assert event["view_external_id"] == "CogniteFile"


def test_workflow_item_skips_empty_aliases() -> None:
    item = {
        "externalId": "ASSET_EMPTY",
        "space": "cdf_cdm",
        "properties": {
            "cdf_cdm": {"CogniteAsset/v1": {"aliases": [], "name": "No aliases"}}
        },
    }
    event = workflow_item_to_subscription_event(
        item,
        watch_property="aliases",
        views=VIEWS,
        watch_view_keys=["asset"],
    )
    assert event is not None
    assert event["after"]["properties"]["aliases"] == []


def test_workflow_items_to_subscription_events_batch() -> None:
    events = workflow_items_to_subscription_events(
        [SAMPLE_ASSET_ITEM, SAMPLE_FILE_ITEM],
        watch_property="aliases",
        views=VIEWS,
        watch_view_keys=["asset", "file"],
    )
    assert len(events) == 2
    assert {e["externalId"] for e in events} == {"ASSET_P101", "FILE_DOC_1"}


def test_handle_aliases_subscription_payload_items() -> None:
    client = MagicMock()
    runtime = {
        "subscription_config": {
            "enabled": True,
            "watch_property": "aliases",
            "watch_view_keys": ["asset", "file"],
            "instance_spaces": [],
        },
        "direct_relation_config": {"views": VIEWS, "links": {}},
        "scope_config": {},
        "storage_config": {"backend": "raw", "raw": {"database": "db"}},
        "target_driven_config": {},
    }
    with patch(
        "inverted_index.subscription.process_target_driven_contextualization",
        return_value={"references_found": 1, "links_created": 1},
    ) as proc, patch(
        "inverted_index.subscription.should_skip_target_driven",
        return_value=False,
    ):
        result = handle_aliases_subscription_payload(
            client,
            {"items": [SAMPLE_ASSET_ITEM]},
            dry_run=True,
            runtime_config=runtime,
        )

    assert result.get("status") == "ok"
    assert result.get("trigger") == "workflow_data_modeling"
    assert result.get("processed") == 1
    assert result.get("ok_count") == 1
    proc.assert_called_once()
    assert proc.call_args.kwargs["incoming_view_key"] == "asset"


def test_handle_aliases_subscription_payload_skips_empty_aliases() -> None:
    client = MagicMock()
    runtime = {
        "subscription_config": {
            "enabled": True,
            "watch_property": "aliases",
            "watch_view_keys": ["asset"],
            "instance_spaces": [],
        },
        "direct_relation_config": {"views": VIEWS, "links": {}},
        "scope_config": {},
        "storage_config": {"backend": "raw", "raw": {"database": "db"}},
        "target_driven_config": {},
    }
    item = {
        "externalId": "ASSET_EMPTY",
        "space": "cdf_cdm",
        "properties": {"cdf_cdm": {"CogniteAsset/v1": {"aliases": []}}},
    }
    with patch(
        "inverted_index.subscription.process_target_driven_contextualization",
    ) as proc:
        result = handle_aliases_subscription_payload(
            client,
            {"items": [item]},
            dry_run=True,
            runtime_config=runtime,
        )

    assert result.get("processed") == 1
    assert result["results"][0].get("reason") == "query_property_unchanged_or_empty"
    proc.assert_not_called()
