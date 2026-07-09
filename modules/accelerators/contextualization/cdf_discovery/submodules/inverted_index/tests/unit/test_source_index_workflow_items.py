"""Unit tests for source index workflow item mapping."""

from __future__ import annotations

from inverted_index.config import INDEX_FIELD_CONFIG
from inverted_index.source_index import (
    resolve_source_watch_view_keys,
    workflow_item_to_source_index_event,
    workflow_items_to_source_index_events,
)

VIEWS = {
    "file": {"space": "cdf_cdm", "external_id": "CogniteFile", "version": "v1"},
    "asset": {"space": "cdf_cdm", "external_id": "CogniteAsset", "version": "v1"},
    "equipment": {"space": "cdf_cdm", "external_id": "CogniteEquipment", "version": "v1"},
    "timeseries": {"space": "cdf_cdm", "external_id": "CogniteTimeSeries", "version": "v1"},
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

SAMPLE_EQUIPMENT_ITEM = {
    "externalId": "EQ_1001",
    "space": "cdf_cdm",
    "properties": {
        "cdf_cdm": {
            "CogniteEquipment/v1": {
                "name": "P-101A",
                "description": "",
            }
        }
    },
}


def test_resolve_source_watch_view_keys_from_index_field_config() -> None:
    keys = resolve_source_watch_view_keys(INDEX_FIELD_CONFIG, {"views": VIEWS})
    assert set(keys) == {"file", "asset", "equipment", "timeseries"}


def test_workflow_item_to_source_index_event_file() -> None:
    event = workflow_item_to_source_index_event(
        SAMPLE_FILE_ITEM,
        views=VIEWS,
        watch_view_keys=["file", "asset", "equipment", "timeseries"],
        index_field_config=INDEX_FIELD_CONFIG,
        scope_config={},
    )
    assert event is not None
    assert event["externalId"] == "FILE_DOC_1"
    assert event["view_external_id"] == "CogniteFile"
    assert "name" in event["watch_properties"]


def test_workflow_item_to_source_index_event_equipment() -> None:
    event = workflow_item_to_source_index_event(
        SAMPLE_EQUIPMENT_ITEM,
        views=VIEWS,
        watch_view_keys=["file", "asset", "equipment", "timeseries"],
        index_field_config=INDEX_FIELD_CONFIG,
        scope_config={},
    )
    assert event is not None
    assert event["view_external_id"] == "CogniteEquipment"


def test_workflow_item_skips_empty_properties() -> None:
    item = {
        "externalId": "ASSET_EMPTY",
        "space": "cdf_cdm",
        "properties": {"cdf_cdm": {"CogniteAsset/v1": {"description": ""}}},
    }
    event = workflow_item_to_source_index_event(
        item,
        views=VIEWS,
        watch_view_keys=["asset"],
        index_field_config=INDEX_FIELD_CONFIG,
        scope_config={},
    )
    assert event is not None
    assert event["properties"]["description"] == ""


def test_workflow_items_to_source_index_events_batch() -> None:
    events = workflow_items_to_source_index_events(
        [SAMPLE_FILE_ITEM, SAMPLE_EQUIPMENT_ITEM],
        views=VIEWS,
        watch_view_keys=["file", "equipment"],
        index_field_config=INDEX_FIELD_CONFIG,
        scope_config={},
    )
    assert len(events) == 2
    assert {e["externalId"] for e in events} == {"FILE_DOC_1", "EQ_1001"}
