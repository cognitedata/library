"""Unit tests for the alarm event ingest handler. No network access."""

import pytest
from pydantic import ValidationError

from handler import (
    DEFAULT_SYNC_INITIALIZE_CURSOR,
    FUNCTION_MAX_RETRIES,
    AlarmRow,
    FunctionOutput,
    HandlerInput,
    configure_sdk_retries,
    filter_missing,
    plan_ingest,
    row_to_record,
)

INSTANCE_SPACE = "inst_location"


def make_raw_row(key: str = "alm_pt92501_c1_raised", **overrides: str) -> dict[str, str]:
    row = {
        "key": key,
        "historianTagName": "VAL_23-PT-92501:X.Value",
        "asset_externalId": "23-PT-92501",
        "timeseries_externalId": "TS-PT92501",
        "operatingLimit_externalId": "23-PT-92501_limit",
        "eventTime": "2026-11-10T06:12:00Z",
        "eventType": "Raised",
        "severity": "High",
        "limitType": "High",
        "value": "46.8",
        "message": "23-PT-92501 pressure above high limit 45 barg",
    }
    row.update(overrides)
    return row


def test_row_to_record_maps_properties_and_directs() -> None:
    record = row_to_record(AlarmRow.model_validate(make_raw_row()), INSTANCE_SPACE)

    assert record.space == INSTANCE_SPACE
    assert record.external_id == "alm_pt92501_c1_raised"
    assert record.properties == {
        "eventTime": "2026-11-10T06:12:00.000Z",
        "eventType": "Raised",
        "severity": "High",
        "limitType": "High",
        "value": 46.8,
        "message": "23-PT-92501 pressure above high limit 45 barg",
        "asset": {"space": INSTANCE_SPACE, "externalId": "23-PT-92501"},
        "timeSeries": {"space": INSTANCE_SPACE, "externalId": "TS-PT92501"},
        "operatingLimit": {"space": INSTANCE_SPACE, "externalId": "23-PT-92501_limit"},
    }


def test_row_to_record_omits_empty_optional_values() -> None:
    row = AlarmRow.model_validate(make_raw_row(operatingLimit_externalId="", value="", message=""))

    properties = row_to_record(row, INSTANCE_SPACE).properties

    assert "operatingLimit" not in properties
    assert "value" not in properties
    assert "message" not in properties


def test_row_rejects_unknown_event_type() -> None:
    with pytest.raises(ValidationError):
        AlarmRow.model_validate(make_raw_row(eventType="Escalated"))


def test_filter_missing_drops_records_already_in_stream() -> None:
    records = [
        row_to_record(AlarmRow.model_validate(make_raw_row(key=key)), INSTANCE_SPACE)
        for key in ("alm_1", "alm_2", "alm_3")
    ]

    missing = filter_missing(records, existing_external_ids={"alm_2"})

    assert [record.external_id for record in missing] == ["alm_1", "alm_3"]


def test_plan_ingest_counts_read_invalid_skipped_and_new() -> None:
    raw_rows = [
        make_raw_row(key="alm_1"),
        make_raw_row(key="alm_2"),
        make_raw_row(key="alm_bad", eventTime="not-a-time"),
    ]

    plan = plan_ingest(raw_rows, INSTANCE_SPACE, existing_external_ids={"alm_1"})

    assert (plan.read, plan.invalid, plan.skipped) == (3, 1, 1)
    assert [record.external_id for record in plan.new_records] == ["alm_2"]


def test_handler_input_defaults_sync_cursor_and_requires_contract_keys() -> None:
    data = {
        "stream_external_id": "pi_all_alarm_event",
        "instance_space": INSTANCE_SPACE,
        "enterprise_space": "dm_dom_industry",
        "raw_database": "db_industry_sample",
        "raw_table": "alarm_event",
    }

    assert HandlerInput.model_validate(data).sync_initialize_cursor == DEFAULT_SYNC_INITIALIZE_CURSOR
    with pytest.raises(ValidationError):
        HandlerInput.model_validate({k: v for k, v in data.items() if k != "raw_table"})


def test_function_output_serializes_status_contract() -> None:
    output = FunctionOutput(status="succeeded", read=3, invalid=0, skipped=1, ingested=2)

    assert output.model_dump() == {
        "status": "succeeded",
        "read": 3,
        "invalid": 0,
        "skipped": 1,
        "ingested": 2,
        "reason": None,
    }


def test_configure_sdk_retries_caps_function_retries() -> None:
    configure_sdk_retries()

    from cognite.client import global_config

    assert global_config.max_retries == FUNCTION_MAX_RETRIES


def test_collect_existing_external_ids_skips_deleted_and_other_spaces() -> None:
    from handler import collect_existing_external_ids

    class FakeRecord:
        def __init__(self, space: str, external_id: str, status: str = "created") -> None:
            self.space = space
            self.external_id = external_id
            self.status = status

    chunks = [
        [
            FakeRecord(INSTANCE_SPACE, "alm_1"),
            FakeRecord(INSTANCE_SPACE, "alm_gone", status="deleted"),
            FakeRecord("other_space", "alm_other"),
        ]
    ]

    assert collect_existing_external_ids(chunks, INSTANCE_SPACE) == {"alm_1"}
