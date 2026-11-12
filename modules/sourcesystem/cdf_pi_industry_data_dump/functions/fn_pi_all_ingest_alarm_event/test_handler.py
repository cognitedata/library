"""Unit tests for the alarm event ingest handler. No network access."""

from datetime import UTC, datetime

import pytest
from handler import (
    AlarmRow,
    HandlerInput,
    default_last_updated_time_gt,
    filter_missing,
    plan_ingest,
    row_to_record,
)
from pydantic import ValidationError

INSTANCE_SPACE = "inst_industry"


def make_raw_row(key: str = "ALM-PT92501-C1-Raised", **overrides: str) -> dict[str, str]:
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
    assert record.external_id == "ALM-PT92501-C1-Raised"
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
        for key in ("ALM-1", "ALM-2", "ALM-3")
    ]

    missing = filter_missing(records, existing_external_ids={"ALM-2"})

    assert [record.external_id for record in missing] == ["ALM-1", "ALM-3"]


def test_plan_ingest_counts_read_invalid_skipped_and_new() -> None:
    raw_rows = [
        make_raw_row(key="ALM-1"),
        make_raw_row(key="ALM-2"),
        make_raw_row(key="ALM-BAD", eventTime="not-a-time"),
    ]

    plan = plan_ingest(raw_rows, INSTANCE_SPACE, existing_external_ids={"ALM-1"})

    assert (plan.read, plan.invalid, plan.skipped) == (3, 1, 1)
    assert [record.external_id for record in plan.new_records] == ["ALM-2"]


def test_default_last_updated_time_gt_is_utc_iso_before_now() -> None:
    now = datetime(2026, 11, 12, 8, 0, 0, tzinfo=UTC)

    assert default_last_updated_time_gt(now) == "2026-11-05T08:00:00.000Z"


def test_handler_input_requires_contract_keys_and_optional_lower_bound() -> None:
    data = {
        "stream_external_id": "pi_all_alarm_event",
        "instance_space": INSTANCE_SPACE,
        "enterprise_space": "dm_dom_industry",
        "raw_database": "raw_sample_all_pi",
        "raw_table": "alarm_event",
    }

    assert HandlerInput.model_validate(data).last_updated_time_gt is None
    with pytest.raises(ValidationError):
        HandlerInput.model_validate({k: v for k, v in data.items() if k != "raw_table"})
