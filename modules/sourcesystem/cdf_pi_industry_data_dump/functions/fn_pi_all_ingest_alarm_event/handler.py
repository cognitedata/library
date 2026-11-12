"""Sample stand-in for a PI alarm extractor.

Reads alarm rows from a RAW table, maps them to ``AlarmEvent`` records and ingests the ones that are not yet in the
alarm stream. Production setups write to the stream directly from an extractor; the Records service cannot be
populated through RAW + Transformations.

The Records API needs cognite-sdk >= 8.24, so the records imports are deferred to the functions that call the API.
That keeps the mapping and duplicate-filter logic importable (and unit-testable) without it.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Literal

from cognite.client import CogniteClient
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

log = logging.getLogger(__name__)

ALARM_CONTAINER_EXTERNAL_ID = "AlarmEvent"
DEFAULT_WINDOW_DAYS = 7
FILTER_LIMIT = 1000
INGEST_BATCH_SIZE = 1000


class HandlerInput(BaseModel):
    """Function ``data`` contract."""

    model_config = ConfigDict(extra="ignore")

    stream_external_id: str
    instance_space: str
    enterprise_space: str
    raw_database: str
    raw_table: str
    last_updated_time_gt: str | None = None


class AlarmRow(BaseModel):
    """One validated row of the RAW ``alarm_event`` table."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    external_id: str = Field(alias="key")
    asset_external_id: str | None = Field(default=None, alias="asset_externalId")
    time_series_external_id: str | None = Field(default=None, alias="timeseries_externalId")
    operating_limit_external_id: str | None = Field(default=None, alias="operatingLimit_externalId")
    event_time: datetime = Field(alias="eventTime")
    event_type: Literal["Raised", "Acknowledged", "Cleared"] = Field(alias="eventType")
    severity: Literal["Low", "Medium", "High", "Critical"] | None = None
    limit_type: Literal["High", "Low"] | None = Field(default=None, alias="limitType")
    value: float | None = None
    message: str | None = None

    @field_validator(
        "asset_external_id",
        "time_series_external_id",
        "operating_limit_external_id",
        "severity",
        "limit_type",
        "value",
        "message",
        mode="before",
    )
    @classmethod
    def blank_to_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value


class AlarmRecord(BaseModel):
    """A record ready to be written to the ``AlarmEvent`` container."""

    space: str
    external_id: str
    properties: dict[str, object]


class IngestPlan(BaseModel):
    """Outcome of mapping and de-duplicating the RAW rows."""

    read: int
    invalid: int
    skipped: int
    new_records: list[AlarmRecord]


def default_last_updated_time_gt(now: datetime) -> str:
    """Lower bound for the ``lastUpdatedTime`` range filter: the sample window before ``now``."""
    return (now - timedelta(days=DEFAULT_WINDOW_DAYS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _direct(space: str, external_id: str | None) -> dict[str, str] | None:
    return {"space": space, "externalId": external_id} if external_id else None


def row_to_record(row: AlarmRow, instance_space: str) -> AlarmRecord:
    """Map a validated RAW row to an ``AlarmEvent`` record.

    Args:
        row: Validated RAW row.
        instance_space: Space of the record and of the directly related nodes.

    Returns:
        The record, without properties that are empty in the source.
    """
    event_time = row.event_time if row.event_time.tzinfo else row.event_time.replace(tzinfo=UTC)
    properties: dict[str, object | None] = {
        "eventTime": event_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "eventType": row.event_type,
        "severity": row.severity,
        "limitType": row.limit_type,
        "value": row.value,
        "message": row.message,
        "asset": _direct(instance_space, row.asset_external_id),
        "timeSeries": _direct(instance_space, row.time_series_external_id),
        "operatingLimit": _direct(instance_space, row.operating_limit_external_id),
    }
    return AlarmRecord(
        space=instance_space,
        external_id=row.external_id,
        properties={name: value for name, value in properties.items() if value is not None},
    )


def filter_missing(records: list[AlarmRecord], existing_external_ids: set[str]) -> list[AlarmRecord]:
    """Drop records whose external id is already in the stream."""
    return [record for record in records if record.external_id not in existing_external_ids]


def plan_ingest(raw_rows: list[dict[str, object]], instance_space: str, existing_external_ids: set[str]) -> IngestPlan:
    """Validate and map the RAW rows, then keep only records that are not in the stream yet.

    Args:
        raw_rows: RAW rows as dicts, including the row ``key``.
        instance_space: Space for the records and the directly related nodes.
        existing_external_ids: External ids already present in the stream.

    Returns:
        Counts and the records to ingest.
    """
    records: list[AlarmRecord] = []
    invalid = 0
    for raw_row in raw_rows:
        try:
            records.append(row_to_record(AlarmRow.model_validate(raw_row), instance_space))
        except ValidationError as error:
            invalid += 1
            log.warning(f"Skipping invalid alarm_event row {raw_row.get('key')}: {error}")
    new_records = filter_missing(records, existing_external_ids)
    return IngestPlan(
        read=len(raw_rows),
        invalid=invalid,
        skipped=len(records) - len(new_records),
        new_records=new_records,
    )


def read_raw_rows(client: CogniteClient, raw_database: str, raw_table: str) -> list[dict[str, object]]:
    """Read all rows of a RAW table, adding the row key as ``key`` when it is not a column."""
    rows = client.raw.rows.list(raw_database, raw_table, limit=-1)
    return [{"key": row.key, **(row.columns or {})} for row in rows]


def fetch_existing_external_ids(client: CogniteClient, params: HandlerInput, last_updated_time_gt: str) -> set[str]:
    """Return external ids of ``AlarmEvent`` records in the stream updated after ``last_updated_time_gt``."""
    from cognite.client.data_classes.data_modeling.records import RecordContainerId, TimeRange
    from cognite.client.data_classes.filters import HasData

    container = RecordContainerId(space=params.enterprise_space, external_id=ALARM_CONTAINER_EXTERNAL_ID)
    existing = client.data_modeling.records.filter(
        stream_id=params.stream_external_id,
        last_updated_time=TimeRange(gt=last_updated_time_gt),
        filter=HasData(containers=[container]),
        limit=FILTER_LIMIT,
    )
    if len(existing) >= FILTER_LIMIT:
        log.warning(f"Duplicate check hit the {FILTER_LIMIT} record limit; narrow last_updated_time_gt")
    return {record.external_id for record in existing if record.space == params.instance_space}


def ingest_records(client: CogniteClient, params: HandlerInput, records: list[AlarmRecord]) -> int:
    """Ingest records into the alarm stream in batches. Returns the number of records written."""
    from cognite.client.data_classes.data_modeling.records import RecordContainerId, RecordSource, RecordWrite

    container = RecordContainerId(space=params.enterprise_space, external_id=ALARM_CONTAINER_EXTERNAL_ID)
    for start in range(0, len(records), INGEST_BATCH_SIZE):
        batch = records[start : start + INGEST_BATCH_SIZE]
        client.data_modeling.records.ingest(
            [
                RecordWrite(
                    space=record.space,
                    external_id=record.external_id,
                    sources=[RecordSource(source=container, properties=record.properties)],
                )
                for record in batch
            ],
            stream_id=params.stream_external_id,
        )
    return len(records)


def handle(data: dict, client: CogniteClient) -> dict[str, int]:
    """CDF Function entry point.

    Args:
        data: Contract with ``stream_external_id``, ``instance_space``, ``enterprise_space``, ``raw_database``,
            ``raw_table`` and optional ``last_updated_time_gt`` (ISO-8601).
        client: Injected client; its session carries the credentials.

    Returns:
        Counts: ``read``, ``invalid``, ``skipped`` (already in the stream) and ``ingested``.
    """
    logging.basicConfig(level=logging.INFO)
    params = HandlerInput.model_validate(data)
    last_updated_time_gt = params.last_updated_time_gt or default_last_updated_time_gt(datetime.now(UTC))

    raw_rows = read_raw_rows(client, params.raw_database, params.raw_table)
    existing = fetch_existing_external_ids(client, params, last_updated_time_gt)
    plan = plan_ingest(raw_rows, params.instance_space, existing)
    ingested = ingest_records(client, params, plan.new_records)

    counts = {"read": plan.read, "invalid": plan.invalid, "skipped": plan.skipped, "ingested": ingested}
    log.info(f"Alarm events for stream {params.stream_external_id}: {counts}")
    return counts
