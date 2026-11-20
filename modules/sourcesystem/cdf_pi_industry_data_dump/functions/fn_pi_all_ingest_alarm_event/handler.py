"""Sample stand-in for a PI alarm extractor.

Reads alarm rows from a RAW table, maps them to ``AlarmEvent`` records and ingests the ones that are not yet in the
alarm stream. Production setups write to the stream directly from an extractor; the Records service cannot be
populated through RAW + Transformations.

The Records API needs cognite-sdk >= 8.24, so the records imports are deferred to the functions that call the API.
That keeps the mapping and duplicate-filter logic importable (and unit-testable) without it.

Logging uses ``print`` (not the ``logging`` module): on Azure, the most common Functions host, ``logging`` can
suppress tracebacks in function logs.
"""

import json
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Literal

from cognite.client import CogniteClient, global_config
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

ALARM_CONTAINER_EXTERNAL_ID = "AlarmEvent"
# sync page size; also used as the ingest batch size (within per-request caps).
PAGE_SIZE = 1000
# Steady pacing between ingest batches so a larger sample does not burst the 10-minute stream budget.
BATCH_PAUSE_S = 0.5
# Cap SDK retries inside a Function so retries cannot overrun the ~9 minute execution window.
FUNCTION_MAX_RETRIES = 3
# Default sync lookback covers BasicArchive; ImmutableTestStream retains less, so sync just returns what exists.
DEFAULT_SYNC_INITIALIZE_CURSOR = "365d-ago"


class HandlerInput(BaseModel):
    """Function ``data`` contract."""

    model_config = ConfigDict(extra="ignore")

    stream_external_id: str
    instance_space: str
    enterprise_space: str
    raw_database: str
    raw_table: str
    sync_initialize_cursor: str = DEFAULT_SYNC_INITIALIZE_CURSOR


class FunctionOutput(BaseModel):
    """JSON-serializable return contract."""

    status: Literal["succeeded", "deferred", "failed"]
    read: int = 0
    invalid: int = 0
    skipped: int = 0
    ingested: int = 0
    reason: str | None = None


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


def _direct(space: str, external_id: str | None) -> dict[str, str] | None:
    return {"space": space, "externalId": external_id} if external_id else None


def _log(payload: dict[str, object]) -> None:
    print(json.dumps(payload, default=str))


def configure_sdk_retries() -> None:
    """Keep SDK retries short so they stay inside the Function execution window."""
    global_config.max_retries = FUNCTION_MAX_RETRIES
    global_config.silence_feature_preview_warnings = True


def row_to_record(row: AlarmRow, instance_space: str) -> AlarmRecord:
    """Map a validated RAW row to an ``AlarmEvent`` record.

    Args:
        row: Validated RAW row.
        instance_space: Space of the record and of every direct (asset, timeSeries, operatingLimit).

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


def plan_ingest(
    raw_rows: list[dict[str, object]],
    instance_space: str,
    existing_external_ids: set[str],
) -> IngestPlan:
    """Validate and map the RAW rows, then keep only records that are not in the stream yet.

    Args:
        raw_rows: RAW rows as dicts, including the row ``key``.
        instance_space: Space of the records and of every direct.
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
            _log({"event": "invalid_row", "key": raw_row.get("key"), "error": str(error)})
    new_records = filter_missing(records, existing_external_ids)
    seen: set[str] = set()
    deduped_records: list[AlarmRecord] = []
    for record in new_records:
        if record.external_id in seen:
            continue
        seen.add(record.external_id)
        deduped_records.append(record)
    return IngestPlan(
        read=len(raw_rows),
        invalid=invalid,
        skipped=len(records) - len(deduped_records),
        new_records=deduped_records,
    )


def read_raw_rows(client: CogniteClient, raw_database: str, raw_table: str) -> list[dict[str, object]]:
    """Read all rows of a RAW table, adding the row key as ``key`` when it is not a column."""
    rows = client.raw.rows.list(raw_database, raw_table, limit=-1)
    return [{"key": row.key, **(row.columns or {})} for row in rows]


def collect_existing_external_ids(chunks: Iterable[Iterable[object]], instance_space: str) -> set[str]:
    """Collect live record external ids from ``sync`` chunks (skips tombstones and other spaces)."""
    existing: set[str] = set()
    for chunk in chunks:
        for record in chunk:
            if getattr(record, "status", None) == "deleted":
                continue
            space = getattr(record, "space", None)
            external_id = getattr(record, "external_id", None)
            if space == instance_space and isinstance(external_id, str):
                existing.add(external_id)
    return existing


def fetch_existing_external_ids(client: CogniteClient, params: HandlerInput) -> set[str]:
    """Return external ids of ``AlarmEvent`` records already in the stream via ``sync``.

    ``sync`` pages with a cursor, so this covers more than one ``filter`` page (max 1000).
    Tombstones (``status == deleted``) are ignored. Idempotency still relies on deterministic
    external ids and byte-identical payloads on immutable re-ingest.
    """
    from cognite.client.data_classes.data_modeling.records import RecordContainerId
    from cognite.client.data_classes.filters import HasData

    container = RecordContainerId(space=params.enterprise_space, external_id=ALARM_CONTAINER_EXTERNAL_ID)
    chunks = client.data_modeling.records.sync(
        stream_id=params.stream_external_id,
        filter=HasData(containers=[container]),
        chunk_size=PAGE_SIZE,
        initialize_cursor=params.sync_initialize_cursor,
    )
    return collect_existing_external_ids(chunks, params.instance_space)


def _count_ids_in_stream(client: CogniteClient, params: HandlerInput, external_ids: list[str]) -> int:
    """One-shot sync count of how many of ``external_ids`` are visible in the stream."""
    from cognite.client.data_classes.data_modeling.records import RecordContainerId
    from cognite.client.data_classes.filters import And, Equals, HasData, In

    container = RecordContainerId(space=params.enterprise_space, external_id=ALARM_CONTAINER_EXTERNAL_ID)
    found: set[str] = set()
    chunk_size = 100
    for start in range(0, len(external_ids), chunk_size):
        chunk = external_ids[start : start + chunk_size]
        for sync_chunk in client.data_modeling.records.sync(
            stream_id=params.stream_external_id,
            filter=And(
                HasData(containers=[container]),
                Equals(["space"], params.instance_space),
                In(["externalId"], chunk),
            ),
            chunk_size=PAGE_SIZE,
            initialize_cursor=params.sync_initialize_cursor,
        ):
            for record in sync_chunk:
                if getattr(record, "status", None) == "deleted":
                    continue
                external_id = getattr(record, "external_id", None)
                if isinstance(external_id, str):
                    found.add(external_id)
    return len(found)


def verify_ingested(
    client: CogniteClient,
    params: HandlerInput,
    external_ids: list[str],
    *,
    attempts: int = 5,
    pause_s: float = 3.0,
) -> int:
    """Poll until ingested ids are searchable (writes are not read-after-write).

    Args:
        client: CDF client.
        params: Function input (stream and spaces).
        external_ids: Ids that were just written.
        attempts: How many times to poll (default ~15s total).
        pause_s: Sleep between polls.

    Returns:
        Number of distinct external ids found for ``params.instance_space``.
    """
    if not external_ids:
        return 0

    found = 0
    for attempt in range(1, attempts + 1):
        found = _count_ids_in_stream(client, params, external_ids)
        if found >= len(external_ids):
            return found
        if attempt < attempts:
            time.sleep(pause_s)
    return found


def ingest_records(client: CogniteClient, params: HandlerInput, records: list[AlarmRecord]) -> int:
    """Ingest records into the alarm stream in paced batches. Returns the number of records written."""
    from cognite.client.data_classes.data_modeling.records import RecordContainerId, RecordSource, RecordWrite

    container = RecordContainerId(space=params.enterprise_space, external_id=ALARM_CONTAINER_EXTERNAL_ID)
    total_batches = (len(records) + PAGE_SIZE - 1) // PAGE_SIZE if records else 0
    for batch_index, start in enumerate(range(0, len(records), PAGE_SIZE)):
        batch = records[start : start + PAGE_SIZE]
        writes = [
            RecordWrite(
                space=record.space,
                external_id=record.external_id,
                sources=[RecordSource(source=container, properties=record.properties)],
            )
            for record in batch
        ]
        client.data_modeling.records.ingest(
            writes,
            stream_id=params.stream_external_id,
        )
        if batch_index + 1 < total_batches:
            time.sleep(BATCH_PAUSE_S)
    return len(records)


def handle(data: dict, client: CogniteClient) -> dict[str, int | str | None]:
    """CDF Function entry point.

    Args:
        data: Contract with ``stream_external_id``, ``instance_space``, ``enterprise_space``,
            ``raw_database``, ``raw_table`` and optional ``sync_initialize_cursor`` (default ``365d-ago``).
        client: Injected client; its session carries the credentials.

    Returns:
        ``status`` plus counts: ``read``, ``invalid``, ``skipped``, ``ingested``.
    """
    configure_sdk_retries()
    params = HandlerInput.model_validate(data)

    raw_rows = read_raw_rows(client, params.raw_database, params.raw_table)
    existing = fetch_existing_external_ids(client, params)
    plan = plan_ingest(raw_rows, params.instance_space, existing)
    ingested = ingest_records(client, params, plan.new_records)

    if plan.new_records:
        # Verify-then-complete: a timeout is not proof of a failed write; confirm ids are searchable.
        verified = verify_ingested(
            client,
            params,
            [record.external_id for record in plan.new_records],
        )
        if verified < len(plan.new_records):
            output = FunctionOutput(
                status="deferred",
                read=plan.read,
                invalid=plan.invalid,
                skipped=plan.skipped,
                ingested=ingested,
                reason=f"verify found {verified}/{len(plan.new_records)} ids; re-run to confirm",
            )
            _log({"event": "alarm_ingest", **output.model_dump()})
            return output.model_dump()

    output = FunctionOutput(
        status="succeeded",
        read=plan.read,
        invalid=plan.invalid,
        skipped=plan.skipped,
        ingested=ingested,
    )
    _log({"event": "alarm_ingest", "stream": params.stream_external_id, **output.model_dump()})
    return output.model_dump()


def run_local() -> None:
    """Run against the project in the environment: ``python handler.py '{"stream_external_id":"..."}'``."""
    import os
    import sys

    from cognite.client import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials

    project, cluster = os.environ["CDF_PROJECT"], os.environ["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cluster}.cognitedata.com/.default"],
    )
    client = CogniteClient(
        ClientConfig(
            client_name="pi-alarm-ingest-local",
            project=project,
            credentials=credentials,
            base_url=f"https://{cluster}.cognitedata.com",
        )
    )
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python handler.py \'{"stream_external_id":"...", ...}\'')
    print(json.dumps(handle(json.loads(sys.argv[1]), client), indent=2, default=str))


if __name__ == "__main__":
    run_local()
