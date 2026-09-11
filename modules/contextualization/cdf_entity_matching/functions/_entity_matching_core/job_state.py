"""The queue of predict jobs submit hands over to collect.

Each running predict job is one row in the state store table already used for the
matching model id. Submit only ever appends a row, and collect only ever deletes the row
it has finished, so two functions share the table without either overwriting the other's
work.
"""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import Row

from config import Config  # isort: skip
from constants import (  # isort: skip
    JOB_COL_CREATED_AT,
    JOB_COL_JOB_ID,
    JOB_COL_JOB_TOKEN,
    JOB_COL_MODEL_ID,
    JOB_COL_SOURCE_COUNT,
    JOB_COL_STAGING_PREFIX,
    JOB_COL_STATUS,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUBMITTED,
    STAT_STORE_PREDICT_JOB_PREFIX,
)
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import create_table  # isort: skip


@dataclass(frozen=True)
class PredictJob:
    """A predict job that has been submitted but not yet collected."""

    job_id: str
    job_token: str | None
    status: str
    created_at: str
    staging_prefix: str
    model_id: str | None = None
    source_count: int | None = None

    @property
    def row_key(self) -> str:
        return job_row_key(self.job_id)

    def as_columns(self) -> dict[str, Any]:
        return {
            JOB_COL_JOB_ID: self.job_id,
            JOB_COL_JOB_TOKEN: self.job_token,
            JOB_COL_STATUS: self.status,
            JOB_COL_CREATED_AT: self.created_at,
            JOB_COL_STAGING_PREFIX: self.staging_prefix,
            JOB_COL_MODEL_ID: self.model_id,
            JOB_COL_SOURCE_COUNT: self.source_count,
        }

    @classmethod
    def from_columns(cls, columns: dict[str, Any]) -> "PredictJob":
        source_count = columns.get(JOB_COL_SOURCE_COUNT)
        return cls(
            job_id=str(columns[JOB_COL_JOB_ID]),
            job_token=columns.get(JOB_COL_JOB_TOKEN) or None,
            status=str(columns.get(JOB_COL_STATUS) or JOB_STATUS_SUBMITTED),
            created_at=str(columns.get(JOB_COL_CREATED_AT) or ""),
            staging_prefix=str(columns.get(JOB_COL_STAGING_PREFIX) or ""),
            model_id=str(columns[JOB_COL_MODEL_ID]) if columns.get(JOB_COL_MODEL_ID) else None,
            source_count=int(source_count) if source_count is not None else None,
        )


def job_row_key(job_id: str) -> str:
    """The state store row key a predict job is recorded under."""
    return f"{STAT_STORE_PREDICT_JOB_PREFIX}{job_id}"


def append_predict_job(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    job_id: str,
    job_token: str | None,
    staging_prefix: str,
    model_id: str | None = None,
    source_count: int | None = None,
) -> PredictJob:
    """Record a submitted predict job, without touching the jobs already queued.

    The row key holds the job id, which is unique per job, so a new submit can never
    land on the row of a job that is still running.

    Returns:
        The queued job.
    """
    job = PredictJob(
        job_id=job_id,
        job_token=job_token,
        status=JOB_STATUS_SUBMITTED,
        created_at=datetime.now(UTC).isoformat(),
        staging_prefix=staging_prefix,
        model_id=model_id,
        source_count=source_count,
    )

    db = config.parameters.raw_db
    table = config.parameters.raw_table_state
    create_table(client, db, table)
    client.raw.rows.insert(db, table, Row(job.row_key, job.as_columns()))

    logger.info(f"Recorded predict job in state store - key: {job.row_key}")
    return job


def list_predict_jobs(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
) -> list[PredictJob]:
    """Every queued predict job, oldest first.

    Ordered by when the job was submitted, with the job id breaking a tie, so collect
    works through the queue in the order submit created it.
    """
    db = config.parameters.raw_db
    table = config.parameters.raw_table_state
    create_table(client, db, table)

    jobs = []
    for row in client.raw.rows.list(db_name=db, table_name=table, limit=-1):
        if not row.key.startswith(STAT_STORE_PREDICT_JOB_PREFIX) or not row.columns:
            continue
        try:
            jobs.append(PredictJob.from_columns(row.columns))
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Ignoring malformed predict job row: {row.key} - error: {e}")

    return sorted(jobs, key=lambda job: (job.created_at, job.job_id))


def mark_job_running(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    job: PredictJob,
) -> PredictJob:
    """Note that collect has started polling this job, keeping its other columns."""
    running = replace(job, status=JOB_STATUS_RUNNING)
    client.raw.rows.insert(
        config.parameters.raw_db,
        config.parameters.raw_table_state,
        Row(running.row_key, running.as_columns()),
    )
    logger.debug(f"Predict job {job.job_id} marked as {JOB_STATUS_RUNNING}")
    return running


def delete_predict_job(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    job: PredictJob,
) -> None:
    """Take a finished job off the queue."""
    client.raw.rows.delete(config.parameters.raw_db, config.parameters.raw_table_state, job.row_key)
    logger.info(f"Removed predict job from state store - key: {job.row_key}")
