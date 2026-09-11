# Generated from functions/_entity_matching_core/staging.py - do not edit this copy.
# Change the source and run: python scripts/sync_entity_matching_core.py
"""Matches parked in RAW while a predict job runs.

Manual and rule based matches are found by submit, but written by collect together with
the matches the model produced. Submit stages them in the good table under a key prefix
of its own so collect can read them back and let them win over any model match for the
same entity.
"""

from typing import TYPE_CHECKING, Any

from cognite.client import CogniteClient
from cognite.client.data_classes import Row

if TYPE_CHECKING:
    from cognite.extractorutils.uploader import RawUploadQueue

from config import Config  # isort: skip
from constants import STAGING_COL_JOB_ID, STAGING_ROW_KEY_PREFIX  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import create_table, raw_row_key  # isort: skip


def staging_prefix(job_id: str) -> str:
    """Row key prefix holding the matches staged for one predict job."""
    return f"{STAGING_ROW_KEY_PREFIX}:{job_id}:"


def is_staged_row(row_key: str) -> bool:
    """Whether a row in the good table is staged work rather than a finished match."""
    return row_key.startswith(f"{STAGING_ROW_KEY_PREFIX}:")


def write_staged_matches(
    client: CogniteClient,
    config: Config,
    raw_uploader: "RawUploadQueue",
    logger: CogniteFunctionLogger,
    job_id: str,
    matches: list[dict[str, Any]],
) -> int:
    """Stage the matches submit already has, so collect can merge them with the ML ones.

    Returns:
        Number of matches staged.
    """
    db = config.parameters.raw_db
    table = config.parameters.raw_table_ctx_good
    create_table(client, db, table)

    prefix = staging_prefix(job_id)
    for match in matches:
        raw_uploader.add_to_upload_queue(
            db,
            table,
            Row(f"{prefix}{raw_row_key(config, match)}", {**match, STAGING_COL_JOB_ID: job_id}),
        )
    raw_uploader.upload()

    logger.info(f"Staged {len(matches)} manual/rule match(es) for job {job_id} under prefix {prefix}")
    return len(matches)


def read_staged_matches(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    job_id: str,
) -> list[dict[str, Any]]:
    """The matches submit staged for this job, as they were before staging."""
    prefix = staging_prefix(job_id)
    matches = []
    for row in client.raw.rows.list(
        db_name=config.parameters.raw_db,
        table_name=config.parameters.raw_table_ctx_good,
        limit=-1,
    ):
        if row.key.startswith(prefix) and row.columns:
            matches.append({key: value for key, value in row.columns.items() if key != STAGING_COL_JOB_ID})

    logger.info(f"Read {len(matches)} staged manual/rule match(es) for job {job_id}")
    return matches


def delete_staged_matches(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    job_id: str,
) -> int:
    """Drop the staging rows of a job whose matches have been written for real.

    Returns:
        Number of rows deleted.
    """
    prefix = staging_prefix(job_id)
    db = config.parameters.raw_db
    table = config.parameters.raw_table_ctx_good
    keys = [
        row.key
        for row in client.raw.rows.list(db_name=db, table_name=table, columns=[], limit=-1)
        if row.key.startswith(prefix)
    ]
    if keys:
        client.raw.rows.delete(db, table, keys)

    logger.debug(f"Deleted {len(keys)} staging row(s) for job {job_id}")
    return len(keys)


def clear_finished_matches(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    table: str,
) -> int:
    """Empty a result table ahead of a full re-run, leaving queued work alone.

    Deleting the table itself would take the matches staged for predict jobs that are
    still running with it, and collect would then have nothing to merge its results
    into.

    Returns:
        Number of rows deleted.
    """
    db = config.parameters.raw_db
    create_table(client, db, table)
    keys = [
        row.key
        for row in client.raw.rows.list(db_name=db, table_name=table, columns=[], limit=-1)
        if not is_staged_row(row.key)
    ]
    if keys:
        client.raw.rows.delete(db, table, keys)

    logger.info(f"Cleared {len(keys)} row(s) from {db}/{table} - staged matches kept")
    return len(keys)
