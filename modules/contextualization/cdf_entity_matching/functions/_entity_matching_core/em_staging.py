"""Matches parked in CDF files while a predict job runs.

Manual and rule based matches are found by submit, but written by collect together with
the matches the model produced. Submit stages them in a temporary CDF file so collect
can read them back and let them win over any model match for the same entity.
"""

import json
from typing import cast

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from em_config import Config  # isort: skip
from em_constants import BATCH_SIZE_API_SUBMIT, KEY_ENTITY_EXT_ID, KEY_ENTITY_SPACE, STAGING_FILE_PREFIX  # isort: skip
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline import create_table  # isort: skip
from em_pipeline_types import StoredMatch  # isort: skip


def _entity_count(matches: list[StoredMatch]) -> int:
    """Distinct entities behind the match rows - one entity can match many targets."""
    return len({(m.get(KEY_ENTITY_SPACE), m[KEY_ENTITY_EXT_ID]) for m in matches})


def staging_file_external_id(job_id: str) -> str:
    """External ID of the temporary file holding matches staged for one predict job."""
    return f"{STAGING_FILE_PREFIX}_{job_id}.json"


def staging_prefix(job_id: str) -> str:
    """External ID of the staged matches file (kept as staging_prefix for backward compatibility)."""
    return staging_file_external_id(job_id)


def write_staged_matches(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    job_id: str,
    matches: list[StoredMatch],
) -> int:
    """Stage the matches submit already has in a CDF file, so collect can merge them with the ML ones.

    Returns:
        Number of entity-target pairs staged.
    """
    file_external_id = staging_file_external_id(job_id)
    content = json.dumps(matches).encode("utf-8")
    client.files.upload_bytes(
        content=content,
        name=file_external_id,
        external_id=file_external_id,
        mime_type="application/json",
        overwrite=True,
    )
    logger.info(
        f"Staged {len(matches)} entity-target pair(s) from manual/rule matching across "
        f"{_entity_count(matches)} entities for job {job_id} in file {file_external_id}"
    )
    return len(matches)


def read_staged_matches(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    job_id: str,
) -> list[StoredMatch]:
    """The matches submit staged for this job, as they were before staging."""
    file_external_id = staging_file_external_id(job_id)
    try:
        content = client.files.download_bytes(external_id=file_external_id)
        matches = [cast(StoredMatch, m) for m in json.loads(content.decode("utf-8"))]
        logger.info(
            f"Read {len(matches)} staged entity-target pair(s) across {_entity_count(matches)} entities "
            f"for job {job_id} from {file_external_id}"
        )
        return matches
    except CogniteAPIError as e:
        if e.code in (400, 404):
            logger.info(f"No staged matches file found for job {job_id} ({file_external_id})")
            return []
        logger.warning(f"Could not read staged matches file {file_external_id}: {type(e)}({e})")
        return []
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.warning(f"Failed to parse staged matches from {file_external_id}: {type(e)}({e})")
        return []


def delete_staged_matches(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    job_id: str,
) -> None:
    """Drop the staged matches file of a job whose matches have been written or failed."""
    file_external_id = staging_file_external_id(job_id)
    try:
        client.files.delete(external_id=file_external_id)
        logger.debug(f"Deleted staged matches file {file_external_id} for job {job_id}")
    except CogniteAPIError as e:
        if e.code not in (400, 404):
            logger.warning(f"Failed to delete staged matches file {file_external_id}: {type(e)}({e})")
    except OSError as e:
        logger.warning(f"Failed to delete staged matches file {file_external_id}: {type(e)}({e})")


def _delete_raw_row_keys_in_batches(
    client: CogniteClient,
    db: str,
    table: str,
    keys: list[str],
) -> None:
    """Delete RAW rows in API-sized chunks (CDF limits deletes to 1000 keys per call)."""
    for start in range(0, len(keys), BATCH_SIZE_API_SUBMIT):
        client.raw.rows.delete(db, table, keys[start : start + BATCH_SIZE_API_SUBMIT])


def clear_finished_matches(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    table: str,
) -> int:
    """Empty a result table ahead of a full re-run.

    Staged matches live in temporary CDF files rather than this table, so deleting all
    rows leaves queued work untouched.

    Returns:
        Number of rows deleted.
    """
    db = config.parameters.raw_db
    create_table(client, db, table)
    keys = [row.key for row in client.raw.rows.list(db_name=db, table_name=table, columns=[], limit=-1)]
    if keys:
        _delete_raw_row_keys_in_batches(client, db, table, keys)

    logger.info(f"Cleared {len(keys)} row(s) from {db}/{table}")
    return len(keys)
