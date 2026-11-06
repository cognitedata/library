"""Matches parked in CDF files while a predict job runs.

Manual and rule based matches are found by submit, but written by collect together with
the matches the model produced. Submit stages them in a temporary CDF file so collect
can read them back and let them win over any model match for the same entity.

The SHA-256 of the file bytes is stored on the predict-job RAW row (scoped to this
module's database). Collect refuses a file whose digest does not match, so a principal
that can overwrite CDF files but not that RAW table cannot plant matches.
"""

import hashlib
import hmac
import json
from typing import cast

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from em_config import Config  # isort: skip
from em_constants import KEY_ENTITY_EXT_ID, KEY_ENTITY_SPACE, STAGING_FILE_PREFIX  # isort: skip
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline import create_table  # isort: skip
from em_pipeline_types import StoredMatch  # isort: skip


def _entity_count(matches: list[StoredMatch]) -> int:
    """Distinct entities behind the match rows - one entity can match many targets."""
    return len({(m.get(KEY_ENTITY_SPACE), m[KEY_ENTITY_EXT_ID]) for m in matches})


def content_digest(content: bytes) -> str:
    """SHA-256 of file bytes, stored in RAW next to the file's external ID."""
    return hashlib.sha256(content).hexdigest()


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
    data_set_id: int | None = None,
) -> str:
    """Stage the matches submit already has in a CDF file, so collect can merge them with the ML ones.

    Returns:
        SHA-256 of the staged bytes, to store on the predict-job RAW row.
    """
    file_external_id = staging_file_external_id(job_id)
    content = json.dumps(matches).encode("utf-8")
    digest = content_digest(content)
    client.files.upload_bytes(
        content=content,
        name=file_external_id,
        external_id=file_external_id,
        mime_type="application/json",
        data_set_id=data_set_id,
        overwrite=True,
    )
    logger.info(
        f"Staged {len(matches)} entity-target pair(s) from manual/rule matching across "
        f"{_entity_count(matches)} entities for job {job_id} in file {file_external_id}"
    )
    return digest


def read_staged_matches(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    job_id: str,
    expected_digest: str | None = None,
) -> list[StoredMatch]:
    """The matches submit staged for this job, as they were before staging.

    Raises:
        CogniteAPIError: The file exists but could not be read. Collect deletes the file
            once a job is written, so carrying on with nothing would lose those matches.
        ValueError: The file content is not the JSON submit wrote, or its digest does not
            match the value stored on the job row.
    """
    file_external_id = staging_file_external_id(job_id)
    try:
        content = client.files.download_bytes(external_id=file_external_id)
    except CogniteAPIError as e:
        if e.code in (400, 404):
            logger.info(f"No staged matches file found for job {job_id} ({file_external_id})")
            return []
        logger.error(f"Could not read staged matches file {file_external_id}: {type(e)}({e})")
        raise
    if expected_digest is None or not hmac.compare_digest(content_digest(content), expected_digest):
        raise ValueError(f"Staged matches file {file_external_id} does not match the digest stored for job {job_id}")
    matches = [cast(StoredMatch, m) for m in json.loads(content.decode("utf-8"))]
    logger.info(
        f"Read {len(matches)} staged entity-target pair(s) across {_entity_count(matches)} entities "
        f"for job {job_id} from {file_external_id}"
    )
    return matches


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
    except Exception as e:
        logger.warning(f"Failed to delete staged matches file {file_external_id}: {type(e)}({e})")


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
        client.raw.rows.delete(db, table, keys)

    logger.info(f"Cleared {len(keys)} row(s) from {db}/{table}")
    return len(keys)
