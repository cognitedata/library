from __future__ import annotations

import sys
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from logger import log


def delete_table(client: CogniteClient, db: str, tbl: str) -> None:
    try:
        client.raw.tables.delete(db, [tbl])
    except CogniteAPIError as e:
        # Any other error than table not found, and we re-raise
        if e.code != 404:
            raise


def write_mapping_to_raw(
    client: CogniteClient,
    config: ContextConfig,
    raw_uploader: RawUploadQueue,
    good_matches: list[Row],
    bad_matches: list[Row],
) -> None:
    """
    Write matching results to RAW DB

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        good_matches: list of good matches
        bad_matches: list of bad matches
    """
    log.info(f"Clean up BAD table: {config.rawdb}/{config.raw_table_bad} before writing new status")
    delete_table(client, config.rawdb, config.raw_table_bad)
    delete_table(client, config.rawdb, config.raw_table_good)
    delete_table(client, config.rawdb, config.raw_table_all)

    # if reset mapping, clean up good matches in table
    if config.run_all and not config.debug:
        log.info(
            f"ResetMapping - Cleaning up GOOD table: {config.rawdb}/{config.raw_table_good} "
            "before writing new status"
        )
        delete_table(client, config.rawdb, config.raw_table_good)

    for match in good_matches:
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["3DId"], match))
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_all, Row(match["3DId"], match))
    log.info(f"Added {len(good_matches)} to {config.rawdb}/{config.raw_table_good}")

    for not_match in bad_matches:
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["3DId"], not_match))
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_all, Row(not_match["3DId"], not_match))
    log.info(f"Added {len(bad_matches)} to {config.rawdb}/{config.raw_table_bad}")

    # Upload any remaining RAW cols in queue
    raw_uploader.upload()
