from typing import List

import numpy as np
from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import RawUploadQueue

from config import ContextConfig
from logger import log


def delete_table_if_needed(client: CogniteClient, db: str, tbl: str) -> None:
    try:
        client.raw.tables.delete(db, [tbl])
        log.info(f"Deleted table {db}/{tbl}")
    except CogniteAPIError as e:
        if e.code != 404:
            raise
        log.info(f"Table {tbl} not found in {db}, nothing to delete.")


def clean_data(entries: List[dict]) -> List[dict]:
    cleaned_entries = []
    for entry in entries:
        cleaned_entry = {k: (v if not (isinstance(v, float) and (np.isnan(v) or np.isinf(v))) else None) for k, v in entry.items()}
        cleaned_entries.append(cleaned_entry)
    return cleaned_entries


def write_mapping_to_raw(client: CogniteClient, config: ContextConfig, raw_uploader: RawUploadQueue,
                         good_matches: List[dict], bad_matches: List[dict], manual_entries: List[dict]) -> None:

    # Clean data to ensure no NaN or infinite values
    good_matches = clean_data(good_matches)
    bad_matches = clean_data(bad_matches)
    manual_entries = clean_data(manual_entries)

    if good_matches:
        log.info(f"Cleaning up GOOD table: {config.rawdb}/{config.raw_table_good}")
        delete_table_if_needed(client, config.rawdb, config.raw_table_good)
        for match in good_matches:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["nodeId"], match))
        raw_uploader.upload()
        log.info(f"Added {len(good_matches)} rows to {config.rawdb}/{config.raw_table_good}")

    if bad_matches:
        log.info(f"Cleaning up BAD table: {config.rawdb}/{config.raw_table_bad}")
        delete_table_if_needed(client, config.rawdb, config.raw_table_bad)
        for not_match in bad_matches:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["nodeId"], not_match))
        raw_uploader.upload()
        log.info(f"Added {len(bad_matches)} rows to {config.rawdb}/{config.raw_table_bad}")

    if manual_entries:
        log.info(f"Cleaning up MANUAL table: {config.rawdb}/{config.raw_table_manual}")
        delete_table_if_needed(client, config.rawdb, config.raw_table_manual)
        for entry in manual_entries:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_manual, Row(entry["nodeId"], entry))
        raw_uploader.upload()
        log.info(f"Added {len(manual_entries)} rows to {config.rawdb}/{config.raw_table_manual}")

    log.info("Upload complete.")
