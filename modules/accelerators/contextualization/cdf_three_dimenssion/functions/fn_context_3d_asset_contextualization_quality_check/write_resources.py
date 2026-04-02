from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import RawUploadQueue
from typing import List
from config import ContextConfig
import numpy as np
from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import RawUploadQueue
from typing import List


def delete_table_if_needed(client: CogniteClient, db: str, tbl: str) -> None:
    try:
        client.raw.tables.delete(db, [tbl])
        print(f"INFO: Deleted table {db}/{tbl}")
    except CogniteAPIError as e:
        # Any other error than table not found, and we re-raise
        if e.code != 404:
            raise
        else:
            print(f"INFO: Table {tbl} not found in {db}, nothing to delete.")


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
        print(f"INFO: Clean up GOOD table: {config.rawdb}/{config.raw_table_good} before writing new status")
        delete_table_if_needed(client, config.rawdb, config.raw_table_good)
        for match in good_matches:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["3DId"], match))
        raw_uploader.upload()
        print(f"INFO: Added {len(good_matches)} to {config.rawdb}/{config.raw_table_good}")

    if bad_matches:
        print(f"INFO: Clean up BAD table: {config.rawdb}/{config.raw_table_bad} before writing new status")
        delete_table_if_needed(client, config.rawdb, config.raw_table_bad)
        for not_match in bad_matches:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["3DId"], not_match))
        raw_uploader.upload()
        print(f"INFO: Added {len(bad_matches)} to {config.rawdb}/{config.raw_table_bad}")

    if manual_entries:
        print(f"INFO: Clean up MANUAL table: {config.rawdb}/{config.raw_table_manual} before writing new status")
        delete_table_if_needed(client, config.rawdb, config.raw_table_manual)
        for entry in manual_entries:
            raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_manual, Row(entry["3DId"], entry))
        raw_uploader.upload()
        print(f"INFO: Added {len(manual_entries)} to {config.rawdb}/{config.raw_table_manual}")

    print("INFO: Upload complete.")
