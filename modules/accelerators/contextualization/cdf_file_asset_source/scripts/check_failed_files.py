#!/usr/bin/env python3
"""
Diagnostic script to check the state table for failed files.
"""

import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from modules.create_asset_hierarchy_from_files.functions.fn_dm_extract_assets_by_pattern.dependencies import (
    create_client,
    get_env_variables,
)


def check_failed_files(
    raw_db: str = "db_extract_assets_by_pattern",
    raw_table_state: str = "extract_assets_by_pattern_state",
) -> None:
    """Check the state table for all files and their status."""

    env_config = get_env_variables()
    client = create_client(env_config)

    print(f"Checking {raw_db}.{raw_table_state}...")
    print("=" * 80)

    try:
        rows = client.raw.rows.list(raw_db, raw_table_state).to_pandas()

        if rows.empty:
            print("Table is empty.")
            return

        print(f"\nTotal rows: {len(rows)}")
        print("\nAll files and their status:")
        print("-" * 80)

        failed_count = 0
        status_counts = {}

        for key, row in rows.iterrows():
            file_id = key
            # Check top-level status column
            top_level_status = str(row.get("status", "")).strip()
            # Check status in state JSON
            state_json = row.get("state", "{}")
            state_data = {}
            if isinstance(state_json, str):
                try:
                    state_data = json.loads(state_json)
                    state_status = str(state_data.get("status", "")).strip()
                except:
                    state_status = ""
            else:
                state_data = state_json if isinstance(state_json, dict) else {}
                state_status = str(state_data.get("status", "")).strip()

            # Use top-level status if available, otherwise state status
            status = top_level_status if top_level_status else state_status

            top_level_attempts = row.get("attempts", "")
            state_attempts = state_data.get("attempts", "")
            attempts = top_level_attempts if top_level_attempts else state_attempts

            file_name = state_data.get("file_info", {}).get("name", f"file_{file_id}")
            last_error = state_data.get("last_error", "")

            # Normalize status for counting (case-insensitive, strip whitespace)
            status_normalized = status.lower().strip() if status else ""
            status_counts[status] = status_counts.get(status, 0) + 1

            # Check for failed status (case-insensitive)
            is_failed = status_normalized == "failed"

            if is_failed:
                failed_count += 1
                print(f"  FAILED: {file_name} (ID: {file_id})")
                print(
                    f"    Top-level status: '{top_level_status}', State status: '{state_status}'"
                )
                print(
                    f"    Top-level attempts: '{top_level_attempts}', State attempts: '{state_attempts}'"
                )
                if last_error:
                    print(f"    Last error: {last_error[:100]}...")
            else:
                # Show all files with details if status is not what we expect
                if status_normalized not in ["pending", "success", "processing"]:
                    print(
                        f"  UNEXPECTED STATUS: {file_name} (ID: {file_id}, status: '{status}', attempts: {attempts})"
                    )
                else:
                    print(
                        f"  {file_name} (ID: {file_id}, status: '{status}', attempts: {attempts})"
                    )

        print("\n" + "=" * 80)
        print("Status Summary:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count}")
        print(f"\nTotal failed files: {failed_count}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Check failed files in state table")
    parser.add_argument(
        "--raw-db",
        type=str,
        default="db_extract_assets_by_pattern",
        help="RAW database name",
    )
    parser.add_argument(
        "--raw-table-state",
        type=str,
        default="extract_assets_by_pattern_state",
        help="RAW state table name",
    )

    args = parser.parse_args()

    check_failed_files(
        raw_db=args.raw_db,
        raw_table_state=args.raw_table_state,
    )
