#!/usr/bin/env python3
"""
Script to reset the attempts counter for failed files in the state table.

This script resets the attempts counter and status for files that have failed,
allowing them to be retried in the next pipeline run.
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
from modules.create_asset_hierarchy_from_files.functions.fn_dm_extract_assets_by_pattern.logger import (
    CogniteFunctionLogger,
)
from modules.create_asset_hierarchy_from_files.functions.fn_dm_extract_assets_by_pattern.pipeline import (
    _load_state_from_raw,
    _save_state_to_raw_direct,
)


def reset_failed_file_attempts(
    raw_db: str = "db_extract_assets_by_pattern",
    raw_table_state: str = "extract_assets_by_pattern_state",
    results_field: str = "results",
    reset_all_failed: bool = False,
    max_attempts: int = 3,
) -> None:
    """
    Reset attempts counter to 0 and status to 'pending' for failed files in the state table.

    Args:
        raw_db: RAW database name
        raw_table_state: State table name
        results_field: Field name for results in state table
        reset_all_failed: If True, reset all failed files. If False, only reset files that exceeded max_attempts.
        max_attempts: Maximum attempts threshold (only used if reset_all_failed is False)
    """

    # Create client and logger
    logger = CogniteFunctionLogger(log_level="INFO")
    env_config = get_env_variables()
    client = create_client(env_config)

    logger.info("=" * 80)
    logger.info("Resetting attempts counter for failed files")
    logger.info("=" * 80)
    logger.info(f"RAW Database: {raw_db}")
    logger.info(f"RAW Table: {raw_table_state}")
    logger.info(f"Results Field: {results_field}")
    logger.info("")

    # Load state from RAW
    logger.info("Loading state from RAW table...")
    state_store = _load_state_from_raw(
        client, raw_db, raw_table_state, results_field, logger
    )
    logger.info(f"Loaded state for {len(state_store)} file(s)")

    # Find failed files - check both state_data and also query RAW directly
    failed_files = []

    # First, check state_data from _load_state_from_raw
    for file_id, state_data in state_store.items():
        status = state_data.get("status", "pending")
        if status == "failed":
            attempts = state_data.get("attempts", 0)
            # Filter by attempts if reset_all_failed is False
            if reset_all_failed or attempts >= max_attempts:
                file_name = state_data.get("file_info", {}).get(
                    "name", f"file_{file_id}"
                )
                failed_files.append((file_id, state_data, attempts, file_name))

    # Also check RAW table directly for any files with failed status in top-level column
    # that might not have been loaded correctly
    try:
        rows = client.raw.rows.list(raw_db, raw_table_state).to_pandas()
        if not rows.empty:
            for key, row in rows.iterrows():
                file_id = int(key)
                # Skip if already in failed_files
                if any(f[0] == file_id for f in failed_files):
                    continue

                # Check top-level status column
                top_level_status = str(row.get("status", "")).strip().lower()
                if top_level_status == "failed":
                    # Load state for this file
                    state_json = row.get("state", "{}")
                    if isinstance(state_json, str):
                        try:
                            state_data = json.loads(state_json)
                        except:
                            state_data = {}
                    else:
                        state_data = state_json if isinstance(state_json, dict) else {}

                    attempts = state_data.get("attempts", 0)
                    # Filter by attempts if reset_all_failed is False
                    if reset_all_failed or attempts >= max_attempts:
                        file_name = state_data.get("file_info", {}).get(
                            "name", f"file_{file_id}"
                        )
                        failed_files.append((file_id, state_data, attempts, file_name))
    except Exception as e:
        logger.warning(f"Error checking RAW table directly: {e}")

    logger.info(f"\nFound {len(failed_files)} file(s) with status='failed'")

    if not failed_files:
        logger.info("No failed files found. Nothing to reset.")
        return

    # Show summary of attempts before reset
    logger.info("\nAttempts distribution before reset:")
    attempts_dist = {}
    for file_id, state_data, attempts, file_name in failed_files:
        attempts_dist[attempts] = attempts_dist.get(attempts, 0) + 1
        logger.info(f"  - {file_name} (ID: {file_id}, attempts: {attempts})")

    for attempts_count in sorted(attempts_dist.keys(), reverse=True):
        file_count = attempts_dist[attempts_count]
        logger.info(f"  {attempts_count} attempts: {file_count} file(s)")

    # Reset attempts to 0 and status to 'pending' for all failed files
    logger.info(
        f"\nResetting attempts to 0 and status to 'pending' for {len(failed_files)} failed file(s)..."
    )

    updated_count = 0
    error_count = 0

    for file_id, state_data, old_attempts, file_name in failed_files:
        try:
            # Reset attempts to 0, status to 'pending', and clear last_error
            state_data["attempts"] = 0
            state_data["status"] = "pending"
            state_data["last_error"] = None

            # Save updated state back to RAW
            _save_state_to_raw_direct(
                client,
                raw_db,
                raw_table_state,
                file_id,
                state_data,
                results_field,
                logger,
            )

            updated_count += 1
            logger.info(f"  ✓ Reset {file_name} (ID: {file_id})")

        except Exception as e:
            error_count += 1
            logger.error(f"  ✗ Error updating file {file_id} ({file_name}): {e}")
            import traceback

            logger.error(traceback.format_exc())

    logger.info("")
    logger.info("=" * 80)
    logger.info("SUMMARY:")
    logger.info(f"  Total failed files found: {len(failed_files)}")
    logger.info(f"  Successfully updated: {updated_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info("=" * 80)

    if updated_count > 0:
        logger.info(
            f"\n✓ Successfully reset attempts to 0 and status to 'pending' for {updated_count} failed file(s)"
        )
        logger.info("  These files will now be retried on the next pipeline run.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Reset attempts counter for failed files in extract_assets_by_pattern_state"
    )
    parser.add_argument(
        "--raw-db",
        type=str,
        default="db_extract_assets_by_pattern",
        help="RAW database name (default: db_extract_assets_by_pattern)",
    )
    parser.add_argument(
        "--raw-table-state",
        type=str,
        default="extract_assets_by_pattern_state",
        help="RAW state table name (default: extract_assets_by_pattern_state)",
    )
    parser.add_argument(
        "--results-field",
        type=str,
        default="results",
        help="Results field name in state (default: results)",
    )
    parser.add_argument(
        "--reset-all-failed",
        action="store_true",
        help="Reset all failed files, not just those that exceeded max_attempts",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Maximum attempts threshold (only used if --reset-all-failed is not set, default: 3)",
    )

    args = parser.parse_args()

    try:
        reset_failed_file_attempts(
            raw_db=args.raw_db,
            raw_table_state=args.raw_table_state,
            results_field=args.results_field,
            reset_all_failed=args.reset_all_failed,
            max_attempts=args.max_attempts,
        )
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
