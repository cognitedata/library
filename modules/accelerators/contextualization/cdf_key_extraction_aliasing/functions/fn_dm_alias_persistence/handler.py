"""
CDF Handler for Alias Persistence

This module provides a CDF-compatible handler function that writes aliases
back to source entities in the CDF data model.
"""

import os
from typing import Any, Dict, Optional

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from ..cdf_fn_common.function_logging import resolve_function_logger
from ..cdf_fn_common.scope_document_dm import ensure_alias_persistence_from_scope_dm
from .dependencies import create_client, get_env_variables


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
) -> Dict[str, Any]:
    """
    CDF-compatible handler function for alias persistence.

    This function reads aliasing results from workflow data and writes
    aliases back to source entities in the CDF data model.

    Args:
        data: Dictionary containing:
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - aliasing_results: Optional results from aliasing task (list of dicts)
            - raw_db/raw_table_aliases: If aliasing_results not provided, load from RAW
            - aliasWritebackProperty or alias_writeback_property: Optional DM property
              name for the alias list (default: aliases)
            - writeForeignKeyReferences / write_foreign_key_references: Optional bool
            - foreignKeyWritebackProperty / foreign_key_writeback_property: DM property for
              FK strings (e.g. references_found) when FK write-back is enabled
            - source_raw_db, source_raw_table_key, source_raw_read_limit: read FK JSON from
              key-extraction RAW (optional if entities_keys_extracted carries FKs)
            - source_instance_space, source_view_space, source_view_external_id,
              source_view_version: required for FK-only entities when not in entities_keys_extracted
        client: CogniteClient instance (required)

    Returns:
        Dictionary with status and results
    """
    log: Any = None

    try:
        loglevel = data.get("logLevel", "INFO")
        log = resolve_function_logger(data, None)
        log.info(f"Starting alias persistence with loglevel = {loglevel}")

        if not client:
            raise ValueError("CogniteClient is required for alias persistence")

        if data.get("configuration") or data.get("scope_document"):
            ensure_alias_persistence_from_scope_dm(data, client)

        # Call pipeline function
        from .pipeline import persist_aliases_to_entities

        persist_aliases_to_entities(client=client, logger=log, data=data)

        # Return a JSON-safe, compact summary (workflow-friendly).
        return {
            "status": "succeeded",
            "summary": {
                "aliasing_results_loaded_from_raw": int(
                    data.get("aliasing_results_loaded_from_raw", 0)
                ),
                "entities_updated": int(data.get("entities_updated", 0)),
                "entities_failed": int(data.get("entities_failed", 0)),
                "aliases_persisted": int(data.get("aliases_persisted", 0)),
                "foreign_keys_persisted": int(data.get("foreign_keys_persisted", 0)),
                "entities_fk_updated": int(data.get("entities_fk_updated", 0)),
            },
        }

    except Exception as e:
        message = f"Alias persistence failed: {e!s}"

        if log:
            log.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file and CDF config)."""
    # Use dependencies helper to get environment variables and create client
    env_config = get_env_variables()
    client = create_client(env_config, debug=False)

    scope_cdf_suffix = os.getenv("SCOPE_CDF_SUFFIX") or os.getenv("SITE_ABBREVIATION", "SITE")

    # Test data
    data = {
        "logLevel": "DEBUG",
        "verbose": False,
        # Either provide aliasing_results directly, OR point to RAW to load them.
        "aliasing_results": [],
        "raw_db": "db_tag_aliasing",
        "raw_table_aliases": f"{scope_cdf_suffix}_aliases",
        "raw_read_limit": 100,
    }

    # Run handler
    print("Starting alias persistence pipeline...")
    result = handle(data, client)

    if result["status"] == "succeeded":
        print("Pipeline completed successfully!")
    else:
        print(f"Pipeline failed: {result.get('message', 'Unknown error')}")

    return result


if __name__ == "__main__":
    try:
        result = run_locally()
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
