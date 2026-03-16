"""
CDF Handler for Alias Persistence

This module provides a CDF-compatible handler function that writes aliases
back to source entities in the CDF data model.
"""

import sys
from pathlib import Path
from typing import Any, Dict
sys.path.append(str(Path(__file__).parent.parent))

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .dependencies import create_client, create_logger_service, get_env_variables


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for alias persistence.

    This function reads aliasing results from workflow data and writes
    aliases back to source entities in the CDF data model.

    Args:
        data: Dictionary containing:
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - aliasing_results: Results from aliasing task (required)
            - entities_keys_extracted: Key extraction results to map aliases to entities
        client: CogniteClient instance (required)

    Returns:
        Dictionary with status and results
    """
    logger = None

    try:
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)
        logger = create_logger_service(loglevel, verbose)
        logger.info(f"Starting alias persistence with loglevel = {loglevel}")

        if not client:
            raise ValueError("CogniteClient is required for alias persistence")

        # Call pipeline function
        from .pipeline import persist_aliases_to_entities

        persist_aliases_to_entities(client=client, logger=logger, data=data)

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Alias persistence failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file and CDF config)."""
    # Use dependencies helper to get environment variables and create client
    env_config = get_env_variables()
    client = create_client(env_config, debug=False)

    # Test data
    data = {
        "logLevel": "DEBUG",
        "verbose": False,
        "aliasing_results": [
            {
                "original_tag": "P-101",
                "aliases": ["P-101", "P_101", "P101", "PUMP-P-101"],
                "metadata": {},
            }
        ],
        "entities_keys_extracted": {
            "ASSET-P-101": {
                "name": {
                    "P-101": {"confidence": 0.9, "extraction_type": "candidate_key"}
                }
            }
        },
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
