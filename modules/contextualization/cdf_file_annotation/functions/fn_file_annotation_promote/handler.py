import os
import sys
import time
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_logger_service,
    create_entity_search_service,
    create_cache_service,
)
from services.PromoteService import GeneralPromoteService


def handle(data: dict, function_call_info: dict, client: CogniteClient):
    """Main entry point for the Cognite Function."""
    start_time = datetime.now(timezone.utc)

    config, client = create_config_service(function_data=data)
    logger = create_logger_service(data.get("logLevel", "DEBUG"), data.get("logPath"))

    # Create service dependencies
    entity_search_service = create_entity_search_service(config, client, logger)
    cache_service = create_cache_service(config, client, logger, entity_search_service)

    # Create promote service with injected dependencies
    promote_service = GeneralPromoteService(
        client=client,
        config=config,
        logger=logger,
        entity_search_service=entity_search_service,
        cache_service=cache_service,
    )

    try:
        # Run in a loop for a maximum of 7 minutes
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            result = promote_service.run()
            if result == "Done":
                logger.info("No more candidates to process. Exiting.", section="END")
                break
            time.sleep(10)  # Pause between batches

        return {"status": "success", "message": "promote function completed a cycle."}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", section="BOTH")
        return {"status": "failure", "message": str(e)}


def run_locally(config_file: dict):
    """Entry point for local execution and debugging."""
    from dependencies import create_client, get_env_variables

    env_vars = get_env_variables()
    client = create_client(env_vars)

    # Mock function_call_info for local runs
    function_call_info = {"function_id": "local", "call_id": "local"}

    handle(config_file, function_call_info, client)


if __name__ == "__main__":
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
        "logPath": sys.argv[3],
    }
    run_locally(config_file)
