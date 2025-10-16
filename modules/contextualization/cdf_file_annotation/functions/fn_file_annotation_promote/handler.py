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
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger
from services.EntitySearchService import EntitySearchService
from services.CacheService import CacheService


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict[str, str]:
    """
    Main entry point for the Cognite Function - promotes pattern-mode annotations.

    This function runs in a loop for up to 7 minutes, processing batches of pattern-mode
    annotations. For each batch:
    1. Retrieves candidate edges (pattern-mode annotations pointing to sink node)
    2. Searches for matching entities using EntitySearchService (with caching)
    3. Updates edges and RAW tables based on search results
    4. Pauses 10 seconds between batches

    Pattern-mode annotations are created when diagram detection finds text matching
    regex patterns but can't match it to the provided entity list. This function
    attempts to resolve those annotations post-hoc.

    Args:
        data: Function input data containing:
            - ExtractionPipelineExtId: ID of extraction pipeline for config
            - logLevel: Logging level (DEBUG, INFO, WARNING, ERROR)
            - logPath: Optional path for writing logs to file
        function_call_info: Metadata about the function call (not currently used)
        client: Pre-initialized CogniteClient for API interactions

    Returns:
        Dictionary with execution status:
        - {"status": "success", "message": "..."} on normal completion
        - {"status": "failure", "message": "..."} on error

    Raises:
        Exception: Any unexpected errors are caught, logged, and returned in status dict
    """
    start_time: datetime = datetime.now(timezone.utc)

    config: Config
    config, client = create_config_service(function_data=data)
    logger: CogniteFunctionLogger = create_logger_service(data.get("logLevel", "DEBUG"), data.get("logPath"))

    # Create service dependencies
    entity_search_service: EntitySearchService = create_entity_search_service(config, client, logger)
    cache_service: CacheService = create_cache_service(config, client, logger, entity_search_service)

    # Create promote service with injected dependencies
    promote_service: GeneralPromoteService = GeneralPromoteService(
        client=client,
        config=config,
        logger=logger,
        entity_search_service=entity_search_service,
        cache_service=cache_service,
    )

    try:
        # Run in a loop for a maximum of 7 minutes
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            result: str | None = promote_service.run()
            if result == "Done":
                logger.info("No more candidates to process. Exiting.", section="END")
                break
            time.sleep(10)  # Pause between batches

        return {"status": "success", "message": "promote function completed a cycle."}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", section="BOTH")
        return {"status": "failure", "message": str(e)}


def run_locally(config_file: dict) -> None:
    """
    Entry point for local execution and debugging.

    Runs the promote function locally using environment variables for authentication
    instead of Cognite Functions runtime. Useful for development and testing.

    Args:
        config_file: Configuration dictionary containing:
            - ExtractionPipelineExtId: ID of extraction pipeline for config
            - logLevel: Logging level (DEBUG, INFO, WARNING, ERROR)
            - logPath: Path for writing logs to file

    Returns:
        None (execution results are logged)

    Raises:
        ValueError: If required environment variables are missing
    """
    from dependencies import create_client, get_env_variables
    from utils.DataStructures import EnvConfig

    env_vars: EnvConfig = get_env_variables()
    client: CogniteClient = create_client(env_vars)

    # Mock function_call_info for local runs
    function_call_info: dict[str, str] = {"function_id": "local", "call_id": "local"}

    handle(config_file, function_call_info, client)


if __name__ == "__main__":
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
        "logPath": sys.argv[3],
    }
    run_locally(config_file)
