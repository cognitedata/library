import sys
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_logger_service,
    create_entity_search_service,
    create_cache_service,
)
from services.ConfigService import format_promote_config
from services.PromoteService import GeneralPromoteService
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger
from services.EntitySearchService import EntitySearchService
from services.CacheService import CacheService
from utils.DataStructures import PromoteTracker


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict[str, str]:
    """
    Main entry point for the Cognite Function - promotes pattern-mode annotations.

    This function runs in a loop for up to 7 minutes, processing batches of pattern-mode
    annotations. For each batch:
    1. Retrieves candidate edges (pattern-mode annotations pointing to sink node)
    2. Searches for matching entities using EntitySearchService (with caching)
    3. Updates edges and RAW tables based on search results

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

    config_instance: Config
    config_instance, client = create_config_service(function_data=data, client=client)
    logger_instance: CogniteFunctionLogger = create_logger_service(data.get("logLevel", "DEBUG"), data.get("logPath"))
    tracker_instance: PromoteTracker = PromoteTracker()

    entity_search_service: EntitySearchService = create_entity_search_service(config_instance, client, logger_instance)
    cache_service: CacheService = create_cache_service(config_instance, client, logger_instance, entity_search_service)
    promote_service: GeneralPromoteService = GeneralPromoteService(
        client=client,
        config=config_instance,
        logger=logger_instance,
        tracker=tracker_instance,
        entity_search_service=entity_search_service,
        cache_service=cache_service,
    )

    logger_instance.info(format_promote_config(config_instance, data["ExtractionPipelineExtId"]), section="START")
    run_status: str = "success"
    try:
        # Run in a loop for a maximum of 7 minutes b/c serverless functions can run for max 10 minutes before hardware dies
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            result: str | None = promote_service.run()
            if result == "Done":
                logger_instance.info("No more candidates to process. Exiting.", section="END")
                break
            # Log batch report and pause between batches
            logger_instance.info(tracker_instance.generate_local_report(), section="START")
        return {"status": run_status, "data": data}
    except Exception as e:
        run_status = "failure"
        msg: str = f"{str(e)}"
        logger_instance.error(f"An unexpected error occurred: {msg}", section="BOTH")
        return {"status": run_status, "message": msg}
    finally:
        # Generate overall summary report
        logger_instance.info(tracker_instance.generate_overall_report(), section="BOTH")


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
    config_instance: Config
    config_instance, client = create_config_service(function_data=config_file)
    logger_instance: CogniteFunctionLogger = create_logger_service(
        config_file.get("logLevel", "DEBUG"), config_file.get("logPath")
    )
    tracker_instance: PromoteTracker = PromoteTracker()

    # Create service dependencies
    entity_search_service: EntitySearchService = create_entity_search_service(config_instance, client, logger_instance)
    cache_service: CacheService = create_cache_service(config_instance, client, logger_instance, entity_search_service)

    # Create promote service with injected dependencies
    promote_service: GeneralPromoteService = GeneralPromoteService(
        client=client,
        config=config_instance,
        logger=logger_instance,
        tracker=tracker_instance,
        entity_search_service=entity_search_service,
        cache_service=cache_service,
    )
    logger_instance.info(format_promote_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START")
    try:
        # Run in a loop for a maximum of 7 minutes b/c serverless functions can run for max 10 minutes before hardware dies
        while True:
            result: str | None = promote_service.run()
            if result == "Done":
                logger_instance.info("No more candidates to process. Exiting.", section="END")
                break
            # Log batch report and pause between batches
            logger_instance.info(tracker_instance.generate_local_report(), section="START")
    except Exception as e:
        run_status = "failure"
        msg: str = f"{str(e)}"
        logger_instance.error(f"An unexpected error occurred: {msg}", section="BOTH")
    finally:
        # Generate overall summary report
        logger_instance.info(tracker_instance.generate_overall_report(), section="BOTH")


if __name__ == "__main__":
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
        "logPath": sys.argv[3],
    }
    run_locally(config_file)
