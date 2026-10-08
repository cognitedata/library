from datetime import UTC, datetime, timedelta

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_entity_search_service,
    create_general_pipeline_service,
    create_logger_service,
    create_promote_cache_service,
)
from fa_constants import FUNCTION_TIME_BUDGET_MINUTES
from services.config_service import Config, format_promote_config
from services.entity_search_service import EntitySearchService
from services.logger_service import CogniteFunctionLogger
from services.pipeline_service import IPipelineService
from services.promote_cache_service import CacheService
from services.promote_service import GeneralPromoteService
from utils.data_structures import PromoteTracker

from stages.stage_runtime import STAGE_REPORTABLE_ERRORS, StageInput


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
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
        {"status": "success", "data": ...} when the stage finishes or hits its time budget.

    Raises:
        STAGE_REPORTABLE_ERRORS: Logged, then re-raised so the CDF function call fails.
    """
    start_time: datetime = datetime.now(UTC)
    stage_input = StageInput.model_validate(data)

    logger_instance: CogniteFunctionLogger = create_logger_service(stage_input.log_level, stage_input.log_path)
    tracker_instance: PromoteTracker = PromoteTracker()
    pipeline_instance: IPipelineService = create_general_pipeline_service(
        client, pipeline_ext_id=stage_input.extraction_pipeline_ext_id
    )
    run_status: str = "failure"
    try:
        config_instance: Config
        config_instance, client = create_config_service(function_data=data, client=client)
        entity_search_service: EntitySearchService = create_entity_search_service(
            config_instance, client, logger_instance
        )
        cache_service: CacheService = create_promote_cache_service(
            config_instance, client, logger_instance, entity_search_service
        )
        promote_service: GeneralPromoteService = GeneralPromoteService(
            client=client,
            config=config_instance,
            logger=logger_instance,
            tracker=tracker_instance,
            entity_search_service=entity_search_service,
            cache_service=cache_service,
        )

        logger_instance.info(
            format_promote_config(config_instance, stage_input.extraction_pipeline_ext_id), section="START"
        )
        # Run in a loop for a maximum of 7 minutes b/c serverless functions can run for max 10 minutes before hardware dies
        while datetime.now(UTC) - start_time < timedelta(minutes=FUNCTION_TIME_BUDGET_MINUTES):
            logger_instance.start_run()
            result: str | None = promote_service.run()
            if result == "Done":
                logger_instance.info("No more candidates to process. Exiting.", section="END")
                break
            # Log batch report and pause between batches
            logger_instance.info(tracker_instance.generate_local_report(), section="START")
        run_status = "success"
        return {"status": run_status, "data": data}
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Promote stage failed", error=e, section="BOTH")
        raise
    finally:
        # Generate overall summary report
        logger_instance.info(tracker_instance.generate_overall_report(), section="BOTH")
        function_id = function_call_info.get("function_id")
        call_id = function_call_info.get("call_id")
        pipeline_instance.update_extraction_pipeline(msg=tracker_instance.generate_ep_run(function_id, call_id))
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str]) -> None:
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
    logger_instance: CogniteFunctionLogger = create_logger_service(
        config_file.get("logLevel", "DEBUG"), config_file.get("logPath")
    )
    tracker_instance: PromoteTracker = PromoteTracker()
    try:
        config_instance: Config
        config_instance, client = create_config_service(function_data=config_file)
        entity_search_service: EntitySearchService = create_entity_search_service(
            config_instance, client, logger_instance
        )
        cache_service: CacheService = create_promote_cache_service(
            config_instance, client, logger_instance, entity_search_service
        )
        promote_service: GeneralPromoteService = GeneralPromoteService(
            client=client,
            config=config_instance,
            logger=logger_instance,
            tracker=tracker_instance,
            entity_search_service=entity_search_service,
            cache_service=cache_service,
        )
        logger_instance.info(
            format_promote_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START"
        )
        # Run in a loop for a maximum of 7 minutes b/c serverless functions can run for max 10 minutes before hardware dies
        while True:
            logger_instance.start_run()
            result: str | None = promote_service.run()
            if result == "Done":
                logger_instance.info("No more candidates to process. Exiting.", section="END")
                break
            # Log batch report and pause between batches
            logger_instance.info(tracker_instance.generate_local_report(), section="START")
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Promote stage failed", error=e, section="BOTH")
        raise
    finally:
        # Generate overall summary report
        logger_instance.info(tracker_instance.generate_overall_report(), section="BOTH")
        logger_instance.close()
