import time
from datetime import UTC, datetime, timedelta

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_general_annotation_service,
    create_general_data_model_service,
    create_general_entity_cache_service,
    create_general_pipeline_service,
    create_logger_service,
    create_write_logger_service,
    get_pipeline_data_set_id,
)
from fa_constants import FUNCTION_TIME_BUDGET_MINUTES
from services.AnnotationService import IAnnotationService
from services.ConfigService import format_launch_config
from services.DataModelService import IDataModelService
from services.EntityCacheService import ICacheService
from services.LaunchService import (
    AbstractLaunchService,
    DeployedRateLimitPolicy,
    GeneralLaunchService,
    LocalRateLimitPolicy,
    RateLimitPolicy,
)
from services.PipelineService import IPipelineService
from utils.DataStructures import PerformanceTracker

from stages.stage_runtime import STAGE_REPORTABLE_ERRORS, failure_response


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the launch function and create implementations of the interfaces
    3. Run the launch instance until...
        4. It's been 7 minutes
        5. There are no files left that need to be launched
    NOTE: Cognite functions have a run-time limit of 10 minutes.
    Don't want the function to die at the 10minute mark since there's no guarantee all code will execute.
    Thus we set a timelimit of 7 minutes (conservative) so that code execution is guaranteed.
    documentation on the calling a function can be found here...  https://api-docs.cognite.com/20230101/tag/Function-calls/operation/postFunctionsCall
    """
    start_time = datetime.now(UTC)
    deadline = time.monotonic() + FUNCTION_TIME_BUDGET_MINUTES * 60
    log_level = data.get("logLevel", "INFO")

    config_instance, client = create_config_service(function_data=data, client=client)
    logger_instance = create_logger_service(log_level)
    tracker_instance = PerformanceTracker()
    pipeline_instance: IPipelineService = create_general_pipeline_service(
        client, pipeline_ext_id=data["ExtractionPipelineExtId"]
    )
    launch_instance: AbstractLaunchService = _create_launch_service(
        config=config_instance,
        client=client,
        logger=logger_instance,
        tracker=tracker_instance,
        function_call_info=function_call_info,
        rate_limit_policy=DeployedRateLimitPolicy(),
        data_set_id=get_pipeline_data_set_id(client, data["ExtractionPipelineExtId"]),
        entity_read_deadline=deadline,
    )

    logger_instance.info(format_launch_config(config_instance, data["ExtractionPipelineExtId"]), section="START")
    run_status: str = "success"
    try:
        while datetime.now(UTC) - start_time < timedelta(minutes=FUNCTION_TIME_BUDGET_MINUTES):
            logger_instance.start_run()
            if launch_instance.run() == "Done":
                return {"status": run_status, "data": data}
            logger_instance.info(tracker_instance.generate_local_report())
        return {"status": run_status, "data": data}
    except STAGE_REPORTABLE_ERRORS as e:
        run_status = "failure"
        logger_instance.error(message="Launch stage failed", error=e, section="BOTH")
        return failure_response(e)
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Launch"), "BOTH")
        function_id = function_call_info.get("function_id")
        call_id = function_call_info.get("call_id")
        pipeline_instance.update_extraction_pipeline(
            msg=tracker_instance.generate_ep_run("Launch", function_id, call_id)
        )
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str], log_path: str | None = None):
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the Launch function and create implementations of the interfaces
    3. Run the launch instance until
        4. There are no files left that need to be launched
    """
    log_level = config_file.get("logLevel", "DEBUG")
    config_instance, client = create_config_service(function_data=config_file)

    if log_path:
        logger_instance = create_write_logger_service(log_level=log_level, filepath=log_path)
    else:
        logger_instance = create_logger_service(log_level=log_level)
    tracker_instance = PerformanceTracker()
    launch_instance: AbstractLaunchService = _create_launch_service(
        config=config_instance,
        client=client,
        logger=logger_instance,
        tracker=tracker_instance,
        function_call_info={"function_id": None, "call_id": None},
        rate_limit_policy=LocalRateLimitPolicy(),
        data_set_id=get_pipeline_data_set_id(client, config_file["ExtractionPipelineExtId"]),
        entity_read_deadline=None,
    )

    logger_instance.info(format_launch_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START")
    try:
        while True:
            logger_instance.start_run()
            if launch_instance.run() == "Done":
                break
            logger_instance.info(tracker_instance.generate_local_report())
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Launch stage failed", error=e, section="END")
        raise
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Launch"), "BOTH")
        logger_instance.close()


def _create_launch_service(
    config,
    client,
    logger,
    tracker,
    function_call_info,
    rate_limit_policy: RateLimitPolicy,
    data_set_id: int | None,
    entity_read_deadline: float | None,
) -> AbstractLaunchService:
    cache_instance: ICacheService = create_general_entity_cache_service(config, client, logger)
    data_model_instance: IDataModelService = create_general_data_model_service(
        config, client, logger, data_set_id, entity_read_deadline
    )
    annotation_instance: IAnnotationService = create_general_annotation_service(config, client, logger)
    launch_instance: AbstractLaunchService = GeneralLaunchService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        data_model_service=data_model_instance,
        cache_service=cache_instance,
        annotation_service=annotation_instance,
        function_call_info=function_call_info,
        rate_limit_policy=rate_limit_policy,
    )
    return launch_instance
