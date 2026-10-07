from datetime import UTC, datetime, timedelta

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_general_data_model_service,
    create_general_pipeline_service,
    create_logger_service,
    create_write_logger_service,
)
from fa_constants import FUNCTION_TIME_BUDGET_MINUTES
from services.config_service import Config, format_prepare_config
from services.data_model_service import IDataModelService
from services.logger_service import CogniteFunctionLogger
from services.pipeline_service import IPipelineService
from services.prepare_service import AbstractPrepareService, GeneralPrepareService
from utils.data_structures import PerformanceTracker

from stages.stage_runtime import STAGE_REPORTABLE_ERRORS, StageInput


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the prepare function and create implementations of the interfaces
    3. Run the prepare instance until...
        4. It's been 7 minutes
        5. There are no files left that need to be prepared
    NOTE: Cognite functions have a run-time limit of 10 minutes.
    Don't want the function to die at the 10minute mark since there's no guarantee all code will execute.
    Thus we set a timelimit of 7 minutes (conservative) so that code execution is guaranteed.
    documentation on the calling a function can be found here...  https://api-docs.cognite.com/20230101/tag/Function-calls/operation/postFunctionsCall
    """
    start_time = datetime.now(UTC)
    stage_input = StageInput.model_validate(data)

    logger_instance = create_logger_service(stage_input.log_level)
    tracker_instance = PerformanceTracker()
    pipeline_instance: IPipelineService = create_general_pipeline_service(
        client, pipeline_ext_id=stage_input.extraction_pipeline_ext_id
    )
    run_status: str = "failure"
    try:
        config_instance, client = create_config_service(function_data=data, client=client)
        prepare_instance: AbstractPrepareService = _create_prepare_service(
            config=config_instance,
            client=client,
            logger=logger_instance,
            tracker=tracker_instance,
            function_call_info=function_call_info,
        )

        logger_instance.info(
            format_prepare_config(config_instance, stage_input.extraction_pipeline_ext_id), section="START"
        )
        while datetime.now(UTC) - start_time < timedelta(minutes=FUNCTION_TIME_BUDGET_MINUTES):
            logger_instance.start_run()
            if prepare_instance.run() == "Done":
                break
            logger_instance.info(tracker_instance.generate_local_report())
        run_status = "success"
        return {"status": run_status, "data": data}
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Prepare stage failed", error=e, section="BOTH")
        raise
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Prepare"), "BOTH")
        # only want to report on the count of successful and failed files in ep_logs if there were files that were processed or an error occured
        # else run log will be too messy.
        function_id = function_call_info.get("function_id")
        call_id = function_call_info.get("call_id")
        pipeline_instance.update_extraction_pipeline(
            msg=tracker_instance.generate_ep_run("Prepare", function_id, call_id)
        )
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str], log_path: str | None = None) -> None:
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the Prepare function and create implementations of the interfaces
    3. Run the prepare instance until
        4. There are no files left that need to be prepared
    """
    log_level = config_file.get("logLevel", "DEBUG")
    if log_path:
        logger_instance = create_write_logger_service(log_level=log_level, filepath=log_path)
    else:
        logger_instance = create_logger_service(log_level=log_level)
    tracker_instance = PerformanceTracker()
    try:
        config_instance, client = create_config_service(function_data=config_file)
        prepare_instance: AbstractPrepareService = _create_prepare_service(
            config=config_instance,
            client=client,
            logger=logger_instance,
            tracker=tracker_instance,
            function_call_info={"function_id": None, "call_id": None},
        )

        logger_instance.info(
            format_prepare_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START"
        )
        while True:
            logger_instance.start_run()
            if prepare_instance.run() == "Done":
                break
            logger_instance.info(tracker_instance.generate_local_report())
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Prepare stage failed", error=e, section="END")
        raise
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Prepare"), "BOTH")
        logger_instance.close()


def _create_prepare_service(
    config: Config,
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    tracker: PerformanceTracker,
    function_call_info: dict,
) -> AbstractPrepareService:
    data_model_instance: IDataModelService = create_general_data_model_service(config, client, logger)
    prepare_instance: AbstractPrepareService = GeneralPrepareService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        data_model_service=data_model_instance,
        function_call_info=function_call_info,
    )
    return prepare_instance
