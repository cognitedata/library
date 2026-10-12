import random
import time
from datetime import UTC, datetime, timedelta

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_general_apply_service,
    create_general_pipeline_service,
    create_general_retrieve_service,
    create_logger_service,
    create_write_logger_service,
)
from fa_constants import FUNCTION_TIME_BUDGET_MINUTES
from services.apply_service import IApplyService
from services.config_service import Config, format_finalize_config
from services.finalize_service import AbstractFinalizeService, GeneralFinalizeService
from services.logger_service import CogniteFunctionLogger
from services.pipeline_service import IPipelineService
from services.retrieve_service import IRetrieveService
from utils.data_structures import PerformanceTracker

from stages.stage_runtime import STAGE_REPORTABLE_ERRORS, StageInput


def handle(data: dict[str, object], function_call_info: dict[str, object], client: CogniteClient) -> dict[str, object]:
    """
    Main entry point for the cognite function.
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the finalize function and create implementations of the interfaces
    3. Run the finalize instance until...
        4. It's been 7 minutes
        5. There are no jobs left to process
    6. Generate a report that includes capturing the annotations in RAW
    NOTE: Cognite functions have a run-time limit of 10 minutes.
    Don't want the function to die at the 10minute mark since there's no guarantee all code will execute.
    Thus we set a timelimit of 7 minutes (conservative) so that code execution is guaranteed.
    Documentation on calling a function:
    https://api-docs.cognite.com/20230101/tag/Function-calls/operation/postFunctionsCall
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
        finalize_instance = _create_finalize_service(
            config_instance, client, logger_instance, tracker_instance, function_call_info
        )

        logger_instance.info(
            format_finalize_config(config_instance, stage_input.extraction_pipeline_ext_id), section="START"
        )
        # NOTE: Random delay to stagger API requests and avoid empty results under high concurrency.
        delay = random.uniform(0.1, 1.0)
        time.sleep(delay)
        while datetime.now(UTC) - start_time < timedelta(minutes=FUNCTION_TIME_BUDGET_MINUTES):
            logger_instance.start_run()
            if finalize_instance.run() == "Done":
                break
            logger_instance.info(tracker_instance.generate_local_report(), "START")
        run_status = "success"
        return {"status": run_status, "data": data}
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Finalize stage failed", error=e, section="BOTH")
        raise
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Finalize"), "BOTH")
        function_id = function_call_info.get("function_id")
        call_id = function_call_info.get("call_id")
        pipeline_instance.update_extraction_pipeline(
            msg=tracker_instance.generate_ep_run("Finalize", function_id, call_id)
        )
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str], log_path: str | None = None) -> None:
    """
    Main entry point for local runs/debugging.
    (mimics parallel execution by using threads. Not the same as cognite functions but similar.)
    1. Create an instance of config, logger, and tracker
    2. Create an instance of the finalize function and create implementations of the interfaces
    3. Run the finalize instance until...
        4. There are no jobs left to process
    5. Generate a report that includes capturing the annotations in RAW
    """
    log_level = config_file.get("logLevel", "DEBUG")
    if log_path:
        logger_instance = create_write_logger_service(log_level=log_level, filepath=log_path)
    else:
        logger_instance = create_logger_service(log_level=log_level)

    tracker_instance = PerformanceTracker()
    try:
        config_instance, client = create_config_service(function_data=config_file)
        finalize_instance = _create_finalize_service(
            config_instance,
            client,
            logger_instance,
            tracker_instance,
            function_call_info={"function_id": None, "call_id": None},
        )

        logger_instance.info(
            format_finalize_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START"
        )
        while True:
            logger_instance.start_run()
            if finalize_instance.run():
                break
            logger_instance.info(tracker_instance.generate_local_report(), "START")
    except STAGE_REPORTABLE_ERRORS as e:
        logger_instance.error(message="Finalize stage failed", error=e, section="BOTH")
        raise
    finally:
        logger_instance.info(tracker_instance.generate_overall_report("Finalize"), "BOTH")
        logger_instance.close()


def _create_finalize_service(
    config: Config,
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    tracker: PerformanceTracker,
    function_call_info: dict,
) -> AbstractFinalizeService:
    """
    Instantiate Finalize with interfaces.
    """
    retrieve_instance: IRetrieveService = create_general_retrieve_service(client, config, logger)
    apply_instance: IApplyService = create_general_apply_service(client, config, logger)
    finalize_instance = GeneralFinalizeService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        retrieve_service=retrieve_instance,
        apply_service=apply_instance,
        function_call_info=function_call_info,
    )
    return finalize_instance
