import random
import sys
import time
from datetime import datetime, timedelta, timezone

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_general_apply_service,
    create_general_pipeline_service,
    create_general_retrieve_service,
    create_logger_service,
    create_write_logger_service,
)
from services.ApplyService import IApplyService
from services.ConfigService import format_finalize_config
from services.FinalizeService import AbstractFinalizeService, GeneralFinalizeService
from services.PipelineService import IPipelineService
from services.RetrieveService import IRetrieveService
from utils.DataStructures import PerformanceTracker


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
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
    documentation on the calling a function can be found here...  https://api-docs.cognite.com/20230101/tag/Function-calls/operation/postFunctionsCall
    """
    start_time = datetime.now(timezone.utc)
    log_level = data.get("logLevel", "INFO")

    config_instance, client = create_config_service(function_data=data, client=client)
    logger_instance = create_logger_service(log_level)
    tracker_instance = PerformanceTracker()
    pipeline_instance: IPipelineService = create_general_pipeline_service(
        client, pipeline_ext_id=data["ExtractionPipelineExtId"]
    )
    finalize_instance = _create_finalize_service(
        config_instance, client, logger_instance, tracker_instance, function_call_info
    )

    logger_instance.info(format_finalize_config(config_instance, data["ExtractionPipelineExtId"]), section="START")
    run_status: str = "success"
    # NOTE: a random delay to stagger API requests. Used to prevent API load shedding that can return empty results under high concurrency.
    delay = random.uniform(0.1, 1.0)
    time.sleep(delay)
    try:
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            if finalize_instance.run() == "Done":
                return {"status": run_status, "data": data}
            logger_instance.info(tracker_instance.generate_local_report(), "START")
        return {"status": run_status, "data": data}
    except Exception as e:
        run_status = "failure"
        msg = f"{str(e)}"
        logger_instance.error(message=msg, section="BOTH")
        return {"status": run_status, "message": msg}
    finally:
        logger_instance.info(tracker_instance.generate_overall_report(), "BOTH")
        function_id = function_call_info.get("function_id")
        call_id = function_call_info.get("call_id")
        pipeline_instance.update_extraction_pipeline(
            msg=tracker_instance.generate_ep_run("Finalize", function_id, call_id)
        )
        pipeline_instance.upload_extraction_pipeline(status=run_status)


def run_locally(config_file: dict[str, str], log_path: str | None = None):
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
    config_instance, client = create_config_service(function_data=config_file)

    if log_path:
        logger_instance = create_write_logger_service(log_level=log_level, filepath=log_path)
    else:
        logger_instance = create_logger_service(log_level=log_level)

    tracker_instance = PerformanceTracker()
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
    try:
        while True:
            if finalize_instance.run():
                break
            logger_instance.info(tracker_instance.generate_local_report(), "START")
    except Exception as e:
        logger_instance.error(
            message=f"Ran into the following error: \n{e}",
            section="BOTH",
        )
    finally:
        logger_instance.info(tracker_instance.generate_overall_report(), "BOTH")
        logger_instance.close()


def _create_finalize_service(config, client, logger, tracker, function_call_info) -> AbstractFinalizeService:
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


if __name__ == "__main__":
    # NOTE: Receives the arguments from .vscode/launch.json. Mimics arguments that are passed into the serverless function.
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
    }
    log_path = sys.argv[3] if len(sys.argv) > 3 else None
    run_locally(config_file, log_path)
