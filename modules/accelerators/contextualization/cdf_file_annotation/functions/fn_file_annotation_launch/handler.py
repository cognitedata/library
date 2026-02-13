import sys
from datetime import datetime, timedelta, timezone

from cognite.client import CogniteClient
from dependencies import (
    create_config_service,
    create_general_annotation_service,
    create_general_cache_service,
    create_general_data_model_service,
    create_general_pipeline_service,
    create_logger_service,
    create_write_logger_service,
)
from services.AnnotationService import IAnnotationService
from services.CacheService import ICacheService
from services.ConfigService import format_launch_config
from services.DataModelService import IDataModelService
from services.LaunchService import (
    AbstractLaunchService,
    GeneralLaunchService,
    LocalLaunchService,
)
from services.PipelineService import IPipelineService
from utils.DataStructures import PerformanceTracker


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
    start_time = datetime.now(timezone.utc)
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
    )

    logger_instance.info(format_launch_config(config_instance, data["ExtractionPipelineExtId"]), section="START")
    run_status: str = "success"
    try:
        while datetime.now(timezone.utc) - start_time < timedelta(minutes=7):
            if launch_instance.run() == "Done":
                return {"status": run_status, "data": data}
            logger_instance.info(tracker_instance.generate_local_report())
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
    launch_instance: AbstractLaunchService = _create_local_launch_service(
        config=config_instance,
        client=client,
        logger=logger_instance,
        tracker=tracker_instance,
        function_call_info={"function_id": None, "call_id": None},
    )

    logger_instance.info(format_launch_config(config_instance, config_file["ExtractionPipelineExtId"]), section="START")
    try:
        while True:
            if launch_instance.run() == "Done":
                break
            logger_instance.info(tracker_instance.generate_local_report())
    except Exception as e:
        logger_instance.error(
            message=f"Ran into the following error: \n{e}",
            section="END",
        )
    finally:
        logger_instance.info(tracker_instance.generate_overall_report(), "BOTH")
        logger_instance.close()


def _create_launch_service(config, client, logger, tracker, function_call_info) -> AbstractLaunchService:
    cache_instance: ICacheService = create_general_cache_service(config, client, logger)
    data_model_instance: IDataModelService = create_general_data_model_service(config, client, logger)
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
    )
    return launch_instance


def _create_local_launch_service(config, client, logger, tracker, function_call_info) -> AbstractLaunchService:
    cache_instance: ICacheService = create_general_cache_service(config, client, logger)
    data_model_instance: IDataModelService = create_general_data_model_service(config, client, logger)
    annotation_instance: IAnnotationService = create_general_annotation_service(config, client, logger)
    launch_instance: AbstractLaunchService = LocalLaunchService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        data_model_service=data_model_instance,
        cache_service=cache_instance,
        annotation_service=annotation_instance,
        function_call_info=function_call_info,
    )
    return launch_instance


if __name__ == "__main__":
    # NOTE: Receives the arguments from .vscode/launch.json. Mimics arguments that are passed into the serverless function.
    config_file = {
        "ExtractionPipelineExtId": sys.argv[1],
        "logLevel": sys.argv[2],
    }
    log_path = sys.argv[3]
    run_locally(config_file, log_path)
