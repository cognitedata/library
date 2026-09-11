"""Entity matching, part two: collecting the predict jobs submit started.

Queued jobs are polled oldest first. A job that finishes has its matches merged with the
manual and rule based ones submit staged, written to RAW and the data model, and is then
taken off the queue. A job that is still running when the run's time is up stays queued
for the next run, which is a normal outcome rather than a failure.
"""

import time
from collections.abc import Iterator
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob

from config import Config  # isort: skip
from constants import (  # isort: skip
    ENTITY_MATCHING_JOB_STATUS_PATH,
    JOB_API_STATUS_COMPLETED,
    JOB_API_STATUS_FAILED,
    JOB_RESULT_ITEMS,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    POLL_BACKOFF_SECONDS,
    POLL_BUDGET_SECONDS,
    STATUS_FAILURE,
    STATUS_SUCCESS,
)
from job_state import (  # isort: skip
    PredictJob,
    delete_predict_job,
    list_predict_jobs,
    mark_job_running,
)
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import (  # isort: skip
    select_and_apply_matches,
    update_pipeline_run,
    write_mapping_to_raw,
)
from pipeline_optimizations import cleanup_memory, monitor_memory_usage, time_operation  # isort: skip
from staging import delete_staged_matches, read_staged_matches  # isort: skip


def poll_intervals() -> Iterator[int]:
    """Seconds to wait between polls: 5, then 15, then 30 for as long as it takes."""
    yield from POLL_BACKOFF_SECONDS[:-1]
    while True:
        yield POLL_BACKOFF_SECONDS[-1]


def _as_contextualization_job(client: CogniteClient, job: PredictJob) -> ContextualizationJob:
    """Rebuild the SDK job from what the state store row holds."""
    return ContextualizationJob(
        job_id=int(job.job_id),
        model_id=int(job.model_id) if job.model_id else None,
        status=job.status,
        status_path=ENTITY_MATCHING_JOB_STATUS_PATH,
        job_token=job.job_token,
        cognite_client=client,
    )


def wait_for_job(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    job: PredictJob,
    seconds_left: float,
) -> tuple[ContextualizationJob, str]:
    """Poll one predict job until it finishes or the run is out of time.

    Args:
        job: Queued job to poll.
        seconds_left: How long this run may still spend waiting.

    Returns:
        The SDK job and its last known status. A status that is neither Completed nor
        Failed means the time ran out with the job still running.
    """
    sdk_job = _as_contextualization_job(client, job)
    deadline = time.monotonic() + seconds_left
    intervals = poll_intervals()

    status = sdk_job.update_status()
    logger.debug(f"Poll job {job.job_id}: status={status}")

    while status not in (JOB_API_STATUS_COMPLETED, JOB_API_STATUS_FAILED):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        interval = min(next(intervals), remaining)
        logger.debug(f"Next poll for job {job.job_id} in {interval:.0f}s")
        time.sleep(interval)
        status = sdk_job.update_status()
        logger.debug(f"Poll job {job.job_id}: status={status}")

    return sdk_job, status


def collect_entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config,
) -> None:
    """Work through the queue of predict jobs until it is empty or the time is up.

    Args:
        client: Cognite client.
        logger: Function logger.
        data: Function invocation payload, carrying the extraction pipeline external id.
        config: Configuration read from that extraction pipeline.

    Raises:
        Exception: Whatever reading the queue or writing results raises, after the
            failure is reported on the extraction pipeline run.
    """
    started = time.monotonic()
    pipeline_ext_id = data["ExtractionPipelineExtId"]
    collected_matches, collected_bad_matches = 0, 0

    try:
        if config.parameters.debug:
            logger = CogniteFunctionLogger(LOG_LEVEL_DEBUG)
            logger.debug("**** Write debug messages *****")

        from cognite.extractorutils.uploader import RawUploadQueue

        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level=LOG_LEVEL_INFO)
        monitor_memory_usage(logger, "Pipeline start")

        jobs = list_predict_jobs(client, config, logger)
        logger.info(f"Found {len(jobs)} pending predict job(s) in state table")
        if not jobs:
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, 0, 0, "No predict jobs to collect")
            return

        failed_jobs: list[str] = []
        collected_jobs: list[str] = []
        for position, job in enumerate(jobs, start=1):
            seconds_left = POLL_BUDGET_SECONDS - (time.monotonic() - started)
            if seconds_left <= 0:
                logger.warning(
                    f"Time budget of {POLL_BUDGET_SECONDS}s used up before job {job.job_id} - "
                    "next collect run will continue"
                )
                break

            logger.info(
                f"Processing predict job - jobId: {job.job_id} "
                f"(createdAt={job.created_at}, position {position} of {len(jobs)})"
            )
            job = mark_job_running(client, config, logger, job)

            with time_operation(f"Poll predict job {job.job_id}", logger):
                sdk_job, status = wait_for_job(client, logger, job, seconds_left)

            if status == JOB_API_STATUS_FAILED:
                logger.error(f"Predict job {job.job_id} failed: {sdk_job.error_message}")
                delete_staged_matches(client, config, logger, job.job_id)
                delete_predict_job(client, config, logger, job)
                failed_jobs.append(job.job_id)
                continue

            if status != JOB_API_STATUS_COMPLETED:
                logger.warning(
                    f"Predict job {job.job_id} still running after {POLL_BUDGET_SECONDS}s - "
                    "exiting, next collect run will continue"
                )
                break

            logger.info(f"Predict job {job.job_id} completed - collecting results")
            match_results = sdk_job.result[JOB_RESULT_ITEMS]
            staged_matches = read_staged_matches(client, config, logger, job.job_id)

            with time_operation("Select and apply matches", logger):
                good_matches, bad_matches, cnt_entity_matching = select_and_apply_matches(  # type: ignore
                    client, config, logger, staged_matches, match_results
                )

            with time_operation("Write mapping to RAW", logger):
                write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, logger)

            delete_staged_matches(client, config, logger, job.job_id)
            delete_predict_job(client, config, logger, job)

            collected_matches += cnt_entity_matching
            collected_bad_matches += len(bad_matches)
            collected_jobs.append(job.job_id)
            logger.info(
                f"Job {job.job_id}: {cnt_entity_matching} match(es) from the model applied, "
                f"{len(staged_matches)} staged manual/rule match(es) kept"
            )
            cleanup_memory()

        remaining = len(jobs) - len(collected_jobs) - len(failed_jobs)
        if remaining > 0:
            logger.info(f"Collect run ended with {remaining} of {len(jobs)} job(s) still queued")
        monitor_memory_usage(logger, "Pipeline end")

        if failed_jobs:
            update_pipeline_run(
                client,
                logger,
                pipeline_ext_id,
                STATUS_FAILURE,
                collected_matches,
                collected_bad_matches,
                f"Predict job(s) failed: {', '.join(failed_jobs)}",
            )
            return

        dm_msg = (
            "Relationships updated in the DM (dmUpdate: True)"
            if config.parameters.dm_update
            else "Relationships NOT updated in DM, only updated the RAW tables (dmUpdate: False)"
        )
        update_pipeline_run(
            client,
            logger,
            pipeline_ext_id,
            STATUS_SUCCESS,
            collected_matches,
            collected_bad_matches,
            f"Collected {len(collected_jobs)} predict job(s), {remaining} still queued. {dm_msg}",
        )

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, STATUS_FAILURE, collected_matches, collected_bad_matches, msg)
        raise
