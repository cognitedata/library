"""Entity matching, part two: collecting the predict jobs submit started.

Queued jobs are polled in parallel (one worker per job, at most ten). A job that finishes
has its matches merged with the manual and rule based ones submit staged, written to RAW
and the data model, and is then taken off the queue. Jobs still running when the run's
time is up stay queued for the next run, which is a normal outcome rather than a failure.
"""

import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob

from em_config import Config  # isort: skip
from em_constants import (  # isort: skip
    COLLECT_MAX_WORKERS,
    ENTITY_MATCHING_JOB_STATUS_PATH,
    JOB_API_STATUS_COMPLETED,
    JOB_API_STATUS_FAILED,
    JOB_RESULT_ITEMS,
    LOG_LEVEL_INFO,
    POLL_BUDGET_SECONDS,
    POLL_INTERVAL_SECONDS,
    STATUS_FAILURE,
    STATUS_SUCCESS,
)
from em_job_state import (  # isort: skip
    PredictJob,
    delete_predict_job,
    list_predict_jobs,
    mark_job_running,
)
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline import (  # isort: skip
    select_and_apply_matches,
    update_pipeline_run,
    write_mapping_to_raw,
)
from em_pipeline_optimizations import time_operation  # isort: skip
from em_pipeline_types import FunctionInputData  # isort: skip
from em_staging import delete_staged_matches, read_staged_matches  # isort: skip

# RawUploadQueue is only constructed at runtime; importing it lazily lets em_collect be
# imported (e.g. for unit tests) without cognite-extractor-utils installed.
if TYPE_CHECKING:
    from cognite.extractorutils.uploader import RawUploadQueue


def _raw_upload_queue(client: CogniteClient) -> "RawUploadQueue":
    from cognite.extractorutils.uploader import RawUploadQueue

    return RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level=LOG_LEVEL_INFO)


@dataclass(frozen=True)
class CollectJobOutcome:
    """How one queued predict job ended in this collect run."""

    job_id: str
    status: str
    model_matches: int = 0
    bad_matches: int = 0


def poll_intervals() -> Iterator[int]:
    """Seconds to wait between polls, 30s for as long as it takes."""
    while True:
        yield POLL_INTERVAL_SECONDS


def _as_contextualization_job(client: CogniteClient, job: PredictJob) -> ContextualizationJob:
    """Rebuild the SDK job from what the state store row holds."""
    return ContextualizationJob(  # type: ignore[abstract]
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
    cdf_lock: threading.Lock | None = None,
) -> tuple[ContextualizationJob, str]:
    """Poll one predict job until it finishes or the run is out of time.

    Args:
        job: Queued job to poll.
        seconds_left: How long this run may still spend waiting.
        cdf_lock: Optional lock around SDK calls when several jobs are polled at once.

    Returns:
        The SDK job and its last known status. A status that is neither Completed nor
        Failed means the time ran out with the job still running.
    """
    sdk_job = _as_contextualization_job(client, job)
    deadline = time.monotonic() + seconds_left
    intervals = poll_intervals()

    def poll_status() -> str:
        if cdf_lock is None:
            return sdk_job.update_status()
        with cdf_lock:
            return sdk_job.update_status()

    status = poll_status()
    logger.debug(f"Poll job {job.job_id}: status={status}")

    while status not in (JOB_API_STATUS_COMPLETED, JOB_API_STATUS_FAILED):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        interval = min(next(intervals), remaining)
        logger.debug(f"Next poll for job {job.job_id} in {interval:.0f}s")
        time.sleep(interval)
        status = poll_status()
        logger.debug(f"Poll job {job.job_id}: status={status}")

    return sdk_job, status


def drain_job_queue(
    jobs: list[PredictJob],
    collect_one: Callable[[PredictJob, float], CollectJobOutcome],
    *,
    deadline: float,
    max_workers: int = COLLECT_MAX_WORKERS,
    now: Callable[[], float] = time.monotonic,
) -> list[CollectJobOutcome]:
    """Poll queued jobs in parallel until they finish or the deadline is reached.

    At most `max_workers` jobs are in flight. When one finishes and time remains, the
    next queued job is started.
    """
    pending = list(jobs)
    outcomes: list[CollectJobOutcome] = []
    in_flight: dict[Future[CollectJobOutcome], PredictJob] = {}

    def submit(pool: ThreadPoolExecutor, job: PredictJob) -> None:
        seconds_left = max(0.0, deadline - now())
        in_flight[pool.submit(collect_one, job, seconds_left)] = job

    def take_finished(finished: set[Future[CollectJobOutcome]]) -> None:
        for future in finished:
            in_flight.pop(future)
            outcomes.append(future.result())

    workers = max(1, min(max_workers, len(jobs) or 1))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while pending or in_flight:
            if now() >= deadline:
                if in_flight:
                    take_finished(set(wait(in_flight).done))
                break
            while pending and len(in_flight) < max_workers:
                submit(pool, pending.pop(0))
            if not in_flight:
                break
            done, _ = wait(in_flight, timeout=max(0.0, deadline - now()), return_when=FIRST_COMPLETED)
            take_finished(set(done))
    return outcomes


def collect_entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: FunctionInputData,
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
        raw_uploader = _raw_upload_queue(client)

        jobs = list_predict_jobs(client, config, logger)
        logger.info(f"Found {len(jobs)} pending predict job(s) in state table")
        if not jobs:
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, 0, 0, "No predict jobs to collect")
            return

        cdf_lock = threading.Lock()

        def collect_one(job: PredictJob, seconds_left: float) -> CollectJobOutcome:
            return _collect_one_job(client, logger, config, raw_uploader, cdf_lock, job, seconds_left, len(jobs))

        logger.info(
            f"Collecting with up to {min(COLLECT_MAX_WORKERS, len(jobs))} worker(s), "
            f"{POLL_INTERVAL_SECONDS}s poll interval, {POLL_BUDGET_SECONDS}s budget"
        )
        outcomes = drain_job_queue(
            jobs,
            collect_one,
            deadline=started + POLL_BUDGET_SECONDS,
            max_workers=COLLECT_MAX_WORKERS,
        )

        failed_jobs = [o.job_id for o in outcomes if o.status == JOB_API_STATUS_FAILED]
        collected_jobs = [o.job_id for o in outcomes if o.status == JOB_API_STATUS_COMPLETED]
        collected_matches = sum(o.model_matches for o in outcomes)
        collected_bad_matches = sum(o.bad_matches for o in outcomes)
        remaining = len(jobs) - len(collected_jobs) - len(failed_jobs)
        if remaining > 0:
            logger.info(f"Collect run ended with {remaining} of {len(jobs)} job(s) still queued")

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
        update_pipeline_run(
            client, logger, pipeline_ext_id, STATUS_FAILURE, collected_matches, collected_bad_matches, msg
        )
        raise


def _collect_one_job(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    raw_uploader: "RawUploadQueue",
    cdf_lock: threading.Lock,
    job: PredictJob,
    seconds_left: float,
    queue_size: int,
) -> CollectJobOutcome:
    """Poll one job and, when it finishes, write its matches. Thread-safe against `cdf_lock`."""
    with cdf_lock:
        job = mark_job_running(client, config, logger, job)
        logger.info(f"Processing predict job - jobId: {job.job_id} ({queue_size} queued)")

    if seconds_left <= 0:
        logger.warning(
            f"Time budget of {POLL_BUDGET_SECONDS}s used up before job {job.job_id} - next collect run will continue"
        )
        return CollectJobOutcome(job.job_id, job.status)

    with time_operation(f"Poll predict job {job.job_id}", logger):
        sdk_job, status = wait_for_job(client, logger, job, seconds_left, cdf_lock)

    if status == JOB_API_STATUS_FAILED:
        logger.error(f"Predict job {job.job_id} failed: {sdk_job.error_message}")
        with cdf_lock:
            delete_staged_matches(client, logger, job.job_id)
            delete_predict_job(client, config, logger, job)
        return CollectJobOutcome(job.job_id, status)

    if status != JOB_API_STATUS_COMPLETED:
        logger.warning(
            f"Predict job {job.job_id} still running after {POLL_BUDGET_SECONDS}s - next collect run will continue"
        )
        return CollectJobOutcome(job.job_id, status)

    match_results = sdk_job.result.get(JOB_RESULT_ITEMS, []) if sdk_job.result else []
    submitted = f", submitted {job.source_count} source record(s)" if job.source_count is not None else ""
    logger.info(f"Predict job {job.job_id} completed - collecting {len(match_results)} result(s){submitted}")

    with cdf_lock:
        try:
            staged_matches = read_staged_matches(client, logger, job.job_id, job.staging_digest)
        except ValueError as e:
            # Fails the same way on every run, so leaving the job queued would block it for good.
            logger.error(f"Dropping predict job {job.job_id}, its staged matches cannot be trusted: {e}")
            delete_staged_matches(client, logger, job.job_id)
            delete_predict_job(client, config, logger, job)
            return CollectJobOutcome(job.job_id, JOB_API_STATUS_FAILED)
        with time_operation("Select and apply matches", logger):
            good_matches, bad_matches, cnt_entity_matching = select_and_apply_matches(
                client, config, logger, staged_matches, match_results
            )
        with time_operation("Write mapping to RAW", logger):
            write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, logger)
        delete_staged_matches(client, logger, job.job_id)
        delete_predict_job(client, config, logger, job)

    logger.info(
        f"Job {job.job_id}: {cnt_entity_matching} match(es) from the model applied, "
        f"{len(staged_matches)} staged manual/rule match(es) kept"
    )
    return CollectJobOutcome(job.job_id, status, cnt_entity_matching, len(bad_matches))
