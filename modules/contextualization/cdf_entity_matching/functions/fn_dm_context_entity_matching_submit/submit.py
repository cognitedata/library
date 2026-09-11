# Generated from functions/_entity_matching_core/submit.py - do not edit this copy.
# Change the source and run: python scripts/sync_entity_matching_core.py
"""Entity matching, part one: everything up to starting the predict job in CDF.

Manual and rule based matches are found here and staged in RAW, the predict job is
started without waiting for it, and the job is added to the queue collect works through.
The run ends as soon as CDF has accepted the job, so a long prediction can no longer
time the function out.
"""

from typing import Any

from cognite.client import CogniteClient

from config import Config  # isort: skip
from constants import (  # isort: skip
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    PROP_COL_NAME,
    QUERY_FILTER_TYPE_TARGETS,
    STAT_STORE_MATCH_MODEL_ID,
    STATUS_FAILURE,
    STATUS_SUCCESS,
)
from job_state import append_predict_job  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from pipeline import (  # isort: skip
    apply_manual_mappings,
    apply_rule_mappings,
    get_all_targets,
    get_new_entities,
    instance_key,
    read_manual_mappings,
    read_rule_mappings,
    read_state_store,
    submit_predict_job,
    update_pipeline_run,
)
from pipeline_optimizations import cleanup_memory, monitor_memory_usage, time_operation  # isort: skip
from staging import clear_finished_matches, staging_prefix, write_staged_matches  # isort: skip


def submit_entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config,
) -> None:
    """Find the matches that need no model, then start the predict job for the rest.

    Args:
        client: Cognite client.
        logger: Function logger.
        data: Function invocation payload, carrying the extraction pipeline external id.
        config: Configuration read from that extraction pipeline.

    Raises:
        Exception: Whatever the pipeline steps raise, after the failure is reported on
            the extraction pipeline run.
    """
    good_matches: list[dict[str, Any]] = []
    match_count = 0

    pipeline_ext_id = data["ExtractionPipelineExtId"]
    try:
        if config.parameters.debug:
            logger = CogniteFunctionLogger(LOG_LEVEL_DEBUG)
            logger.debug("**** Write debug messages and only process one entity *****")

        logger.debug("Initiate RAW upload queue used to store output from entity matching")
        from cognite.extractorutils.uploader import RawUploadQueue

        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level=LOG_LEVEL_INFO)

        matching_model_id = ""
        if config.parameters.run_all:
            logger.warning("runAll enabled - clearing earlier results, queued predict jobs are kept")
            clear_finished_matches(client, config, logger, config.parameters.raw_table_ctx_bad)
            clear_finished_matches(client, config, logger, config.parameters.raw_table_ctx_good)
        else:
            matching_model_id = read_state_store(client, config, logger, STAT_STORE_MATCH_MODEL_ID)

        monitor_memory_usage(logger, "Pipeline start")

        with time_operation("Read manual mappings", logger):
            manual_mappings, manual_mappings_input = read_manual_mappings(client, logger, config)

        with time_operation("Read rule mappings", logger):
            logger.debug(f"Rule based matches use the '{PROP_COL_NAME}' property")
            rule_mappings = read_rule_mappings(client, logger, config)

        with time_operation("Read targets", logger):
            targets = get_all_targets(client, logger, config, rule_mappings)
        monitor_memory_usage(logger, "After targets loaded")

        if len(targets) == 0:
            logger.warning(f"No {QUERY_FILTER_TYPE_TARGETS} found based on configuration, please check the configuration")
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, 0, 0, "No targets to match against")
            return

        with time_operation("Apply manual mappings", logger):
            good_matches, cnt_manual_mappings = apply_manual_mappings(client, logger, config, raw_uploader, manual_mappings, manual_mappings_input, good_matches, targets)
        logger.info(f"Manual mappings: {cnt_manual_mappings} match(es) applied")

        with time_operation("Read new entities", logger):
            # Only manual mappings have run, and those carry the space of the node they
            # were read from - unlike matches from the matching API, where it can be None.
            matched_entities = [
                instance_key(match[KEY_ENTITY_SPACE], match[KEY_ENTITY_EXT_ID]) for match in good_matches
            ]
            new_entities = get_new_entities(client, config, logger, matched_entities, rule_mappings)
        monitor_memory_usage(logger, "After new entities loaded")
        cleanup_memory()

        logger.info(f"New entities to match: {len(new_entities)}")
        if len(new_entities) == 0:
            logger.info("No new entities to process - predict not started")
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, cnt_manual_mappings, 0, "No new entities, predict not started")
            return

        with time_operation("Apply rule based mappings", logger):
            good_matches, cnt_rule_mappings = apply_rule_mappings(client, config, logger, good_matches, targets, new_entities)  # type: ignore
        logger.info(f"Rule mappings: {cnt_rule_mappings} additional match(es)")

        with time_operation("Start entity matching predict job", logger):
            job = submit_predict_job(client, config, logger, matching_model_id, targets, new_entities)  # type: ignore

        job_id = str(job.job_id)
        logger.info(f"Predict job submitted - jobId: {job_id}")

        # The queue entry is written last: collect only ever sees a job whose matches are
        # already staged, so a failure in between leaves an ignored job rather than a
        # job whose manual and rule matches it cannot find.
        with time_operation("Stage manual and rule matches", logger):
            write_staged_matches(client, config, raw_uploader, logger, job_id, good_matches)

        append_predict_job(
            client,
            config,
            logger,
            job_id=job_id,
            job_token=job.job_token,
            staging_prefix=staging_prefix(job_id),
            model_id=str(job.model_id) if job.model_id else None,
            source_count=len(new_entities),
        )

        match_count = cnt_manual_mappings + cnt_rule_mappings
        cleanup_memory()
        monitor_memory_usage(logger, "Pipeline end")

        update_pipeline_run(
            client,
            logger,
            pipeline_ext_id,
            STATUS_SUCCESS,
            match_count,
            0,
            f"Predict submitted (jobId={job_id}), collect pending",
        )

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, STATUS_FAILURE, match_count, 0, msg)
        raise
