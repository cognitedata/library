"""Entity matching, part one: everything up to starting the predict job in CDF.

Manual and rule based matches are found here and staged in RAW, the predict job is
started without waiting for it, and the job is added to the queue collect works through.
The run ends as soon as CDF has accepted the job, so a long prediction can no longer
time the function out.
"""

from typing import TYPE_CHECKING

from cognite.client import CogniteClient

from em_config import Config  # isort: skip
from em_constants import (  # isort: skip
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    LOG_LEVEL_INFO,
    PROP_COL_NAME,
    QUERY_FILTER_TYPE_TARGETS,
    STAT_STORE_MATCH_MODEL_ID,
    STATUS_FAILURE,
    STATUS_SUCCESS,
)
from em_job_state import append_predict_job  # isort: skip
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline import (  # isort: skip
    apply_manual_mappings,
    apply_rule_mappings,
    file_data_set_id,
    get_new_entities,
    instance_key,
    read_manual_mappings,
    read_rule_mappings,
    read_state_store,
    submit_predict_job,
    update_pipeline_run,
    write_mapping_to_raw,
)
from em_pipeline_optimizations import time_operation  # isort: skip
from em_pipeline_types import EntityMatchSource, FunctionInputData, StoredMatch  # isort: skip
from em_scope import scope_batches  # isort: skip
from em_staging import clear_finished_matches, staging_prefix, write_staged_matches  # isort: skip
from em_targets import get_all_targets  # isort: skip

# RawUploadQueue is only constructed at runtime; importing it lazily lets em_submit be
# imported (e.g. for unit tests) without cognite-extractor-utils installed.
if TYPE_CHECKING:
    from cognite.extractorutils.uploader import RawUploadQueue


def _raw_upload_queue(client: CogniteClient) -> "RawUploadQueue":
    from cognite.extractorutils.uploader import RawUploadQueue

    return RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level=LOG_LEVEL_INFO)


def predict_job_entity_counts(
    scoped_entities: list[EntityMatchSource],
    staged_matches: list[StoredMatch],
) -> tuple[int, int, int, int]:
    """How many records this predict job covers, and how many unique entities are still to match.

    Args:
        scoped_entities: Sources that would be sent to the matching API for this scope.
        staged_matches: Manual/rule matches already found for this job (or carried on the first job).

    Returns:
        Source-record count, unique entity count, unique entities already matched, unique entities left for the model.
    """
    entity_ids = {instance_key(e[KEY_ENTITY_SPACE], e[KEY_ENTITY_EXT_ID]) for e in scoped_entities}
    already = {instance_key(match.get(KEY_ENTITY_SPACE), match[KEY_ENTITY_EXT_ID]) for match in staged_matches}
    already_in_job = entity_ids & already
    return len(scoped_entities), len(entity_ids), len(already_in_job), len(entity_ids - already)


def unmatched_source_records(
    scoped_entities: list[EntityMatchSource],
    staged_matches: list[StoredMatch],
) -> list[EntityMatchSource]:
    """The source records whose entity no manual or rule mapping has matched yet.

    Args:
        scoped_entities: Sources in this scope.
        staged_matches: Manual/rule matches already found for this job.

    Returns:
        The sources to send to the matching API.
    """
    already = {instance_key(match.get(KEY_ENTITY_SPACE), match[KEY_ENTITY_EXT_ID]) for match in staged_matches}
    return [e for e in scoped_entities if instance_key(e[KEY_ENTITY_SPACE], e[KEY_ENTITY_EXT_ID]) not in already]


def submit_entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: FunctionInputData,
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
    good_matches: list[StoredMatch] = []
    match_count = 0

    pipeline_ext_id = data["ExtractionPipelineExtId"]
    try:
        logger.debug("Initiate RAW upload queue used to store output from entity matching")
        raw_uploader = _raw_upload_queue(client)

        matching_model_id = ""
        if config.parameters.run_all:
            logger.warning("runAll enabled - clearing earlier results, queued predict jobs are kept")
            clear_finished_matches(client, config, logger, config.parameters.raw_table_ctx_bad)
            clear_finished_matches(client, config, logger, config.parameters.raw_table_ctx_good)
        else:
            matching_model_id = read_state_store(client, config, logger, STAT_STORE_MATCH_MODEL_ID)

        with time_operation("Read manual mappings", logger):
            manual_mappings, manual_mappings_input = read_manual_mappings(client, logger, config)

        with time_operation("Read rule mappings", logger):
            logger.debug(f"Rule based matches use the '{PROP_COL_NAME}' property")
            rule_mappings = read_rule_mappings(client, logger, config)

        with time_operation("Read targets", logger):
            targets = get_all_targets(client, logger, config, rule_mappings)

        if len(targets) == 0:
            logger.warning(
                f"No {QUERY_FILTER_TYPE_TARGETS} found based on configuration, please check the configuration"
            )
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, 0, None, "No targets to match against")
            return

        with time_operation("Apply manual mappings", logger):
            good_matches, cnt_manual_mappings = apply_manual_mappings(
                client,
                logger,
                config,
                raw_uploader,
                manual_mappings,
                manual_mappings_input,
                good_matches,
                targets,
            )
        logger.info(f"Manual mappings: {cnt_manual_mappings} entity(ies) matched")

        with time_operation("Read new entities", logger):
            # Only manual mappings have run, and those carry the space of the node they
            # were read from - unlike matches from the matching API, where it can be None.
            matched_entities = [
                instance_key(match[KEY_ENTITY_SPACE], match[KEY_ENTITY_EXT_ID]) for match in good_matches
            ]
            new_entities = get_new_entities(client, config, logger, matched_entities, rule_mappings)

        # An entity with several search property values is submitted once per value, so
        # the source record count is not the entity count.
        submitted_entities = len({instance_key(e[KEY_ENTITY_SPACE], e[KEY_ENTITY_EXT_ID]) for e in new_entities})
        logger.info(f"New entities to match: {submitted_entities} ({len(new_entities)} source record(s) submitted)")
        if len(new_entities) == 0:
            logger.info("No new entities to process - predict not started")
            update_pipeline_run(
                client,
                logger,
                pipeline_ext_id,
                STATUS_SUCCESS,
                cnt_manual_mappings,
                None,
                "No new entities, predict not started",
            )
            return

        # Scoping matches each scope against its own targets, with a predict job of its own.
        batches = scope_batches(config.parameters, logger, targets, new_entities)
        if not batches:
            update_pipeline_run(
                client,
                logger,
                pipeline_ext_id,
                STATUS_SUCCESS,
                cnt_manual_mappings,
                None,
                f"No {QUERY_FILTER_TYPE_TARGETS} in scope of the new entities, predict not started",
            )
            return

        # Manual matches are staged once, with the first job.
        staged_matches = good_matches
        data_set_id = file_data_set_id(client, config)
        cnt_rule_mappings = 0
        job_ids: list[str] = []
        total_entities_to_match = 0
        total_source_records = 0
        for scoped_targets, scoped_entities in batches:
            with time_operation("Apply rule based mappings", logger):
                staged_matches, cnt_scope_rules = apply_rule_mappings(
                    client, config, logger, staged_matches, scoped_targets, scoped_entities
                )
            cnt_rule_mappings += cnt_scope_rules

            _, unique_entities, already_matched, entities_to_match = predict_job_entity_counts(
                scoped_entities, staged_matches
            )
            unmatched_entities = unmatched_source_records(scoped_entities, staged_matches)
            if not unmatched_entities:
                # Collect would only have written these matches to RAW, so do that here.
                logger.info(f"All {unique_entities} entities in scope already matched - predict not started")
                write_mapping_to_raw(client, config, raw_uploader, staged_matches, [], logger)
                staged_matches = []
                continue

            source_records = len(unmatched_entities)
            total_entities_to_match += entities_to_match
            total_source_records += source_records

            with time_operation("Start entity matching predict job", logger):
                job = submit_predict_job(client, config, logger, matching_model_id, scoped_targets, unmatched_entities)
            if job.model_id:
                matching_model_id = str(job.model_id)

            job_id = str(job.job_id)
            already_note = f", {already_matched} already matched by rule" if already_matched else ""
            logger.info(
                f"Predict job submitted - jobId: {job_id}, {entities_to_match} entities to match "
                f"({source_records} source record(s) of {unique_entities} unique{already_note})"
            )

            # The queue entry is written last: collect only ever sees a job whose matches are
            # already staged, so a failure in between leaves an ignored job rather than a
            # job whose manual and rule matches it cannot find.
            with time_operation("Stage manual and rule matches", logger):
                staging_digest = write_staged_matches(client, logger, job_id, staged_matches, data_set_id)

            append_predict_job(
                client,
                config,
                logger,
                job_id=job_id,
                job_token=job.job_token,
                staging_prefix=staging_prefix(job_id),
                staging_digest=staging_digest,
                model_id=str(job.model_id) if job.model_id else None,
                source_count=source_records,
            )
            job_ids.append(job_id)
            staged_matches = []
        logger.info(f"Rule mappings: {cnt_rule_mappings} additional entity(ies) matched")
        logger.info(
            f"Submitted {len(job_ids)} predict job(s) totalling {total_entities_to_match} entities to match "
            f"({total_source_records} source record(s) sent)"
        )

        match_count = cnt_manual_mappings + cnt_rule_mappings
        if job_ids:
            run_message = f"Predict submitted (jobId={', '.join(job_ids)}), collect pending"
        else:
            run_message = "All entities matched by manual or rule mapping, predict not started"

        update_pipeline_run(
            client,
            logger,
            pipeline_ext_id,
            STATUS_SUCCESS,
            match_count,
            None,
            run_message,
            input_count=cnt_manual_mappings + submitted_entities,
        )

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, STATUS_FAILURE, match_count, None, msg)
        raise
