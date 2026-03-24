import json
import re
import sys
import traceback
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRun, Row
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    NodeApply,
    NodeOrEdgeData,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue
from config import Config, ViewPropertyConfig
from constants import (
    BATCH_SIZE_API_SUBMIT,
    BATCH_SIZE_ENTITIES,
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_TARGET,
    COL_KEY_MAN_MAPPING_ENTITY,
    COL_KEY_RULE_REGEXP_TARGET,
    COL_KEY_RULE_REGEXP_ENTITY,
    COL_MATCH_KEY,
    FILTER_PATH_NODE_EXTERNAL_ID,
    FUNCTION_ID,
    JOB_RESULT_ITEMS,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_MATCH_VALUE,
    KEY_TARGET_NAME,
    KEY_TARGET_VIEW_ID,
    KEY_TARGET_LINKS,
    KEY_ENTITY_EXISTING_TARGETS,
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_RULE_KEYS,
    KEY_TARGET_RULE_KEYS,
    KEY_ENTITY_MATCH_VALUE,
    KEY_ENTITY_NAME,
    KEY_ENTITY_VIEW_ID,
    KEY_MATCHES,
    KEY_MATCH_TYPE,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE,
    KEY_RULE_KEYS,
    KEY_SCORE,
    KEY_SOURCE,
    KEY_TARGET,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    MATCHING_LIMIT_SOURCES_TARGETS,
    MATCH_TYPE_ENTITY,
    MATCH_TYPE_MANUAL,
    MATCH_TYPE_RULE,
    MAX_LINKS_PER_ENTITY,
    ML_MODEL_FEATURE_TYPE,
    PLACEHOLDER_NO_MATCH,
    PLACEHOLDER_NO_MATCH_TARGET,
    PROP_COL_EXTERNAL_ID,
    PROP_COL_SPACE,
    PROP_COL_LINK_NAME,
    PROP_COL_NAME,
    QUERY_FILTER_TYPE_TARGETS,
    QUERY_FILTER_TYPE_ENTITIES,
    SCORE_MANUAL_RULE_MATCH,
    STAT_STORE_MATCH_MODEL_ID,
    STAT_STORE_VALUE,
    STATUS_FAILURE,
    STATUS_SUCCESS,
)
from logger import CogniteFunctionLogger

sys.path.append(str(Path(__file__).parent))

# 🚀 OPTIMIZATION IMPORTS
try:
    from pipeline_optimizations import (
        cleanup_memory,
        monitor_memory_usage,
        time_operation,
    )
except ImportError:
    from contextlib import contextmanager
    
    @contextmanager
    def time_operation(operation_name, logger):
        """Fallback if optimizations not available"""
        yield
    
    def monitor_memory_usage(logger, operation_name=""): pass
    def cleanup_memory(): pass




def entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config
) -> None:
    """Entity matching pipeline

    Args:
        client (CogniteClient): Cognite client
        logger (CogniteFunctionLogger): Logger
        data (dict[str, Any]): Data from CDF
        config (Config): Configuration

    Raises:
        Exception: Exception
    """
    good_matches = []
    len_good_matches, len_bad_matches = 0, 0

    try:
        pipeline_ext_id = data["ExtractionPipelineExtId"]
        logger.info(f"Starting entity matching function: {FUNCTION_ID} with loglevel = {data.get('logLevel', LOG_LEVEL_INFO)},  reading parameters from extraction pipeline config: {pipeline_ext_id}")

        not_matches_count, match_count = 0, 0
        matching_model_id = None
        if config.parameters.debug:
            logger = CogniteFunctionLogger(LOG_LEVEL_DEBUG)
            logger.debug("**** Write debug messages and only process one entity *****")

        logger.debug("Initiate RAW upload queue used to store output from entity matching")
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level=LOG_LEVEL_INFO)

        # Check if we should run all entities (then delete state content in RAW) or just new entities
        if config.parameters.run_all:
            logger.debug("Run all entities, delete state content in RAW since we are rerunning based on all input")
            delete_table(client, config.parameters.raw_db, config.parameters.raw_tale_ctx_bad)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_tale_ctx_good)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_state)
        else:
            logger.debug("Get entity entity matching model ID from state store")
            matching_model_id = read_state_store(client, config, logger, STAT_STORE_MATCH_MODEL_ID)

        logger.info("Read manual mappings to be used in entity matching")
        manual_mappings, manual_mappings_input = read_manual_mappings(client, logger, config)

        logger.info(f"Read rule mappings to be used in entity matching, NOTE: Uses '{PROP_COL_NAME}' property for rule based matches")
        rule_mappings = read_rule_mappings(client, logger, config)

        logger.info(f"Read all {QUERY_FILTER_TYPE_TARGETS} that are input for matching ( based on TAG filtering IF given in config)")
        targets = get_all_targets(client, logger, config, rule_mappings)
        if len(targets) == 0:
            logger.warning(f"No {QUERY_FILTER_TYPE_TARGETS} found based on configuration, please check the configuration")
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, match_count, not_matches_count, None)
            return

        logger.info("Start by applying manual mappings")
        good_matches, cnt_manual_mappings = apply_manual_mappings(client, logger, config, raw_uploader, manual_mappings, manual_mappings_input, good_matches, targets)
    
        logger.info("Read new entities (ex: time series) that has been updated since last run")
        list_good_matches = [match[KEY_ENTITY_EXT_ID] for match in good_matches]
        new_entities = get_new_entities(client, config, logger, list_good_matches, rule_mappings)

        logger.info(f"Start processing of new entities ({len(new_entities)})")
        if len(new_entities) == 0:
            logger.info("No new entities to process, we are done - just update pipeline run")
            update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, match_count, not_matches_count, None)
            return

        logger.info(f"Applying rule based mappings - using provided reg expressions to match entities to {QUERY_FILTER_TYPE_TARGETS}")
        good_matches, cnt_rule_mappings = apply_rule_mappings(client, config, logger, good_matches, targets, new_entities)  # type: ignore

        logger.info("NOTE: the matching runs in CDF, and the process could here be split into two steps to avoid long running jobs")
        match_results = get_matches(client, config, logger, matching_model_id or "", targets, new_entities)  # type: ignore

        good_matches, bad_matches, cnt_entity_matching = select_and_apply_matches(client, config, logger, good_matches, match_results)  # type: ignore
        write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, logger)

        len_good_matches = cnt_manual_mappings + cnt_rule_mappings + cnt_entity_matching
        len_bad_matches = len(bad_matches)
        if config.parameters.dm_update:
            msg = f"Relationships updated in the DM (dmUpdate: True)"
        else:
            msg = f"Relationships NOT updated in DM, only updated the RAW tables (dmUpdate: False)"
        update_pipeline_run(client, logger, pipeline_ext_id, STATUS_SUCCESS, len_good_matches, len_bad_matches, msg)

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, STATUS_FAILURE, len_good_matches, len_bad_matches, msg)
        raise Exception("msg")


def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    match_count: int = 0,
    not_matches_count: int = 0,
    input_msg: Optional[str] = None
) -> None:

    total_entities = match_count + not_matches_count
    if status == STATUS_SUCCESS:
        msg = (
            f"Entity matching of: {total_entities} input entities, Matched: {match_count} "
            f" - NOT matched due to low score: {not_matches_count}"
        )
        logger.info(msg)
        if input_msg:
            logger.info(input_msg)
    else:
        msg = (
            f"Entity matching of: {total_entities} input entities, Matched: {match_count} "
            f" - NOT matched due to low score: {not_matches_count}, "
            f"{input_msg or 'Unknown error'}, traceback:\n{traceback.format_exc()}"
        )
        logger.error(msg)

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(
            extpipe_external_id=xid,
            status=status,
            message=shorten(msg, 1000)
        )
    )


def read_state_store(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    key: str,
) -> str:
    value = None
    db = config.parameters.raw_db
    table = config.parameters.raw_table_state

    logger.info(f"Read state from DB: {db} Table: {table} Key: {key}")

    logger.debug("Create DB / Table for state if it does not exist")
    create_table(client, db, table)

    row_list = client.raw.rows.list(db_name=db, table_name=table, columns=[STAT_STORE_VALUE], limit=-1)
    for row in row_list:
        if row.key == key and row.columns:
            value = row.columns[STAT_STORE_VALUE]

    return value or ""


def update_state_store(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    value: str,
    type: str,
) -> None:

    # Create DB / Table for state if it does not exist
    create_table(client, config.parameters.raw_db, config.parameters.raw_table_state)

    state_row = Row(type, {STAT_STORE_VALUE: value})
    client.raw.rows.insert(config.parameters.raw_db, config.parameters.raw_table_state, state_row)

    logger.debug(f"Update state store DB: {config.parameters.raw_db} Table: {config.parameters.raw_table_state} Key: {type} Value: {value}")


def manual_table_exists(
    client: CogniteClient, 
    config: Config
) -> bool:
    tables = client.raw.tables.list(config.parameters.raw_db, limit=None)
    return any(tbl.name == config.parameters.raw_tale_ctx_manual for tbl in tables)


def rule_table_exists(
    client: CogniteClient, 
    config: Config
) -> bool:
    tables = client.raw.tables.list(config.parameters.raw_db, limit=None)
    return any(tbl.name == config.parameters.raw_tale_ctx_rule for tbl in tables)


def read_manual_mappings(
    client: CogniteClient, 
    logger: CogniteFunctionLogger,
    config: Config
) -> list[Row]:
    manual_mappings = []
    manual_mappings_input = {}
    seen_mappings = set()
    try:
        if not manual_table_exists(client, config):
            return manual_mappings

        row_list = client.raw.rows.list(config.parameters.raw_db, config.parameters.raw_tale_ctx_manual, limit=-1)
        for row in row_list:
            if not (
                config.parameters.run_all
                or not row.columns
                or COL_KEY_MAN_CONTEXTUALIZED not in row.columns
                or row.columns[COL_KEY_MAN_CONTEXTUALIZED] is not True
            ):
                continue

            # Make sure we don't add duplicate TS external IDs
            if not row.columns:
                continue
            entity = row.columns[COL_KEY_MAN_MAPPING_ENTITY].strip()
            if entity not in seen_mappings:
                seen_mappings.add(entity)
                manual_mappings.append(
                    {
                        KEY_RULE: row.key,
                        COL_KEY_MAN_MAPPING_ENTITY: entity,
                        COL_KEY_MAN_MAPPING_TARGET: row.columns[COL_KEY_MAN_MAPPING_TARGET].strip(),
                    }
                )
                manual_mappings_input[row.key] = row.columns

        logger.info(f"Number of manual mappings in table: {config.parameters.raw_db}/{config.parameters.raw_tale_ctx_manual}: {len(manual_mappings)}")

    except Exception as e:
        logger.error(f"Read manual mappings. Error: {type(e)}({e})")

    return manual_mappings, manual_mappings_input



def apply_manual_mappings(
    client: CogniteClient, 
    logger: CogniteFunctionLogger,
    config: Config, 
    raw_uploader: RawUploadQueue, 
    manual_mappings: list[Row],
    manual_mappings_input: dict[str, dict[str, Any]],
    good_matches: list[dict[str, Any]] = [],
    targets: list[dict[str, Any]] = []
) -> list[dict[str, Any]]:

    entity_view_id = config.data.entity_view.as_view_id()
    item_update = []
    clean_target_list = []
    cnt = 0

    try:
        targets_lookup = {target[KEY_TARGET_EXT_ID]: target for target in targets}
        entity_list = [mapping[COL_KEY_MAN_MAPPING_ENTITY] for mapping in manual_mappings]
        lookup_mapping = {mapping[COL_KEY_MAN_MAPPING_ENTITY]: mapping[COL_KEY_MAN_MAPPING_TARGET] for mapping in manual_mappings}
        key_lookup = {mapping[COL_KEY_MAN_MAPPING_ENTITY]: mapping[KEY_RULE] for mapping in manual_mappings}

        # Split entity_list into batches
        num_batches = (len(entity_list) - 1) // BATCH_SIZE_ENTITIES + 1 if entity_list else 0
        
        if num_batches > 1:
            logger.info(f"Entity list has {len(entity_list)} items, splitting into {num_batches} batches of up to {BATCH_SIZE_ENTITIES}")
        
        # Process in batches
        for batch_idx in range(0, len(entity_list), BATCH_SIZE_ENTITIES):
            batch_entity_list = entity_list[batch_idx:batch_idx + BATCH_SIZE_ENTITIES]
            batch_num = batch_idx // BATCH_SIZE_ENTITIES + 1
            
            if num_batches > 1:
                logger.info(f"Processing batch {batch_num}/{num_batches} with {len(batch_entity_list)} entities")
            
            # Get instances for current batch
            instances = list_instances_by_external_id_direct(
                client=client,
                config=config,
                external_id=batch_entity_list,
                logger=logger
            )

            # Process entities in current batch
            
            for entity in instances:
                cnt += 1
                if entity.external_id not in lookup_mapping:
                    continue
                    
                target_ext_id = lookup_mapping[entity.external_id]
                if not target_ext_id:
                    logger.warning(f"Manual mapping target ref is empty for entity: {entity.external_id}, skipping")
                    continue

                targets = []
                if not config.parameters.remove_old_links: 
                    # keep old target links
                    targets = get_links_from_entity(entity.properties[entity_view_id][PROP_COL_LINK_NAME])   
                else:
                    clean_target_list = clean_links(config, entity.external_id, clean_target_list)
                
                targets.append(target_ext_id)

                item_update = add_to_items(config, 
                                           logger, 
                                           item_update,
                                           targets,
                                           entity.external_id,
                                           entity_view_id)

                if target_ext_id in targets_lookup:
                    target = targets_lookup[target_ext_id]
                    target_name = target[KEY_ORG_NAME]
                    target_view_id = str(config.data.target_view.as_view_id())
                else:
                    target_name = PLACEHOLDER_NO_MATCH_TARGET
                    target_view_id = PLACEHOLDER_NO_MATCH_TARGET

                good_matches.append(
                    {
                        KEY_MATCH_TYPE: MATCH_TYPE_MANUAL,
                        KEY_ENTITY_EXT_ID: entity.external_id,
                        KEY_ENTITY_NAME: entity.properties[entity_view_id][PROP_COL_NAME],
                        KEY_ENTITY_MATCH_VALUE: entity.external_id,
                        KEY_ENTITY_VIEW_ID: str(config.data.entity_view.as_view_id()),
                        KEY_ENTITY_EXISTING_TARGETS: entity.properties[entity_view_id][PROP_COL_LINK_NAME],
                        KEY_SCORE: SCORE_MANUAL_RULE_MATCH,
                        KEY_TARGET_NAME: target_name,
                        KEY_TARGET_MATCH_VALUE: target_ext_id,
                        KEY_TARGET_EXT_ID: target_ext_id,
                        KEY_TARGET_VIEW_ID: target_view_id,
                    }
                )

                mapping = {}
                row_key = key_lookup[entity.external_id]
                mapping = manual_mappings_input[row_key].copy()
                mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
                raw_uploader.add_to_upload_queue(config.parameters.raw_db, config.parameters.raw_tale_ctx_manual, Row(row_key, mapping))
            
                # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
                if not config.parameters.debug and config.parameters.dm_update and cnt % BATCH_SIZE_API_SUBMIT == 0:
                    if len(clean_target_list) > 0:
                        client.data_modeling.instances.apply(clean_target_list)
                        clean_target_list = []

                    logger.info(f"==> Mapping table based matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(manual_mappings)}")
                    client.data_modeling.instances.apply(item_update)
                    item_update = []  # Reset item_update after applying

            if num_batches > 1:
                logger.info(f"Completed batch {batch_num}/{num_batches}")

        if not config.parameters.debug and config.parameters.dm_update:
            if len(clean_target_list) > 0:
                client.data_modeling.instances.apply(clean_target_list)

            # Apply the updates to the data model
            client.data_modeling.instances.apply(item_update)
            if len(item_update) > 0:
                if cnt == 0:
                    logger.info("==> Mapping table based matching - No items added to data model based on new items found and manual mappings")
                else:
                    logger.info(f"==> Mapping table based matching - Adding remaining batch of {len(item_update)} items to data model, total count: {cnt} / {len(manual_mappings)}")

            raw_uploader.upload()

        return good_matches, cnt

    except Exception as e:
        logger.error(f"ERROR: Not able run manual mapping for {manual_mappings} - error: {e}")
        return good_matches, cnt


def get_links_from_entity(
    links_property: list[dict[str, Any]]
) -> list[str]:

    links = []
    if len(links_property) > 0:
        for link in links_property:
            links.append(link[PROP_COL_EXTERNAL_ID])
    return links


def list_instances_by_external_id_direct(
    client: CogniteClient,
    config: Config,
    external_id: [str],
    logger: CogniteFunctionLogger = None
) -> list:
    """
    List instances by their direct external ID match.
    
    Args:
        client: CogniteClient instance
        config: Configuration object containing entity_view settings
        external_id: The external ID to search for (could be timeseries external ID)
        logger: Optional logger for debugging
        
    Returns:
        List of matching instances
    """
    if logger:
        logger.debug(f"Searching for instances with external ID: {external_id}")
    
    entity_view_id = config.data.entity_view.as_view_id()
    
    # Filter by external_id
    external_id_filter = dm.filters.In(
        FILTER_PATH_NODE_EXTERNAL_ID,
        external_id
    )
    
    has_data_filter = dm.filters.HasData(views=[entity_view_id])
    combined_filter = dm.filters.And(has_data_filter, external_id_filter)
    
    matching_instances = client.data_modeling.instances.list(
        space=config.data.entity_view.instance_space,
        sources=[entity_view_id],
        filter=combined_filter,
        limit=-1
    )
    
    if logger:
        logger.info(f"Found {len(matching_instances)} instances with for manual mappings")
        logger.debug(f"Found instances with external ID: {external_id}")
    
    return matching_instances

def read_rule_mappings(
    client: CogniteClient, 
    logger: CogniteFunctionLogger,
    config: Config
) -> list[Row]:
    rule_mappings = []
    
    try:
        if not rule_table_exists(client, config):
            return rule_mappings

        row_list = client.raw.rows.list(config.parameters.raw_db, config.parameters.raw_tale_ctx_rule, limit=-1)
        idx = 1
        for row in row_list:
            if not row.columns:
                continue
            rule_mappings.append(
                {
                        KEY_RULE: f"{idx}",
                        COL_KEY_RULE_REGEXP_ENTITY: row.columns[COL_KEY_RULE_REGEXP_ENTITY].strip(),
                        COL_KEY_RULE_REGEXP_TARGET: row.columns[COL_KEY_RULE_REGEXP_TARGET].strip(),
                }
            )
            idx += 1
        logger.info(f"Number of mapping rules : {len(rule_mappings)}")
    except Exception as e:
        logger.error(f"Read rule based mappings. Error: {type(e)}({e})")

    return rule_mappings


def get_all_targets(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    rule_mappings: list[Row] | None = None
) -> list[dict[str, Any]]:

    targets = []
    job_config = config.data
    search_property = job_config.target_view.search_property

    # `instances.list(..., sources=[view])` already scopes to instances with data in the view.
    # Skipping extra HasData in the filter significantly reduces graph query load.
    is_selected = get_query_filter(
        QUERY_FILTER_TYPE_TARGETS,
        job_config.target_view,
        config.parameters.run_all,
        logger,
        include_has_data=False,
    )

    all_targets = client.data_modeling.instances.list(
        space=job_config.target_view.instance_space,
        sources=[job_config.target_view.as_view_id()],
        filter=is_selected,
        limit=1000
    )

    logger.info(f"Number of {QUERY_FILTER_TYPE_TARGETS} to process: {len(all_targets)}, NOTE: Rule based regular expressions are applied to the '{PROP_COL_NAME}' property")
    for target in all_targets:
        org_name = str(target.properties[job_config.target_view.as_view_id()][PROP_COL_NAME])

        rule_keys = []
        if rule_mappings:
            for rule in rule_mappings:
                reg_exp = str(rule[COL_KEY_RULE_REGEXP_TARGET])
                match = re.search(reg_exp, org_name)

                if match:
                    # Concatenate the captured groups directly
                    cleaned_value = rule[KEY_RULE] + "_" + "".join(match.groups())  # groups() returns a tuple of all captured groups
                    logger.debug(f"Cleaned value (using capture groups): {cleaned_value}")
                    rule_keys.append(cleaned_value)

        if search_property in target.properties[job_config.target_view.as_view_id()]:
            match_properties = target.properties[job_config.target_view.as_view_id()][search_property]
            if not isinstance(match_properties, list):
                match_properties = [match_properties]
        else:
            match_properties = [org_name]  

        for match_property in match_properties:
            targets.append(
                {
                    KEY_TARGET_EXT_ID: target.external_id,
                    KEY_ORG_NAME: org_name,
                    KEY_NAME: match_property,
                    KEY_RULE_KEYS: rule_keys if rule_keys else None,
                }
            )
    logger.debug(f"Number {QUERY_FILTER_TYPE_TARGETS} added as entities: {len(targets)}")

    return targets


def get_new_entities(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    list_good_entities: list[str] | None = None,
    rule_mappings: list[Row] | None = None
) -> list[dict[str, Any]]:

    entities_source = []

    entity_view_config= config.data.entity_view
    entity_view_id = entity_view_config.as_view_id()

    logger.debug(f"Get new entities from view: {entity_view_id}, based on config: {entity_view_config}")
    is_selected = get_query_filter(QUERY_FILTER_TYPE_ENTITIES, entity_view_config, config.parameters.run_all, logger)

    new_entities = client.data_modeling.instances.list(
        space=entity_view_config.instance_space,
        sources=[entity_view_id],
        filter=is_selected,
        limit=-1
    )
 
    item_update = []


    logger.info(f"Number of new entities to process: {len(new_entities)} NOTE: Rule based regular expressions are applied to the '{PROP_COL_NAME}' property")
    for entity in new_entities:
        # test if just matched and skip if so
        if list_good_entities and entity.external_id in list_good_entities:
            logger.debug(f"Entity: {entity.external_id} just matched, skipping")
            continue
        # Rule based matching uses the name property to match entities to targets
        org_name = str(entity.properties[entity_view_id][PROP_COL_NAME])

        rule_keys = []
        if rule_mappings:
            for rule in rule_mappings:
                reg_exp = str(rule[COL_KEY_RULE_REGEXP_ENTITY])
                match = re.search(reg_exp, org_name)

                if match:
                    # Concatenate the captured groups directly
                    cleaned_value = rule[KEY_RULE] + "_" + "".join(match.groups())  # groups() returns a tuple of all captured groups
                    logger.debug(f"Cleaned value (using capture groups): {cleaned_value}")
                    rule_keys.append(cleaned_value)
                    
        targets = []
        if not config.parameters.remove_old_links or not config.parameters.dm_update: # if dmUpdate is False, keep old target links    
            # keep old target links
            targets = entity.properties[entity_view_id][PROP_COL_LINK_NAME]
        else:
            item_update = clean_links(config, entity.external_id, item_update)

        # add entities for files used to match between file references in P&ID to other files
        search_prop = entity_view_config.search_property
        if search_prop in entity.properties[entity_view_id]:
            prop_value = entity.properties[entity_view_id][search_prop]
            entity_names = prop_value if isinstance(prop_value, list) else [str(prop_value)]
        else:
            entity_names = [org_name]
        for entity_name in entity_names: 
            
            entities_source.append(
                {
                    KEY_ENTITY_EXT_ID: entity.external_id,
                    KEY_NAME: entity_name,
                    KEY_ORG_NAME: org_name,
                    KEY_TARGET_LINKS: json.dumps(targets),
                    KEY_RULE_KEYS: rule_keys if rule_keys else None,
                }
            )
            logger.debug(f"Entity: {entity.external_id} - {entity_name} ({org_name})")

    logger.info(f"Num new entities: {len(entities_source)} from view: {entity_view_id}")

    if config.parameters.remove_old_links and len(item_update) > 0:
        client.data_modeling.instances.apply(item_update)
        logger.info(f"Cleaned up {QUERY_FILTER_TYPE_TARGETS} links for {len(item_update)} entities")

    return entities_source


def clean_links(
    config: Config,
    entity_ext_id: str,
    item_update: list[NodeApply]
) -> list[NodeApply]:

    entity_view_id = config.data.entity_view.as_view_id()

    item_update.append(
        NodeApply(
            space=config.data.entity_view.instance_space,
            external_id=entity_ext_id,
            sources=[
                NodeOrEdgeData(
                    source=entity_view_id,
                    properties={PROP_COL_LINK_NAME: None},
                )
            ],
        )
    )  

    return item_update

def get_query_filter(
    type: str,
    view_config: ViewPropertyConfig,
    run_all: bool,
    logger: CogniteFunctionLogger,
    include_has_data: bool = True,
) -> dm.filters.Filter | None:
    filters: list[dm.filters.Filter] = []
    dbg_msg = f"For view: {view_config.as_view_id()}"

    if include_has_data:
        filters.append(dm.filters.HasData(views=[view_config.as_view_id()]))
        dbg_msg = f"{dbg_msg} - Entity filter: HasData = True"
    else:
        dbg_msg = f"{dbg_msg} - Entity filter: HasData skipped (sources already scope instances)"

    # Check if the view entity already is matched or not
    if type == QUERY_FILTER_TYPE_ENTITIES and not run_all:
        is_matched = dm.filters.Exists(view_config.as_property_ref(PROP_COL_LINK_NAME))
        not_matched = dm.filters.Not(is_matched)
        filters.append(not_matched)
        dbg_msg = f"{dbg_msg} Entity filtering on: '{PROP_COL_LINK_NAME}' - NOT EXISTS"

    if view_config.filter_property and view_config.filter_values:
        is_filter_param = dm.filters.In(
            view_config.as_property_ref(
                view_config.filter_property),
                view_config.filter_values
        )
        filters.append(is_filter_param)
        dbg_msg = f"{dbg_msg} Entity filtering on: '{view_config.filter_values}' IN: '{view_config.filter_property}'"

    logger.info(dbg_msg)

    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return dm.filters.And(*filters)


def get_matches(
    client: CogniteClient, 
    config: Config,
    logger: CogniteFunctionLogger,
    matching_model_id: str,
    match_to: list[dict[str, Any]], 
    match_from: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Create / Update entity matching model and run job to get matches

    Returns:
        list of matches
    """
    try:
        if matching_model_id:
            logger.debug(f"Get existing matching model: {matching_model_id}")
            model = client.entity_matching.retrieve(id=int(matching_model_id))
        else:
            model = client.entity_matching.fit(
                sources=match_from[:MATCHING_LIMIT_SOURCES_TARGETS],
                targets=match_to[:MATCHING_LIMIT_SOURCES_TARGETS],
                match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
                feature_type=ML_MODEL_FEATURE_TYPE,
            )
            logger.info(f"Created new matching model: {model.id}")
            update_state_store(client, config, logger, str(model.id), STAT_STORE_MATCH_MODEL_ID)

        if not model:
            raise Exception("Failed to create or retrieve matching model")

        job = model.predict(
            sources=match_from, 
            targets=match_to, 
            num_matches=1
        )

        return job.result[JOB_RESULT_ITEMS]

    except Exception as e:
        logger.error(f"ERROR: Failed to get matching model and run prediction. Error: {type(e)}({e})")
        raise


def apply_rule_mappings(
    client: CogniteClient, 
    config: Config, 
    logger: CogniteFunctionLogger,
    good_matches: list[dict[str, Any]],
    target_dest: list[dict[str, Any]], 
    new_entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    # Use set instead of list for O(1) lookups
    good_matches_set = {f"{match[KEY_TARGET_EXT_ID]}_{match[KEY_ENTITY_EXT_ID]}"
                        for match in good_matches}
    matched_entity_ids = {match[KEY_ENTITY_EXT_ID] for match in good_matches}

    key_field = KEY_RULE_KEYS  # The field in the dictionaries that contains the rule keys
    num_added_matches = 0
    cnt = 0
    
    try:
        # Build an inverted index for target_dest
        # Format: { 'rule_key_value': [dict_from_target_dest_1, dict_from_target_dest_2, ...] }
        index1 = defaultdict(list)
        matches = defaultdict(list)  # defaultdict(lambda: [[], []])

        for d1 in target_dest:
            if not d1.get(key_field, []):  # Ensure the key_field exists
                continue  # Skip if no rule keys are present
            for r_key in d1.get(key_field, []): # Use .get() for safety
                index1[r_key].append(d1)

        # To avoid duplicate matches (e.g., if A1 matches B1, we don't want B1 matching A1 back)
        # or to handle cases where multiple rule_keys cause the same pair to match,
        # we use a set of unique pairs.
        unique_matches_tracker = set()

        for d2 in new_entities:
            if not d2.get(key_field, []):  # Ensure the key_field exists
                continue  # Skip if no rule keys are present
            set2 = set(d2.get(key_field, [])) # Convert to set once

            # Skip if entity already has been matched
            if d2[KEY_ENTITY_EXT_ID] in matched_entity_ids:
                logger.debug(f"Entity: {d2['entity_ext_id']} already has been matched manually, skipping")
                continue
        
            for r_key_from_d2 in set2:
                if r_key_from_d2 in index1:
                    for d1_match in index1[r_key_from_d2]:
                        # Create a canonical tuple for uniqueness tracking
                        # Ensure consistent order (e.g., by ID)
                        pair = tuple(sorted((d2[KEY_ENTITY_EXT_ID], d1_match[KEY_TARGET_EXT_ID])))

                        if pair not in unique_matches_tracker:
                            match_key = f"{d1_match[KEY_TARGET_EXT_ID]}_{d2[KEY_ENTITY_EXT_ID]}"
                            if match_key in good_matches_set:
                                logger.debug(f"Match already exists in good matches: {d1_match[KEY_TARGET_EXT_ID]} - {d2[KEY_ENTITY_EXT_ID]}")
                                continue
                            good_matches_set.add(match_key)

                            unique_target_list = list(set(matches[d2[KEY_ENTITY_EXT_ID]]))
                            if d1_match[KEY_TARGET_EXT_ID] and d1_match[KEY_TARGET_EXT_ID] not in unique_target_list:
                                unique_target_list.append(d1_match[KEY_TARGET_EXT_ID])
                                num_added_matches += 1
                                good_matches.append(
                                    {
                                        KEY_MATCH_TYPE: MATCH_TYPE_RULE,
                                        KEY_ENTITY_EXT_ID: d2[KEY_ENTITY_EXT_ID],
                                        KEY_ENTITY_NAME: d2[KEY_ORG_NAME],
                                        KEY_ENTITY_MATCH_VALUE: d2[KEY_NAME],
                                        KEY_ENTITY_VIEW_ID: str(config.data.entity_view.as_view_id()),
                                        KEY_ENTITY_EXISTING_TARGETS: d2[KEY_TARGET_LINKS],
                                        KEY_ENTITY_RULE_KEYS: json.dumps(d2[KEY_RULE_KEYS]),
                                        KEY_SCORE: SCORE_MANUAL_RULE_MATCH,
                                        KEY_TARGET_NAME: d1_match[KEY_ORG_NAME],
                                        KEY_TARGET_MATCH_VALUE: d1_match[KEY_NAME],
                                        KEY_TARGET_EXT_ID: d1_match[KEY_TARGET_EXT_ID],
                                        KEY_TARGET_VIEW_ID: str(config.data.target_view.as_view_id()),
                                        KEY_TARGET_RULE_KEYS: json.dumps(d1_match[KEY_RULE_KEYS]),
                                    }
                                )

                            existing_target_list = json.loads(d2[KEY_TARGET_LINKS])
                            if len(existing_target_list) > 0:
                                for target in existing_target_list:
                                    if PROP_COL_EXTERNAL_ID in target and target[PROP_COL_EXTERNAL_ID] not in unique_target_list:
                                        unique_target_list.append(target[PROP_COL_EXTERNAL_ID])

                            matches[d2[KEY_ENTITY_EXT_ID]] = unique_target_list
                            unique_matches_tracker.add(pair)

        item_update = []
        
        # Iterate through the matches and prepare the item updates
        
        for entity_ext_id in matches:
            cnt += 1
            target_ext_ids = matches[entity_ext_id] # Assuming the first target is the one to match with

            item_update = add_to_items(config, 
                                    logger, 
                                    item_update,
                                    target_ext_ids,
                                    entity_ext_id,
                                    config.data.entity_view.as_view_id())
            
            # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
            if not config.parameters.debug and config.parameters.dm_update and cnt % BATCH_SIZE_API_SUBMIT == 0:
                logger.info(f"==> Rule based matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(matches)}")
                client.data_modeling.instances.apply(item_update)
                item_update = []  # Reset item_update after applying

        if not config.parameters.debug and config.parameters.dm_update:
            # Apply the updates to the data model
            client.data_modeling.instances.apply(item_update)
 
            if cnt == 0:
                logger.info("==> Rule based matching - No items added to data model based on new items found and rule based mappings")
            else:
                logger.info(f"==> Rule based matching - Adding remaining batch of {len(item_update)} items to data model, total count: {cnt} / {len(matches)}")
                logger.info(f"==> Rule based matching - Total number of new matches based on rules matching one or more targets per entity added: {num_added_matches}")

        return good_matches, cnt

    except Exception as e:
        logger.error(f"ERROR: Not able run rule based mapping - error: {e}")
        return good_matches, cnt




def select_and_apply_matches(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    good_matches: list[dict[str, Any]],
    match_results: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Select and apply matches based on filtering threshold. Matches with score above threshold are updating time series
    with target ID When matches are updated, metadata property with information about the match is added to time series
    to indicate that it has been matched.

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        match_results: list of matches from entity matching
        entity_meta_dict: dictionary with time series id and metadata

    Returns:
        list of good matches
        list of bad matches
    """
    bad_matches = []
    item_update = []
    cnt = 0

    entity_view_id = config.data.entity_view.as_view_id()
    target_view_id = config.data.target_view.as_view_id()

    # Use set instead of list for O(1) lookups
    good_matches_set = {f"{match[KEY_TARGET_EXT_ID]}_{match[KEY_ENTITY_EXT_ID]}"
                        for match in good_matches}
    matched_entity_ids = {match[KEY_ENTITY_EXT_ID] for match in good_matches}
    new_good_matches = []
    try:
        for match in match_results:
            # Skip if entity already has been matched
            if match[KEY_SOURCE][KEY_ENTITY_EXT_ID] in matched_entity_ids:
                continue

            if match[KEY_MATCHES]:
                if match[KEY_MATCHES][0][KEY_SCORE] >= config.parameters.auto_approval_threshold:
                    entity_ext_id = match[KEY_SOURCE][KEY_ENTITY_EXT_ID]
                    target_ext_id = match[KEY_MATCHES][0][KEY_TARGET][KEY_TARGET_EXT_ID]

                    match_key = f"{target_ext_id}_{entity_ext_id}"
                    if match_key in good_matches_set:
                        logger.debug(f"Match already exists in good matches: {target_ext_id} - {entity_ext_id}")
                        continue
                    else:
                        good_matches_set.add(match_key)

                    new_good_matches.append(add_to_dict(match, str(entity_view_id), str(target_view_id)))
                else:
                    bad_matches.append(add_to_dict(match, str(entity_view_id), str(target_view_id)))
            else:
                bad_matches.append(add_to_dict(match, str(entity_view_id), str(target_view_id)))

        logger.info(f"Got {len(new_good_matches)} matches with score >= {config.parameters.auto_approval_threshold}")
        logger.info(f"Got {len(bad_matches)} matches with score < {config.parameters.auto_approval_threshold}")

        # Update time series with matches
        for match in new_good_matches:
            cnt += 1
            entity_ext_id = match[KEY_ENTITY_EXT_ID]
            target_ext_id = match[KEY_TARGET_EXT_ID]
            entity_targets = match[KEY_ENTITY_EXISTING_TARGETS]
 
            item_update = add_to_items(config, 
                                       logger, 
                                       item_update,
                                       [target_ext_id],
                                       entity_ext_id,
                                       entity_view_id,
                                       entity_targets)

            # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
            if not config.parameters.debug and config.parameters.dm_update and cnt % BATCH_SIZE_API_SUBMIT == 0:
                logger.info(f"==> Entity matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(new_good_matches)}")
                client.data_modeling.instances.apply(item_update)
                item_update = []  # Reset item_update after applying

        if not config.parameters.debug and config.parameters.dm_update:
            client.data_modeling.instances.apply(item_update)
            if cnt == 0:
                logger.info("==> Entity matching - No items added to data model based on new items found and entity matching")
            else:
                logger.info(f"==> Entity matching - Adding remaining batch of {len(item_update)} items to data model, total count: {cnt} / {len(new_good_matches)}")


        return good_matches + new_good_matches, bad_matches, cnt

    except Exception as e:
        print(f"ERROR: Failed to parse results from entity matching - error: {type(e)}({e})")
        return good_matches, [], cnt  # type: ignore

def add_to_items(
    config: Config,
    logger: CogniteFunctionLogger,
    item_update: list[NodeApply],
    target_ext_ids: list[str],
    entity_ext_id: str,
    entity_view_id: dm.ViewId,
    entity_targets: Optional[str] = None  
) -> list[NodeApply]:

    targets = []

    if entity_targets:
        entity_targets_array = json.loads(entity_targets)
        if len(entity_targets_array) > 0:  # Check if there are existing targets
            for target in entity_targets_array:
                targets.append(DirectRelationReference(space=target[PROP_COL_SPACE], external_id=target[PROP_COL_EXTERNAL_ID]))

    # Add new targets to the entity
    for target_ext_id in target_ext_ids:  
        if not target_ext_id:
            logger.warning(f"Asset external ID is empty for entity: {entity_ext_id}, skipping")
            continue
        if target_ext_id in [target.external_id for target in targets]:
            logger.debug(f"Asset: {target_ext_id} already exists in entity: {entity_ext_id}, skipping")
            continue
        logger.debug(f"Adding target: {target_ext_id} to entity: {entity_ext_id}")   
        targets.append(
            DirectRelationReference(
                space=config.data.target_view.instance_space,
                external_id=target_ext_id
            )
        )

    if len(targets) > MAX_LINKS_PER_ENTITY:
        logger.warning(f"Entity: {entity_ext_id} has more than {MAX_LINKS_PER_ENTITY} targets - has {len(targets)} targets, will only add {MAX_LINKS_PER_ENTITY} - TODO look into your rule/matching model to prevent to wide matching")
        targets = targets[:MAX_LINKS_PER_ENTITY]
        

    item_update.append(
        NodeApply(
            space=config.data.entity_view.instance_space,
            external_id=entity_ext_id,
            sources=[
                NodeOrEdgeData(
                    source=dm.ViewId.load(entity_view_id),  # type: ignore
                    properties={PROP_COL_LINK_NAME: targets},
                )
            ],
        )
    )       

    logger.debug(f"Added entity: {entity_ext_id} to target: {target_ext_id}")
    return item_update


def add_to_dict(
        match: dict[str, Any],
        entity_view_id: str,
        target_view_id: str,
) -> dict[str, Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """
    source = match[KEY_SOURCE]
    if len(match[KEY_MATCHES]) > 0:
        target = match[KEY_MATCHES][0][KEY_TARGET]
        score = match[KEY_MATCHES][0][KEY_SCORE]
        target_name = target[KEY_ORG_NAME]
        target_match_value = target[KEY_NAME]
        target_ext_id = target[KEY_TARGET_EXT_ID]
    else:
        score = 0
        target_name = PLACEHOLDER_NO_MATCH
        target_match_value = PLACEHOLDER_NO_MATCH
        target_ext_id = PLACEHOLDER_NO_MATCH
        target_view_id = PLACEHOLDER_NO_MATCH
    return {
        KEY_MATCH_TYPE: MATCH_TYPE_ENTITY,
        KEY_ENTITY_EXT_ID: source[KEY_ENTITY_EXT_ID],
        KEY_ENTITY_NAME: source[KEY_ORG_NAME],
        KEY_ENTITY_MATCH_VALUE: source[KEY_NAME],
        KEY_ENTITY_VIEW_ID: str(entity_view_id),
        KEY_ENTITY_EXISTING_TARGETS: source[KEY_TARGET_LINKS],
        KEY_SCORE: round(score, 2),
        KEY_TARGET_NAME: target_name,
        KEY_TARGET_MATCH_VALUE: target_match_value,
        KEY_TARGET_EXT_ID: target_ext_id,
        KEY_TARGET_VIEW_ID: str(target_view_id),
    }


def write_mapping_to_raw(
    client: CogniteClient,
    config: Config,
    raw_uploader: RawUploadQueue,
    good_matches: list[dict[str, Any]],
    bad_matches: list[dict[str, Any]],
    logger: CogniteFunctionLogger
) -> None:
    """
    Write matching results to RAW DB

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        good_matches: list of good matches
        bad_matches: list of bad matches
    """

    raw_db = config.parameters.raw_db
    raw_tale_ctx_bad = config.parameters.raw_tale_ctx_bad
    raw_tale_ctx_good = config.parameters.raw_tale_ctx_good

    try:
        if config.parameters.run_all and not config.parameters.debug:
            logger.info(f"Clean up BAD table: {raw_db}/{raw_tale_ctx_bad} before writing new status")
            delete_table(client, raw_db, raw_tale_ctx_bad)

            logger.info(f"Clean up GOOD table: {raw_db}/{raw_tale_ctx_good} before writing new status")
            delete_table(client, raw_db, raw_tale_ctx_good)

            logger.info("Create DB / Table for DB: {raw_db}  Tables: {raw_tale_ctx_bad} and {raw_tale_ctx_good} if it does not exist")
            create_table(client, raw_db, raw_tale_ctx_bad)
            create_table(client, raw_db, raw_tale_ctx_good)

            for match in good_matches:
                raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_good, Row(match[KEY_ENTITY_EXT_ID], match))  # type: ignore
                logger.debug(f"Added matched entity: {match[KEY_ENTITY_EXT_ID]} to {raw_db}/{raw_tale_ctx_good}")

            for not_match in bad_matches:
                raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_bad, Row(not_match[KEY_ENTITY_EXT_ID], not_match))  # type: ignore
                logger.debug(f"Added NOT matched entity: {not_match[KEY_ENTITY_EXT_ID]} to {raw_db}/{raw_tale_ctx_bad}")

            # Upload any remaining RAW cols in queue
            raw_uploader.upload()
    except Exception as e:
        logger.error(f"ERROR: Failed to write mapping to RAW DB - error: {type(e)}({e})")
        raise Exception(f"Failed to write mapping to RAW DB - error: {type(e)}({e})")


def create_table(client: CogniteClient, raw_db: str, tbl: str) -> None:
    try:
        client.raw.databases.create(raw_db)
    except Exception:
        pass

    try:
        client.raw.tables.create(raw_db, tbl)
    except Exception:
        pass

def delete_table(client: CogniteClient, db: str, tbl: str) -> None:
    try:
        client.raw.tables.delete(db, [tbl])
    except CogniteAPIError as e:
        # Any other error than table not found, and we re-raise
        if e.code != 404:
            raise

