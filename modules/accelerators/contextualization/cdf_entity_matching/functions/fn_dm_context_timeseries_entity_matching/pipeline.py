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
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_ASSET,
    COL_KEY_MAN_MAPPING_ENTITY,
    COL_KEY_RULE_REGEXP_ASSET,
    COL_KEY_RULE_REGEXP_ENTITY,
    COL_MATCH_KEY,
    FUNCTION_ID,
    ML_MODEL_FEATURE_TYPE,
    STAT_STORE_MATCH_MODEL_ID,
    STAT_STORE_VALUE,
    BATCH_SIZE_API_SUBMIT,
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




def asset_entity_matching(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config
) -> None:
    """
    Read configuration and start P&ID annotation process by
    1. Reading initial parameters from the pipeline run
    2. Reading configuration from the config file
    3. Read manual mappings from RAW, and apply them to the entity matching
    4. Read rule mappings from RAW
    5. Get the list of entities to be matched (all, or new entities/unmatched entities)
    6. Get assets to be matched to from the entities
    7. Apply rule based mappings to match entities to assets
    8. Use existing or Create a matching model and run the matching job or read existing model
    9. Run the matching job
    10. Get the matching results and select the good and bad matches
    11. Write the good and bad matches to RAW
    12. Update the pipeline run with the status and number of matches

    """
    good_matches = []
    len_good_matches, len_bad_matches = 0, 0

    try:
        pipeline_ext_id = data["ExtractionPipelineExtId"]
        logger.info(f"Starting entity matching function: {FUNCTION_ID} with loglevel = {data.get('logLevel', 'INFO')},  reading parameters from extraction pipeline config: {pipeline_ext_id}")

        not_matches_count, match_count = 0, 0
        matching_model_id = None
        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug("**** Write debug messages and only process one entity *****")

        logger.debug("Initiate RAW upload queue used to store output from entity matching")
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level="INFO")

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

        logger.info("Read rule mappings to be used in entity matching, NOTE: Uses 'name' property for rule based matches")
        rule_mappings = read_rule_mappings(client, logger, config)

        logger.info("Read all assets that are input for matching assets ( based on TAG filtering IF given in config)")
        asset_target = get_all_assets(client, logger, config, rule_mappings)
        if len(asset_target) == 0:
            logger.warning("No assets found based on configuration, please check the configuration")
            update_pipeline_run(client, logger, pipeline_ext_id, "success", match_count, not_matches_count, None)
            return

        logger.info("Start by applying manual mappings")
        good_matches, cnt_manual_mappings = apply_manual_mappings(client, logger, config, raw_uploader, manual_mappings, manual_mappings_input, good_matches, asset_target)
    
        logger.info("Read new entities (ex: time series) that has been updated since last run")
        list_good_matches = [match["entity_ext_id"] for match in good_matches]
        new_entities = get_new_entities(client, config, logger, list_good_matches, rule_mappings)

        logger.info(f"Start processing of new entities ({len(new_entities)})")
        if len(new_entities) == 0:
            logger.info("No new entities to process, we are done - just update pipeline run")
            update_pipeline_run(client, logger, pipeline_ext_id, "success", match_count, not_matches_count, None)
            return

        logger.info("Applying rule based mappings - using provided reg expressions to match entities to assets")
        good_matches, cnt_rule_mappings = apply_rule_mappings(client, config, logger, good_matches, asset_target, new_entities)  # type: ignore

        logger.info("NOTE: the matching runs in CDF, and the process could here be split into two steps to avoid long running jobs")
        match_results = get_matches(client, config, logger, matching_model_id or "", asset_target, new_entities)  # type: ignore

        good_matches, bad_matches, cnt_entity_matching = select_and_apply_matches(client, config, logger, good_matches, match_results)  # type: ignore
        write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, logger)

        len_good_matches = cnt_manual_mappings + cnt_rule_mappings + cnt_entity_matching
        len_bad_matches = len(bad_matches)

        update_pipeline_run(client, logger, pipeline_ext_id, "success", len_good_matches, len_bad_matches, None)

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", len_good_matches, len_bad_matches, msg)
        raise Exception("msg")


def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    match_count: int = 0,
    not_matches_count: int = 0,
    error: Optional[str] = None
) -> None:

    total_entities = match_count + not_matches_count
    if status == "success":
        msg = (
            f"Entity matching of: {total_entities} input entities, Matched: {match_count} "
            f" - NOT matched due to low score: {not_matches_count}"
        )
        logger.info(msg)
    else:
        msg = (
            f"Entity matching of: {total_entities} input entities, Matched: {match_count} "
            f" - NOT matched due to low score: {not_matches_count}, "
            f"{error or 'Unknown error'}, traceback:\n{traceback.format_exc()}"
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
                        "key": row.key,
                        COL_KEY_MAN_MAPPING_ENTITY: entity,
                        COL_KEY_MAN_MAPPING_ASSET: row.columns[COL_KEY_MAN_MAPPING_ASSET].strip(),
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
    asset_target: list[dict[str, Any]] = []
) -> list[dict[str, Any]]:

    entity_view_id = config.data.entity_view.as_view_id()
    item_update = []
    clean_asset_list = []
    cnt = 0

    try:
        asset_target_lookup = {asset["asset_ext_id"]: asset for asset in asset_target}
        entity_list = [mapping[COL_KEY_MAN_MAPPING_ENTITY] for mapping in manual_mappings]
        lookup_mapping = {mapping[COL_KEY_MAN_MAPPING_ENTITY]: mapping[COL_KEY_MAN_MAPPING_ASSET] for mapping in manual_mappings}
        key_lookup = {mapping[COL_KEY_MAN_MAPPING_ENTITY]:mapping["key"] for mapping in manual_mappings}

        # Split entity_list into batches of 5000
        BATCH_SIZE = 5000
        num_batches = (len(entity_list) - 1) // BATCH_SIZE + 1 if entity_list else 0
        
        if num_batches > 1:
            logger.info(f"Entity list has {len(entity_list)} items, splitting into {num_batches} batches of up to {BATCH_SIZE}")
        
        # Process in batches (single batch if <= 5000)
        for batch_idx in range(0, len(entity_list), BATCH_SIZE):
            batch_entity_list = entity_list[batch_idx:batch_idx + BATCH_SIZE]
            batch_num = batch_idx // BATCH_SIZE + 1
            
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
                    
                asset_ext_id = lookup_mapping[entity.external_id]
                if not asset_ext_id:
                    logger.warning(f"Manual mapping asset ref is empty for entity: {entity.external_id}, skipping")
                    continue

                assets = []
                if not config.parameters.remove_old_asset_links: 
                    # keep old asset links
                    assets = get_assets_from_entity(entity.properties[entity_view_id]["assets"])   
                else:
                    clean_asset_list = clean_asset_links(config, entity.external_id, clean_asset_list)
                
                assets.append(asset_ext_id)

                item_update = add_to_items(config, 
                                           logger, 
                                           item_update,
                                           assets,
                                           entity.external_id,
                                           entity_view_id)

                if asset_ext_id in asset_target_lookup:
                    asset = asset_target_lookup[asset_ext_id]
                    asset_name = asset["org_name"]
                    asset_view_id = str(config.data.asset_view.as_view_id())
                else:
                    asset_name = "_no_match_on_asset_ext_id_"
                    asset_view_id = "_no_match_on_asset_ext_id_"

                good_matches.append(
                    {
                        "match_type": "Manual Mapping",
                        "entity_ext_id": entity.external_id,
                        "entity_name": entity.properties[entity_view_id]["name"],
                        "entity_match_value": entity.external_id,
                        "entity_view_id": str(config.data.entity_view.as_view_id()),
                        "entity_existing_assets": entity.properties[entity_view_id]["assets"],
                        "score": 1,  # Assuming manual mappings are always 100% accurate
                        "asset_name": asset_name,
                        "asset_match_value": asset_ext_id,
                        "asset_ext_id": asset_ext_id,
                        "asset_view_id": asset_view_id,
                    }
                )

                mapping = {}
                row_key = key_lookup[entity.external_id]
                mapping = manual_mappings_input[row_key].copy()
                mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
                raw_uploader.add_to_upload_queue(config.parameters.raw_db, config.parameters.raw_tale_ctx_manual, Row(row_key, mapping))
            
                # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
                if not config.parameters.debug and cnt % BATCH_SIZE_API_SUBMIT == 0:
                    if len(clean_asset_list) > 0:
                        client.data_modeling.instances.apply(clean_asset_list)
                        clean_asset_list = []

                    logger.info(f"==> Mapping table based matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(manual_mappings)}")
                    client.data_modeling.instances.apply(item_update)
                    item_update = []  # Reset item_update after applying

            if num_batches > 1:
                logger.info(f"Completed batch {batch_num}/{num_batches}")

        if not config.parameters.debug:
            if len(clean_asset_list) > 0:
                client.data_modeling.instances.apply(clean_asset_list)

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


def get_assets_from_entity(
    assets_links_property: list[dict[str, Any]]
) -> list[str]:

    assets = []
    if len(assets_links_property) > 0:
        for asset in assets_links_property:
            assets.append(asset["externalId"])
    return assets


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
        ["node", "externalId"],
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
                        "key": f"{idx}",
                        COL_KEY_RULE_REGEXP_ENTITY: row.columns[COL_KEY_RULE_REGEXP_ENTITY].strip(),
                        COL_KEY_RULE_REGEXP_ASSET: row.columns[COL_KEY_RULE_REGEXP_ASSET].strip(),
                }
            )
            idx += 1
        logger.info(f"Number of mapping rules : {len(rule_mappings)}")
    except Exception as e:
        logger.error(f"Read rule based mappings. Error: {type(e)}({e})")

    return rule_mappings


def get_all_assets(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    rule_mappings: list[Row] | None = None
) -> list[dict[str, Any]]:

    asset_target = []
    job_config = config.data
    search_property = job_config.asset_view.search_property

    is_selected = get_query_filter("assets", job_config.asset_view, config.parameters.run_all, logger)

    all_assets = client.data_modeling.instances.list(
        space=job_config.asset_view.instance_space,
        sources=[job_config.asset_view.as_view_id()],
        filter=is_selected,
        limit=-1
    )

    logger.info(f"Number of assets to process: {len(all_assets)} NOTE: Rule based regular expressions are applied to the 'name' property")
    for asset in all_assets:
        org_name = str(asset.properties[job_config.asset_view.as_view_id()]["name"])

        rule_keys = []
        if rule_mappings:
            for rule in rule_mappings:
                reg_exp = str(rule[COL_KEY_RULE_REGEXP_ASSET])
                match = re.search(reg_exp, org_name)

                if match:
                    # Concatenate the captured groups directly
                    cleaned_value = rule["key"] + "_" + "".join(match.groups()) # groups() returns a tuple of all captured groups
                    logger.debug(f"Cleaned value (using capture groups): {cleaned_value}")
                    rule_keys.append(cleaned_value)

        if search_property in asset.properties[job_config.asset_view.as_view_id()]:
            match_properties = asset.properties[job_config.asset_view.as_view_id()][search_property]
            if not isinstance(match_properties, list):
                match_properties = [match_properties]
        else:
            match_properties = [org_name]  

        for match_property in match_properties:
            asset_target.append(
            {
                "asset_ext_id": asset.external_id,
                "org_name": org_name,
                "name": match_property,
                "rule_keys": rule_keys if rule_keys else None,
            }
    )
    logger.debug(f"Number assets added as entities: {len(asset_target)}")

    return asset_target


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
    is_selected = get_query_filter("entities", entity_view_config, config.parameters.run_all, logger)

    new_entities = client.data_modeling.instances.list(
        space=entity_view_config.instance_space,
        sources=[entity_view_id],
        filter=is_selected,
        limit=-1
    )
 
    item_update = []


    logger.info(f"Number of new entities to process: {len(new_entities)} NOTE: Rule based regular expressions are applied to the 'name' property")
    for entity in new_entities:
        # test if just matched and skip if so
        if list_good_entities and entity.external_id in list_good_entities:
            logger.debug(f"Entity: {entity.external_id} just matched, skipping")
            continue
        # Rule based matching uses the name property to match entities to assets
        org_name = str(entity.properties[entity_view_id]["name"])

        rule_keys = []
        if rule_mappings:
            for rule in rule_mappings:
                reg_exp = str(rule[COL_KEY_RULE_REGEXP_ENTITY])
                match = re.search(reg_exp, org_name)

                if match:
                    # Concatenate the captured groups directly
                    cleaned_value = rule["key"] + "_" + "".join(match.groups()) # groups() returns a tuple of all captured groups
                    logger.debug(f"Cleaned value (using capture groups): {cleaned_value}")
                    rule_keys.append(cleaned_value)
                    
        assets = []
        if not config.parameters.remove_old_asset_links: 
            # keep old asset links
            assets = entity.properties[entity_view_id]["assets"]
        else:
            item_update = clean_asset_links(config, entity.external_id, item_update)

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
                    "entity_ext_id": entity.external_id,
                    "name": entity_name, 
                    "org_name": org_name,
                    "assets": json.dumps(assets),
                    "rule_keys": rule_keys if rule_keys else None,
                })
            logger.debug(f"Entity: {entity.external_id} - {entity_name} ({org_name})")

    logger.info(f"Num new entities: {len(entities_source)} from view: {entity_view_id}")

    if config.parameters.remove_old_asset_links and len(item_update) > 0:
        client.data_modeling.instances.apply(item_update)
        logger.info(f"Cleaned up asset links for {len(item_update)} entities")

    return entities_source


def clean_asset_links(
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
                    properties= {"assets": None},
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
) -> dm.filters.Filter:

    is_view = dm.filters.HasData(views=[view_config.as_view_id()])
    is_selected = dm.filters.And(is_view)
    dbg_msg = f"For for view: {view_config.as_view_id()} - Entity filter: HasData = True"

    # Check if the view entity already is matched or not
    if type == "entities" and not run_all:  
        is_matched = dm.filters.Exists(view_config.as_property_ref("assets"))
        not_matched = dm.filters.Not(is_matched)
        is_selected = dm.filters.And(is_selected, not_matched)
        dbg_msg = f"{dbg_msg} Entity filtering on: 'assets' - NOT EXISTS"

    if view_config.filter_property and view_config.filter_values:
        is_filter_param = dm.filters.In(
            view_config.as_property_ref(
                view_config.filter_property),
                view_config.filter_values
        )
        is_selected = dm.filters.And(is_selected, is_filter_param)
        dbg_msg = f"{dbg_msg} Entity filtering on: '{view_config.filter_values}' IN: '{view_config.filter_property}'"

    logger.debug(dbg_msg)

    return is_selected


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
                sources=match_from[:10000],  # Limit to 10000 entities
                targets=match_to[:10000],  # Limit to 10000 assets
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

        return job.result["items"]

    except Exception as e:
        logger.error(f"ERROR: Failed to get matching model and run prediction. Error: {type(e)}({e})")
        raise


def apply_rule_mappings(
    client: CogniteClient, 
    config: Config, 
    logger: CogniteFunctionLogger,
    good_matches: list[dict[str, Any]],
    asset_dest: list[dict[str, Any]], 
    new_entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    # Use set instead of list for O(1) lookups
    good_matches_set = {f"{match['asset_ext_id']}_{match['entity_ext_id']}"
                        for match in good_matches}
    matched_entity_ids = {match["entity_ext_id"] for match in good_matches}


    key_field = "rule_keys"  # The field in the dictionaries that contains the rule keys
    num_added_matches = 0
    cnt = 0
    
    try:
        # Build an inverted index for asset_dest
        # Format: { 'rule_key_value': [dict_from_asset_dest_1, dict_from_asset_dest_2, ...] }
        index1 = defaultdict(list)
        matches = defaultdict(list)  # defaultdict(lambda: [[], []])

        for d1 in asset_dest:
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
            if d2['entity_ext_id'] in matched_entity_ids:
                logger.debug(f"Entity: {d2['entity_ext_id']} already has been matched manually, skipping")
                continue
        
            for r_key_from_d2 in set2:
                if r_key_from_d2 in index1:
                    for d1_match in index1[r_key_from_d2]:
                        # Create a canonical tuple for uniqueness tracking
                        # Ensure consistent order (e.g., by ID)
                        pair = tuple(sorted((d2['entity_ext_id'], d1_match['asset_ext_id'])))

                        if pair not in unique_matches_tracker:
                            # matches.append((d1_match, d2))
                            match_key = f"{d1_match['asset_ext_id']}_{d2['entity_ext_id']}"
                            if match_key in good_matches_set:
                                logger.debug(f"Match already exists in good matches: {d1_match['asset_ext_id']} - {d2['entity_ext_id']}")
                                continue
                            good_matches_set.add(match_key)

                            unique_asset_list = list(set(matches[d2['entity_ext_id']]))
                            if d1_match['asset_ext_id'] and d1_match['asset_ext_id'] not in unique_asset_list:
                                unique_asset_list.append(d1_match['asset_ext_id'])
                                num_added_matches += 1
                                good_matches.append(
                                    {
                                        "match_type": "Rule Based Mapping",
                                        "entity_ext_id": d2['entity_ext_id'],
                                        "entity_name": d2['org_name'],
                                        "entity_match_value": d2['name'],
                                        "entity_view_id": str(config.data.entity_view.as_view_id()),
                                        "entity_existing_assets": d2['assets'],
                                        "entity_rule_keys": json.dumps(d2['rule_keys']),
                                        "score": 1,  # Assuming rule based mappings are always 100% accurate
                                        "asset_name": d1_match['org_name'],
                                        "asset_match_value": d1_match['name'],
                                        "asset_ext_id": d1_match['asset_ext_id'],
                                        "asset_view_id": str(config.data.asset_view.as_view_id()),
                                        "asset_rule_keys": json.dumps(d1_match['rule_keys']),
                                    }
                                )

                            existing_asset_list = json.loads(d2['assets'])
                            if len(existing_asset_list) > 0:
                                for asset in existing_asset_list:
                                    if 'externalId' in asset and asset['externalId'] not in unique_asset_list:
                                        unique_asset_list.append(asset['externalId'])

                            matches[d2['entity_ext_id']] = unique_asset_list
                            unique_matches_tracker.add(pair)

        item_update = []
        
        # Iterate through the matches and prepare the item updates
        
        for entity_ext_id in matches:
            cnt += 1
            asset_ext_ids = matches[entity_ext_id] # Assuming the first asset is the one to match with

            item_update = add_to_items(config, 
                                    logger, 
                                    item_update,
                                    asset_ext_ids,
                                    entity_ext_id,
                                    config.data.entity_view.as_view_id())
            
            # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
            if not config.parameters.debug and cnt % BATCH_SIZE_API_SUBMIT == 0:
                logger.info(f"==> Rule based matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(matches)}")
                client.data_modeling.instances.apply(item_update)
                item_update = []  # Reset item_update after applying

        if not config.parameters.debug:
            # Apply the updates to the data model
            client.data_modeling.instances.apply(item_update)
 
            if cnt == 0:
                logger.info("==> Rule based matching - No items added to data model based on new items found and rule based mappings")
            else:
                logger.info(f"==> Rule based matching - Adding remaining batch of {len(item_update)} items to data model, total count: {cnt} / {len(matches)}")
                logger.info(f"==> Rule based matching - Total number of new matches based on rules matching one or more assets per entity added: {num_added_matches}")

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
    with asset ID When matches are updated, metadata property with information about the match is added to time series
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
    asset_view_id = config.data.asset_view.as_view_id()

    # Use set instead of list for O(1) lookups
    good_matches_set = {f"{match['asset_ext_id']}_{match['entity_ext_id']}"
                        for match in good_matches}
    matched_entity_ids = {match["entity_ext_id"] for match in good_matches}
    new_good_matches = []
    try:
        for match in match_results:
            # Skip if entity already has been matched
            if match["source"]["entity_ext_id"] in matched_entity_ids:
                continue

            if match["matches"]:
                if match["matches"][0]["score"] >= config.parameters.auto_approval_threshold:
                    entity_ext_id = match["source"]["entity_ext_id"]
                    asset_ext_id = match["matches"][0]["target"]["asset_ext_id"]

                    match_key = f"{asset_ext_id}_{entity_ext_id}"
                    if match_key in good_matches_set:
                        logger.debug(f"Match already exists in good matches: {asset_ext_id} - {entity_ext_id}")
                        continue
                    else:
                        good_matches_set.add(match_key)

                    new_good_matches.append(add_to_dict(match, str(entity_view_id), str(asset_view_id)))
                else:
                    bad_matches.append(add_to_dict(match, str(entity_view_id), str(asset_view_id)))
            else:
                bad_matches.append(add_to_dict(match, str(entity_view_id), str(asset_view_id)))

        logger.info(f"Got {len(new_good_matches)} matches with score >= {config.parameters.auto_approval_threshold}")
        logger.info(f"Got {len(bad_matches)} matches with score < {config.parameters.auto_approval_threshold}")

        # Update time series with matches
        for match in new_good_matches:
            cnt += 1
            entity_ext_id = match["entity_ext_id"]
            asset_ext_id = match["asset_ext_id"]
            entity_assets = match["entity_existing_assets"]
 
            item_update = add_to_items(config, 
                                       logger, 
                                       item_update,
                                       [asset_ext_id],
                                       entity_ext_id,
                                       entity_view_id,
                                       entity_assets)

            # Apply the updates to the data model in batches of BATCH_SIZE_API_SUBMIT
            if not config.parameters.debug and cnt % BATCH_SIZE_API_SUBMIT == 0:
                logger.info(f"==> Entity matching - Adding batch of {len(item_update)} items to data model, total count: {cnt} / {len(new_good_matches)}")
                client.data_modeling.instances.apply(item_update)
                item_update = []  # Reset item_update after applying

        if not config.parameters.debug:
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
    asset_ext_ids: list[str],
    entity_ext_id: str,
    entity_view_id: dm.ViewId,
    entity_assets: Optional[str] = None  
) -> list[NodeApply]:

    assets = []

    if entity_assets:
        entity_assets_array = json.loads(entity_assets)
        if len(entity_assets_array) > 0:  # Check if there are existing assets
            for asset in entity_assets_array:
                assets.append(DirectRelationReference(space=asset["space"], external_id=asset["externalId"]))

    # Add new assets to the entity
    for asset_ext_id in asset_ext_ids:  
        if not asset_ext_id:
            logger.warning(f"Asset external ID is empty for entity: {entity_ext_id}, skipping")
            continue
        if asset_ext_id in [asset.external_id for asset in assets]:
            logger.debug(f"Asset: {asset_ext_id} already exists in entity: {entity_ext_id}, skipping")
            continue
        logger.debug(f"Adding asset: {asset_ext_id} to entity: {entity_ext_id}")   
        assets.append(
            DirectRelationReference(
                space=config.data.asset_view.instance_space,
                external_id=asset_ext_id
            )
        )

    if len(assets) > 1000:
        logger.warning(f"Entity: {entity_ext_id} has more than 1000 assets - has {len(assets)} assets, will only add 1000 - TODO look into your rule/matching model to prevent to wide matching")
        assets = assets[:1000]
        

    item_update.append(
        NodeApply(
            space=config.data.entity_view.instance_space,
            external_id=entity_ext_id,
            sources=[
                NodeOrEdgeData(
                    source=dm.ViewId.load(entity_view_id),  # type: ignore
                    properties= {"assets": assets},
                )
            ],
        )
    )       

    logger.debug(f"Added entity: {entity_ext_id} to asset: {asset_ext_id}")
    return item_update


def add_to_dict(
        match: dict[str, Any],
        entity_view_id: str,
        asset_view_id: str,
) -> dict[str, Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """
    source = match["source"]
    if len(match["matches"]) > 0:
        target = match["matches"][0]["target"]
        score = match["matches"][0]["score"]
        asset_name = target["org_name"]
        asset_match_value = target["name"]
        asset_ext_id = target["asset_ext_id"] 
    else:
        score = 0
        asset_name = "_no_match_"
        asset_match_value = "_no_match_"
        asset_ext_id = "_no_match_"
        asset_view_id = "_no_match_"
    return {
        "match_type": "Entity Matching",
        "entity_ext_id": source["entity_ext_id"],
        "entity_name": source["org_name"],
        "entity_match_value": source["name"],
        "entity_view_id": str(entity_view_id),
        "entity_existing_assets": source["assets"],
        "score": round(score, 2),
        "asset_name": asset_name,
        "asset_match_value": asset_match_value,
        "asset_ext_id": asset_ext_id,
        "asset_view_id": str(asset_view_id),
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
                raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_good, Row(match["entity_ext_id"], match))  # type: ignore
                logger.debug(f"Added matched entity: {match['entity_ext_id']} to {raw_db}/{raw_tale_ctx_good}")

            for not_match in bad_matches:
                raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_bad, Row(not_match["entity_ext_id"], not_match))  # type: ignore
                logger.debug(f"Added NOT matched entity: {not_match['entity_ext_id']} to {raw_db}/{raw_tale_ctx_bad}")

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

