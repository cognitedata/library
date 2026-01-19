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
    OPTIMIZATIONS_AVAILABLE = True
except ImportError:
    OPTIMIZATIONS_AVAILABLE = False
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

        logger.debug("Read manual mappings to be used in entity matching")
        manual_mappings = read_manual_mappings(client, logger, config)

        logger.debug("Start by applying manual mappings - NOTE manual mappings will write over existing mappings")
        manual_result = apply_manual_mappings(client, logger, config, raw_uploader, manual_mappings, good_matches)
        good_matches = manual_result if manual_result is not None else []
    
        logger.debug("Read rule mappings to be used in entity matching")
        rule_mappings = read_rule_mappings(client, logger, config)

        logger.debug("Get entities (ex: time series) that has been updated since last run")
        new_entities = get_new_entities(client, config, logger, None, rule_mappings, None)

        logger.debug(f"Number of new entities to process are: {len(new_entities)}")
        if len(new_entities) == 0:
            logger.debug("No new entities to process, we are done - just update pipeline run")
            update_pipeline_run(client, logger, pipeline_ext_id, "success", match_count, not_matches_count, None)
            return

        logger.debug("Get all assets that are input for matching assets ( based on TAG filtering given in config)")
        asset_dest = get_all_assets(client, logger, config, rule_mappings)
        if len(asset_dest) == 0:
            logger.warning("No assets found based on configuration, please check the configuration")
            update_pipeline_run(client, logger, pipeline_ext_id, "success", match_count, not_matches_count, None)
            return

        while len(new_entities) > 0:

            logger.info("Applying rule based mappings - using provided reg expressions to match entities to assets")
            good_matches = apply_rule_mappings(client, config, logger, good_matches, asset_dest, new_entities)  # type: ignore

            match_results = get_matches(client, config, logger, matching_model_id or "", asset_dest, new_entities)  # type: ignore

            good_matches, bad_matches = select_and_apply_matches(client, config, logger, good_matches, match_results)  # type: ignore
            write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches, logger)

            len_good_matches = len(good_matches)
            len_bad_matches = len(bad_matches)

            match_count += len_good_matches
            not_matches_count += len_bad_matches
            update_pipeline_run(client, logger, pipeline_ext_id, "success", match_count, not_matches_count, None)

            list_good_matches = [match["entity_ext_id"] for match in good_matches]
            logger.info("Get more entities (ex: time series) in the next batch")
            new_entities = get_new_entities(client, config, logger, list_good_matches, rule_mappings, bad_matches)  # type: ignore

            if config.parameters.debug:
                break

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", match_count, not_matches_count, msg)
        raise Exception("msg")



def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    match_count: int,
    not_matches_count: int,
    error: Optional[str] = None
) -> None:

    if status == "success":
        msg = (
            f"Entity matching of: {match_count} input entities, "
            f" - NOT matched due to low score: {not_matches_count}"
        )
        logger.info(msg)
    else:
        msg = (
            f"Entity matching of: {match_count} input entities, low score items not matches : {not_matches_count}, "
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

    state_row = None

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

        logger.info(f"Number of manual mappings: {len(manual_mappings)}")

    except Exception as e:
        logger.error(f"Read manual mappings. Error: {type(e)}({e})")

    return manual_mappings


def resolve_asset_external_id(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    asset_ref: str
) -> Optional[str]:
    if not asset_ref:
        return None

    # Temporary compatibility for old WMT:<TAG_NAME> references
    if asset_ref.startswith("WMT:"):
        asset_name = asset_ref.split("WMT:", 1)[1]
        asset_view = config.data.asset_view
        is_view = dm.filters.HasData(views=[asset_view.as_view_id()])
        by_name = dm.filters.Equals(asset_view.as_property_ref("name"), asset_name)
        by_alias = dm.filters.In(asset_view.as_property_ref("aliases"), [asset_name])
        is_selected = dm.filters.And(is_view, dm.filters.Or(by_name, by_alias))

        assets = client.data_modeling.instances.list(
            space=asset_view.instance_space,
            sources=[asset_view.as_view_id()],
            filter=is_selected,
            limit=1,
        )
        if assets:
            logger.debug(f"Resolved manual asset ref {asset_ref} -> {assets[0].external_id}")
            return assets[0].external_id

        logger.warning(f"Manual asset ref {asset_ref} not found by name/aliases; using as-is")

    return asset_ref


def apply_manual_mappings(
    client: CogniteClient, 
    logger: CogniteFunctionLogger,
    config: Config, 
    raw_uploader: RawUploadQueue, 
    manual_mappings: list[Row],
    good_matches: list[dict[str, Any]] = []
) -> list[dict[str, Any]]:

    entity_view_id = config.data.entity_view.as_view_id()
    item_update = []

    try:
        entity_list = [mapping[COL_KEY_MAN_MAPPING_ENTITY] for mapping in manual_mappings]
        lookup_mapping = {mapping["key"]: mapping[COL_KEY_MAN_MAPPING_ASSET] for mapping in manual_mappings}
                                  
        is_filter_param = dm.filters.In(
            config.data.entity_view.as_property_ref(
                "name"),
                entity_list
        )
        is_view = dm.filters.HasData(views=[config.data.entity_view.as_view_id()])
        is_selected = dm.filters.And(is_view, is_filter_param)

        all_entities = client.data_modeling.instances.list(
            space=config.data.entity_view.instance_space,
            sources=[entity_view_id],
            filter=is_selected,
            limit=-1
        )

        for entity in all_entities:
            if entity.external_id in lookup_mapping:
                asset_ext_id = lookup_mapping[entity.external_id]
                asset_ext_id = resolve_asset_external_id(client, config, logger, asset_ext_id)
                if not asset_ext_id:
                    logger.warning(f"Manual mapping asset ref is empty for entity: {entity.external_id}, skipping")
                    continue
            else:
                continue

            item_update = add_to_items(config, 
                                       logger, 
                                       item_update,
                                       [asset_ext_id],
                                       entity.external_id,
                                       entity_view_id)
            good_matches.append(
                {
                    "match_type": "Manual Mapping",
                    "entity_ext_id": entity.external_id,
                    "entity_name": entity.properties[entity_view_id]["name"],
                    "entity_view_id": str(config.data.entity_view.as_view_id()),
                    "entity_assets": entity.properties[entity_view_id]["assets"],
                    "score": 1,  # Assuming manual mappings are always 100% accurate
                    "asset_name": entity.properties[entity_view_id]["name"],
                    "asset_ext_id": asset_ext_id,
                    "asset_view_id": str(config.data.asset_view.as_view_id()),
                }
            )

            mapping = {}
            mapping[COL_KEY_MAN_MAPPING_ENTITY] = entity.properties[entity_view_id]["name"]
            mapping[COL_KEY_MAN_MAPPING_ASSET] = asset_ext_id
            mapping[COL_KEY_MAN_CONTEXTUALIZED] = True
            raw_uploader.add_to_upload_queue(config.parameters.raw_db, config.parameters.raw_tale_ctx_good, Row(entity.external_id, mapping))

        logger.debug(f"Number of items to update based on manual updates: {len(item_update)}")
        if not config.parameters.debug:
            # Apply the updates to the data model
            client.data_modeling.instances.apply(item_update)
            raw_uploader.upload()

        return good_matches

    except Exception as e:
        logger.error(f"ERROR: Not able run manual mapping for {manual_mappings} - error: {e}")
        return good_matches


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
    type = "assets"
    job_config = config.data
    search_property = job_config.asset_view.search_property

    is_selected = get_query_filter(type, job_config.asset_view, config.parameters.run_all, logger)

    all_assets = client.data_modeling.instances.list(
        space=job_config.asset_view.instance_space,
        sources=[job_config.asset_view.as_view_id()],
        filter=is_selected,
        limit=-1
    )
 
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
    rule_mappings: list[Row] | None = None,
    bad_matches: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:

    entities_source = []
    type = "entities"

    entity_view_config= config.data.entity_view
    entity_view_id = entity_view_config.as_view_id()

    logger.debug(f"Get new entities from view: {entity_view_id}, based on config: {entity_view_config}")
    is_selected = get_query_filter(type, entity_view_config, config.parameters.run_all, logger)

    new_entities = client.data_modeling.instances.list(
        space=entity_view_config.instance_space,
        sources=[entity_view_id],
        filter=is_selected,
        limit=-1
    )
 
    item_update = []

    # Create set of bad entity IDs for O(1) lookup
    bad_entity_ids = {match["entity_ext_id"] for match in bad_matches} if bad_matches else set()

    for entity in new_entities:
        # test if just matched and skip if so
        if list_good_entities and entity.external_id in list_good_entities:
            logger.debug(f"Entity: {entity.external_id} just matched, skipping")
            continue
        # Check if entity ID is in bad_matches properly
        if entity.external_id in bad_entity_ids:
            logger.debug(f"Entity: {entity.external_id} is in bad matches, skipping")
            continue
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
        if "aliases" in entity.properties[entity_view_id]:
            aliases = entity.properties[entity_view_id]["aliases"]
            entity_names = aliases if isinstance(aliases, list) else [str(aliases)]
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

    logger.info(f"Num new entities: {len(item_update)} from view: {entity_view_id}")

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
            logger.debug(f"Created new matching model: {model.id}")
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

    key_field = "rule_keys"  # The field in the dictionaries that contains the rule keys
    
    try:
        # Build an inverted index for asset_dest
        # Format: { 'rule_key_value': [dict_from_asset_dest_1, dict_from_asset_dest_2, ...] }
        index1 = defaultdict(list)
        matches = defaultdict(lambda: [[], []])

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

                            good_matches.append(
                                {
                                    "match_type": "Rule Based Mapping",
                                    "entity_ext_id": d2['entity_ext_id'],
                                    "entity_name": d2['org_name'],
                                    "entity_view_id": str(config.data.entity_view.as_view_id()),
                                    "entity_assets": d2['assets'],
                                    "score": 1,  # Assuming rule based mappings are always 100% accurate
                                    "asset_name": d1_match['org_name'],
                                    "asset_ext_id": d1_match['asset_ext_id'],
                                    "asset_view_id": str(config.data.asset_view.as_view_id()),
                                }
                            )

                            matches[d2['entity_ext_id']][0].append(d1_match['asset_ext_id'])
                            existing_asset_list = json.loads(d2['assets'])
                            if len(existing_asset_list) > 0:
                                matches[d2['entity_ext_id']][1] = matches[d2['entity_ext_id']][1] + existing_asset_list
                                
                            unique_matches_tracker.add(pair)

        item_update = []
        
        # Iterate through the matches and prepare the item updates
        cnt = 0
        for entity_ext_id in matches:
            cnt += 1
            asset_ext_ids = matches[entity_ext_id][0] # Assuming the first asset is the one to match with
            entity_assets = matches[entity_ext_id][1]

            item_update = add_to_items(config, 
                                    logger, 
                                    item_update,
                                    asset_ext_ids,
                                    entity_ext_id,
                                    config.data.entity_view.as_view_id(),
                                    json.dumps(entity_assets))   
                
            if not config.parameters.debug and cnt % 1000 == 0:
                client.data_modeling.instances.apply(item_update)
                item_update = []  # Reset item_update after applying


        logger.info(f"Number of items to update based on rule mapping: {len(item_update)}")
        if not config.parameters.debug:
            # Apply the updates to the data model
            client.data_modeling.instances.apply(item_update)

        return good_matches

    except Exception as e:
        logger.error(f"ERROR: Not able run rule based mapping - error: {e}")
        return good_matches




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

    entity_view_id = config.data.entity_view.as_view_id()
    asset_view_id = config.data.asset_view.as_view_id()

    # Use set instead of list for O(1) lookups
    good_matches_set = {f"{match['asset_ext_id']}_{match['entity_ext_id']}"
                        for match in good_matches}
    matched_entity_ids = {match["entity_ext_id"] for match in good_matches}
    new_good_matches = []
    try:
        for match in match_results:
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
                    if match["source"]["entity_ext_id"] not in matched_entity_ids:
                        bad_matches.append(add_to_dict(match, str(entity_view_id), str(asset_view_id)))
            else:
                if match["source"]["entity_ext_id"] not in matched_entity_ids:
                    bad_matches.append(add_to_dict(match, str(entity_view_id), str(asset_view_id)))

        logger.info(f"INFO: Got {len(new_good_matches)} matches with score >= {config.parameters.auto_approval_threshold}")
        logger.info(f"INFO: Got {len(bad_matches)} matches with score < {config.parameters.auto_approval_threshold}")

        # Update time series with matches
        for match in new_good_matches:
            entity_ext_id = match["entity_ext_id"]
            asset_ext_id = match["asset_ext_id"]
            entity_assets = match["entity_assets"]
 
            item_update = add_to_items(config, 
                                       logger, 
                                       item_update,
                                       [asset_ext_id],
                                       entity_ext_id,
                                       entity_view_id,
                                       entity_assets)
 
        if not config.parameters.debug:
            client.data_modeling.instances.apply(item_update)

        return good_matches + new_good_matches, bad_matches

    except Exception as e:
        print(f"ERROR: Failed to parse results from entity matching - error: {type(e)}({e})")
        return good_matches, []  # type: ignore

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
        asset_ext_id = target["asset_ext_id"] 
    else:
        score = 0
        asset_name = "_no_match_"
        asset_ext_id = None
    return {
        "match_type": "Entity Matching",
        "entity_ext_id": source["entity_ext_id"],
        "entity_name": source["org_name"],
        "entity_view_id": str(entity_view_id),
        "entity_assets": source["assets"],
        "score": round(score, 2),
        "asset_name": asset_name,
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
        logger.info(f"INFO: Clean up BAD table: {raw_db}/{raw_tale_ctx_bad} before writing new status")
        delete_table(client, raw_db, raw_tale_ctx_bad)

        # if reset mapping, clean up good matches in table
        if config.parameters.run_all and not config.parameters.debug:
            logger.info(
                f"INFO: ResetMapping - Cleaning up GOOD table: {raw_db}/{raw_tale_ctx_good} "
                "before writing new status"
            )
            delete_table(client, raw_db, raw_tale_ctx_good)

        logger.debug("Create DB / Table for DB: {raw_db}  Tables: {doc_tag} and {doc_doc} if it does not exist")
        create_table(client, raw_db, raw_tale_ctx_bad)
        create_table(client, raw_db, raw_tale_ctx_good)

        for match in good_matches:
            raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_good, Row(match["entity_ext_id"], match))  # type: ignore
        logger.info(f"INFO: Added {len(good_matches)} to {raw_db}/{raw_tale_ctx_good}")

        for not_match in bad_matches:
            raw_uploader.add_to_upload_queue(raw_db, raw_tale_ctx_bad, Row(not_match["entity_ext_id"], not_match))  # type: ignore
        logger.info(f"INFO: Added {len(bad_matches)} to {raw_db}/{raw_tale_ctx_bad}")

        # Upload any remaining RAW cols in queue
        if not config.parameters.debug:
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

