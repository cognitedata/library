from cognite.client import data_modeling as dm
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue
from logger import CogniteFunctionLogger

from typing import Any, Optional

from config import Config

from cognite.client.exceptions import CogniteAPIError
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    ExtractionPipelineRun,
    Row
)

def key_extraction(
        client: CogniteClient,
        logger: CogniteFunctionLogger,
        data: dict[str, Any],
        config: Config
) -> None:
    pipeline_ext_id = data["ExtractionPipelineExtId"]
    patterns_matched = 0
    try:
        logger.info(
            f"Starting Key Extraction Function with loglevel = {data.get('logLevel', 'INFO')},  reading parameters from extraction pipeline config: {pipeline_ext_id}")

        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug(f"**** Write debug messages and only process one entity *****")

        logger.debug(f"Initiate RAW upload queue used to store ouput from key extraction")
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level="INFO")

        # if needed, we will not do key extraction on entities that have already been processed, if so desired al a the config
        entities_to_exclude = []

        # Delete pre-existing keys made in previous runs
        if config.parameters.remove_old_keys:
            logger.debug(f"Run all entities, delete state content in RAW since we are rerunning based on all input")
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_key)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_state)
        else:
            entities_to_exclude = read_table(client, config.parameters.raw_db, config.parameters.raw_table_key)

        logger.debug("Get entities (ex: time series)")
        entities = get_entities(client, config, logger, None)
    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", patterns_matched)

def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    patterns_matched: Optional[int],
    error: Optional[str] = None
) -> None:

    if status == "success":
        msg = (

        )
        logger.info(msg)
    else:
        msg = (

        )
        logger.error(msg)

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(
            extpipe_external_id=xid,
            status=status,
            message=shorten(msg, 1000)
        )
    )

def get_target_entities(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    excluded_entities: list[str]
) -> list[dict[str, Any]]:

    entities_source = []
    type = "entities"

    entity_view_config= config.data.target_query.target_view
    entity_view_id = entity_view_config.as_view_id()

    logger.debug(f"Get new entities from view: {entity_view_id}, based on config: {entity_view_config}")
    is_selected = get_query_filter(type, config.data.target_query, config.parameters.run_all, logger)

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
                reg_exp = str(rule[COL_KEY_RULE_REGEXP_TARGET])
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

# Get the pre-processed keys from the key table
def read_table(client: CogniteClient, db: str, tbl: str) -> list[str]:
    try:
        rows = client.raw.rows.list(db, [tbl]).to_pandas()
        return rows.index.tolist()
    except:
        raise Exception("Failed to retrieve the indexes from the key table")