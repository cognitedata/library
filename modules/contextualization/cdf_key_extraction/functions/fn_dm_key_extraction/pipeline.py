from cognite.client import data_modeling as dm
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue
from logger import CogniteFunctionLogger

from typing import Any, Optional

from config import Config, SourceViewConfig

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
        entities_to_exclude = read_table_keys(client, config.parameters.raw_db, config.parameters.raw_table_key)

        logger.debug("Get entities (ex: time series)")

        # TODO wrap this in performance benchmark
        entities = get_target_entities(client, config, logger, entities_to_exclude)

        entities_to_exclude = []

        # Delete pre-existing keys made in previous runs
        if config.parameters.remove_old_keys:
            logger.debug(f"Run all entities, delete state content in RAW since we are rerunning based on all input")
            # TODO Get the entities that we are going to extract keys from, delete all rows with those keys (externalId)
            # delete_table(client, config.parameters.raw_db, config.parameters.raw_table_key)
            # delete_table(client, config.parameters.raw_db, config.parameters.raw_table_state)

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
    excluded_entities: list[str] # List of external ids to exclude in the query
) -> list[dict[str, Any]]:

    entities_source = []
    type = "entities"

    entity_view_config= config.data.target_query.target_view
    entity_view_id = entity_view_config.as_view_id()

    logger.debug(f"Get new entities from view: {entity_view_id}, based on config: {entity_view_config}")
    is_selected = get_query_filter(type, config.data.target_query, config.parameters.run_all, excluded_entities, ogger)

    new_entities = client.data_modeling.instances.list(
        space=entity_view_config.instance_space,
        sources=[entity_view_id],
        filter=is_selected,
        limit=-1
    )

    item_updates = []

    for entity in new_entities:
        for rule in config.data.extraction_rules:
            new_item = [{"external_id": entity.external_id}]
            entity_dump = entity.dump()
            for source_field in rule.method_parameters.source_fields:
                if entity_dump.get(source_field.name, None) is None:
                    if source_field.required:
                        logger.error(f"Missing required field '{source_field.name}' in entity: {entity_dump}")
                        continue
                else:
                    new_item[source_field.name] = entity_dump[str(source_field.name)]

            item_updates.append(new_item)

    logger.info(f"Num new entities: {len(item_updates)} from view: {entity_view_id}")

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
def read_table_keys(client: CogniteClient, db: str, tbl: str) -> list[str]:
    try:
        rows = client.raw.rows.list(db, [tbl]).to_pandas()
        return rows[rows['keys'].notna()].index.tolist()
    except:
        raise Exception("Failed to retrieve the indexes from the key table")
    
def get_query_filter(
    type: str,
    entity_config: SourceViewConfig,
    run_all: bool,
    excluded_entities: list[str],
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:
    entity_view_id = entity_config.as_view_id()
    is_view = dm.filters.HasData(views=[entity_view_id])
    is_selected = dm.filters.And(is_view)
    dbg_msg = f"For for view: {entity_view_id} - Entity filter: HasData = True"

    # Check if the view entity already is matched or not
    # if type == "entities" and not run_all:
    #     is_matched = dm.filters.Exists(entity_view_id.as_property_ref("assets"))
    #     not_matched = dm.filters.Not(is_matched)
    #     is_selected = dm.filters.And(is_selected, not_matched)
    #     dbg_msg = f"{dbg_msg} Entity filtering on: 'assets' - NOT EXISTS"

    # if view_config.filter_property and view_config.filter_values:
    #     is_filter_param = dm.filters.In(
    #         view_config.as_property_ref(
    #             view_config.filter_property),
    #             view_config.filter_values
    #     )
    #     is_selected = dm.filters.And(is_selected, is_filter_param)
    #     dbg_msg = f"{dbg_msg} Entity filtering on: '{view_config.filter_values}' IN: '{view_config.filter_property}'"

    if excluded_entities != []:
        is_excluded = dm.filters.Not(
            dm.filters.In(
                entity_view_id.as_property_ref("external_id"),
                excluded_entities
            )
        )

        is_selected = dm.filters.And(is_selected, is_excluded)
        dbg_msg = f"{dbg_msg} Entity filtering on: '{len(excluded_entities)}' entities IN: 'external_id'"

    if len(entity_config.filters) > 0:
        is_selected = dm.filters.And(is_selected, entity_config.build_filter())

    logger.debug(dbg_msg)

    return is_selected
