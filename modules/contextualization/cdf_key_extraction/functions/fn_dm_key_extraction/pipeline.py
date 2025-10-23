from cognite.client import data_modeling as dm
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue
from logger import CogniteFunctionLogger

from typing import Any, Optional

from config import Config, SourceViewConfig

from ExtractionRuleManager import ExtractionRuleManager

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
    status = 'failure'
    pipeline_run_id = None
    keys_extracted = 0

    try:
        logger.info(
            f"Starting Key Extraction Function with loglevel = {data.get('logLevel', 'INFO')},  reading parameters from extraction pipeline config: {pipeline_ext_id}")

        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug(f"**** Write debug messages and only process one entity *****")

        logger.debug(f"Initiate RAW upload queue used to store ouput from key extraction")
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level="INFO")

        extraction_rule_manager = ExtractionRuleManager(config.data.extraction_rules, config.data.field_selection_strategy, logger)

        # TODO Check if extraction rule manager instantiation went well. Log number of rules found, the type of each, their sources, etc...
        if not extraction_rule_manager:
            logger.error("Failed to instantiate ExtractionRuleManager")
            return

        # Log extraction rule information
        extraction_rule_manager.get_info()

        # If we are not overwriting keys, we don't need to process the existing ones. but if there are new rules????
        if not config.parameters.overwrite:
            logger.debug(f"Reading existing entities in table {config.parameters.raw_table_key} in database {config.parameters.raw_db}")
            entities_to_exclude = read_table_keys(client, config.parameters.raw_db, config.parameters.raw_table_key)
            logger.debug(f"Persisting existing keys extracted for {len(entities_to_exclude)} entities")
        else:
            entities_to_exclude = []
            logger.debug("Removing old keys")
        
        logger.debug("Geting entities...")

        # TODO wrap this in performance benchmark
        entities_source_fields = get_target_entities(client, config, logger, entities_to_exclude)

        # Now that we have our entities and our extraction rule manager, we can begin the extraction
        # create an empty dictionary where the key is external id of an entity and the value is a list of keys extracted
        entities_keys_extracted = dict.fromkeys(entities_source_fields, {})

        # ================== PROCESS EXTRACTION RULES ====================
        # TODO we could wrap this in a performance benchmark or function
        for rule in extraction_rule_manager.get_sorted_rules():
            for entity in entities_source_fields.keys():
                try:
                    logger.verbose('INFO', f"Processing entity: {entity} with rule: {rule}")
                    field_keys = extraction_rule_manager.execute_rule(
                        rule,
                        entities_source_fields[entity],
                        entities_keys_extracted[entity]
                    )
                    
                    # We may want to replace this code with the code that adds a new row to the raw_uploader
                    for field_rule_name, keys in field_keys.items():
                        logger.verbose('DEBUG', f"Entity: {entity}, Field rule: {field_rule_name}, Keys: {keys}")
                        if field_rule_name in entities_keys_extracted[entity]:
                            # Extend the existing list of keys
                            entities_keys_extracted[entity][field_rule_name].extend(keys)
                        else:
                            # Add the new field_rule and its keys
                            entities_keys_extracted[entity][field_rule_name] = keys
                    
                        # TODO add keys extracted to some reporting service
                        keys_extracted += sum(len(keys))
                        logger.verbose('DEBUG', f"Total keys extracted so far: {keys_extracted}")
                except Exception as e:
                    logger.error(f"Error processing entity: {entity}, rule: {rule}, error: {e}")

            # TODO probably going to want some sort of staistic here for # of keys extracted, success rate, etc...
            logger.info(f"Finished processing entities for rule ID: {rule}")
        # ================================================================

        # TODO Based on selection strategy, take the highest confidence score, combine some stuff, who really knows...

        # Perhaps do a clean up step here

        # Check if the raw table for keys exists
        create_table(client, config.parameters.raw_db, config.parameters.raw_table_key)

        # check if the raw table for state exists
        create_table(client, config.parameters.raw_db, config.parameters.raw_table_state)

        # ===========================
        #| UPLOAD KEYS TO RAW TABLE |
        # ===========================
        # TODO wrap this in another function or performance benchmark
        for ext_id, field_rule_keys in entities_keys_extracted.items():
            if field_rule_keys:
                columns = {}
                for field_rule_name, keys in field_rule_keys.items():
                    columns[field_rule_name.upper()] = keys

                new_row = Row(key=ext_id, columns=columns)
                logger.verbose('DEBUG', f"Adding row to upload queue: {new_row}")

                raw_uploader.add_to_upload_queue(
                    database=config.parameters.raw_db,
                    table=config.parameters.raw_table_key,
                    raw_row=new_row
                )

        # maybe some diagnostics, logging, dashboard metrics here?

        # trigger the upload queue
        logger.debug(f'Uploading {raw_uploader.upload_queue_size} rows to RAW')
        try:
            raw_uploader.upload()
        except Exception as e:
            logger.error(f"Failed to upload rows to RAW, error: {e}")

        # TODO there will probably be a more detailed way to define a success/failure, but this will do for now
        if keys_extracted > 0 and raw_uploader.rows_written > 0:
            status = "success"

        # update the pipeline run
        pipeline_run_id = update_pipeline_run(
            client=client,
            logger=logger,
            xid=pipeline_ext_id,
            status=status,
            keys_extracted=keys_extracted
        )

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        pipeline_run_id = update_pipeline_run(client, logger, pipeline_ext_id, "failure", keys_extracted, msg)

    # TODO Add the metrics to some reporting service, where we track the metrics based on pipeline_run_id
    logger.info(f"Pipeline run ID: {pipeline_run_id}")

def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    keys_extracted: Optional[int],
    error: Optional[str] = None
) -> int:

    if status == "success":
        msg = (
            f"Succesfully extracted {keys_extracted} keys"
        )
        logger.info(msg)
    else:
        logger.error(msg)

    return client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(
            extpipe_external_id=xid,
            status=status,
            message=shorten(msg, 1000)
        )
    ).id

def get_target_entities(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    excluded_entities: list[str] # List of external ids to exclude in the query
) -> dict[str, dict[str, Any]]:

    if config.data.source_views == []:
        logger.error("No Source Views defined for key extraction")
        raise ValueError("No Source Views defined for key extraction")

    entities_source = {}
    type = "entities"

    entity_view_configs = config.data.source_views

    for entity_view_config in entity_view_configs:
        entity_view_id = entity_view_config.as_view_id()

        logger.debug(f"Building query filter...")
        is_selected = get_query_filter(type, entity_view_config, config.parameters.run_all, excluded_entities, logger)

        new_entities = []

        try:
            logger.debug(f"Get new entities from view: {entity_view_id}")
            new_entities = client.data_modeling.instances.list(
                instance_type="node",
                space=entity_view_config.instance_space,
                sources=[entity_view_id],
                filter=is_selected,
                limit=100
            )

            logger.debug(f"Listed {len(new_entities)} new entities from view: {entity_view_id}")

            item_updates = {}

            for entity in new_entities:
                entity_external_id = entity.external_id
                entity_props_dump = entity.dump()['properties'][entity_view_id.space][f'{entity_view_id.external_id}/{entity_view_id.version}']
                
                # Initialize entity fields dictionary if not exists
                if entity_external_id not in item_updates:
                    item_updates[entity_external_id] = {}
                
                # Collect all source fields from all extraction rules for this entity
                for rule in config.data.extraction_rules:
                    for source_field in rule.source_fields:
                        field_value = entity_props_dump.get(source_field.field_name, None)
                        
                        if field_value is None:
                            if source_field.required:
                                logger.verbose('WARNING', f"Missing required field '{source_field.field_name}' in entity: {entity_external_id}")
                        else:
                            # Add the field to the entity's field collection

                            if source_field.preprocessing:
                                logger.verbose('INFO', f"Applying preprocessing {source_field.preprocessing} to field '{source_field.field_name}' in entity: {entity_external_id}")
                                field_value = apply_preprocessing(field_value, source_field.preprocessing)

                            item_updates[entity_external_id][source_field.field_name] = field_value

            logger.info(f"Num new entities: {len(item_updates)} from view: {entity_view_id}")
            entities_source.update(item_updates)

        except Exception as e:
            logger.error(f"Error while extracting entities from view: {entity_view_id}, Error: {e!s}")

    return entities_source

def apply_preprocessing(field_value: str, preprocessing: list[str]) -> str:
    for task in preprocessing:
        match task:
            case 'trim':
                field_value = field_value.strip()
            case 'lowercase':
                field_value = field_value.lower()
            case 'uppercase':
                field_value = field_value.upper()
            case _:
                pass
    
    return field_value

def table_exists(client: CogniteClient, db_name: str, tb_name: str) -> bool:
    return db_name in client.raw.databases.list(-1) and tb_name in client.raw.tables.list(db_name, -1)

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
    is_selected = is_view
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

    if entity_config.filters and len(entity_config.filters) > 0:
        is_selected = dm.filters.And(is_selected, entity_config.build_filter())

    logger.debug(dbg_msg)

    return is_selected
