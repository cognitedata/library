"""
CDF Pipeline for Key Extraction

This module provides the main pipeline function that processes entities
from CDF data model views and extracts keys using the KeyExtractionEngine.
"""

import time
import json
from typing import Any, Dict, Optional
from .config import Config
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import RawUploadQueue

from modules.contextualization.key_extraction_aliasing.functions.fn_dm_key_extraction.services.ApplyService import GeneralApplyService
from .engine.key_extraction_engine import KeyExtractionEngine

logger = None  # Use CogniteFunctionLogger directly


def key_extraction(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
    engine: KeyExtractionEngine,
    cdf_config: Config = None,
) -> None:
    """
    Main pipeline function for key extraction in CDF format.

    This function processes entities from CDF data model views, extracts keys
    using the KeyExtractionEngine, and uploads results to RAW tables.

    Args:
        client: CogniteClient instance (optional if not using CDF)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing pipeline parameters and results
        engine: Initialized KeyExtractionEngine instance
        cdf_config: Optional CDF Config object (if using CDF format)
    """
    pipeline_ext_id = data.get("ExtractionPipelineExtId", "unknown")
    status = "failure"
    pipeline_run_id = None
    keys_extracted = 0

    try:
        logger.info(f"Starting Key Extraction Pipeline: {pipeline_ext_id}")

        # Determine if using CDF format or standalone
        use_cdf_format = cdf_config is not None and client is not None

        if use_cdf_format:
            # Extract parameters from CDF config
            raw_db = cdf_config.parameters.raw_db
            raw_table_key = cdf_config.parameters.raw_table_key
            raw_table_state = cdf_config.parameters.raw_table_state
            overwrite = cdf_config.parameters.overwrite

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_key={raw_table_key}"
            )

            # Initialize RAW upload queue
            raw_uploader = RawUploadQueue(
                cdf_client=client, max_queue_size=500000, trigger_log_level="INFO"
            )

            # Get entities from source views
            entities_source_fields = _get_target_entities_cdf(
                client, cdf_config, logger
            )

        else:
            # Standalone mode - entities should be provided in data
            entities_source_fields = data.get("entities", {})
            logger.info(
                f"Using standalone mode with {len(entities_source_fields)} entities"
            )

        # Process entities with extraction engine
        entities_keys_extracted = {}

        for entity_id, entity_fields in entities_source_fields.items():
            # Convert entity fields to format expected by engine
            entity = {"id": entity_id, "externalId": entity_id, **entity_fields}
            entity_type = entity_fields.get("entity_type", "asset")

            # Extract keys
            result = engine.extract_keys(entity, entity_type)

            # Store entity metadata including view information
            entity_metadata = {
                "keys": result.candidate_keys,
                "view_space": entity_fields.get("view_space"),
                "view_external_id": entity_fields.get("view_external_id"),
                "view_version": entity_fields.get("view_version"),
                "instance_space": entity_fields.get("instance_space"),
                "entity_type": entity_type,
                "resource_property": cdf_config.data.source_view.resource_property,
                "space": cdf_config.data.source_view.instance_space
            }
            entities_keys_extracted[entity_id] = entity_metadata
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW if using CDF format
        if use_cdf_format:
            # Ensure state table exists
            _create_table_if_not_exists(client, raw_db, raw_table_state, logger)

            # Group results by rule_id
            results_by_rule = {}
            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", [])
                for key in field_keys:
                    rule_id = key.rule_id
                    if rule_id not in results_by_rule:
                        results_by_rule[rule_id] = {}
                    
                    # if ext_id not in results_by_rule[rule_id]:
                    #     results_by_rule[rule_id][ext_id] = []
                    
                    if ext_id not in results_by_rule[rule_id]:
                        results_by_rule[rule_id][ext_id] = {
                            'value': [key.value],
                            'extraction_type': key.extraction_type.value if hasattr(key.extraction_type, 'value') else str(key.extraction_type),
                            'source_field': key.source_field,
                            'confidence': key.confidence,
                            'method': key.method.value if hasattr(key.method, 'value') else str(key.method),
                            'metadata': key.metadata,
                            'resource_property': entity_metadata.get("resource_property")
                        }
                    else:
                        results_by_rule[rule_id][ext_id]['value'].extend([key.value])

            # Upload to rule-specific tables
            for rule_id, entities_for_rule in results_by_rule.items():
                rule_table_name = f"{raw_table_key}_{rule_id}"
                _create_table_if_not_exists(client, raw_db, rule_table_name, logger)
                
                for ext_id, rule_results in entities_for_rule.items():
                    new_row = Row(key=ext_id, columns=rule_results)
                    raw_uploader.add_to_upload_queue(
                        database=raw_db, table=rule_table_name, raw_row=new_row
                    )

            logger.debug(f"Uploading {raw_uploader.upload_queue_size} rows to RAW")
            try:
                raw_uploader.upload()
                logger.info("Successfully uploaded keys to RAW")
            except Exception as e:
                logger.error(f"Failed to upload rows to RAW: {e}")
                raise

        # Update pipeline run status
        if use_cdf_format and client:
            from cognite.client.data_classes import ExtractionPipelineRun
            from cognite.client.utils._text import shorten

            if keys_extracted > 0:
                status = "success"
                message = f"Successfully extracted {keys_extracted} keys"
            else:
                status = "failure"
                message = "No keys were extracted and no keys were uploaded"

            try:
                pipeline_run = client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=pipeline_ext_id,
                        status=status,
                        message=shorten(message, 1000),
                    )
                )
                pipeline_run_id = pipeline_run.id
                logger.info(f"Pipeline run ID: {pipeline_run_id}")
            except Exception as e:
                logger.warning(f"Failed to create pipeline run: {e}")

        # Apply new keys to nodes if so desired
        # TODO handle the return result of apply_service.run() a little more elegantly 
        if cdf_config.parameters.apply:
            apply_service = GeneralApplyService(client, cdf_config, logger)
            apply_service.run()


        # Store results in data dict for return
        data["keys_extracted"] = keys_extracted
        data["entities_keys_extracted"] = entities_keys_extracted
        data["status"] = status
        data["pipeline_run_id"] = pipeline_run_id

    except Exception as e:
        message = f"Pipeline failed: {e!s}"
        logger.error(message)

        # Update pipeline run with failure
        if use_cdf_format and client and pipeline_ext_id:
            try:
                from cognite.client.data_classes import ExtractionPipelineRun
                from cognite.client.utils._text import shorten

                client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=pipeline_ext_id,
                        status="failure",
                        message=shorten(message, 1000),
                    )
                )
            except Exception:
                pass

        raise


def _get_target_entities_cdf(
    client: CogniteClient,
    config: Config,
    logger: Any
) -> Dict[str, Dict[str, Any]]:
    """
    Get entities from CDF data model views.

    Args:
        client: CogniteClient instance
        config: CDF Config object
        logger: Logger instance

    Returns:
        Dictionary mapping entity external IDs to their field values
    """
    entities_source = {}
    if not config.data.source_view:
        logger.error("No Source View defined for key extraction")
        raise ValueError("No Source View defined for key extraction")
    
    raw_db, raw_table_key, overwrite = config.parameters.raw_db, config.parameters.raw_table_key, config.parameters.overwrite

    excluded_entities = []
    if not overwrite:
        excluded_entities = _read_table_keys(client, raw_db, raw_table_key)
        logger.debug(f"Excluding {len(excluded_entities)} existing entities")

    entity_view_config = config.data.source_view
    entity_view_id = entity_view_config.as_view_id()

    logger.debug(f"Querying view: {entity_view_id}")

    # Build filter
    filter_expr = _build_filter(
        entity_view_config, config.parameters.run_all, excluded_entities, logger
    )

# try:
    # Query instances - I know this while loop is horrendous but we gotta do what we gotta do
    instances = None
    while(instances == None):
        try:
            instances = client.data_modeling.instances.list(
                instance_type="node",
                space=entity_view_config.instance_space,
                sources=[entity_view_id],
                filter=filter_expr,
                limit=None,
            )

            logger.debug(
                f"Retrieved {len(instances)} instances from view: {entity_view_id}"
            )
        except:
            continue

        time.sleep(2)

    # make a df that is the external ids of the source instances
    node_df = instances.to_pandas(expand_properties=True)

    source_table_joined_data = node_df.copy()

    # Here's where we yoink the tables from RAW
    raw_tables_data = {}
    for source_table in config.data.source_tables or []:
        try:
            table_rows = client.raw.rows.retrieve_dataframe(
                db_name=source_table.database_name, 
                table_name=source_table.table_name,
                limit=None,
                partitions=client.config.max_workers,
                columns=source_table.columns
            )

            # Drop rows with invalid join field values
            join_field = source_table.join_fields.get('table_field') if source_table.join_fields else None
            if join_field and join_field in table_rows.columns:
                valid_rows = table_rows[join_field].notnull() & (table_rows[join_field] != "")
                invalid_count = len(table_rows) - valid_rows.sum()
                if invalid_count > 0:
                    logger.verbose(
                        "WARNING",
                        f"Dropping {invalid_count} rows from table '{source_table.table_name}' due to invalid join field '{join_field}'",
                    )
                table_rows = table_rows[valid_rows]
                # Reset index to join field for efficient lookup
                # table_rows = table_rows.set_index(join_field, drop=False)
            else:
                logger.verbose(
                    "WARNING",
                    f"Join field '{join_field}' not found in table '{source_table.table_name}'. No rows dropped.",
                )


            table_rows = table_rows.rename(columns={'primary_key': 'primary_key_col'})
            raw_tables_data[source_table.table_id] = table_rows
            logger.debug(
                f"Retrieved {len(table_rows)} rows from RAW table: {source_table.table_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to retrieve RAW table '{source_table.table_name}': {e}"
            )
            raw_tables_data[source_table.table_id] = None

        # make a copy of that dataframe we made earlier for the node externalIds, and take the join field from there too
        # join_instance_df = node_df[source_table.join_fields.get('view_field')] #].to_frame(name=source_table.join_fields.get('view_field'))

        # Now do a left join of the source table df onto the join_instance_df using view_field == table_field as the condition
        # node_df.to_csv('bingus3.csv')
        # source_table_joined_data.to_csv('bingus1.csv')
        # raw_tables_data[source_table.table_id].to_csv('bingus4.csv')
        instance_table_data_df = pd.merge(node_df, raw_tables_data[source_table.table_id], left_on=source_table.join_fields.get('view_field'), right_on=source_table.join_fields.get('table_field'), how='left')

        # instance_table_data_df.to_csv('bingus2.csv')
        cols_to_keep = instance_table_data_df.columns.difference(source_table_joined_data.columns).to_list()
        cols_to_keep.append('external_id')
        # source_table_joined_data = pd.concat([instance_table_data_df, source_table_joined_data], axis=1).reset_index(drop=True).reindex(columns=['external_id'])
        source_table_joined_data = pd.merge(source_table_joined_data, instance_table_data_df[cols_to_keep], on='external_id', how='left')
        # Now that we have a dataframe with the columns of the source table we need on the row(s) with the keys their respective instance externalIds,
        # We can then dump that dataframe into the raw_tables_data under the 'source_table_name' key as a dictionary, with externalId as the key

        # each row will be the externalId of the instance, and each column will be named the source table it is refrencing, and it will contian a dictionary of the rows within the table that matched the join  condition

        # Later, we will access each good row 
    
    def process_instance(instance: Any) -> Optional[Dict[str, Any]]:
        """
        Process a single instance to extract entity data and fields.
        Accesses raw_tables_data, entity_view_id, entity_view_config, config, and logger from outer scope.
        """
        try:
            entity_external_id = instance.external_id
            entity_props = (
                instance.dump()
                .get("properties", {})
                .get(entity_view_id.space, {})
                .get(f"{entity_view_id.external_id}/{entity_view_id.version}", {})
            )

            # Initialize entity fields
            entity_data = {
                "entity_type": entity_view_config.entity_type.value,
                "view_space": entity_view_id.space,
                "view_external_id": entity_view_id.external_id,
                "view_version": entity_view_id.version,
                "instance_space": entity_view_config.instance_space,
                "table_data": dict()
            }

            # Collect source fields from extraction rules
            for rule in config.data.extraction_rules:
                source_fields = rule.source_fields
                if not isinstance(source_fields, list):
                    source_fields = [source_fields]

                for source_field in source_fields:
                    field_name = source_field.field_name
                    
                    if source_field.table_id:
                        field_name = '_'.join([source_field.table_id, source_field.field_name])
                        #table_data_entry =  # entity_data.get('table_data', {}).get(source_field.field_name, {})
                        field_value = source_table_joined_data.at[entity_external_id, source_field.field_name]
                    else:
                        field_value = entity_props.get(field_name)
                    
                    if field_value is None:
                        if source_field.required:
                            logger.verbose(
                                "WARNING",
                                f"Missing required field '{field_name}' in entity: {entity_external_id}",
                            )
                    else:
                        # Apply preprocessing
                        if (
                            hasattr(source_field, "preprocessing")
                            and source_field.preprocessing
                        ):
                            field_value = _apply_preprocessing(
                                field_value, source_field.preprocessing
                            )

                        # View source fields are stored by 'rule_name'_'field_name'
                        # Table source fields are stored by 'rule_name'_'table_id'_'field_name'
                        if source_field.table_id:
                            entity_data['table_data'][
                                '_'.join([rule.name, field_name])
                            ] = field_value
                        else:
                            entity_data[
                                '_'.join([rule.name, field_name])
                            ] = field_value

            return {
                "external_id": entity_external_id,
                "data": entity_data
            }
            
        except Exception as e:
            logger.error(f"Error processing instance: {e}")
            traceback.print_exc()
            return None

    # Extract field values in parallel using nested function with shared data access
    logger.debug(f"Processing {len(instances)} instances in parallel...")
    
    source_table_joined_data.set_index('external_id', drop=False, inplace=True)

    with ThreadPoolExecutor(max_workers=min(32, len(instances))) as executor:
        # Submit all instances for processing - raw_tables_data is accessed via closure, not passed
        future_to_instance = {
            executor.submit(process_instance, instance): instance 
            for instance in instances
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_instance):
            try:
                entity_data = future.result()
                if entity_data:
                    entity_external_id = entity_data["external_id"]
                    entities_source[entity_external_id] = entity_data["data"]
            except Exception as e:
                instance = future_to_instance[future]
                logger.error(f"Error processing instance {instance.external_id}: {e}")
                traceback.print_exc()

    logger.info(
        f"Processed {len(entities_source)} entities from view: {entity_view_id}"
    )

    # except Exception as e:
    #     logger.error(f"Error querying view {entity_view_id}: {e}")
    #     traceback.print_exc()
    #     raise

    return entities_source


def _build_filter(
    entity_config: Any, run_all: bool, excluded_entities: list[str], logger: Any
) -> dm.filters.Filter:
    """Build filter expression for querying entities."""
    entity_view_id = entity_config.as_view_id()
    is_view = dm.filters.HasData(views=[entity_view_id])
    is_selected = is_view

    # Exclude already processed entities
    if excluded_entities:
        is_excluded = dm.filters.Not(
            dm.filters.In(
                entity_view_id.as_property_ref("external_id"), excluded_entities
            )
        )
        is_selected = dm.filters.And(is_selected, is_excluded)

    # Apply custom filters
    if entity_config.filters and len(entity_config.filters) > 0:
        is_selected = dm.filters.And(is_selected, entity_config.build_filter())

    return is_selected


def _apply_preprocessing(field_value: str, preprocessing: list[str]) -> str:
    """Apply preprocessing steps to field value."""
    for task in preprocessing:
        if isinstance(field_value, str):
            if task.lower() == "trim":
                field_value = field_value.strip()
            elif task.lower() == "lowercase":
                field_value = field_value.lower()
            elif task.lower() == "uppercase" or task.lower() == "upper":
                field_value = field_value.upper()

    return field_value


from .common.cdf_utils import create_table_if_not_exists as _create_table_if_not_exists


def _read_table_keys(client: CogniteClient, db: str, tbl: str) -> list[str]:
    """Read existing entity keys from RAW table."""
    try:
        rows = client.raw.rows.list(db, [tbl]).to_pandas()
        return rows.index.tolist() if not rows.empty else []
    except Exception:
        return []
