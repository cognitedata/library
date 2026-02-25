"""
CDF Pipeline for Key Extraction

This module provides the main pipeline function that processes entities
from CDF data model views and extracts keys using the KeyExtractionEngine.
"""

import logging
from typing import Any, Dict, Optional

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import RawUploadQueue

from .common.logger import CogniteFunctionLogger
from .engine.key_extraction_engine import ExtractionResult, KeyExtractionEngine

logger = None  # Use CogniteFunctionLogger directly


def key_extraction(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
    engine: KeyExtractionEngine,
    cdf_config: Any = None,
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
            run_all = cdf_config.parameters.run_all
            debug = cdf_config.parameters.debug

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_key={raw_table_key}"
            )

            # Initialize RAW upload queue
            raw_uploader = RawUploadQueue(
                cdf_client=client, max_queue_size=500000, trigger_log_level="INFO"
            )

            # Get entities from source views
            entities_source_fields = _get_target_entities_cdf(
                client, cdf_config, logger, overwrite, raw_db, raw_table_key
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

            # Store results organized by field_name with extraction type metadata
            keys = {}
            for key in result.candidate_keys:
                field_name = key.source_field
                if field_name not in keys:
                    keys[field_name] = {}
                keys[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": key.extraction_type.value,
                }

            # Store entity metadata including view information
            entity_metadata = {
                "keys": keys,
                "view_space": entity_fields.get("view_space"),
                "view_external_id": entity_fields.get("view_external_id"),
                "view_version": entity_fields.get("view_version"),
                "instance_space": entity_fields.get("instance_space"),
                "entity_type": entity_type,
            }
            entities_keys_extracted[entity_id] = entity_metadata
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW if using CDF format
        if use_cdf_format:
            # Ensure tables exist
            _create_table_if_not_exists(client, raw_db, raw_table_key, logger)
            _create_table_if_not_exists(client, raw_db, raw_table_state, logger)

            # Upload keys organized by field_name
            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", {})
                if field_keys:
                    columns = {}
                    for field_name, keys_dict in field_keys.items():
                        # Convert dict of {key: {confidence, extraction_type}} to list format expected by RAW
                        # Column names use field_name (e.g., "NAME", "DESCRIPTION")
                        columns[field_name.upper()] = list(keys_dict.keys())

                    new_row = Row(key=ext_id, columns=columns)
                    raw_uploader.add_to_upload_queue(
                        database=raw_db, table=raw_table_key, raw_row=new_row
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
    config: Any,
    logger: Any,
    overwrite: bool,
    raw_db: str,
    raw_table_key: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Get entities from CDF data model views.

    Args:
        client: CogniteClient instance
        config: CDF Config object
        logger: Logger instance
        overwrite: Whether to overwrite existing keys
        raw_db: RAW database name
        raw_table_key: RAW table name for keys

    Returns:
        Dictionary mapping entity external IDs to their field values
    """
    entities_source = {}

    if not config.data.source_views:
        logger.error("No Source Views defined for key extraction")
        raise ValueError("No Source Views defined for key extraction")

    excluded_entities = []
    if not overwrite:
        excluded_entities = _read_table_keys(client, raw_db, raw_table_key)
        logger.debug(f"Excluding {len(excluded_entities)} existing entities")

    for entity_view_config in config.data.source_views:
        entity_view_id = entity_view_config.as_view_id()

        logger.debug(f"Querying view: {entity_view_id}")

        # Build filter
        filter_expr = _build_filter(
            entity_view_config, config.parameters.run_all, excluded_entities, logger
        )

        try:
            # Query instances
            instances = client.data_modeling.instances.list(
                instance_type="node",
                space=entity_view_config.instance_space,
                sources=[entity_view_id],
                filter=filter_expr,
                limit=entity_view_config.batch_size,
            )

            logger.debug(
                f"Retrieved {len(instances)} instances from view: {entity_view_id}"
            )

            # Extract field values
            for instance in instances:
                entity_external_id = instance.external_id
                entity_props = (
                    instance.dump()
                    .get("properties", {})
                    .get(entity_view_id.space, {})
                    .get(f"{entity_view_id.external_id}/{entity_view_id.version}", {})
                )

                # Initialize entity fields
                if entity_external_id not in entities_source:
                    entities_source[entity_external_id] = {
                        "entity_type": entity_view_config.entity_type.value,
                        "view_space": entity_view_id.space,
                        "view_external_id": entity_view_id.external_id,
                        "view_version": entity_view_id.version,
                        "instance_space": entity_view_config.instance_space,
                    }

                # Collect source fields from extraction rules
                for rule in config.data.extraction_rules:
                    source_fields = rule.source_fields
                    if not isinstance(source_fields, list):
                        source_fields = [source_fields]

                    for source_field in source_fields:
                        field_name = source_field.field_name
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

                            entities_source[entity_external_id][
                                field_name
                            ] = field_value

            logger.info(
                f"Processed {len(entities_source)} entities from view: {entity_view_id}"
            )

        except Exception as e:
            logger.error(f"Error querying view {entity_view_id}: {e}")
            raise

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
