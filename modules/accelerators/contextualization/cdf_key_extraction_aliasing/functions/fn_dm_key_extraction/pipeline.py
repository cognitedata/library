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
from .services.ApplyService import GeneralApplyService

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

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_key={raw_table_key}"
            )

            # Initialize RAW upload queue
            raw_uploader = RawUploadQueue(
                cdf_client=client, max_queue_size=500000, trigger_log_level="INFO"
            )

            # Get entities from source view
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

            # Store entity metadata with candidate_keys list (for ApplyService and rule_id grouping)
            entity_metadata = {
                "keys": result.candidate_keys,
                "view_space": entity_fields.get("view_space"),
                "view_external_id": entity_fields.get("view_external_id"),
                "view_version": entity_fields.get("view_version"),
                "instance_space": entity_fields.get("instance_space"),
                "entity_type": entity_type,
                "resource_property": cdf_config.data.source_view.resource_property if cdf_config else None,
                "space": cdf_config.data.source_view.instance_space if cdf_config else entity_fields.get("instance_space"),
            }
            entities_keys_extracted[entity_id] = entity_metadata
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW if using CDF format (group by rule_id, one table per rule)
        if use_cdf_format:
            _create_table_if_not_exists(client, raw_db, raw_table_state, logger)
            results_by_rule = {}
            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", [])
                for key in field_keys:
                    rule_id = key.rule_id
                    if rule_id not in results_by_rule:
                        results_by_rule[rule_id] = {}
                    if ext_id not in results_by_rule[rule_id]:
                        results_by_rule[rule_id][ext_id] = {
                            "value": [key.value],
                            "extraction_type": key.extraction_type.value if hasattr(key.extraction_type, "value") else str(key.extraction_type),
                            "source_field": key.source_field,
                            "confidence": key.confidence,
                            "method": key.method.value if hasattr(key.method, "value") else str(key.method),
                            "metadata": key.metadata,
                            "resource_property": entity_metadata.get("resource_property"),
                        }
                    else:
                        results_by_rule[rule_id][ext_id]["value"].append(key.value)
            for rule_id, entities_for_rule in results_by_rule.items():
                rule_table_name = f"{raw_table_key}_{rule_id}"
                _create_table_if_not_exists(client, raw_db, rule_table_name, logger)
                for ext_id, rule_results in entities_for_rule.items():
                    new_row = Row(key=ext_id, columns=rule_results)
                    raw_uploader.add_to_upload_queue(
                        database=raw_db,
                        table=rule_table_name,
                        raw_row=new_row,
                    )
            logger.debug(f"Uploading {raw_uploader.upload_queue_size} rows to RAW")
            try:
                raw_uploader.upload()
                logger.info("Successfully uploaded keys to RAW")
            except Exception as e:
                logger.error(f"Failed to upload rows to RAW: {e}")
                raise

        # Apply keys to nodes if configured
        if use_cdf_format and cdf_config and cdf_config.parameters.apply:
            try:
                apply_service = GeneralApplyService(client, cdf_config, logger)
                apply_service.run()
            except Exception as e:
                logger.error(f"Apply service failed: {e}")

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
) -> Dict[str, Dict[str, Any]]:
    """
    Get entities from CDF data model views.

    Args:
        client: CogniteClient instance
        config: CDF Config object (with data.source_view or data.source_views)
        logger: Logger instance

    Returns:
        Dictionary mapping entity external IDs to their field values
    """
    entities_source = {}
    source_views = getattr(config.data, "source_views", None) or (
        [config.data.source_view] if getattr(config.data, "source_view", None) else []
    )
    if not source_views:
        logger.error("No Source View(s) defined for key extraction")
        raise ValueError("No Source View(s) defined for key extraction")

    raw_db = config.parameters.raw_db
    raw_table_key = config.parameters.raw_table_key
    overwrite = config.parameters.overwrite
    excluded_entities = []
    if not overwrite:
        excluded_entities = _read_table_keys(client, raw_db, raw_table_key)
        logger.debug(f"Excluding {len(excluded_entities)} existing entities")

    for entity_view_config in source_views:
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
                        source_fields = [source_fields] if source_fields else []
                    for source_field in source_fields:
                        field_name = getattr(source_field, "field_name", source_field)
                        field_value = entity_props.get(field_name)
                        if field_value is not None:
                            if hasattr(source_field, "preprocessing") and source_field.preprocessing:
                                field_value = _apply_preprocessing(
                                    str(field_value), source_field.preprocessing
                                )
                            entities_source[entity_external_id][
                                f"{rule.name}_{field_name}"
                            ] = field_value
                        elif getattr(source_field, "required", False):
                            logger.verbose(
                                "WARNING",
                                f"Missing required field '{field_name}' in entity: {entity_external_id}",
                            )

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
