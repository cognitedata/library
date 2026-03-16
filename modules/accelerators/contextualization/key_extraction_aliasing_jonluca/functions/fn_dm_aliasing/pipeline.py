"""
CDF Pipeline for Tag Aliasing

This module provides the main pipeline function that processes tags
and generates aliases using the AliasingEngine.
"""

import logging
from typing import Any, Dict, List, Optional

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .common.logger import CogniteFunctionLogger
from .engine.tag_aliasing_engine import AliasingEngine, AliasingResult

logger = None  # Use CogniteFunctionLogger directly


def tag_aliasing(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
    engine: AliasingEngine,
) -> None:
    """
    Main pipeline function for tag aliasing in CDF format.

    This function processes tags and entities, generates aliases using
    the AliasingEngine, and optionally uploads results to CDF.

    Args:
        client: CogniteClient instance (optional)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing tags, entities, and results
        engine: Initialized AliasingEngine instance
    """
    try:
        logger.info("Starting Tag Aliasing Pipeline")

        # Get tags to process
        tags = data.get("tags", [])
        entities = data.get("entities", [])

        # If called from workflow after key extraction, extract tags from key extraction results
        if not tags and not entities:
            entities_keys_extracted = data.get("entities_keys_extracted", {})
            if entities_keys_extracted:
                logger.info(
                    f"Extracting tags from key extraction results: {len(entities_keys_extracted)} entities"
                )
                # Extract all candidate keys as tags for aliasing
                # Build mapping of tag -> entity info for later use
                tag_to_entity_map = {}
                tags = []

                for entity_id, entity_metadata in entities_keys_extracted.items():
                    keys_by_field = entity_metadata.get("keys", {})
                    view_space = entity_metadata.get("view_space")
                    view_external_id = entity_metadata.get("view_external_id")
                    view_version = entity_metadata.get("view_version")
                    instance_space = entity_metadata.get("instance_space")
                    entity_type = entity_metadata.get("entity_type")

                    # Collect all candidate key values as potential tags
                    for field_name, key_values in keys_by_field.items():
                        for key_value, key_info in key_values.items():
                            # Only process candidate keys (not foreign keys or document references)
                            extraction_type = (
                                key_info.get("extraction_type")
                                if isinstance(key_info, dict)
                                else None
                            )
                            if (
                                extraction_type == "candidate_key"
                                or extraction_type is None
                            ):
                                if key_value not in tags:
                                    tags.append(key_value)
                                    tag_to_entity_map[key_value] = []
                                # Store entity info for this tag
                                tag_to_entity_map[key_value].append(
                                    {
                                        "entity_id": entity_id,
                                        "field_name": field_name,
                                        "view_space": view_space,
                                        "view_external_id": view_external_id,
                                        "view_version": view_version,
                                        "instance_space": instance_space,
                                        "entity_type": entity_type,
                                    }
                                )

                logger.info(
                    f"Extracted {len(tags)} unique candidate key tags from key extraction results"
                )
                # Store tag mapping for alias persistence
                data["_tag_to_entity_map"] = tag_to_entity_map

        # If entities are provided, extract tags from them
        if entities and not tags:
            tags = [
                entity.get("tag") or entity.get("name") or entity.get("externalId")
                for entity in entities
                if entity.get("tag") or entity.get("name") or entity.get("externalId")
            ]

        if not tags:
            logger.warning("No tags provided for aliasing")
            data["aliasing_results"] = []
            return

        logger.info(f"Processing {len(tags)} tags for aliasing")

        # Process each tag
        aliasing_results = []
        tag_to_entity_map = data.get("_tag_to_entity_map", {})

        for i, tag in enumerate(tags):
            # Get context from corresponding entity if available
            context = None
            entity_type = None
            entity_info_list = tag_to_entity_map.get(tag, [])

            if entities and i < len(entities):
                entity = entities[i]
                entity_type = entity.get("entity_type")
                context = entity.get("context", {})
                # Also include entity metadata as context
                if "metadata" in entity:
                    context.update(entity["metadata"])
            elif entity_info_list:
                # Use first entity info for context
                first_entity_info = entity_info_list[0]
                entity_type = first_entity_info.get("entity_type")

            # Generate aliases
            result = engine.generate_aliases(
                tag=tag, entity_type=entity_type, context=context
            )

            # Build result with entity mapping information
            aliasing_result = {
                "original_tag": result.original_tag,
                "aliases": result.aliases,
                "metadata": result.metadata,
            }

            # Include entity information for persistence
            if entity_info_list:
                aliasing_result["entities"] = entity_info_list

            aliasing_results.append(aliasing_result)

            logger.debug(f"Generated {len(result.aliases)} aliases for tag '{tag}'")

        # Store results
        data["aliasing_results"] = aliasing_results
        data["total_tags_processed"] = len(tags)
        data["total_aliases_generated"] = sum(
            len(r["aliases"]) for r in aliasing_results
        )

        logger.info(
            f"Completed aliasing: {len(tags)} tags processed, "
            f"{data['total_aliases_generated']} total aliases generated"
        )

        # Optionally upload to CDF RAW if client is provided
        if client and data.get("upload_to_raw", False):
            raw_db = data.get("raw_db", "aliasing_db")
            raw_table = data.get("raw_table", "aliases")

            _upload_aliases_to_raw(client, raw_db, raw_table, aliasing_results, logger)

    except Exception as e:
        message = f"Aliasing pipeline failed: {e!s}"
        logger.error(message)
        raise


def _upload_aliases_to_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table: str,
    aliasing_results: List[Dict[str, Any]],
    logger: Any,
) -> None:
    """Upload aliasing results to CDF RAW."""
    try:
        from cognite.client.data_classes import Row
        from cognite.extractorutils.uploader import RawUploadQueue

        # Ensure tables exist
        _create_table_if_not_exists(client, raw_db, raw_table, logger)

        # Create upload queue
        raw_uploader = RawUploadQueue(
            cdf_client=client, max_queue_size=50000, trigger_log_level="INFO"
        )

        # Add rows
        for result in aliasing_results:
            row = Row(
                key=result["original_tag"],
                columns={
                    "aliases": result["aliases"],
                    "total_aliases": len(result["aliases"]),
                    "metadata": str(result["metadata"]),
                },
            )
            raw_uploader.add_to_upload_queue(
                database=raw_db, table=raw_table, raw_row=row
            )

        # Upload
        logger.debug(f"Uploading {raw_uploader.upload_queue_size} rows to RAW")
        raw_uploader.upload()
        logger.info("Successfully uploaded aliasing results to RAW")

    except Exception as e:
        logger.error(f"Failed to upload aliases to RAW: {e}")
        raise


from .common.cdf_utils import create_table_if_not_exists as _create_table_if_not_exists
