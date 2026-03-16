"""
CDF Pipeline for Alias Persistence

This module provides the main pipeline function that reads aliasing results
and writes aliases back to source entities in the CDF data model.
"""

import logging
from typing import Any, Dict, Optional

try:
    from cognite.client import CogniteClient
    from cognite.client import data_modeling as dm
    from cognite.client.data_classes.data_modeling.ids import ViewId
    from cognite.client.data_classes.data_modeling.instances import (
        NodeApply,
        NodeOrEdgeData,
    )

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    dm = None
    NodeApply = None
    NodeOrEdgeData = None
    ViewId = None

from .common.logger import CogniteFunctionLogger

logger = None  # Use CogniteFunctionLogger directly


def persist_aliases_to_entities(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Main pipeline function for persisting aliases to CDF entities.

    This function reads aliasing results from workflow data and writes
    aliases back to source entities by updating entity properties.

    Args:
        client: CogniteClient instance (required)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing aliasing_results and entities_keys_extracted
    """
    try:
        logger.info("Starting Alias Persistence Pipeline")

        if not client:
            raise ValueError("CogniteClient is required for alias persistence")

        # Get aliasing results
        aliasing_results = data.get("aliasing_results", [])
        if not aliasing_results:
            logger.warning("No aliasing results found to persist")
            data["aliases_persisted"] = 0
            return

        logger.info(f"Found {len(aliasing_results)} aliasing results to persist")

        # Group aliases by entity using view information from aliasing results
        entity_aliases = {}
        aliases_persisted_count = 0

        for aliasing_result in aliasing_results:
            original_tag = aliasing_result.get("original_tag")
            aliases = aliasing_result.get("aliases", [])
            entity_info_list = aliasing_result.get("entities", [])

            if not original_tag or not aliases:
                continue

            # Use entity information from aliasing results (includes view info)
            for entity_info in entity_info_list:
                entity_id = entity_info.get("entity_id")
                view_space = entity_info.get("view_space")
                view_external_id = entity_info.get("view_external_id")
                view_version = entity_info.get("view_version")
                instance_space = entity_info.get("instance_space")
                field_name = entity_info.get("field_name")

                if (
                    not entity_id
                    or not view_space
                    or not view_external_id
                    or not view_version
                ):
                    logger.warning(
                        f"Missing view information for entity {entity_id}, skipping"
                    )
                    continue

                # Create entity key for grouping
                entity_key = (
                    f"{view_space}:{view_external_id}/{view_version}:{entity_id}"
                )

                if entity_key not in entity_aliases:
                    entity_aliases[entity_key] = {
                        "entity_id": entity_id,
                        "view_space": view_space,
                        "view_external_id": view_external_id,
                        "view_version": view_version,
                        "instance_space": instance_space,
                        "aliases": [],
                        "candidate_keys": [],
                        "field_names": set(),
                    }

                entity_aliases[entity_key]["aliases"].extend(aliases)
                entity_aliases[entity_key]["candidate_keys"].append(original_tag)
                entity_aliases[entity_key]["field_names"].add(field_name)
                aliases_persisted_count += len(aliases)

        logger.info(
            f"Prepared aliases for {len(entity_aliases)} entities "
            f"({aliases_persisted_count} total aliases)"
        )

        # Persist aliases to entities using CogniteDescribable view
        # Target: cdf_cdm CogniteDescribable/v1
        target_view_space = "cdf_cdm"
        target_view_external_id = "CogniteDescribable"
        target_view_version = "v1"

        # Verify imports are available
        if not CDF_AVAILABLE:
            raise ValueError("CogniteClient not available. Install cognite-sdk.")
        if ViewId is None or NodeApply is None or NodeOrEdgeData is None:
            raise ValueError(
                "CDF data modeling imports (ViewId, NodeApply, NodeOrEdgeData) not available"
            )

        target_view_id = ViewId(
            space=target_view_space,
            external_id=target_view_external_id,
            version=target_view_version,
        )

        logger.info(f"Targeting {target_view_id} view for alias persistence")

        persisted_count = 0
        failed_count = 0

        for entity_key, alias_data in entity_aliases.items():
            try:
                entity_id = alias_data["entity_id"]
                instance_space = alias_data.get("instance_space")
                aliases = list(set(alias_data["aliases"]))  # Deduplicate

                # Use instance_space from source entity, fallback to target view space if not available
                if not instance_space:
                    instance_space = target_view_space

                # Query the entity instance through CogniteDescribable view
                # Note: external_id is an instance property, not a view property
                # We need to query all instances and filter by external_id in Python,
                # or use a different approach
                logger.debug(
                    f"Querying entity {entity_id} from {target_view_id} view in space {instance_space}"
                )

                # Try to retrieve the instance directly by external_id
                # If the view doesn't have external_id as a property, we'll need to query differently
                try:
                    instances = client.data_modeling.instances.retrieve(
                        nodes=[entity_id],
                        space=instance_space,
                        sources=[target_view_id],
                    )
                    if not instances:
                        instances = []
                except Exception as retrieve_error:
                    logger.debug(
                        f"Direct retrieve failed for {entity_id}, trying list: {retrieve_error}"
                    )
                    # Fallback: list and filter by external_id in results
                    try:
                        all_instances = client.data_modeling.instances.list(
                            instance_type="node",
                            space=instance_space,
                            sources=[target_view_id],
                            limit=1000,  # Adjust limit as needed
                        )
                        instances = [
                            inst
                            for inst in all_instances
                            if getattr(inst, "external_id", None) == entity_id
                        ]
                    except Exception as list_error:
                        logger.warning(
                            f"Failed to query entity {entity_id}: {list_error}"
                        )
                        instances = []

                if not instances:
                    logger.warning(
                        f"Entity {entity_id} not found in {target_view_id} view, skipping"
                    )
                    failed_count += 1
                    continue

                # Prepare property update with aliases
                # Writing to CogniteDescribable view which should have an "aliases" property
                # Note: Only include properties that exist in the view schema
                properties_update = {
                    "aliases": aliases,
                }

                # Apply update using data modeling API
                logger.info(
                    f"Updating entity {entity_id} with {len(aliases)} aliases "
                    f"in {target_view_id} view"
                )

                # Build sources list - NodeOrEdgeData objects
                sources = [
                    NodeOrEdgeData(source=target_view_id, properties=properties_update)
                ]

                client.data_modeling.instances.apply(
                    nodes=[
                        NodeApply(
                            space=instance_space,
                            external_id=entity_id,
                            sources=sources,
                        )
                    ]
                )

                persisted_count += 1
                logger.debug(
                    f"Successfully persisted {len(aliases)} aliases for entity {entity_id} "
                    f"in {target_view_id}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to persist aliases for entity {entity_id}: {e}",
                    exc_info=True,
                )
                failed_count += 1
                continue

        # Store persistence results
        data["aliases_persisted"] = aliases_persisted_count
        data["entities_updated"] = persisted_count
        data["entities_failed"] = failed_count
        data["entity_aliases"] = {
            entity_key: {
                "entity_id": alias_data["entity_id"],
                "target_view_space": target_view_space,
                "target_view_external_id": target_view_external_id,
                "target_view_version": target_view_version,
                "source_view_space": alias_data.get("view_space"),
                "source_view_external_id": alias_data.get("view_external_id"),
                "source_view_version": alias_data.get("view_version"),
                "aliases": list(set(alias_data["aliases"])),  # Deduplicate
                "candidate_keys": alias_data["candidate_keys"],
                "field_names": list(alias_data["field_names"]),
            }
            for entity_key, alias_data in entity_aliases.items()
        }

        logger.info(
            f"Completed alias persistence: {persisted_count} entities updated, "
            f"{failed_count} entities failed, "
            f"{aliases_persisted_count} aliases persisted"
        )

    except Exception as e:
        message = f"Alias persistence pipeline failed: {e!s}"
        logger.error(message)
        raise
