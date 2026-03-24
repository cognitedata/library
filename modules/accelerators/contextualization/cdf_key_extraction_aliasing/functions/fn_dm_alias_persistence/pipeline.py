"""
CDF Pipeline for Alias Persistence

This module provides the main pipeline function that reads aliasing results
and writes aliases back to source entities in the CDF data model.
"""

import logging
import json
from typing import Any, Dict, List, Optional, Tuple

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes.data_modeling.ids import ViewId
    from cognite.client.data_classes.data_modeling.instances import (
        NodeApply,
        NodeOrEdgeData,
    )

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    NodeApply = None
    NodeOrEdgeData = None
    ViewId = None

from common.logger import CogniteFunctionLogger

logger = None  # Use CogniteFunctionLogger directly


def _load_aliasing_results_from_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_aliases: str,
    logger: Any,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Read aliasing results from RAW.

    Expected schema (written by Key-Aliasing):
      - key: original_tag
      - columns.aliases: list[str]
      - columns.metadata_json: json string
      - columns.entities_json: json string (list of entity mappings) (optional)
    """
    try:
        rows = client.raw.rows.list(raw_db, raw_table_aliases, limit=limit)
    except Exception as e:
        logger.error(f"Failed reading RAW rows db={raw_db} table={raw_table_aliases}: {e}")
        raise

    def _normalize_aliases(value: Any) -> List[str]:
        """Normalize RAW row `aliases` column into a list of strings."""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None and str(v) != ""]
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return []
            # Fusion RAW UI sometimes renders list columns as "List: a,b,c"
            if s.lower().startswith("list:"):
                s = s.split(":", 1)[1].strip()
            # Try JSON first (preferred)
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [
                        str(v)
                        for v in parsed
                        if v is not None and isinstance(v, (str, int, float, bool))
                    ]
            except Exception:
                pass
            # Try YAML as a fallback (covers "['a','b']" representations)
            try:
                import yaml  # local import to keep deps optional

                parsed = yaml.safe_load(s)
                if isinstance(parsed, list):
                    return [str(v) for v in parsed if v is not None and str(v) != ""]
            except Exception:
                pass
            # Comma-separated fallback (covers "a,b,c" and "List: a,b,c")
            if "," in s:
                parts = [p.strip() for p in s.split(",")]
                return [p for p in parts if p]
            # Last resort: treat whole string as one alias token
            return [s]
        # Unexpected type -> best effort stringification
        return [str(value)]

    results: List[Dict[str, Any]] = []
    for row in rows:
        cols = getattr(row, "columns", {}) or {}
        aliases = _normalize_aliases(cols.get("aliases"))
        metadata_json = cols.get("metadata_json")
        entities_json = cols.get("entities_json")

        metadata = {}
        if isinstance(metadata_json, str) and metadata_json:
            try:
                metadata = json.loads(metadata_json)
            except Exception:
                metadata = {}

        entities = []
        if isinstance(entities_json, str) and entities_json:
            try:
                entities = json.loads(entities_json)
            except Exception:
                entities = []

        results.append(
            {
                "original_tag": getattr(row, "key", None),
                "aliases": aliases,
                "metadata": metadata,
                "entities": entities,
            }
        )
    return results


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
        data: Dictionary containing aliasing_results and entities_keys_extracted.
            Optional: aliasWritebackProperty or alias_writeback_property — DM property
            to write the alias list to (default "aliases").
    """
    try:
        logger.info("Starting Alias Persistence Pipeline")

        if not client:
            raise ValueError("CogniteClient is required for alias persistence")

        alias_writeback_property = _resolve_alias_writeback_property(data)
        data["alias_writeback_property"] = alias_writeback_property

        # Get aliasing results
        aliasing_results = data.get("aliasing_results", [])
        if not aliasing_results:
            raw_db = data.get("raw_db")
            raw_table_aliases = data.get("raw_table_aliases") or data.get("raw_table")
            if raw_db and raw_table_aliases:
                raw_limit = int(data.get("raw_read_limit", 1000))
                logger.info(
                    f"No aliasing_results provided; loading from RAW db={raw_db} table={raw_table_aliases} limit={raw_limit}"
                )
                aliasing_results = _load_aliasing_results_from_raw(
                    client=client,
                    raw_db=raw_db,
                    raw_table_aliases=raw_table_aliases,
                    logger=logger,
                    limit=raw_limit,
                )
                data["aliasing_results_loaded_from_raw"] = len(aliasing_results)

        if not aliasing_results:
            logger.warning("No aliasing results found to persist")
            data["aliases_persisted"] = 0
            return

        logger.info(f"Found {len(aliasing_results)} aliasing results to persist")
        logger.info(
            f"Alias write-back property: {alias_writeback_property!r} "
            f"(set aliasWritebackProperty or alias_writeback_property to override)"
        )

        # Group aliases by entity using view information from aliasing results.
        # Note: a single tag's aliases can map to multiple entities, so counts can be > number of
        # aliasing rows in RAW.
        entity_aliases = {}
        aliases_planned_count = 0

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
                aliases_planned_count += len(aliases)

        logger.info(
            f"Prepared aliases for {len(entity_aliases)} entities "
            f"({aliases_planned_count} planned alias writes)"
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
        aliases_persisted_count = 0

        for entity_key, alias_data in entity_aliases.items():
            try:
                entity_id = alias_data["entity_id"]
                instance_space = alias_data.get("instance_space")
                aliases = list(set(alias_data["aliases"]))  # Deduplicate

                # Use instance_space from source entity, fallback to target view space if not available
                if not instance_space:
                    instance_space = target_view_space

                # Prepare property update with aliases
                # Writing to CogniteDescribable view which should have an "aliases" property
                # Note: Only include properties that exist in the view schema
                properties_update = {alias_writeback_property: aliases}

                # Apply update using data modeling API
                logger.info(
                    f"Updating entity {entity_id} with {len(aliases)} values "
                    f"on property {alias_writeback_property!r} in {target_view_id} view"
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
                aliases_persisted_count += len(aliases)
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
        data["aliases_planned"] = aliases_planned_count
        data["aliases_persisted"] = aliases_persisted_count
        data["entities_updated"] = persisted_count
        data["entities_failed"] = failed_count
        data["entity_aliases"] = {
            entity_key: {
                "entity_id": alias_data["entity_id"],
                "target_view_space": target_view_space,
                "target_view_external_id": target_view_external_id,
                "target_view_version": target_view_version,
                "alias_writeback_property": alias_writeback_property,
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
