"""Orchestrate key extraction, aliasing, and persistence for the local CLI."""
import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (
    persist_aliases_to_entities,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
    _dedupe_foreign_key_references,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.dm_filter_utils import (
    property_reference_for_filter,
)

from .report import ensure_results_dir


def run_pipeline(
    args: argparse.Namespace,
    logger: logging.Logger,
    client: Any,
    extraction_config: Dict[str, Any],
    aliasing_config: Dict[str, Any],
    source_views: List[Dict[str, Any]],
    alias_writeback_property: Optional[str],
    write_foreign_key_references: bool,
    foreign_key_writeback_property: Optional[str],
) -> None:
    extraction_engine = KeyExtractionEngine(extraction_config)
    aliasing_engine = AliasingEngine(aliasing_config, client=client)

    all_extraction_items: List[Dict[str, Any]] = []
    aliasing_items: List[Dict[str, Any]] = []
    # Data structure for persistence function (matches workflow format)
    entities_keys_extracted: Dict[str, Dict[str, Any]] = {}
    aliasing_results: List[Dict[str, Any]] = []

    # Process each source view from config
    for view_config in source_views:
        view_space = view_config.get("view_space", "cdf_cdm")
        view_external_id = view_config.get("view_external_id", "CogniteAsset")
        view_version = view_config.get("view_version", "v1")
        instance_space = view_config.get("instance_space")
        entity_type = view_config.get("entity_type", "asset")
        batch_size = (
            view_config.get("batch_size") or view_config.get("limit") or args.limit
        )
        # 0 means no limit (fetch all instances)
        effective_limit = batch_size if batch_size > 0 else None
        filters = view_config.get("filters", [])
        include_properties = view_config.get("include_properties", [])

        _isp = instance_space if instance_space else "(not set; list space=None or use filters)"
        logger.info(
            f"Processing view {view_space}/{view_external_id}/{view_version} "
            f"(instance_space: {_isp}, entity_type: {entity_type}, limit: {batch_size if batch_size else 'all'})..."
        )

        # Query data modeling instances
        try:
            from cognite.client import data_modeling as dm
            from cognite.client.data_classes.data_modeling.ids import ViewId

            view_id = ViewId(
                space=view_space, external_id=view_external_id, version=view_version
            )

            # Build filter expression from configuration
            filter_expressions = []

            # Base filter: ensure instance has data from this view
            filter_expressions.append(dm.filters.HasData(views=[view_id]))

            # Add custom filters from configuration
            if filters:
                for filter_config in filters:
                    operator = str(filter_config.get("operator", "")).upper()
                    target_property = filter_config.get("target_property")
                    values = filter_config.get("values", [])
                    property_scope = str(
                        filter_config.get("property_scope", "view")
                    ).lower()

                    if not target_property:
                        continue

                    property_ref = property_reference_for_filter(
                        view_id, target_property, property_scope
                    )

                    if operator == "EQUALS":
                        if isinstance(values, str):
                            eq_vals = [values]
                        elif isinstance(values, list):
                            eq_vals = values
                        elif values is None:
                            eq_vals = []
                        else:
                            eq_vals = [values]
                        if len(eq_vals) == 1:
                            filter_expressions.append(
                                dm.filters.Equals(property_ref, eq_vals[0])
                            )
                        elif len(eq_vals) > 1:
                            equals_filters = [
                                dm.filters.Equals(property_ref, val) for val in eq_vals
                            ]
                            filter_expressions.append(dm.filters.Or(*equals_filters))

                    elif operator == "IN":
                        if isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.In(property_ref, values)
                            )

                    elif operator == "CONTAINSALL":
                        if values and isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.ContainsAll(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "CONTAINSANY":
                        if values and isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.ContainsAny(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "EXISTS":
                        if property_scope == "node":
                            filter_expressions.append(
                                dm.filters.Exists(property=property_ref)
                            )
                        else:
                            filter_expressions.append(
                                dm.filters.HasData(
                                    views=[view_id], properties=[target_property]
                                )
                            )

                    elif operator == "SEARCH":
                        if values:
                            logger.warning(
                                f"SEARCH operator not fully supported, using IN for property {target_property}"
                            )
                            if isinstance(values, list):
                                filter_expressions.append(
                                    dm.filters.In(property_ref, values)
                                )
                            else:
                                filter_expressions.append(
                                    dm.filters.In(property_ref, [values])
                                )

            # Combine all filters with AND
            final_filter = (
                dm.filters.And(*filter_expressions)
                if len(filter_expressions) > 1
                else filter_expressions[0]
                if filter_expressions
                else None
            )

            # Query instances using list method (supports filters)
            # Try with filters first, fall back to no filters if filter fails
            instances = None
            if final_filter is not None:
                try:
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space if instance_space else None,
                        sources=[view_id],
                        filter=final_filter,
                        limit=effective_limit,
                    )
                except Exception as filter_error:
                    logger.warning(
                        f"Filter failed for view {view_external_id}: {filter_error}. "
                        f"Retrying without filters..."
                    )
                    # Fall back to query without filters
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space if instance_space else None,
                        sources=[view_id],
                        limit=effective_limit,
                    )
            else:
                # No filters configured, query without filters
                instances = client.data_modeling.instances.list(
                    instance_type="node",
                    space=instance_space if instance_space else None,
                    sources=[view_id],
                    limit=effective_limit,
                )
        except Exception as e:
            logger.warning(
                f"Failed to fetch instances from view {view_external_id}: {e}"
            )
            continue

        # Convert instances to dict format expected by extraction engine
        instances_dicts: List[Dict[str, Any]] = []
        for instance in instances:
            # Get instance identifier
            instance_external_id = getattr(instance, "external_id", None)
            instance_id = instance_external_id or str(
                getattr(instance, "instance_id", "")
            )

            # Extract properties from CDM structure (same as in pipeline)
            instance_dump = instance.dump()
            entity_props = (
                instance_dump.get("properties", {})
                .get(view_space, {})
                .get(f"{view_external_id}/{view_version}", {})
            )

            # Build entity dict with flattened properties
            # If include_properties is specified, only include those properties
            node_space = getattr(instance, "space", None)
            if include_properties:
                filtered_props = {
                    prop: entity_props.get(prop)
                    for prop in include_properties
                    if prop in entity_props
                }
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    "space": node_space,
                    **filtered_props,
                }
            else:
                # Include all properties if no filter specified
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    "space": node_space,
                    **entity_props,  # Spread extracted properties at top level
                }
            instances_dicts.append(entity_dict)

        logger.info(f"  Fetched {len(instances_dicts)} instances")

        # Run extraction for this view
        view_extraction_items: List[Dict[str, Any]] = []
        view_iso = view_config.get("exclude_self_referencing_keys")
        for entity in instances_dicts:
            res = extraction_engine.extract_keys(
                entity,
                entity_type=entity_type,
                exclude_self_referencing_keys=view_iso,
            )
            entity_id = res.entity_id

            # Build entities_keys_extracted structure for persistence (workflow format)
            keys_by_field = {}
            for key in res.candidate_keys:
                field_name = key.source_field
                if field_name not in keys_by_field:
                    keys_by_field[field_name] = {}
                # Handle both enum and string extraction_type
                extraction_type_value = (
                    key.extraction_type.value
                    if hasattr(key.extraction_type, "value")
                    else key.extraction_type
                )
                keys_by_field[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": extraction_type_value,
                }

            fk_refs = _dedupe_foreign_key_references(res)
            row_instance_space = entity.get("space") or instance_space
            entities_keys_extracted[entity_id] = {
                "keys": keys_by_field,
                "foreign_key_references": fk_refs,
                "view_space": view_space,
                "view_external_id": view_external_id,
                "view_version": view_version,
                "instance_space": row_instance_space,
                "entity_type": entity_type,
            }

            view_extraction_items.append(
                {
                    "entity": entity,  # Pass entity dict as-is with all properties
                    "view_external_id": view_external_id,
                    "extraction_result": {
                        "entity_id": res.entity_id,
                        "entity_type": res.entity_type,
                        "candidate_keys": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.candidate_keys
                        ],
                        "foreign_key_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.foreign_key_references
                        ],
                        "document_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.document_references
                        ],
                        "metadata": res.metadata,
                    },
                }
            )

        # Run aliasing for each candidate key from this view
        logger.info(f"  Running aliasing on extracted candidate keys...")
        for item in view_extraction_items:
            entity = item["entity"]
            entity_id = entity.get("id")
            row_instance_space = entity.get("space") or instance_space
            context = {
                "site": entity.get("site"),
                "unit": entity.get("unit"),
                "equipment_type": entity.get("equipmentType")
                or entity.get("equipment_type"),
                "instance_space": row_instance_space,
                "view_external_id": view_external_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "entity_external_id": entity.get("externalId"),
            }
            for k in item["extraction_result"]["candidate_keys"]:
                tag = k["value"]
                source_field = k.get("source_field")
                aliases_result = aliasing_engine.generate_aliases(
                    tag=tag, entity_type=entity_type, context=context
                )
                # Sort aliases alphabetically (case-insensitive, then case-sensitive)
                sorted_aliases = sorted(
                    aliases_result.aliases, key=lambda x: (x.lower(), x)
                )

                aliasing_items.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": entity_type,
                        "view_external_id": view_external_id,
                        "base_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                    }
                )

                # Build aliasing_results structure for persistence (workflow format)
                aliasing_results.append(
                    {
                        "original_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                        "entities": [
                            {
                                "entity_id": entity_id,
                                "field_name": source_field,
                                "view_space": view_space,
                                "view_external_id": view_external_id,
                                "view_version": view_version,
                                "instance_space": row_instance_space,
                                "entity_type": entity_type,
                            }
                        ],
                    }
                )

        all_extraction_items.extend(view_extraction_items)

    # Write results
    results_dir = ensure_results_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_path = results_dir / f"{ts}_cdf_extraction.json"
    aliasing_path = results_dir / f"{ts}_cdf_aliasing.json"

    with extraction_path.open("w", encoding="utf-8") as f:
        json.dump({"results": all_extraction_items}, f, indent=2)

    # Sort aliasing results by entity_id, then base_tag
    sorted_aliasing_items = sorted(
        aliasing_items, key=lambda x: (x.get("entity_id", ""), x.get("base_tag", ""))
    )

    with aliasing_path.open("w", encoding="utf-8") as f:
        json.dump({"results": sorted_aliasing_items}, f, indent=2)

    logger.info(f"✓ Wrote extraction results: {extraction_path}")
    logger.info(f"✓ Wrote aliasing results:   {aliasing_path}")

    extracted_fk_count = sum(
        len(item.get("extraction_result", {}).get("foreign_key_references") or [])
        for item in all_extraction_items
    )
    entities_with_fk = sum(
        1
        for item in all_extraction_items
        if item.get("extraction_result", {}).get("foreign_key_references")
    )
    logger.info(
        f"Extraction: {extracted_fk_count} foreign key reference(s) in JSON across "
        f"{entities_with_fk} entities (extracted, not yet written to the data model)."
    )

    wfk = write_foreign_key_references or args.write_foreign_keys
    fk_prop = foreign_key_writeback_property
    if args.foreign_key_writeback_property:
        fk_prop = args.foreign_key_writeback_property.strip() or fk_prop

    # Persist aliases to CogniteDescribable view (unless dry-run)
    if args.dry_run:
        logger.info(
            "Dry-run mode: Skipping alias persistence to CDF. "
            f"Would persist {len(aliasing_results)} aliasing results to {len(entities_keys_extracted)} entities"
        )
        if extracted_fk_count and not wfk:
            logger.info(
                "FK write-back to DM is off (set write_foreign_key_references in scope YAML, "
                "env WRITE_FOREIGN_KEY_REFERENCES, or run with --write-foreign-keys)."
            )
    else:
        logger.info("Persisting aliases to CogniteDescribable view...")
        try:
            persistence_data = {
                "aliasing_results": aliasing_results,
                "entities_keys_extracted": entities_keys_extracted,
                "logLevel": "INFO",
            }
            if alias_writeback_property:
                persistence_data["alias_writeback_property"] = alias_writeback_property
            if wfk:
                persistence_data["write_foreign_key_references"] = True
                if fk_prop:
                    persistence_data["foreign_key_writeback_property"] = fk_prop
            persist_aliases_to_entities(
                client=client,
                logger=logger,
                data=persistence_data,
            )
            fk_written = int(persistence_data.get("foreign_keys_persisted", 0))
            persist_msg = (
                f"✓ Persisted to data model: {persistence_data.get('entities_updated', 0)} entities updated, "
                f"{persistence_data.get('aliases_persisted', 0)} alias value(s) written, "
                f"{fk_written} foreign key value(s) written"
            )
            if not wfk and extracted_fk_count:
                persist_msg += (
                    f" (extraction had {extracted_fk_count} FK ref(s) in JSON; "
                    "enable FK write-back to persist them to DM)"
                )
            elif not wfk:
                persist_msg += " (FK write-back disabled for this run)"
            logger.info(persist_msg)
        except Exception as e:
            logger.error(f"Failed to persist aliases: {e}", exc_info=True)
