"""
CDF Pipeline for Key Extraction

This module provides the main pipeline function that processes entities
from CDF data model views and extracts keys using the KeyExtractionEngine.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row

from .common.logger import CogniteFunctionLogger
from .engine.key_extraction_engine import ExtractionResult, KeyExtractionEngine
from .services.ApplyService import GeneralApplyService

logger = None  # Use CogniteFunctionLogger directly

# RAW column written alongside per-field candidate key columns (alias persistence reads this).
FOREIGN_KEY_REFERENCES_JSON_COLUMN = "FOREIGN_KEY_REFERENCES_JSON"


def _dedupe_foreign_key_references(result: ExtractionResult) -> List[Dict[str, Any]]:
    """One entry per distinct FK value; keep the occurrence with highest confidence."""
    best: Dict[str, Dict[str, Any]] = {}
    for fk in result.foreign_key_references:
        v = getattr(fk, "value", None) or ""
        if not v:
            continue
        conf = float(getattr(fk, "confidence", 0.0) or 0.0)
        entry = {
            "value": v,
            "confidence": conf,
            "source_field": getattr(fk, "source_field", None),
            "rule_id": getattr(fk, "rule_id", None),
        }
        if v not in best or conf > best[v]["confidence"]:
            best[v] = entry
    return list(best.values())


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
    # When running from Workflows we do NOT use Extraction Pipelines.
    # Keep a name for logging/state derived from workflow config if present.
    pipeline_name = data.get("workflow_config_external_id") or "unknown"
    status = "failure"
    pipeline_run_id = None
    keys_extracted = 0
    run_started_at = datetime.now(timezone.utc)

    try:
        logger.info(f"Starting Key Extraction Pipeline: {pipeline_name}")

        # Determine if using CDF format or standalone
        use_cdf_format = cdf_config is not None and client is not None

        if use_cdf_format:
            from cognite.extractorutils.uploader import RawUploadQueue

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
        rule_source_fields_map = {}
        for rule in getattr(engine, "rules", []):
            rule_source_fields_map[rule.name] = [sf.field_name for sf in rule.source_fields]
        rules_used_counts: Dict[str, int] = {}

        for entity_id, entity_fields in entities_source_fields.items():
            # Convert entity fields to format expected by engine
            entity = {"id": entity_id, "externalId": entity_id, **entity_fields}
            entity_type = entity_fields.get("entity_type", "asset")

            # Extract keys
            result = engine.extract_keys(entity, entity_type)
            for k in result.candidate_keys:
                if getattr(k, "rule_name", None):
                    rules_used_counts[k.rule_name] = rules_used_counts.get(k.rule_name, 0) + 1

            # Store results organized by field_name with extraction type metadata
            keys = {}
            for key in result.candidate_keys:
                field_name = key.source_field
                if field_name not in keys:
                    keys[field_name] = {}
                keys[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": key.extraction_type.value,
                    "rule_name": key.rule_name,
                    "matched_source_field": key.source_field,
                    "rule_source_fields": rule_source_fields_map.get(key.rule_name, []),
                }

            fk_refs = _dedupe_foreign_key_references(result)
            # Store entity metadata including view information
            _src_view = (
                getattr(getattr(cdf_config, "data", None), "source_view", None)
                if cdf_config
                else None
            )
            entity_metadata = {
                "keys": keys,
                "foreign_key_references": fk_refs,
                "view_space": entity_fields.get("view_space"),
                "view_external_id": entity_fields.get("view_external_id"),
                "view_version": entity_fields.get("view_version"),
                "instance_space": entity_fields.get("instance_space"),
                "entity_type": entity_type,
                "resource_property": getattr(_src_view, "resource_property", None),
                "space": (
                    getattr(_src_view, "instance_space", None)
                    or entity_fields.get("instance_space")
                ),
            }
            entities_keys_extracted[entity_id] = entity_metadata
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW if using CDF format (one row per entity in raw_table_key)
        if use_cdf_format:
            _create_table_if_not_exists(client, raw_db, raw_table_state, logger)
            _create_table_if_not_exists(client, raw_db, raw_table_key, logger)
            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", {})
                fk_refs = entity_metadata.get("foreign_key_references") or []
                has_candidate_columns = isinstance(field_keys, dict) and bool(field_keys)
                has_fk = bool(fk_refs)
                if not has_candidate_columns and not has_fk:
                    continue
                columns: Dict[str, Any] = {}
                rules_used = set()
                if has_candidate_columns:
                    for field_name, keys_dict in field_keys.items():
                        # Column names use field_name (e.g., "NAME", "DESCRIPTION")
                        columns[field_name.upper()] = list(keys_dict.keys())
                        for _, meta in (keys_dict or {}).items():
                            if isinstance(meta, dict) and meta.get("rule_name"):
                                rules_used.add(str(meta["rule_name"]))
                    columns["RULES_USED_JSON"] = json.dumps(sorted(rules_used))
                else:
                    columns["RULES_USED_JSON"] = json.dumps([])
                if has_fk:
                    columns[FOREIGN_KEY_REFERENCES_JSON_COLUMN] = json.dumps(fk_refs)

                new_row = Row(key=ext_id, columns=columns)
                raw_uploader.add_to_upload_queue(
                    database=raw_db,
                    table=raw_table_key,
                    raw_row=new_row,
                )
            logger.debug(f"Uploading {raw_uploader.upload_queue_size} rows to RAW")
            try:
                raw_uploader.upload()
                logger.info("Successfully uploaded keys to RAW")
            except Exception as e:
                logger.error(f"Failed to upload rows to RAW: {e}")
                raise

        # Determine status for workflow/state reporting.
        total_fk = sum(
            len(em.get("foreign_key_references") or [])
            for em in entities_keys_extracted.values()
        )
        if keys_extracted > 0 or total_fk > 0:
            status = "success"
            message = (
                f"Successfully extracted {keys_extracted} candidate keys"
                + (f", {total_fk} foreign key reference(s)" if total_fk else "")
            )
        else:
            status = "failure"
            message = "No keys were extracted and no keys were uploaded"

        # Write a lightweight state row to RAW (this is separate from extraction pipeline runs).
        # This makes it easy to inspect "what was processed" for a given run from RAW.
        if use_cdf_format and client:
            run_finished_at = datetime.now(timezone.utc)
            run_duration_s = (run_finished_at - run_started_at).total_seconds()
            run_duration_ms = int(run_duration_s * 1000)
            state_key = run_finished_at.strftime("%Y%m%dT%H%M%S.%fZ")
            try:
                client.raw.rows.insert(
                    raw_db,
                    raw_table_state,
                    Row(
                        key=str(state_key),
                        columns={
                            "pipeline_name": pipeline_name,
                            "status": status,
                            "message": message,
                            "keys_extracted": keys_extracted,
                            "entities_processed": len(entities_keys_extracted),
                            "run_started_at": run_started_at.isoformat(),
                            "run_finished_at": run_finished_at.isoformat(),
                            "run_duration_s": run_duration_s,
                            "run_duration_ms": run_duration_ms,
                            "raw_db": raw_db,
                            "raw_table_key": raw_table_key,
                            "raw_table_state": raw_table_state,
                            "max_files": getattr(
                                getattr(cdf_config, "parameters", None), "max_files", None
                            ),
                            "rules_used_counts_json": json.dumps(rules_used_counts),
                        },
                    ),
                )
                logger.info(
                    f"Wrote state row to RAW: db={raw_db} table={raw_table_state} key={state_key}"
                )
            except Exception as e:
                logger.warning(f"Failed writing RAW state row: {e}")

        # Store results in data dict for return
        data["keys_extracted"] = keys_extracted
        data["entities_keys_extracted"] = entities_keys_extracted
        data["status"] = status
        data["message"] = message

    except Exception as e:
        message = f"Pipeline failed: {e!s}"
        logger.error(message)

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

    max_files = getattr(getattr(config, "parameters", None), "max_files", None)
    if isinstance(max_files, bool):  # avoid treating True/False as ints
        max_files = None
    if isinstance(max_files, int) and max_files <= 0:
        max_files = None

    for entity_view_config in config.data.source_views:
        entity_view_id = entity_view_config.as_view_id()

        logger.debug(f"Querying view: {entity_view_id}")

        # Build filter
        filter_expr = _build_filter(
            entity_view_config,
            bool(getattr(getattr(config, "parameters", None), "run_all", True)),
            excluded_entities,
            logger,
        )

        try:
            remaining = None
            if isinstance(max_files, int):
                remaining = max_files - len(entities_source)
                if remaining <= 0:
                    break

            # Query instances
            instances = client.data_modeling.instances.list(
                instance_type="node",
                space=entity_view_config.instance_space,
                sources=[entity_view_id],
                filter=filter_expr,
                limit=min(entity_view_config.batch_size, remaining)
                if isinstance(remaining, int)
                else entity_view_config.batch_size,
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

                # Determine which fields to extract.
                # Prefer extraction_rules.source_fields if available; otherwise fall back to include_properties.
                wanted_fields: list[tuple[str, bool, list[str]]] = []
                extraction_rules = getattr(getattr(config, "data", None), "extraction_rules", None)

                if isinstance(extraction_rules, list) and extraction_rules:
                    # Case 1: list of pydantic objects with `.source_fields`
                    if hasattr(extraction_rules[0], "source_fields"):
                        for rule in extraction_rules:
                            source_fields = getattr(rule, "source_fields", None)
                            if source_fields is None:
                                continue
                            if not isinstance(source_fields, list):
                                source_fields = [source_fields]
                            for sf in source_fields:
                                wanted_fields.append(
                                    (
                                        str(getattr(sf, "field_name", "") or ""),
                                        bool(getattr(sf, "required", False)),
                                        list(getattr(sf, "preprocessing", []) or []),
                                    )
                                )
                    # Case 2: list of dicts from workflow config
                    elif isinstance(extraction_rules[0], dict):
                        for rule in extraction_rules:
                            for sf in (rule.get("source_fields", []) or []):
                                if not isinstance(sf, dict):
                                    continue
                                wanted_fields.append(
                                    (
                                        str(sf.get("field_name") or ""),
                                        bool(sf.get("required") or False),
                                        list(sf.get("preprocessing") or []),
                                    )
                                )

                if not wanted_fields:
                    for p in list(getattr(entity_view_config, "include_properties", []) or []):
                        wanted_fields.append((str(p or ""), False, []))

                for field_name, required, preprocessing in wanted_fields:
                    if not field_name:
                        continue
                    field_value = entity_props.get(field_name)
                    if field_value is None:
                        if required:
                            logger.verbose(
                                "WARNING",
                                f"Missing required field '{field_name}' in entity: {entity_external_id}",
                            )
                        continue

                    if preprocessing:
                        field_value = _apply_preprocessing(field_value, preprocessing)
                    entities_source[entity_external_id][field_name] = field_value

                if isinstance(max_files, int) and len(entities_source) >= max_files:
                    break

            logger.info(
                f"Processed {len(entities_source)} entities from view: {entity_view_id}"
            )

        except Exception as e:
            logger.error(f"Error querying view {entity_view_id}: {e}")
            raise

        if isinstance(max_files, int) and len(entities_source) >= max_files:
            break

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
