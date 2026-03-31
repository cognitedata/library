"""
CDF Pipeline for Key Extraction

This module provides the main pipeline function that processes entities
from CDF data model views and extracts keys using the KeyExtractionEngine.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row

from .common.logger import CogniteFunctionLogger
from .engine.key_extraction_engine import ExtractionResult, KeyExtractionEngine
from .services.ApplyService import GeneralApplyService

from .raw_join_utils import (
    entity_props_for_view,
    merged_join_columns_for_instance,
    preload_raw_lookups,
)
from .utils.rule_utils import get_rule_id
from ..cdf_fn_common.raw_upload import create_raw_upload_queue

logger = None  # Use CogniteFunctionLogger directly

# RAW column written alongside per-field candidate key columns (alias persistence reads this).
FOREIGN_KEY_REFERENCES_JSON_COLUMN = "FOREIGN_KEY_REFERENCES_JSON"

# Unified extraction store: entity rows vs run audit rows (same physical table as raw_table_key).
RECORD_KIND_COLUMN = "RECORD_KIND"
RECORD_KIND_RUN = "run"
RECORD_KIND_ENTITY = "entity"
EXTRACTION_STATUS_COLUMN = "EXTRACTION_STATUS"
EXTRACTION_STATUS_SUCCESS = "success"
EXTRACTION_STATUS_FAILED = "failed"
EXTRACTION_STATUS_EMPTY = "empty"
RAW_COL_UPDATED_AT = "UPDATED_AT"
RAW_COL_RUN_ID = "RUN_ID"
RAW_COL_LAST_ERROR = "LAST_ERROR"


def _raw_row_columns(row: Any) -> Dict[str, Any]:
    cols = getattr(row, "columns", None) or {}
    return dict(cols) if isinstance(cols, dict) else {}


def _norm_status_value(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, list) and raw:
        raw = raw[0]
    return str(raw).strip().lower()


def _is_run_summary_row(columns: Dict[str, Any]) -> bool:
    return _norm_status_value(columns.get(RECORD_KIND_COLUMN)) == RECORD_KIND_RUN


def _entity_row_should_skip_listing(policy: str, columns: Dict[str, Any]) -> bool:
    """Whether this RAW entity row causes its instance external_id to be excluded from DM list."""
    if _is_run_summary_row(columns):
        return False
    st = _norm_status_value(columns.get(EXTRACTION_STATUS_COLUMN))
    if policy == "successful_only":
        return st in (EXTRACTION_STATUS_SUCCESS, EXTRACTION_STATUS_EMPTY)
    return False


def _read_entity_keys_to_exclude(
    client: CogniteClient,
    db: str,
    tbl: str,
    policy: str,
    chunk_size: int,
) -> List[str]:
    """Collect instance external ids to exclude from listing per skip_entity_policy."""
    excluded: List[str] = []
    if policy != "successful_only":
        return excluded
    try:
        for batch in client.raw.rows(db, tbl, chunk_size=chunk_size):
            for row in batch:
                key = getattr(row, "key", None)
                if not key:
                    continue
                cols = _raw_row_columns(row)
                if _is_run_summary_row(cols):
                    continue
                if _entity_row_should_skip_listing(policy, cols):
                    excluded.append(str(key))
    except Exception:
        return []
    return excluded


def _strip_extraction_internal_fields(em: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in em.items() if not str(k).startswith("_extraction")}


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
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    keys_extracted = 0
    run_started_at = datetime.now(timezone.utc)
    raw_db = ""
    raw_table_key = ""
    write_empty_extraction_rows = False

    try:
        logger.info(f"Starting Key Extraction Pipeline: {pipeline_name}")

        # Determine if using CDF format or standalone
        use_cdf_format = cdf_config is not None and client is not None

        if use_cdf_format:
            # Extract parameters from CDF config
            raw_db = cdf_config.parameters.raw_db
            raw_table_key = cdf_config.parameters.raw_table_key
            write_empty_extraction_rows = bool(
                cdf_config.parameters.write_empty_extraction_rows
            )

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_key={raw_table_key}"
            )

            # Initialize RAW upload queue (size from CDF_RAW_UPLOAD_MAX_QUEUE_SIZE or default)
            raw_uploader = create_raw_upload_queue(client)

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
            rn = getattr(rule, "name", None) or getattr(rule, "rule_id", None)
            if rn is None:
                continue
            sfs = getattr(rule, "source_fields", None) or []
            if not isinstance(sfs, list):
                sfs = [sfs]
            rule_source_fields_map[str(rn)] = [
                getattr(sf, "field_name", None)
                or (sf.get("field_name") if isinstance(sf, dict) else None)
                for sf in sfs
            ]
        rules_used_counts: Dict[str, int] = {}

        # Serial extraction per entity; instance listing uses batch_size on the source view upstream.
        for entity_id, entity_fields in entities_source_fields.items():
            ef = dict(entity_fields)
            kex_iso = ef.pop("_kex_exclude_self_referencing_keys", None)
            entity = {"id": entity_id, "externalId": entity_id, **ef}
            entity_type = entity_fields.get("entity_type", "asset")
            _src_view = (
                getattr(getattr(cdf_config, "data", None), "source_view", None)
                if cdf_config
                else None
            )
            base_meta = {
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
            try:
                result = engine.extract_keys(
                    entity,
                    entity_type,
                    exclude_self_referencing_keys=kex_iso,
                )
            except Exception as ex:
                logger.warning(
                    f"Extraction failed for entity {entity_id!r}: {ex!s}"
                )
                entities_keys_extracted[entity_id] = {
                    **base_meta,
                    "keys": {},
                    "foreign_key_references": [],
                    "_extraction_failed": True,
                    "_extraction_error": str(ex)[:4000],
                }
                continue

            for k in result.candidate_keys:
                if getattr(k, "rule_name", None):
                    rules_used_counts[k.rule_name] = rules_used_counts.get(
                        k.rule_name, 0
                    ) + 1

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
            entities_keys_extracted[entity_id] = {
                **base_meta,
                "keys": keys,
                "foreign_key_references": fk_refs,
            }
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW: entity rows + run summary in raw_table_key.
        if use_cdf_format:
            _create_table_if_not_exists(client, raw_db, raw_table_key, logger)
            batch_updated_at = datetime.now(timezone.utc)

            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", {})
                fk_refs = entity_metadata.get("foreign_key_references") or []
                failed = bool(entity_metadata.get("_extraction_failed"))
                err_msg = entity_metadata.get("_extraction_error")
                has_candidate_columns = isinstance(field_keys, dict) and bool(
                    field_keys
                )
                has_fk = bool(fk_refs)
                if failed:
                    ext_status = EXTRACTION_STATUS_FAILED
                elif has_candidate_columns or has_fk:
                    ext_status = EXTRACTION_STATUS_SUCCESS
                else:
                    ext_status = EXTRACTION_STATUS_EMPTY

                if (
                    not failed
                    and not has_candidate_columns
                    and not has_fk
                    and not write_empty_extraction_rows
                ):
                    continue

                columns: Dict[str, Any] = {
                    RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                    EXTRACTION_STATUS_COLUMN: ext_status,
                    RAW_COL_UPDATED_AT: batch_updated_at.isoformat(),
                    RAW_COL_RUN_ID: run_id,
                }
                if failed and err_msg:
                    columns[RAW_COL_LAST_ERROR] = str(err_msg)[:8000]

                rules_used = set()
                if has_candidate_columns:
                    for field_name, keys_dict in field_keys.items():
                        columns[field_name.upper()] = list(keys_dict.keys())
                        for _, meta in (keys_dict or {}).items():
                            if isinstance(meta, dict) and meta.get("rule_name"):
                                rules_used.add(str(meta["rule_name"]))
                    columns["RULES_USED_JSON"] = json.dumps(sorted(rules_used))
                else:
                    columns["RULES_USED_JSON"] = json.dumps([])
                if has_fk:
                    columns[FOREIGN_KEY_REFERENCES_JSON_COLUMN] = json.dumps(fk_refs)

                raw_uploader.add_to_upload_queue(
                    database=raw_db,
                    table=raw_table_key,
                    raw_row=Row(key=ext_id, columns=columns),
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
        n_failed = sum(
            1 for em in entities_keys_extracted.values() if em.get("_extraction_failed")
        )
        if keys_extracted > 0 or total_fk > 0:
            status = "success"
            message = (
                f"Successfully extracted {keys_extracted} candidate keys"
                + (f", {total_fk} foreign key reference(s)" if total_fk else "")
                + (f"; {n_failed} entity failure(s)" if n_failed else "")
            )
        elif n_failed:
            status = "failure"
            message = f"No keys extracted; {n_failed} entity failure(s)"
        else:
            status = "failure"
            message = "No keys were extracted and no keys were uploaded"

        if use_cdf_format and client:
            run_finished_at = datetime.now(timezone.utc)
            run_duration_s = (run_finished_at - run_started_at).total_seconds()
            run_duration_ms = int(run_duration_s * 1000)
            state_key = run_finished_at.strftime("%Y%m%dT%H%M%S.%fZ")
            run_columns = {
                RECORD_KIND_COLUMN: RECORD_KIND_RUN,
                "pipeline_name": pipeline_name,
                "status": status,
                "message": message,
                "keys_extracted": keys_extracted,
                "entities_processed": len(entities_keys_extracted),
                "entities_failed": n_failed,
                "run_started_at": run_started_at.isoformat(),
                "run_finished_at": run_finished_at.isoformat(),
                "run_duration_s": run_duration_s,
                "run_duration_ms": run_duration_ms,
                "raw_db": raw_db,
                "raw_table_key": raw_table_key,
                "skip_entity_policy": getattr(
                    getattr(cdf_config, "parameters", None),
                    "skip_entity_policy",
                    "successful_only",
                ),
                "run_id": run_id,
                "max_files": getattr(
                    getattr(cdf_config, "parameters", None), "max_files", None
                ),
                "rules_used_counts_json": json.dumps(rules_used_counts),
            }
            try:
                client.raw.rows.insert(
                    raw_db,
                    raw_table_key,
                    Row(key=str(state_key), columns=run_columns),
                )
                logger.info(
                    f"Wrote run summary to RAW: db={raw_db} table={raw_table_key} key={state_key}"
                )
            except Exception as e:
                logger.warning(f"Failed writing RAW run summary row to keys table: {e}")

        # Store results in data dict for return
        data["keys_extracted"] = keys_extracted
        data["entities_keys_extracted"] = {
            eid: _strip_extraction_internal_fields(em)
            for eid, em in entities_keys_extracted.items()
        }
        data["run_id"] = run_id
        data["status"] = status
        data["message"] = message

    except Exception as e:
        message = f"Pipeline failed: {e!s}"
        logger.error(message)

        raise


def _iter_rule_source_fields(rule: Any) -> List[Any]:
    """Normalize source_fields on a rule to a list."""
    sf = (
        rule.get("source_fields", None)
        if isinstance(rule, dict)
        else getattr(rule, "source_fields", None)
    )
    if sf is None:
        return []
    return list(sf) if isinstance(sf, list) else [sf]


def _sf_field_name(sf: Any) -> str:
    if isinstance(sf, dict):
        return str(sf.get("field_name") or "")
    return str(getattr(sf, "field_name", "") or "")


def _sf_table_id(sf: Any) -> Optional[str]:
    v = sf.get("table_id") if isinstance(sf, dict) else getattr(sf, "table_id", None)
    return str(v) if v else None


def _sf_required(sf: Any) -> bool:
    if isinstance(sf, dict):
        return bool(sf.get("required"))
    return bool(getattr(sf, "required", False))


def _sf_preprocessing(sf: Any) -> List[str]:
    raw = sf.get("preprocessing") if isinstance(sf, dict) else getattr(sf, "preprocessing", None)
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return list(raw)


def _build_entity_payload_with_rules(
    *,
    instance: Any,
    merged_columns: Optional[Dict[str, Any]],
    extraction_rules: Any,
    entity_view_id: Any,
    entity_view_config: Any,
    logger: Any,
) -> Dict[str, Any]:
    """
    Build entity dict for KeyExtractionEngine: view properties + optional table_data from RAW joins.

    Populates rule-prefixed keys (ruleId_fieldName) for view fields and table_data keys
    (ruleId_tableId_fieldName) for RAW-backed fields, matching engine _get_field_value.
    """
    entity_props = entity_props_for_view(instance, entity_view_id)
    out: Dict[str, Any] = {
        "entity_type": entity_view_config.entity_type.value,
        "view_space": entity_view_id.space,
        "view_external_id": entity_view_id.external_id,
        "view_version": entity_view_id.version,
        "instance_space": (
            getattr(entity_view_config, "instance_space", None)
            or getattr(instance, "space", None)
        ),
        "table_data": {},
    }
    kex_iso = getattr(entity_view_config, "exclude_self_referencing_keys", None)
    if kex_iso is not None:
        out["_kex_exclude_self_referencing_keys"] = kex_iso
    if not isinstance(extraction_rules, list) or not extraction_rules:
        return out

    for rule in extraction_rules:
        rid = get_rule_id(rule)
        if not rid or rid == "unknown":
            continue
        for sf in _iter_rule_source_fields(rule):
            fn = _sf_field_name(sf)
            if not fn:
                continue
            tid = _sf_table_id(sf)
            required = _sf_required(sf)
            pre = _sf_preprocessing(sf)
            field_value = None
            if tid:
                col = f"{tid}__{fn}"
                if merged_columns is not None:
                    field_value = merged_columns.get(col)
                if field_value is None and required:
                    logger.verbose(
                        "WARNING",
                        f"Missing joined RAW column {col!r} for entity {instance.external_id}",
                    )
            else:
                field_value = entity_props.get(fn)
                if field_value is None and required:
                    logger.verbose(
                        "WARNING",
                        f"Missing view field {fn!r} for entity {instance.external_id}",
                    )
            if field_value is None:
                continue
            if not isinstance(field_value, str):
                field_value = str(field_value)
            if pre:
                field_value = _apply_preprocessing(field_value, pre)
            if tid:
                tkey = "_".join([rid, tid, fn])
                out["table_data"][tkey] = field_value
            else:
                out["_".join([rid, fn])] = field_value
    return out


def _get_target_entities_cdf(
    client: CogniteClient,
    config: Any,
    logger: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Get entities from CDF data model views.

    When ``config.data.source_tables`` is non-empty, loads each RAW table once (cached),
    left-joins to instances on configured join keys, and fills ``table_data`` for
    ``source_field.table_id`` references (see key extraction engine ``_get_field_value``).
    """
    entities_source: Dict[str, Dict[str, Any]] = {}
    source_views = getattr(config.data, "source_views", None) or (
        [config.data.source_view] if getattr(config.data, "source_view", None) else []
    )
    if not source_views:
        logger.error("No Source View(s) defined for key extraction")
        raise ValueError("No Source View(s) defined for key extraction")

    raw_db = config.parameters.raw_db
    raw_table_key = config.parameters.raw_table_key
    overwrite = config.parameters.overwrite
    excluded_entities: List[str] = []
    if not overwrite:
        policy = getattr(config.parameters, "skip_entity_policy", "successful_only")
        chunk = getattr(config.parameters, "raw_skip_scan_chunk_size", 5000)
        if policy == "none":
            excluded_entities = []
        else:
            excluded_entities = list(
                dict.fromkeys(
                    _read_entity_keys_to_exclude(
                        client, raw_db, raw_table_key, policy, chunk
                    )
                )
            )
        logger.debug(f"Excluding {len(excluded_entities)} existing entities (policy={policy!r})")

    max_files = getattr(getattr(config, "parameters", None), "max_files", None)
    if isinstance(max_files, bool):
        max_files = None
    if isinstance(max_files, int) and max_files <= 0:
        max_files = None

    source_tables = getattr(config.data, "source_tables", None) or []
    extraction_rules = getattr(getattr(config, "data", None), "extraction_rules", None)
    raw_lookups: Dict[Tuple[str, str], Optional[Dict[str, Dict[str, Any]]]] = {}
    if source_tables:
        raw_lookups = preload_raw_lookups(client, list(source_tables), logger)

    for entity_view_config in source_views:
        entity_view_id = entity_view_config.as_view_id()
        logger.debug(f"Querying view: {entity_view_id}")
        filter_expr = _build_filter(
            entity_view_config,
            excluded_entities,
            logger,
        )

        try:
            remaining = None
            if isinstance(max_files, int):
                remaining = max_files - len(entities_source)
                if remaining <= 0:
                    break

            instances = client.data_modeling.instances.list(
                instance_type="node",
                space=getattr(entity_view_config, "instance_space", None),
                sources=[entity_view_id],
                filter=filter_expr,
                limit=min(entity_view_config.batch_size, remaining)
                if isinstance(remaining, int)
                else entity_view_config.batch_size,
            )

            logger.debug(
                f"Retrieved {len(instances)} instances from view: {entity_view_id}"
            )

            if source_tables:
                if not len(instances):
                    logger.info(f"No instances for view {entity_view_id}; skipping joins.")
                    continue
                for instance in instances:
                    entity_external_id = instance.external_id
                    if entity_external_id in entities_source:
                        continue
                    eprops = entity_props_for_view(instance, entity_view_id)
                    merged_cols = merged_join_columns_for_instance(
                        eprops, list(source_tables), raw_lookups
                    )
                    entities_source[entity_external_id] = _build_entity_payload_with_rules(
                        instance=instance,
                        merged_columns=merged_cols,
                        extraction_rules=extraction_rules,
                        entity_view_id=entity_view_id,
                        entity_view_config=entity_view_config,
                        logger=logger,
                    )
                    if isinstance(max_files, int) and len(entities_source) >= max_files:
                        break
            else:
                for instance in instances:
                    entity_external_id = instance.external_id
                    entity_props = (
                        instance.dump()
                        .get("properties", {})
                        .get(entity_view_id.space, {})
                        .get(
                            f"{entity_view_id.external_id}/{entity_view_id.version}",
                            {},
                        )
                    )

                    if entity_external_id not in entities_source:
                        row: Dict[str, Any] = {
                            "entity_type": entity_view_config.entity_type.value,
                            "view_space": entity_view_id.space,
                            "view_external_id": entity_view_id.external_id,
                            "view_version": entity_view_id.version,
                            "instance_space": (
                                getattr(entity_view_config, "instance_space", None)
                                or getattr(instance, "space", None)
                            ),
                        }
                        kex_iso = getattr(
                            entity_view_config, "exclude_self_referencing_keys", None
                        )
                        if kex_iso is not None:
                            row["_kex_exclude_self_referencing_keys"] = kex_iso
                        entities_source[entity_external_id] = row

                    wanted_fields: List[tuple[str, bool, list[str]]] = []
                    if isinstance(extraction_rules, list) and extraction_rules:
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
                        for p in list(
                            getattr(entity_view_config, "include_properties", []) or []
                        ):
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
    entity_config: Any, excluded_entities: list[str], logger: Any
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


