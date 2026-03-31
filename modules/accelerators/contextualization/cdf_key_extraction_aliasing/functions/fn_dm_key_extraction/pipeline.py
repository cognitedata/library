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
from cognite.client.data_classes.data_modeling.ids import NodeId

from ..cdf_fn_common.full_rescan import resolve_full_rescan
from ..cdf_fn_common.extraction_input_hash import (
    apply_preprocessing,
    compute_extraction_inputs_hash_from_entity_row,
    iter_wanted_fields,
    resolve_source_view_config_for_entity,
)
from ..cdf_fn_common.incremental_scope import (
    EXTRACTION_INPUTS_HASH_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RAW_ROW_KEY_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_DETECTED,
    WORKFLOW_STATUS_EXTRACTED,
    WORKFLOW_STATUS_FAILED,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    discover_single_run_id_for_status,
    iter_cohort_entity_rows,
    norm_workflow_status,
    raw_row_columns,
)
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
DOCUMENT_REFERENCES_JSON_COLUMN = "DOCUMENT_REFERENCES_JSON"

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
    return {
        k: v
        for k, v in em.items()
        if not str(k).startswith("_extraction")
        and k not in ("_cohort_columns", "_raw_row_key")
    }


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


def _dedupe_document_references(result: ExtractionResult) -> List[Dict[str, Any]]:
    """One entry per distinct document reference value; keep highest confidence."""
    best: Dict[str, Dict[str, Any]] = {}
    for doc in result.document_references:
        v = getattr(doc, "value", None) or ""
        if not v:
            continue
        conf = float(getattr(doc, "confidence", 0.0) or 0.0)
        entry = {
            "value": v,
            "confidence": conf,
            "source_field": getattr(doc, "source_field", None),
            "rule_id": getattr(doc, "rule_id", None),
        }
        if v not in best or conf > best[v]["confidence"]:
            best[v] = entry
    return list(best.values())


def _resolve_incremental_run_id(
    client: CogniteClient,
    raw_db: str,
    raw_table_key: str,
    data: Dict[str, Any],
) -> Optional[str]:
    rid = data.get("run_id")
    if rid:
        return str(rid)
    return discover_single_run_id_for_status(
        client, raw_db, raw_table_key, WORKFLOW_STATUS_DETECTED
    )


def _find_view_config_for_row(
    source_views: List[Any], cols: Dict[str, Any]
) -> Optional[Any]:
    vs = cols.get("view_space")
    ve = cols.get("view_external_id")
    vv = cols.get("view_version")
    if not vs or not ve or not vv:
        return None
    for svc in source_views:
        if (
            getattr(svc, "view_space", None) == vs
            and getattr(svc, "view_external_id", None) == ve
            and getattr(svc, "view_version", None) == vv
        ):
            return svc
    return None


def _load_incremental_cohort_entities(
    client: CogniteClient,
    config: Any,
    logger: Any,
    run_id: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Load instances for RAW cohort rows (RUN_ID + WORKFLOW_STATUS=detected).
    Attaches ``_cohort_columns`` and ``_raw_row_key`` for downstream RAW merges.
    """
    raw_db = config.parameters.raw_db
    raw_table_key = config.parameters.raw_table_key
    rows = iter_cohort_entity_rows(
        client,
        raw_db,
        raw_table_key,
        run_id,
        WORKFLOW_STATUS_DETECTED,
    )
    if not rows:
        logger.info("No cohort rows with WORKFLOW_STATUS=detected for this run_id")
        return {}

    source_views = getattr(config.data, "source_views", None) or (
        [config.data.source_view] if getattr(config.data, "source_view", None) else []
    )
    if not source_views:
        raise ValueError("No source_views configured")

    entities_source: Dict[str, Dict[str, Any]] = {}
    extraction_rules = getattr(getattr(config, "data", None), "extraction_rules", None)
    source_tables = getattr(config.data, "source_tables", None) or []

    # Retrieve instances in batches grouped by view
    batch: List[Any] = []
    batch_meta: List[Dict[str, Any]] = []

    def flush_batch() -> None:
        nonlocal batch, batch_meta
        if not batch:
            return
        # Group consecutive batches by view for retrieve_nodes (one view per API call).
        def _view_tuple(vc: Any) -> tuple:
            return (
                getattr(vc, "view_space", None),
                getattr(vc, "view_external_id", None),
                getattr(vc, "view_version", None),
            )

        i = 0
        while i < len(batch):
            meta_i = batch_meta[i]
            evc = meta_i["view_config"]
            vid = evc.as_view_id()
            vt = _view_tuple(evc)
            sub_nodes: List[NodeId] = []
            sub_meta: List[Dict[str, Any]] = []
            j = i
            while j < len(batch) and _view_tuple(batch_meta[j]["view_config"]) == vt:
                sub_nodes.append(batch[j])
                sub_meta.append(batch_meta[j])
                j += 1
            i = j
            try:
                retrieved = client.data_modeling.instances.retrieve_nodes(
                    nodes=sub_nodes,
                    sources=[vid],
                )
            except Exception as ex:
                logger.error(f"retrieve_nodes failed: {ex}")
                retrieved = []

            by_key = {
                (getattr(n, "space", None), getattr(n, "external_id", None)): n
                for n in (retrieved or [])
            }
            for node_id, meta in zip(sub_nodes, sub_meta):
                inst = by_key.get((node_id.space, node_id.external_id))
                if inst is None:
                    logger.warning(
                        f"Instance not found for cohort row key={meta.get('raw_row_key')!r}"
                    )
                    continue
                entity_external_id = inst.external_id
                entity_props = (
                    inst.dump()
                    .get("properties", {})
                    .get(vid.space, {})
                    .get(f"{vid.external_id}/{vid.version}", {})
                )
                evc = meta["view_config"]
                if entity_external_id not in entities_source:
                    entities_source[entity_external_id] = {
                        "entity_type": evc.entity_type.value,
                        "view_space": vid.space,
                        "view_external_id": vid.external_id,
                        "view_version": vid.version,
                        "instance_space": getattr(evc, "instance_space", None)
                        or getattr(inst, "space", None),
                        "_cohort_columns": dict(meta["cohort_columns"]),
                        "_raw_row_key": meta["raw_row_key"],
                    }
                    kex_iso = getattr(evc, "exclude_self_referencing_keys", None)
                    if kex_iso is not None:
                        entities_source[entity_external_id][
                            "_kex_exclude_self_referencing_keys"
                        ] = kex_iso
                wanted_fields = iter_wanted_fields(extraction_rules, evc)

                tgt = entities_source[entity_external_id]
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
                        field_value = apply_preprocessing(str(field_value), preprocessing)
                    tgt[field_name] = field_value

        batch = []
        batch_meta = []

    for row in rows:
        cols = raw_row_columns(row)
        if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_ENTITY:
            continue
        if str(cols.get(RUN_ID_COLUMN) or "") != run_id:
            continue
        st = norm_workflow_status(cols.get(WORKFLOW_STATUS_COLUMN))
        if st and st != WORKFLOW_STATUS_DETECTED:
            continue
        evc = _find_view_config_for_row(source_views, cols)
        if evc is None:
            logger.warning("Skipping cohort row with unknown view metadata")
            continue
        ext = cols.get(EXTERNAL_ID_COLUMN) or getattr(row, "key", None)
        isp = cols.get("instance_space") or getattr(evc, "instance_space", None)
        if not ext or not isp:
            logger.warning("Cohort row missing external_id or instance_space; skipping")
            continue
        nid = NodeId(space=str(isp), external_id=str(ext))
        rk = cols.get(RAW_ROW_KEY_COLUMN) or getattr(row, "key", None)
        batch.append(nid)
        batch_meta.append(
            {
                "view_config": evc,
                "cohort_columns": cols,
                "raw_row_key": str(rk) if rk else "",
            }
        )
        if len(batch) >= 100:
            flush_batch()
    flush_batch()

    if source_tables:
        logger.warning(
            "incremental cohort mode with source_tables: RAW joins not applied; "
            "use non-incremental run or extend cohort loader."
        )

    logger.info(
        f"Incremental cohort loaded {len(entities_source)} entities for run_id={run_id}"
    )
    return entities_source


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
    run_id = ""
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
            _fr = resolve_full_rescan(cdf_config.parameters, data)
            if hasattr(cdf_config, "model_copy"):
                cdf_config = cdf_config.model_copy(
                    update={
                        "parameters": cdf_config.parameters.model_copy(
                            update={"full_rescan": _fr}
                        )
                    }
                )
            else:
                cdf_config.parameters.full_rescan = _fr
            # Extract parameters from CDF config
            raw_db = cdf_config.parameters.raw_db
            raw_table_key = cdf_config.parameters.raw_table_key
            write_empty_extraction_rows = bool(
                cdf_config.parameters.write_empty_extraction_rows
            )

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_key={raw_table_key}"
            )

            incremental = bool(
                getattr(cdf_config.parameters, "incremental_change_processing", False)
            )
            if incremental:
                rid = _resolve_incremental_run_id(client, raw_db, raw_table_key, data)
                if not rid:
                    raise ValueError(
                        "incremental key extraction requires run_id in function data "
                        "or exactly one RUN_ID cohort with WORKFLOW_STATUS=detected in RAW"
                    )
                run_id = rid
                data["run_id"] = run_id
            else:
                run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
                data["run_id"] = run_id

            # Initialize RAW upload queue (size from CDF_RAW_UPLOAD_MAX_QUEUE_SIZE or default)
            raw_uploader = create_raw_upload_queue(client)

            # Get entities from source view (or incremental RAW cohort)
            entities_source_fields = _get_target_entities_cdf(
                client, cdf_config, logger, data
            )

        else:
            # Standalone mode - entities should be provided in data
            entities_source_fields = data.get("entities", {})
            if not run_id:
                run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
                data["run_id"] = run_id
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

        incremental_mode = bool(
            cdf_config
            and getattr(
                getattr(cdf_config, "parameters", None),
                "incremental_change_processing",
                False,
            )
        )

        # Serial extraction per entity; instance listing uses batch_size on the source view upstream.
        for entity_id, entity_fields in entities_source_fields.items():
            ef = dict(entity_fields)
            kex_iso = ef.pop("_kex_exclude_self_referencing_keys", None)
            cohort_columns = ef.pop("_cohort_columns", None)
            raw_row_key = ef.pop("_raw_row_key", None)
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
                "_cohort_columns": cohort_columns,
                "_raw_row_key": raw_row_key,
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
                    "document_references": [],
                    "_extraction_failed": True,
                    "_extraction_error": str(ex)[:4000],
                }
                continue

            for k in result.candidate_keys:
                rule_name = getattr(k, "rule_name", None) or getattr(k, "rule_id", None)
                if rule_name:
                    rules_used_counts[str(rule_name)] = rules_used_counts.get(
                        str(rule_name), 0
                    ) + 1

            keys = {}
            for key in result.candidate_keys:
                field_name = key.source_field
                if field_name not in keys:
                    keys[field_name] = {}
                key_rule_name = getattr(key, "rule_name", None) or getattr(
                    key, "rule_id", None
                )
                keys[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": key.extraction_type.value,
                    "rule_name": key_rule_name,
                    "matched_source_field": key.source_field,
                    "rule_source_fields": rule_source_fields_map.get(key_rule_name, []),
                }

            fk_refs = _dedupe_foreign_key_references(result)
            doc_refs = _dedupe_document_references(result)
            entities_keys_extracted[entity_id] = {
                **base_meta,
                "keys": keys,
                "foreign_key_references": fk_refs,
                "document_references": doc_refs,
            }
            keys_extracted += len(result.candidate_keys)

        logger.info(
            f"Extracted {keys_extracted} keys from {len(entities_keys_extracted)} entities"
        )

        # Upload to RAW: entity rows + run summary in raw_table_key.
        if use_cdf_format:
            _create_table_if_not_exists(client, raw_db, raw_table_key, logger)
            batch_updated_at = datetime.now(timezone.utc)
            incremental_skip_hash = bool(
                getattr(
                    getattr(cdf_config, "parameters", None),
                    "incremental_skip_unchanged_source_inputs",
                    False,
                )
            )
            source_views_for_hash = (
                getattr(cdf_config.data, "source_views", None)
                or (
                    [cdf_config.data.source_view]
                    if getattr(cdf_config.data, "source_view", None)
                    else []
                )
            )
            extraction_rules_for_hash = getattr(
                cdf_config.data, "extraction_rules", None
            )

            for ext_id, entity_metadata in entities_keys_extracted.items():
                field_keys = entity_metadata.get("keys", {})
                fk_refs = entity_metadata.get("foreign_key_references") or []
                doc_refs = entity_metadata.get("document_references") or []
                failed = bool(entity_metadata.get("_extraction_failed"))
                err_msg = entity_metadata.get("_extraction_error")
                cohort_columns = entity_metadata.get("_cohort_columns")
                raw_row_key = entity_metadata.get("_raw_row_key")
                has_candidate_columns = isinstance(field_keys, dict) and bool(
                    field_keys
                )
                has_fk = bool(fk_refs)
                has_doc = bool(doc_refs)
                if failed:
                    ext_status = EXTRACTION_STATUS_FAILED
                elif has_candidate_columns or has_fk or has_doc:
                    ext_status = EXTRACTION_STATUS_SUCCESS
                else:
                    ext_status = EXTRACTION_STATUS_EMPTY

                if (
                    not failed
                    and not has_candidate_columns
                    and not has_fk
                    and not has_doc
                    and not write_empty_extraction_rows
                ):
                    continue

                columns: Dict[str, Any] = {}
                if isinstance(cohort_columns, dict) and cohort_columns:
                    columns.update(dict(cohort_columns))
                columns.update(
                    {
                    RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
                    EXTRACTION_STATUS_COLUMN: ext_status,
                    RAW_COL_UPDATED_AT: batch_updated_at.isoformat(),
                    RAW_COL_RUN_ID: run_id,
                    }
                )
                if incremental_mode:
                    columns[WORKFLOW_STATUS_COLUMN] = (
                        WORKFLOW_STATUS_FAILED if failed else WORKFLOW_STATUS_EXTRACTED
                    )
                    columns[WORKFLOW_STATUS_UPDATED_AT_COLUMN] = (
                        batch_updated_at.isoformat()
                    )
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
                if has_doc:
                    columns[DOCUMENT_REFERENCES_JSON_COLUMN] = json.dumps(doc_refs)

                if incremental_mode and incremental_skip_hash:
                    svc = resolve_source_view_config_for_entity(
                        source_views_for_hash, entity_metadata
                    )
                    if svc is not None:
                        columns[EXTRACTION_INPUTS_HASH_COLUMN] = (
                            compute_extraction_inputs_hash_from_entity_row(
                                entity_metadata,
                                extraction_rules_for_hash,
                                svc,
                                logger=logger,
                            )
                        )

                raw_key = str(raw_row_key) if raw_row_key else str(ext_id)
                raw_uploader.add_to_upload_queue(
                    database=raw_db,
                    table=raw_table_key,
                    raw_row=Row(key=raw_key, columns=columns),
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
        total_doc = sum(
            len(em.get("document_references") or [])
            for em in entities_keys_extracted.values()
        )
        n_failed = sum(
            1 for em in entities_keys_extracted.values() if em.get("_extraction_failed")
        )
        if keys_extracted > 0 or total_fk > 0 or total_doc > 0:
            status = "success"
            message = (
                f"Successfully extracted {keys_extracted} candidate keys"
                + (f", {total_fk} foreign key reference(s)" if total_fk else "")
                + (f", {total_doc} document reference(s)" if total_doc else "")
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
                field_value = apply_preprocessing(field_value, pre)
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
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get entities from CDF data model views.

    When ``config.data.source_tables`` is non-empty, loads each RAW table once (cached),
    left-joins to instances on configured join keys, and fills ``table_data`` for
    ``source_field.table_id`` references (see key extraction engine ``_get_field_value``).

    When ``incremental_change_processing`` is true, loads instances for the RAW cohort
    (``data["run_id"]`` + ``WORKFLOW_STATUS=detected``) instead of listing views.
    """
    data = data or {}
    if bool(getattr(config.parameters, "incremental_change_processing", False)):
        run_id = data.get("run_id")
        if not run_id:
            logger.error("incremental mode requires data['run_id']")
            return {}
        return _load_incremental_cohort_entities(client, config, logger, str(run_id))

    entities_source: Dict[str, Dict[str, Any]] = {}
    source_views = getattr(config.data, "source_views", None) or (
        [config.data.source_view] if getattr(config.data, "source_view", None) else []
    )
    if not source_views:
        logger.error("No Source View(s) defined for key extraction")
        raise ValueError("No Source View(s) defined for key extraction")

    raw_db = config.parameters.raw_db
    raw_table_key = config.parameters.raw_table_key
    full_rescan = resolve_full_rescan(config.parameters, data)
    excluded_entities: List[str] = []
    if not full_rescan:
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

                    wanted_fields = iter_wanted_fields(
                        extraction_rules, entity_view_config
                    )

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
                            field_value = apply_preprocessing(
                                str(field_value), preprocessing
                            )
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


from .common.cdf_utils import create_table_if_not_exists as _create_table_if_not_exists


