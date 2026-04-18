"""
CDF Pipeline for Tag Aliasing

This module provides the main pipeline function that processes tags
and generates aliases using the AliasingEngine.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .common.logger import CogniteFunctionLogger
from cdf_fn_common.incremental_scope import (
    EXTERNAL_ID_COLUMN,
    RAW_ROW_KEY_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RECORD_KIND_RUN,
    RECORD_KIND_WATERMARK,
    RUN_ID_COLUMN,
    WORKFLOW_STATUS_COLUMN,
    WORKFLOW_STATUS_EXTRACTED,
    WORKFLOW_STATUS_ALIASED,
    discover_single_run_id_for_status,
    norm_workflow_status,
    raw_row_columns,
    transition_workflow_status_for_run,
)
from .engine.tag_aliasing_engine import AliasingEngine, AliasingResult

logger = None  # Use CogniteFunctionLogger directly


def _iter_raw_table_rows(
    client: CogniteClient,
    raw_db: str,
    raw_table_key: str,
    *,
    chunk_size: int = 2500,
    max_rows: Optional[int] = None,
) -> Iterable[Any]:
    """
    Iterate RAW rows using the SDK iterator when available (chunked reads),
    else fall back to raw.rows.list.
    """
    rows_api = client.raw.rows
    if callable(rows_api):
        yielded = 0
        for item in rows_api(raw_db, raw_table_key, chunk_size=chunk_size):
            # SDK iterator may yield either single Row objects or per-page lists.
            if hasattr(item, "columns"):
                yield item
                yielded += 1
                if max_rows is not None and yielded >= max_rows:
                    return
                continue

            if isinstance(item, (list, tuple)):
                for row in item:
                    if not hasattr(row, "columns"):
                        continue
                    yield row
                    yielded += 1
                    if max_rows is not None and yielded >= max_rows:
                        return
                continue

            # Defensive fallback for other iterable chunk types (e.g. RowList).
            if hasattr(item, "__iter__") and not isinstance(item, (str, bytes, dict)):
                for row in item:
                    if not hasattr(row, "columns"):
                        continue
                    yield row
                    yielded += 1
                    if max_rows is not None and yielded >= max_rows:
                        return
        return
    limit = max_rows if max_rows is not None else None
    listed = rows_api.list(raw_db, raw_table_key, limit=limit)
    for row in listed:
        yield row


def _load_candidate_keys_from_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_key: str,
    *,
    instance_space: str,
    view_space: str,
    view_external_id: str,
    view_version: str,
    entity_type: str,
    logger: Any,
    limit: Optional[int] = 10000,
    chunk_size: int = 2500,
    run_id: Optional[str] = None,
    workflow_status: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load candidate keys from key-extraction RAW table and build a tag->entity mapping.

    Expected schema (written by Key-Extraction):
      - row.key: entity external id
      - row.columns: { <FIELD_NAME_UPPER>: [<candidate_key_str>, ...], ... }

    When ``limit`` is set, reads at most that many rows (iterator API) and warns if
    the table may have more data. Pass ``limit=None`` to read all rows (use with care).
    """
    tag_to_entity_map: Dict[str, List[Dict[str, Any]]] = {}
    row_count = 0
    truncated = False
    wf_filter = (
        norm_workflow_status(workflow_status) if workflow_status else None
    )
    for row in _iter_raw_table_rows(
        client,
        raw_db,
        raw_table_key,
        chunk_size=chunk_size,
        max_rows=limit,
    ):
        row_count += 1
        columns = raw_row_columns(row)
        rkind = columns.get(RECORD_KIND_COLUMN)
        if rkind in (RECORD_KIND_WATERMARK, RECORD_KIND_RUN):
            continue
        if run_id:
            if rkind == RECORD_KIND_ENTITY:
                if str(columns.get(RUN_ID_COLUMN) or "") != str(run_id):
                    continue
            elif rkind is None:
                continue
            if wf_filter:
                st = norm_workflow_status(columns.get(WORKFLOW_STATUS_COLUMN))
                if st and st != wf_filter:
                    continue
                if not st and wf_filter != WORKFLOW_STATUS_EXTRACTED:
                    continue
        elif wf_filter:
            st = norm_workflow_status(columns.get(WORKFLOW_STATUS_COLUMN))
            if rkind == RECORD_KIND_ENTITY and st and st != wf_filter:
                continue
        entity_id = columns.get(EXTERNAL_ID_COLUMN) or getattr(row, "key", None)
        if not entity_id:
            continue
        if not isinstance(columns, dict):
            continue

        for field_name, values in columns.items():
            if not isinstance(values, list):
                continue
            if str(field_name).upper() in (
                RECORD_KIND_COLUMN,
                RUN_ID_COLUMN,
                WORKFLOW_STATUS_COLUMN,
                EXTERNAL_ID_COLUMN,
                RAW_ROW_KEY_COLUMN,
            ):
                continue
            for v in values:
                if not isinstance(v, str) or not v:
                    continue
                tag_to_entity_map.setdefault(v, []).append(
                    {
                        "entity_id": entity_id,
                        "field_name": str(field_name).lower(),
                        "view_space": view_space,
                        "view_external_id": view_external_id,
                        "view_version": view_version,
                        "instance_space": instance_space,
                        "entity_type": entity_type,
                        "raw_row_key": str(columns.get(RAW_ROW_KEY_COLUMN) or getattr(row, "key", "") or ""),
                    }
                )

    if limit is not None and row_count >= limit:
        truncated = True
        logger.warning(
            "RAW read for candidate keys reached row limit=%s (db=%s table=%s). "
            "Tags may be incomplete; raise limit via source_raw_read_limit or set "
            "environment variable CDF_RAW_TAG_SOURCE_MAX_ROWS (if supported by caller).",
            limit,
            raw_db,
            raw_table_key,
        )

    logger.info(
        f"Loaded {len(tag_to_entity_map)} unique tags from {row_count} RAW rows "
        f"(db={raw_db} table={raw_table_key}"
        + (", truncated" if truncated else "")
        + ")"
    )
    return tag_to_entity_map


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
        run_started_at = datetime.now(timezone.utc)

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
                                rn = None
                                if isinstance(key_info, dict):
                                    rn = key_info.get("rule_name")
                                tag_to_entity_map[key_value].append(
                                    {
                                        "entity_id": entity_id,
                                        "field_name": field_name,
                                        "view_space": view_space,
                                        "view_external_id": view_external_id,
                                        "view_version": view_version,
                                        "instance_space": instance_space,
                                        "entity_type": entity_type,
                                        "rule_name": rn,
                                    }
                                )

                logger.info(
                    f"Extracted {len(tags)} unique candidate key tags from key extraction results"
                )
                # Store tag mapping for alias persistence
                data["_tag_to_entity_map"] = tag_to_entity_map
            else:
                # Workflow-friendly fallback: load tags from key-extraction RAW table.
                source_raw_db = data.get("source_raw_db")
                source_raw_table_key = data.get("source_raw_table_key")
                if client and source_raw_db and source_raw_table_key:
                    instance_space = data.get("source_instance_space")
                    view_space = data.get("source_view_space")
                    view_external_id = data.get("source_view_external_id")
                    view_version = data.get("source_view_version")
                    entity_type = data.get("source_entity_type", "file")

                    if not (
                        instance_space
                        and view_space
                        and view_external_id
                        and view_version
                        and entity_type
                    ):
                        raise ValueError(
                            "Missing RAW fallback mapping inputs. "
                            "Expected source_instance_space/source_view_space/source_view_external_id/source_view_version/source_entity_type."
                        )

                    raw_limit = int(data.get("source_raw_read_limit", 10000))
                    src_run = data.get("source_run_id")
                    if not src_run and data.get("incremental_auto_run_id"):
                        src_run = discover_single_run_id_for_status(
                            client,
                            source_raw_db,
                            source_raw_table_key,
                            WORKFLOW_STATUS_EXTRACTED,
                        )
                        if src_run:
                            data["source_run_id"] = src_run
                    wf_src = data.get("source_workflow_status") or (
                        WORKFLOW_STATUS_EXTRACTED if src_run else None
                    )
                    logger.info(
                        "No entities_keys_extracted provided; loading tags from RAW "
                        f"db={source_raw_db} table={source_raw_table_key} limit={raw_limit}"
                        + (f" run_id={src_run}" if src_run else "")
                    )
                    tag_to_entity_map = _load_candidate_keys_from_raw(
                        client=client,
                        raw_db=source_raw_db,
                        raw_table_key=source_raw_table_key,
                        instance_space=str(instance_space),
                        view_space=str(view_space),
                        view_external_id=str(view_external_id),
                        view_version=str(view_version),
                        entity_type=str(entity_type),
                        logger=logger,
                        limit=raw_limit,
                        run_id=str(src_run) if src_run else None,
                        workflow_status=str(wf_src) if wf_src else None,
                    )
                    data["_tag_to_entity_map"] = tag_to_entity_map
                    tags = list(tag_to_entity_map.keys())

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

        progress_every = max(0, int(data.get("progress_every", 0) or 0))
        if progress_every > 0:
            logger.info(
                "Aliasing: progress log every %s tag(s) (%s total)",
                progress_every,
                len(tags),
            )

        # Process each tag
        aliasing_results = []
        tag_to_entity_map = data.get("_tag_to_entity_map", {})

        for i, tag in enumerate(tags):
            # Get context from corresponding entity if available
            context: Optional[Dict[str, Any]] = None
            entity_type = None
            entity_info_list = tag_to_entity_map.get(tag, [])

            if entities and i < len(entities):
                entity = entities[i]
                entity_type = entity.get("entity_type")
                context = dict(entity.get("context") or {})
                # Also include entity metadata as context
                if "metadata" in entity:
                    context.update(entity["metadata"])
                rn = entity.get("extraction_rule_name") or entity.get("rule_name")
                if rn:
                    context.setdefault("extraction_rule_name", str(rn).strip())
                    context.setdefault("rule_name", str(rn).strip())
            elif entity_info_list:
                # Use first entity info for context
                first_entity_info = entity_info_list[0]
                entity_type = first_entity_info.get("entity_type")
                context = dict(first_entity_info.get("context") or {})

            ext_pipes = getattr(engine, "_extraction_aliasing_pipelines", None) or {}
            rule_name_set: List[str] = []
            for e in entity_info_list:
                if not isinstance(e, dict):
                    continue
                rn = e.get("rule_name")
                if rn is not None and str(rn).strip():
                    rule_name_set.append(str(rn).strip())
            rule_name_set = sorted(set(rule_name_set))

            if ext_pipes:
                if not rule_name_set:
                    rule_iter: List[Optional[str]] = [None]
                else:
                    rule_iter = rule_name_set
                merged_aliases: List[str] = []
                seen_alias: Set[str] = set()
                applied_rules_acc: List[str] = []
                meta_ctx: Any = None
                for rk in rule_iter:
                    ctx = dict(context or {})
                    if rk:
                        ctx["extraction_rule_name"] = rk
                        ctx["rule_name"] = rk
                    result = engine.generate_aliases(
                        tag=tag, entity_type=entity_type, context=ctx
                    )
                    meta_ctx = result.metadata
                    for ar in result.metadata.get("applied_rules") or []:
                        if ar not in applied_rules_acc:
                            applied_rules_acc.append(ar)
                    for a in result.aliases:
                        if a not in seen_alias:
                            seen_alias.add(a)
                            merged_aliases.append(a)
                result_metadata = dict(meta_ctx) if isinstance(meta_ctx, dict) else {}
                result_metadata["applied_rules"] = applied_rules_acc
                result_metadata["context"] = context
                aliasing_result = {
                    "original_tag": tag,
                    "aliases": merged_aliases,
                    "metadata": result_metadata,
                }
            else:
                result = engine.generate_aliases(
                    tag=tag, entity_type=entity_type, context=context
                )
                aliasing_result = {
                    "original_tag": result.original_tag,
                    "aliases": result.aliases,
                    "metadata": result.metadata,
                }

            # Include entity information for persistence
            if entity_info_list:
                aliasing_result["entities"] = entity_info_list

            aliasing_results.append(aliasing_result)

            logger.debug(
                "Generated %s aliases for tag %r",
                len(aliasing_result.get("aliases") or []),
                tag,
            )

            tag_n = i + 1
            if progress_every > 0 and tag_n % progress_every == 0:
                acc_aliases = sum(len(r.get("aliases") or []) for r in aliasing_results)
                logger.info(
                    "Aliasing progress: %s/%s tags, %s aliases accumulated",
                    tag_n,
                    len(tags),
                    acc_aliases,
                )

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

        if (
            client
            and data.get("incremental_transition", True)
            and data.get("source_run_id")
            and data.get("source_raw_db")
            and data.get("source_raw_table_key")
        ):
            n = transition_workflow_status_for_run(
                client,
                str(data["source_raw_db"]),
                str(data["source_raw_table_key"]),
                str(data["source_run_id"]),
                WORKFLOW_STATUS_EXTRACTED,
                WORKFLOW_STATUS_ALIASED,
            )
            logger.info(
                f"Key-extraction RAW WORKFLOW_STATUS: {n} row(s) extracted -> aliased "
                f"(run_id={data['source_run_id']})"
            )
            data["key_extraction_workflow_rows_updated"] = n

        # Optionally upload to CDF RAW if client is provided
        if client and data.get("upload_to_raw", False):
            raw_db = data.get("raw_db", "aliasing_db")
            raw_table = data.get("raw_table", "aliases")
            raw_table_state = data.get("raw_table_state")

            _upload_aliases_to_raw(client, raw_db, raw_table, aliasing_results, logger)

            # Write a lightweight state row (parity with key extraction).
            if raw_table_state:
                _create_table_if_not_exists(client, raw_db, raw_table_state, logger)
                run_finished_at = datetime.now(timezone.utc)
                run_duration_s = (run_finished_at - run_started_at).total_seconds()
                run_duration_ms = int(run_duration_s * 1000)

                # Aggregate some useful audit metrics.
                unique_entities = set()
                rules_applied_counts: Dict[str, int] = {}
                for r in aliasing_results:
                    for e in (r.get("entities") or []):
                        ent = e.get("entity_id")
                        if ent:
                            unique_entities.add(str(ent))
                    meta = r.get("metadata") or {}
                    applied = meta.get("applied_rules") or []
                    if isinstance(applied, list):
                        for rule_name in applied:
                            rules_applied_counts[str(rule_name)] = (
                                rules_applied_counts.get(str(rule_name), 0) + 1
                            )

                status = "success" if tags else "failure"
                message = (
                    f"Generated {data['total_aliases_generated']} aliases for {len(tags)} tags"
                    if tags
                    else "No tags were processed"
                )
                state_key = run_finished_at.strftime("%Y%m%dT%H%M%S.%fZ")
                try:
                    from cognite.client.data_classes import Row

                    client.raw.rows.insert(
                        raw_db,
                        raw_table_state,
                        Row(
                            key=str(state_key),
                            columns={
                                "status": status,
                                "message": message,
                                "total_tags_processed": int(
                                    data.get("total_tags_processed", 0)
                                ),
                                "total_aliases_generated": int(
                                    data.get("total_aliases_generated", 0)
                                ),
                                "unique_entities_covered": len(unique_entities),
                                "run_started_at": run_started_at.isoformat(),
                                "run_finished_at": run_finished_at.isoformat(),
                                "run_duration_s": run_duration_s,
                                "run_duration_ms": run_duration_ms,
                                "raw_db": raw_db,
                                "raw_table_aliases": raw_table,
                                "raw_table_state": raw_table_state,
                                "rules_applied_counts_json": json.dumps(
                                    rules_applied_counts
                                ),
                            },
                        ),
                    )
                    logger.info(
                        f"Wrote aliasing state row to RAW: db={raw_db} table={raw_table_state} key={state_key}"
                    )
                except Exception as e:
                    logger.warning(f"Failed writing aliasing state row to RAW: {e}")

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
        import os

        from cognite.client.data_classes import Row

        from cdf_fn_common.raw_upload import create_raw_upload_queue

        # Ensure tables exist
        _create_table_if_not_exists(client, raw_db, raw_table, logger)

        max_q = int(
            os.environ.get(
                "CDF_RAW_UPLOAD_MAX_QUEUE_SIZE_ALIASES",
                os.environ.get("CDF_RAW_UPLOAD_MAX_QUEUE_SIZE", "50000"),
            )
        )
        raw_uploader = create_raw_upload_queue(
            client, max_queue_size=max_q, trigger_log_level="INFO"
        )

        # Add rows
        for result in aliasing_results:
            entities_json = None
            entities = result.get("entities")
            if entities is not None:
                # Ensure we store JSON to make downstream parsing reliable.
                try:
                    entities_json = json.dumps(entities)
                except Exception:
                    entities_json = json.dumps([])

            row = Row(
                key=result["original_tag"],
                columns={
                    "aliases": result["aliases"],
                    "total_aliases": len(result["aliases"]),
                    # Store metadata + entity mappings as JSON strings so other workflow steps
                    # can read them back and map tag->entity nodes.
                    "metadata_json": json.dumps(result.get("metadata", {})),
                    "entities_json": entities_json,
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
