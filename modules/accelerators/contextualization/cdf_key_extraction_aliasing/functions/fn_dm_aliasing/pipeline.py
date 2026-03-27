"""
CDF Pipeline for Tag Aliasing

This module provides the main pipeline function that processes tags
and generates aliases using the AliasingEngine.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .common.logger import CogniteFunctionLogger
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
        count = 0
        for row in rows_api(raw_db, raw_table_key, chunk_size=chunk_size):
            yield row
            count += 1
            if max_rows is not None and count >= max_rows:
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
    for row in _iter_raw_table_rows(
        client,
        raw_db,
        raw_table_key,
        chunk_size=chunk_size,
        max_rows=limit,
    ):
        row_count += 1
        entity_id = getattr(row, "key", None)
        if not entity_id:
            continue
        columns = getattr(row, "columns", {}) or {}
        if not isinstance(columns, dict):
            continue

        for field_name, values in columns.items():
            if not isinstance(values, list):
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
                    logger.info(
                        "No entities_keys_extracted provided; loading tags from RAW "
                        f"db={source_raw_db} table={source_raw_table_key} limit={raw_limit}"
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

        from ..cdf_fn_common.raw_upload import create_raw_upload_queue

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
