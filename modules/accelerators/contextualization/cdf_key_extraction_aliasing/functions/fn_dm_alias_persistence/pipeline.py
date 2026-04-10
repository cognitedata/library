"""
CDF Pipeline for Alias Persistence

This module provides the main pipeline function that reads aliasing results
and writes aliases back to source entities in the CDF data model.
"""

import logging
import json
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId
    from cognite.client.data_classes.data_modeling.instances import (
        NodeApply,
        NodeOrEdgeData,
    )

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    NodeApply = None
    NodeOrEdgeData = None
    NodeId = None
    ViewId = None

from .common.logger import CogniteFunctionLogger

logger = None  # Use CogniteFunctionLogger directly

FOREIGN_KEY_REFERENCES_JSON_COLUMN = "FOREIGN_KEY_REFERENCES_JSON"


def _resolve_alias_writeback_property(data: Dict[str, Any]) -> str:
    """Target DM property name for alias list (default CogniteDescribable `aliases`)."""
    name = data.get("aliasWritebackProperty") or data.get("alias_writeback_property")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return "aliases"


def _truthy_flag(data: Dict[str, Any], camel: str, snake: str) -> bool:
    v = data.get(camel, data.get(snake))
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)


def _resolve_write_foreign_key_references(data: Dict[str, Any]) -> bool:
    return _truthy_flag(
        data, "writeForeignKeyReferences", "write_foreign_key_references"
    )


def _resolve_foreign_key_writeback_property(data: Dict[str, Any]) -> Optional[str]:
    name = data.get("foreignKeyWritebackProperty") or data.get(
        "foreign_key_writeback_property"
    )
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _resolve_persistence_apply_batch_size(data: Dict[str, Any]) -> int:
    """Max nodes per `instances.apply` call (camel or snake in task data). Default 1000, minimum 1."""
    raw = data.get("persistenceApplyBatchSize") or data.get(
        "persistence_apply_batch_size"
    )
    if raw is None:
        return 1000
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return 1000
    return max(1, n)


class _AliasApplyWorkItem(NamedTuple):
    entity_id: str
    node: Any
    aliases: List[str]
    fk_vals: List[str]


def _normalize_str_list(value: Any) -> List[str]:
    """Normalize scalar/list/raw value to sorted unique non-empty strings."""
    if value is None:
        return []
    if isinstance(value, list):
        vals = [str(v).strip() for v in value if v is not None and str(v).strip()]
        return sorted(set(vals))
    s = str(value).strip()
    if not s:
        return []
    return [s]


def _metrics_for_successful_apply(items: List[_AliasApplyWorkItem]) -> Tuple[int, int, int, int]:
    """Returns persisted_entities, aliases_persisted, foreign_keys_persisted, entities_fk_updated."""
    aliases_n = 0
    fk_n = 0
    entities_with_fk = 0
    for it in items:
        aliases_n += len(it.aliases)
        if it.fk_vals:
            entities_with_fk += 1
            fk_n += len(it.fk_vals)
    return (len(items), aliases_n, fk_n, entities_with_fk)


def _apply_alias_work_recursive(
    client: CogniteClient,
    logger: Any,
    items: List[_AliasApplyWorkItem],
) -> Tuple[int, int, int, int, int]:
    """
    Apply a list of nodes; on failure split into ~quarters until single-node applies.
    Returns (persisted_count, failed_count, aliases_persisted, foreign_keys_persisted, entities_fk_updated).
    """
    if not items:
        return (0, 0, 0, 0, 0)
    try:
        client.data_modeling.instances.apply(nodes=[it.node for it in items])
        pe, an, fn, ef = _metrics_for_successful_apply(items)
        logger.debug(f"Applied {len(items)} instance(s) in one request")
        return (pe, 0, an, fn, ef)
    except Exception as e:
        if len(items) == 1:
            eid = items[0].entity_id
            logger.error(
                f"Failed to persist updates for entity {eid}: {e}",
                exc_info=True,
            )
            return (0, 1, 0, 0, 0)
        sub = max(1, len(items) // 4)
        logger.warning(
            f"Apply batch of {len(items)} instance(s) failed ({e!s}); "
            f"retrying in sub-batches of up to {sub}"
        )
        persisted = failed = 0
        aliases_persisted = foreign_keys_persisted = 0
        entities_fk_updated = 0
        for j in range(0, len(items), sub):
            chunk = items[j : j + sub]
            p, f, a, fk, ef = _apply_alias_work_recursive(client, logger, chunk)
            persisted += p
            failed += f
            aliases_persisted += a
            foreign_keys_persisted += fk
            entities_fk_updated += ef
        return (
            persisted,
            failed,
            aliases_persisted,
            foreign_keys_persisted,
            entities_fk_updated,
        )


def _apply_alias_work_batched(
    client: CogniteClient,
    logger: Any,
    work_items: List[_AliasApplyWorkItem],
    batch_size: int,
) -> Tuple[int, int, int, int, int]:
    """Chunk work_items by batch_size, then apply each chunk with recursive shrink on failure."""
    persisted = failed = 0
    aliases_persisted = foreign_keys_persisted = 0
    entities_fk_updated = 0
    for i in range(0, len(work_items), batch_size):
        batch = work_items[i : i + batch_size]
        logger.info(
            f"Applying instance updates: batch starting at offset {i}, size {len(batch)}"
        )
        p, f, a, fk, ef = _apply_alias_work_recursive(client, logger, batch)
        persisted += p
        failed += f
        aliases_persisted += a
        foreign_keys_persisted += fk
        entities_fk_updated += ef
    return (
        persisted,
        failed,
        aliases_persisted,
        foreign_keys_persisted,
        entities_fk_updated,
    )


def _resolve_foreign_key_writeback_view_tuple(
    data: Dict[str, Any],
) -> Tuple[str, str, str]:
    space = (
        data.get("foreignKeyWritebackViewSpace")
        or data.get("foreign_key_writeback_view_space")
        or "cdf_cdm"
    )
    ext = (
        data.get("foreignKeyWritebackViewExternalId")
        or data.get("foreign_key_writeback_view_external_id")
        or "CogniteDescribable"
    )
    ver = (
        data.get("foreignKeyWritebackViewVersion")
        or data.get("foreign_key_writeback_view_version")
        or "v1"
    )
    return (str(space), str(ext), str(ver))


def _fk_values_from_parsed_json(parsed: Any) -> List[str]:
    if not isinstance(parsed, list):
        return []
    out: List[str] = []
    for item in parsed:
        if isinstance(item, dict) and item.get("value") is not None:
            s = str(item["value"]).strip()
            if s:
                out.append(s)
        elif isinstance(item, str) and item.strip():
            out.append(item.strip())
    return sorted(set(out))


def _load_foreign_key_map_from_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_key: str,
    logger: Any,
    limit: int = 10000,
) -> Dict[str, List[str]]:
    """Map entity external id -> deduplicated FK string values from extraction RAW."""
    try:
        rows = client.raw.rows.list(raw_db, raw_table_key, limit=limit)
    except Exception as e:
        logger.error(
            f"Failed reading FK RAW rows db={raw_db} table={raw_table_key}: {e}"
        )
        raise
    fk_map: Dict[str, List[str]] = {}
    for row in rows:
        key = getattr(row, "key", None)
        if not key:
            continue
        cols = getattr(row, "columns", {}) or {}
        raw_json = cols.get(FOREIGN_KEY_REFERENCES_JSON_COLUMN)
        if not isinstance(raw_json, str) or not raw_json.strip():
            continue
        try:
            parsed = json.loads(raw_json)
        except Exception:
            logger.warning(
                f"Invalid {FOREIGN_KEY_REFERENCES_JSON_COLUMN} JSON for RAW key={key!r}; skipping"
            )
            continue
        vals = _fk_values_from_parsed_json(parsed)
        if vals:
            fk_map[str(key)] = vals
    logger.info(
        f"Loaded foreign key references for {len(fk_map)} entities from RAW "
        f"db={raw_db} table={raw_table_key}"
    )
    return fk_map


def _foreign_key_map_from_entities_keys_extracted(
    entities_keys_extracted: Dict[str, Any],
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for eid, meta in entities_keys_extracted.items():
        if not isinstance(meta, dict):
            continue
        fks = meta.get("foreign_key_references")
        if not isinstance(fks, list) or not fks:
            continue
        vals: Set[str] = set()
        for item in fks:
            if isinstance(item, dict) and item.get("value") is not None:
                s = str(item["value"]).strip()
                if s:
                    vals.add(s)
            elif isinstance(item, str) and item.strip():
                vals.add(item.strip())
        if vals:
            out[str(eid)] = sorted(vals)
    return out


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

    Optionally writes foreign key reference strings to a configurable DM property
    on the same source instances (from key-extraction RAW and/or `entities_keys_extracted`).

    Args:
        client: CogniteClient instance (required)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing aliasing_results and/or RAW pointers.
            Optional: aliasWritebackProperty / alias_writeback_property (default "aliases").
            Optional: writeForeignKeyReferences / write_foreign_key_references,
            foreignKeyWritebackProperty / foreign_key_writeback_property (required if FK write enabled),
            source_raw_db, source_raw_table_key, source_raw_read_limit (FK JSON from extraction RAW),
            source_instance_space, source_view_space, source_view_external_id, source_view_version
            when persisting FK-only entities (not present in aliasing results).
            entities_keys_extracted: optional in-memory FK map (`foreign_key_references` per entity).
            Optional: persistenceApplyBatchSize / persistence_apply_batch_size (default 1000) —
            max nodes per `instances.apply` before splitting; failed batches retry in smaller chunks.
    """
    try:
        logger.info("Starting Alias Persistence Pipeline")

        if not client:
            raise ValueError("CogniteClient is required for alias persistence")

        alias_writeback_property = _resolve_alias_writeback_property(data)
        data["alias_writeback_property"] = alias_writeback_property

        write_fk = _resolve_write_foreign_key_references(data)
        fk_writeback_property = _resolve_foreign_key_writeback_property(data)
        data["write_foreign_key_references"] = write_fk
        if write_fk and not fk_writeback_property:
            raise ValueError(
                "foreignKeyWritebackProperty (or foreign_key_writeback_property) is required "
                "when writeForeignKeyReferences / write_foreign_key_references is true"
            )
        if write_fk and fk_writeback_property:
            data["foreign_key_writeback_property"] = fk_writeback_property

        fk_map: Dict[str, List[str]] = {}
        if write_fk and fk_writeback_property:
            source_raw_db = data.get("source_raw_db")
            source_raw_table_key = data.get("source_raw_table_key")
            fk_limit = int(
                data.get("source_raw_read_limit", data.get("raw_read_limit", 10000))
            )
            if source_raw_db and source_raw_table_key:
                fk_map = _load_foreign_key_map_from_raw(
                    client=client,
                    raw_db=str(source_raw_db),
                    raw_table_key=str(source_raw_table_key),
                    logger=logger,
                    limit=fk_limit,
                )
            eke = data.get("entities_keys_extracted")
            if isinstance(eke, dict) and eke:
                from_entities = _foreign_key_map_from_entities_keys_extracted(eke)
                for eid, vals in from_entities.items():
                    fk_map[eid] = sorted(set(fk_map.get(eid, []) + list(vals)))

        aliasing_results = data.get("aliasing_results", [])
        if not aliasing_results:
            raw_db = data.get("raw_db")
            raw_table_aliases = data.get("raw_table_aliases") or data.get("raw_table")
            if raw_db and raw_table_aliases:
                raw_limit = int(data.get("raw_read_limit", 1000))
                logger.info(
                    f"No aliasing_results provided; loading from RAW db={raw_db} "
                    f"table={raw_table_aliases} limit={raw_limit}"
                )
                aliasing_results = _load_aliasing_results_from_raw(
                    client=client,
                    raw_db=raw_db,
                    raw_table_aliases=raw_table_aliases,
                    logger=logger,
                    limit=raw_limit,
                )
                data["aliasing_results_loaded_from_raw"] = len(aliasing_results)

        has_fk_work = bool(write_fk and fk_writeback_property and fk_map)
        if not aliasing_results and not has_fk_work:
            logger.warning("No aliasing results and no foreign keys to persist")
            data["aliases_persisted"] = 0
            data["foreign_keys_persisted"] = 0
            data["entities_updated"] = 0
            data["entities_failed"] = 0
            return

        if aliasing_results:
            logger.info(f"Found {len(aliasing_results)} aliasing results to persist")
        logger.info(
            f"Alias write-back property: {alias_writeback_property!r} "
            f"(set aliasWritebackProperty or alias_writeback_property to override)"
        )
        if write_fk and fk_writeback_property:
            logger.info(
                f"Foreign key write-back property: {fk_writeback_property!r} "
                f"({len(fk_map)} entities with FK values in map)"
            )

        entity_aliases: Dict[str, Any] = {}
        aliases_planned_count = 0

        for aliasing_result in aliasing_results:
            original_tag = aliasing_result.get("original_tag")
            aliases = aliasing_result.get("aliases", [])
            entity_info_list = aliasing_result.get("entities", [])

            if not original_tag or not aliases:
                continue

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

        seen_ids = {ad["entity_id"] for ad in entity_aliases.values()}
        source_instance_space = data.get("source_instance_space")
        source_view_space = data.get("source_view_space")
        source_view_external_id = data.get("source_view_external_id")
        source_view_version = data.get("source_view_version")
        eke_for_fk = data.get("entities_keys_extracted")
        if not isinstance(eke_for_fk, dict):
            eke_for_fk = {}

        if write_fk and fk_map:
            for eid in fk_map:
                if eid in seen_ids:
                    continue
                meta = eke_for_fk.get(eid) if isinstance(eke_for_fk, dict) else None
                if not isinstance(meta, dict):
                    meta = {}
                inst_s = source_instance_space or meta.get("instance_space")
                vs = source_view_space or meta.get("view_space")
                ve = source_view_external_id or meta.get("view_external_id")
                vv = source_view_version or meta.get("view_version")
                if not (inst_s and vs and ve and vv):
                    raise ValueError(
                        "FK-only entity persistence requires source_instance_space, "
                        "source_view_space, source_view_external_id, and source_view_version "
                        "(in task data or per-entity entries in entities_keys_extracted) "
                        "when write_foreign_key_references is enabled and entities are not "
                        "present in aliasing results."
                    )
                entity_key = f"{vs}:{ve}/{vv}:{eid}"
                entity_aliases[entity_key] = {
                    "entity_id": eid,
                    "view_space": vs,
                    "view_external_id": ve,
                    "view_version": vv,
                    "instance_space": inst_s,
                    "aliases": [],
                    "candidate_keys": [],
                    "field_names": set(),
                }
                seen_ids.add(eid)

        if not entity_aliases:
            logger.warning("No entities to update after grouping aliasing and FK data")
            data["aliases_persisted"] = 0
            data["foreign_keys_persisted"] = 0
            data["entities_updated"] = 0
            data["entities_failed"] = 0
            return

        logger.info(
            f"Prepared updates for {len(entity_aliases)} entities "
            f"({aliases_planned_count} planned alias values)"
        )

        target_view_space = "cdf_cdm"
        target_view_external_id = "CogniteDescribable"
        target_view_version = "v1"

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
        fk_vs, fk_ext, fk_ver = _resolve_foreign_key_writeback_view_tuple(data)
        fk_view_id = ViewId(space=fk_vs, external_id=fk_ext, version=fk_ver)

        logger.info(f"Targeting {target_view_id} view for alias persistence")

        apply_batch_size = _resolve_persistence_apply_batch_size(data)
        data["persistence_apply_batch_size"] = apply_batch_size

        # Read current values so we can avoid no-op writes (which otherwise bump
        # lastUpdatedTime and keep incremental cohorts non-empty forever).
        current_values: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
        try:
            if NodeId is not None:
                node_ids: List[Any] = []
                for _entity_key, alias_data in entity_aliases.items():
                    instance_space = alias_data.get("instance_space") or target_view_space
                    entity_id = alias_data["entity_id"]
                    node_ids.append(
                        NodeId(space=str(instance_space), external_id=str(entity_id))
                    )
                if node_ids:
                    srcs = [target_view_id]
                    if write_fk and fk_writeback_property and fk_view_id != target_view_id:
                        srcs.append(fk_view_id)
                    existing_nodes = client.data_modeling.instances.retrieve_nodes(
                        nodes=node_ids,
                        sources=srcs,
                    )
                    for n in existing_nodes or []:
                        nd = n.dump() if hasattr(n, "dump") else {}
                        props = nd.get("properties", {}) if isinstance(nd, dict) else {}
                        t_props = (
                            props.get(target_view_space, {}).get(
                                f"{target_view_external_id}/{target_view_version}", {}
                            )
                            if isinstance(props, dict)
                            else {}
                        )
                        f_props = (
                            props.get(fk_vs, {}).get(f"{fk_ext}/{fk_ver}", {})
                            if isinstance(props, dict)
                            else {}
                        )
                        current_values[(str(getattr(n, "space", "")), str(getattr(n, "external_id", "")))] = {
                            "aliases": _normalize_str_list(
                                (t_props or {}).get(alias_writeback_property)
                            ),
                            "fk_vals": _normalize_str_list(
                                (f_props or {}).get(fk_writeback_property)
                                if fk_writeback_property
                                else None
                            ),
                        }
        except Exception as ex:
            logger.warning(
                f"Could not pre-read current alias/FK values for no-op filtering: {ex}"
            )

        work_items: List[_AliasApplyWorkItem] = []
        skipped_unchanged = 0

        for _entity_key, alias_data in entity_aliases.items():
            entity_id = alias_data["entity_id"]
            instance_space = alias_data.get("instance_space")
            if not instance_space:
                instance_space = target_view_space

            aliases = sorted(set(alias_data["aliases"]))
            fk_vals = list(fk_map.get(entity_id, [])) if write_fk else []
            fk_vals = sorted(set(fk_vals))

            cur = current_values.get((str(instance_space), str(entity_id)))
            if cur is not None:
                aliases_changed = aliases != cur.get("aliases", [])
                fk_changed = (
                    write_fk
                    and bool(fk_writeback_property)
                    and fk_vals != cur.get("fk_vals", [])
                )
                if not aliases_changed and not fk_changed:
                    skipped_unchanged += 1
                    continue

            sources: List[Any] = []
            if aliases and write_fk and fk_writeback_property and fk_vals:
                if target_view_id == fk_view_id:
                    props = {
                        alias_writeback_property: aliases,
                        fk_writeback_property: fk_vals,
                    }
                    sources = [NodeOrEdgeData(source=target_view_id, properties=props)]
                else:
                    sources = [
                        NodeOrEdgeData(
                            source=target_view_id,
                            properties={alias_writeback_property: aliases},
                        ),
                        NodeOrEdgeData(
                            source=fk_view_id,
                            properties={fk_writeback_property: fk_vals},
                        ),
                    ]
            elif aliases:
                sources = [
                    NodeOrEdgeData(
                        source=target_view_id,
                        properties={alias_writeback_property: aliases},
                    )
                ]
            elif write_fk and fk_writeback_property and fk_vals:
                sources = [
                    NodeOrEdgeData(
                        source=fk_view_id,
                        properties={fk_writeback_property: fk_vals},
                    )
                ]
            else:
                continue

            log_parts = []
            if aliases:
                log_parts.append(
                    f"{len(aliases)} alias value(s) on {alias_writeback_property!r}"
                )
            if fk_vals:
                log_parts.append(
                    f"{len(fk_vals)} FK reference(s) on {fk_writeback_property!r}"
                )
            logger.debug(f"Queued update for entity {entity_id}: " + "; ".join(log_parts))

            work_items.append(
                _AliasApplyWorkItem(
                    entity_id=entity_id,
                    node=NodeApply(
                        space=instance_space,
                        external_id=entity_id,
                        sources=sources,
                    ),
                    aliases=aliases,
                    fk_vals=fk_vals,
                )
            )

        if skipped_unchanged:
            logger.info(
                f"Skipping {skipped_unchanged} entity update(s): aliases/FKs unchanged"
            )

        (
            persisted_count,
            failed_count,
            aliases_persisted_count,
            foreign_keys_persisted_count,
            entities_fk_updated,
        ) = _apply_alias_work_batched(
            client, logger, work_items, apply_batch_size
        )

        data["aliases_planned"] = aliases_planned_count
        data["aliases_persisted"] = aliases_persisted_count
        data["foreign_keys_persisted"] = foreign_keys_persisted_count
        data["entities_fk_updated"] = entities_fk_updated
        data["entities_updated"] = persisted_count
        data["entities_failed"] = failed_count
        data["entity_aliases"] = {
            entity_key: {
                "entity_id": alias_data["entity_id"],
                "target_view_space": target_view_space,
                "target_view_external_id": target_view_external_id,
                "target_view_version": target_view_version,
                "alias_writeback_property": alias_writeback_property,
                "foreign_key_writeback_property": fk_writeback_property
                if write_fk
                else None,
                "foreign_keys": fk_map.get(alias_data["entity_id"], []),
                "source_view_space": alias_data.get("view_space"),
                "source_view_external_id": alias_data.get("view_external_id"),
                "source_view_version": alias_data.get("view_version"),
                "aliases": sorted(set(alias_data["aliases"])),
                "candidate_keys": alias_data["candidate_keys"],
                "field_names": list(alias_data["field_names"]),
            }
            for entity_key, alias_data in entity_aliases.items()
        }

        logger.info(
            f"Completed alias persistence: {persisted_count} entities updated, "
            f"{failed_count} failed, {aliases_persisted_count} alias values persisted, "
            f"{foreign_keys_persisted_count} FK values persisted "
            f"({entities_fk_updated} entities with FK write)"
        )

        sr_persist = data.get("source_run_id")
        if (
            not sr_persist
            and data.get("incremental_auto_run_id")
            and client
            and data.get("source_raw_db")
            and data.get("source_raw_table_key")
        ):
            from cdf_fn_common.incremental_scope import (
                WORKFLOW_STATUS_ALIASED,
                discover_single_run_id_for_status,
            )

            sr_persist = discover_single_run_id_for_status(
                client,
                str(data["source_raw_db"]),
                str(data["source_raw_table_key"]),
                WORKFLOW_STATUS_ALIASED,
            )
            if sr_persist:
                data["source_run_id"] = sr_persist

        if (
            client
            and failed_count == 0
            and persisted_count > 0
            and data.get("incremental_transition", True)
            and data.get("source_run_id")
            and data.get("source_raw_db")
            and data.get("source_raw_table_key")
        ):
            from cdf_fn_common.incremental_scope import (
                WORKFLOW_STATUS_ALIASED,
                WORKFLOW_STATUS_PERSISTED,
                transition_workflow_status_for_run,
            )

            n = transition_workflow_status_for_run(
                client,
                str(data["source_raw_db"]),
                str(data["source_raw_table_key"]),
                str(data["source_run_id"]),
                WORKFLOW_STATUS_ALIASED,
                WORKFLOW_STATUS_PERSISTED,
            )
            data["key_extraction_workflow_rows_persisted"] = n
            logger.info(
                f"Key-extraction RAW WORKFLOW_STATUS: {n} row(s) aliased -> persisted "
                f"(run_id={data['source_run_id']})"
            )

    except Exception as e:
        message = f"Alias persistence pipeline failed: {e!s}"
        logger.error(message)
        raise
