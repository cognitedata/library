"""
Pre-ML mapping: manual and rule-based 3D → CogniteAsset mappings (DM mode).

Applied before entity matching (ML). Writes mappings by updating the
CogniteCADNode.asset direct relation in the Core Data Model instead of
using the classic three_d.asset_mappings API.
"""
from __future__ import annotations

import re
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

from config import get_cad_node_view
from constants import (
    COL_KEY_RULE_REGEXP_ASSET,
    COL_KEY_RULE_REGEXP_ENTITY,
)
from get_resources import manual_table_exists, read_manual_mappings, build_cad_node_lookup, create_cad_node_mappings
from logger import log


def rule_table_exists(client: CogniteClient, config: Any) -> bool:
    """Return True if the rule-based mapping RAW table exists."""
    raw_table_rule = getattr(config, "raw_table_rule", None)
    if not raw_table_rule:
        return False
    tables = client.raw.tables.list(config.rawdb, limit=None)
    return any(tbl.name == raw_table_rule for tbl in tables)


def read_rule_mappings(client: CogniteClient, config: Any) -> list[dict[str, str]]:
    """
    Read rule-based mappings from RAW table.
    Each row: regexpEntity (pattern for 3D node name), regexpAsset (pattern for asset name).
    """
    raw_table_rule = getattr(config, "raw_table_rule", None)
    if not raw_table_rule:
        return []
    try:
        df = client.raw.rows.retrieve_dataframe(
            db_name=config.rawdb, table_name=raw_table_rule, limit=-1
        )
        rules = []
        for _, row in df.iterrows():
            entity_pat = row.get(COL_KEY_RULE_REGEXP_ENTITY) or row.get("regexpEntity")
            asset_pat = row.get(COL_KEY_RULE_REGEXP_ASSET) or row.get("regexpAsset")
            if entity_pat and asset_pat:
                rules.append({
                    "regexp_entity": str(entity_pat).strip(),
                    "regexp_asset": str(asset_pat).strip(),
                })
        log.info(f"Number of rule-based mapping rules: {len(rules)}")
        return rules
    except Exception as e:
        log.warning(f"Could not read rule mappings from {config.rawdb}/{raw_table_rule}: {e}")
        return []


def _node_id_to_name(tree_d_nodes: dict[str, Any]) -> dict[int, str]:
    """Build node3DId -> cleaned node name from tree_d_nodes."""
    out = {}
    for name, node_list in tree_d_nodes.items():
        for n in node_list:
            nid = n["id"] if isinstance(n.get("id"), int) else int(n["id"])
            out[nid] = name
    return out


def _asset_ext_id_to_info(asset_entities: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build asset_external_id -> {name} from asset_entities (DM: id == external_id)."""
    return {a["external_id"]: {"name": a["name"]} for a in asset_entities}


def _qc_friendly_3d_name(s: str) -> str:
    parts = s.split("/")
    return parts[1] if len(parts) >= 2 else s


def apply_manual_mappings(
    client: CogniteClient,
    config: Any,
    manual_mappings: list[dict[str, Any]],
    model_id: int,
    revision_id: int,
    tree_d_nodes: dict[str, Any],
    asset_entities: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[int]]:
    """
    Apply manual mappings: update CogniteCADNode.asset in DM.
    Returns good_matches list and set of matched node3DIds.
    """
    good_matches: list[dict[str, Any]] = []
    matched_node_ids: set[int] = set()
    if not manual_mappings:
        return good_matches, matched_node_ids

    instance_space = config.asset_dm_space
    node_id_to_name = _node_id_to_name(tree_d_nodes)
    asset_info_map = _asset_ext_id_to_info(asset_entities)
    cad_node_lookup = build_cad_node_lookup(client, instance_space)
    dm_mappings: list[tuple[int, str, str]] = []

    for m in manual_mappings:
        node3d_id = int(m["sourceId"])
        asset_ext_id = str(m["targetId"])
        matched_node_ids.add(node3d_id)
        dm_mappings.append((node3d_id, asset_ext_id, instance_space))
        node_name = node_id_to_name.get(node3d_id, "")
        asset_info = asset_info_map.get(asset_ext_id, {})
        good_matches.append({
            "matchType": "manual",
            "score": 1.0,
            "3DName": node_name,
            "3DNameQC": _qc_friendly_3d_name(node_name),
            "3DNameMatched": node_name,
            "3DId": node3d_id,
            "assetName": asset_info.get("name", ""),
            "assetId": asset_ext_id,
            "assetExternalId": asset_ext_id,
        })

    if dm_mappings and not getattr(config, "debug", False):
        create_cad_node_mappings(client, cad_node_lookup, dm_mappings)
        log.info(f"Applied {len(dm_mappings)} manual 3D mappings (DM)")

    return good_matches, matched_node_ids


def apply_rule_mappings(
    client: CogniteClient,
    config: Any,
    tree_d_nodes: dict[str, Any],
    asset_entities: list[dict[str, Any]],
    model_id: int,
    revision_id: int,
    already_matched_node_ids: set[int],
) -> tuple[list[dict[str, Any]], set[int]]:
    """
    Apply rule-based mappings: for each 3D node name not yet matched, try each rule.
    If exactly one asset matches the asset regex, write the mapping via DM.
    """
    good_matches: list[dict[str, Any]] = []
    matched_node_ids: set[int] = set()
    rules = read_rule_mappings(client, config)
    if not rules:
        return good_matches, matched_node_ids

    instance_space = config.asset_dm_space
    asset_by_name = {a["name"]: a for a in asset_entities}
    cad_node_lookup = build_cad_node_lookup(client, instance_space)
    dm_mappings: list[tuple[int, str, str]] = []

    for node_name, node_list in tree_d_nodes.items():
        node_ids = [n["id"] if isinstance(n.get("id"), int) else int(n["id"]) for n in node_list]
        if any(nid in already_matched_node_ids for nid in node_ids):
            continue
        for rule in rules:
            try:
                entity_pat = re.compile(rule["regexp_entity"])
                asset_pat = re.compile(rule["regexp_asset"])
            except re.error:
                log.warning(f"Invalid regex in rule: {rule}")
                continue
            if not entity_pat.search(node_name):
                continue
            matching_assets = [a for a in asset_entities if asset_pat.search(a["name"])]
            if len(matching_assets) != 1:
                continue
            asset = matching_assets[0]
            asset_ext_id = asset["external_id"]
            for node3d_id in node_ids:
                matched_node_ids.add(node3d_id)
                dm_mappings.append((node3d_id, asset_ext_id, instance_space))
                good_matches.append({
                    "matchType": "rule",
                    "score": 1.0,
                    "3DName": node_name,
                    "3DNameQC": _qc_friendly_3d_name(node_name),
                    "3DNameMatched": node_name,
                    "3DId": node3d_id,
                    "assetName": asset["name"],
                    "assetId": asset_ext_id,
                    "assetExternalId": asset_ext_id,
                })
            break

    if dm_mappings and not getattr(config, "debug", False):
        create_cad_node_mappings(client, cad_node_lookup, dm_mappings)
        log.info(f"Applied {len(dm_mappings)} rule-based 3D mappings (DM)")

    return good_matches, matched_node_ids
