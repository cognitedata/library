from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun, ThreeDAssetMapping
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_3D_NODE_NAME,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
)
from get_resources import (
    filter_3d_nodes,
    get_3d_model_id_and_revision_id,
    get_3d_nodes,
    get_asset_id_ext_id_mapping,
    get_assets,
    get_mapping_to_delete,
    get_matches,
    get_treed_asset_mappings,
    manual_table_exists,
    read_manual_mappings,
)
from pre_ml_mappings import (
    apply_manual_mappings as apply_manual_mappings_pre_ml,
    apply_rule_mappings,
    rule_table_exists,
)
from write_resources import write_mapping_to_raw
from apply_dm_cad_contextualization import run as run_apply_dm_cad_contextualization
from logger import log


def annotate_3d_model(client: CogniteClient, config: ContextConfig) -> None:
    """
    Read configuration and start process by
    1. Read RAW table with manual mappings and extract all rows not contextualized
    2. Apply manual mappings from 3D nodes to Asset - this will overwrite any existing mapping
    3. Read all time series not matched (or all if runAll is True)
    4. Read all assets
    5. Run ML contextualization to match 3D Nodes -> Assets
    6. Update 3D Nodes with mapping
    7. Write results matched (good) not matched (bad) to RAW
    8. Output in good/bad table can then be used in workflow to update manual mappings

    Args:
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """
    log.info("Initiating 3D annotation process")

    len_good_matches = 0
    len_bad_matches = 0
    existing_matches = {}
    mapping_to_delete = None
    numAsset = -1 if not config.debug else 10000

    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    try:
        # get model id and revision id based on name
        model_id, revision_id = get_3d_model_id_and_revision_id(client, config, config.three_d_model_name)

        use_dm_cad = getattr(config, "use_dm_cad_contextualization", True)
        if not use_dm_cad:
            if config.run_all or not config.keep_old_mapping:
                mapping_to_delete = get_mapping_to_delete(client, model_id, revision_id)
                client.three_d.asset_mappings.delete(model_id, revision_id, mapping_to_delete)
            if config.keep_old_mapping and not config.run_all:
                existing_matches = get_treed_asset_mappings(client, model_id, revision_id, existing_matches)

        asset_entities = get_assets(client, config, existing_matches, numAsset)
        if not asset_entities:
            raise Exception("WARNING: No assets found for root asset: {config.asset_root_ext_id}")

        three_d_entities, tree_d_nodes = get_3d_nodes(
            client=client, config=config, asset_entities=asset_entities, model_id=model_id, revision_id=revision_id, threed_from_quantum=True)

        good_matches: list[dict[str, Any]] = []
        matched_node_ids: set[int] = set()

        # 1) Manual mappings (before ML)
        if manual_table_exists(client, config):
            manual_mappings = read_manual_mappings(client, config)
            if manual_mappings:
                log.info("Applying manual mappings before ML")
                good_manual, matched_manual = apply_manual_mappings_pre_ml(
                    client, config, manual_mappings, model_id, revision_id, tree_d_nodes, asset_entities
                )
                good_matches.extend(good_manual)
                matched_node_ids |= matched_manual

        # 2) Rule-based mappings (before ML)
        if rule_table_exists(client, config):
            log.info("Applying rule-based mappings before ML")
            good_rule, matched_rule = apply_rule_mappings(
                client, config, tree_d_nodes, asset_entities, model_id, revision_id, matched_node_ids
            )
            good_matches.extend(good_rule)
            matched_node_ids |= matched_rule

        # 3) ML matching for remaining entities
        remaining_entities = [e for e in three_d_entities if e["id"] not in matched_node_ids]
        if len(remaining_entities) > 0:
            match_results = get_matches(client, asset_entities, remaining_entities, [])
            good_ml, bad_matches, existing_matches = select_and_apply_matches(
                client, config, match_results, tree_d_nodes, model_id, revision_id, existing_matches, use_dm_cad
            )
            good_matches = good_matches + good_ml
        else:
            bad_matches = []

        if len(good_matches) > 0 or len(bad_matches) > 0:
            write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches)
        len_good_matches = len(good_matches)
        len_bad_matches = len(bad_matches)

        if use_dm_cad and len_good_matches > 0 and not config.debug:
            run_apply_dm_cad_contextualization(client, config, model_id, revision_id)

        msg = (
            f"Contextualization of 3D to asset root: {config.asset_root_ext_id}, "
            f"num 3D nodes contextualized: {len_good_matches}, num 3D nodes NOT contextualized: {len_bad_matches} "
            f"(score below {config.match_threshold})"
        )
        log.info(msg)
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(
                extpipe_external_id=config.extraction_pipeline_ext_id,
                status="success",
                message=msg,
            )
        )
    except Exception as e:
        msg = f"Contextualization of 3D to root asset: {config.asset_root_ext_id} failed - Message: {e!s}"
        log.error(msg)
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(
                extpipe_external_id=config.extraction_pipeline_ext_id,
                status="failure",
                message=shorten(msg, 1000),
            )
        )


def select_and_apply_matches(
    client: CogniteClient,
    config: ContextConfig,
    match_results: list[ContextualizationJob],
    tree_d_nodes: dict[str, Any],
    model_id: int,
    revision_id: int,
    existing_matches: dict[str, Any],
    use_dm_cad_contextualization: bool = False,
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    """
    Select and apply matches based on filtering threshold. Matches with score above threshold are updating time series
    with asset ID When matches are updated, metadata property with information about the match is added to time series
    to indicate that it has been matched.

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        match_results: list of matches from entity matching
        ts_meta_dict: dictionary with time series id and metadata

    Returns:
        list of good matches
        list of bad matches
    """
    good_matches = []
    bad_matches = []
    mapped_node = []
    asset_mappings = []

    try:
        for match in match_results:
            if match["matches"]:
                if match["matches"][0]["score"] >= config.match_threshold:
                    good_matches.append(add_to_dict(match))
                else:
                    bad_matches.append(add_to_dict(match))
            else:
                bad_matches.append(add_to_dict(match))

        log.info(f"Got {len(good_matches)} matches with score >= {config.match_threshold}")
        log.info(f"Got {len(bad_matches)} matches with score < {config.match_threshold}")

        for match in good_matches:
            node_str = match["3DNameMatched"]

            if node_str not in mapped_node:
                mapped_node.append(node_str)

                asset_id = match["assetId"]
                node_ids = tree_d_nodes[node_str]

                for node_id in node_ids:
                    if asset_id in existing_matches:
                        # Ensure it's a list before appending
                        if isinstance(existing_matches[asset_id], list):
                            existing_matches[asset_id].append(node_id["id"])
                        else:
                            existing_matches[asset_id] = [existing_matches[asset_id], node_id["id"]]
                    else:
                        existing_matches[asset_id] = [node_id["id"]]

                    asset_mappings.append(
                        ThreeDAssetMapping(
                            node_id=node_id["id"],
                            asset_id=asset_id,
                        )
                    )

                if len(asset_mappings) > 0 and len(asset_mappings) % 10000 == 0:
                    if not config.debug and not use_dm_cad_contextualization:
                        client.three_d.asset_mappings.create(
                            model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
                        )
                        log.info(f"Updated {len(asset_mappings)} 3D mappings")
                        asset_mappings = []

        if not config.debug and not use_dm_cad_contextualization and asset_mappings:
            client.three_d.asset_mappings.create(
                model_id=model_id, revision_id=revision_id, asset_mapping=asset_mappings
            )
            log.info(f"Updated {len(asset_mappings)} nodes with 3D mappings")

        return good_matches, bad_matches, existing_matches

    except Exception as e:
        log.error(f"Failed to parse results from entity matching - error: {type(e)}({e})")


def add_to_dict(match: dict[Any]) -> dict[Any]:
    """
    Add match to dictionary

    Args:
        match: dictionary with match information
    Returns:
        dictionary with match information
    """

    try:
        mFrom = match["source"]

        if len(match["matches"]) > 0:
            mTo = match["matches"][0]["target"]
            score = match["matches"][0]["score"]
            asset_name = mTo["name"]
            asset_id = mTo["id"]
            asset_external_id = mTo["external_id"]
        else:
            score = 0
            asset_name = "_no_match_"
            asset_id = None
            asset_external_id = None

        return {
            "matchType": "ml",
            "score": score,
            "3DName": mFrom["org_name"],
            "3DNameQC": get_qc_friendly_3d_name(mFrom["org_name"]),
            "3DNameMatched": mFrom["name"],
            "3DId": mFrom["id"],
            "assetName": asset_name,
            "assetId": asset_id,
            "assetExternalId": asset_external_id
        }
    except Exception as e:
        raise Exception(f"ERROR: Not able to parse return object: {match} - error: {e}")


def get_qc_friendly_3d_name(s: str) -> str:
    splits = s.split("/")
    splits_length = len(splits)

    if splits_length >= 2:
        return splits[1]
    else:
        return s


def remove_old_mappings(
    client: CogniteClient,
    mapping_to_delete: list[ThreeDAssetMapping],
    existing_matches: dict[str, Any],
    model_id: int,
    revision_id: int,
) -> int:
    delete_mapping = []

    for mapping in mapping_to_delete:
        asset_id = mapping.asset_id

        if asset_id not in existing_matches:
            delete_mapping.append(mapping)

    if len(delete_mapping) > 0:
        client.three_d.asset_mappings.delete(model_id=model_id, revision_id=revision_id, asset_mapping=delete_mapping)

    log.info(f"Deleted {len(delete_mapping)} old mappings")

    return len(delete_mapping)
