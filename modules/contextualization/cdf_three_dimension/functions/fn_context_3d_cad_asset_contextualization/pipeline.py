from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob, ExtractionPipelineRun
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from get_resources import (
    get_3d_model_id_and_revision_id,
    get_3d_nodes,
    get_assets,
    get_matches,
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
from constants import MAX_MODEL_SIZE_TO_CREATE_MODEL


def _shorten(text: str, max_len: int) -> str:
    """Truncate text to max_len characters, appending an ellipsis when cut."""
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


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
    numAsset = (MAX_MODEL_SIZE_TO_CREATE_MODEL + 1) if not config.debug else 10000

    raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500_000, trigger_log_level="INFO")

    def _write_failure_run(exc: Exception) -> None:
        _asset_ref = (
            config.asset_root_ext_id
            or config.asset_subtree_external_ids
            or config.three_d_model_name
        )
        msg = f"Contextualization of 3D to root asset: {_asset_ref} failed - Message: {exc!s}"
        log.error(msg)
        try:
            client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=config.extraction_pipeline_ext_id,
                    status="failure",
                    message=_shorten(msg, 1000),
                )
            )
        except Exception:
            log.error("Also failed to write failure run to extraction pipeline")

    try:
        # get model id and revision id based on name
        model_id, revision_id = get_3d_model_id_and_revision_id(client, config, config.three_d_model_name)

        asset_entities = get_assets(client, config, numAsset)
        if not asset_entities:
            raise ValueError(f"No assets found for root asset: {config.asset_root_ext_id}")

        three_d_entities, tree_d_nodes = get_3d_nodes(
            client=client, config=config, asset_entities=asset_entities, model_id=model_id, revision_id=revision_id,
            threed_from_quantum=config.threed_from_quantum)

        good_matches: list[dict[str, Any]] = []
        matched_node_ids: set[int] = set()

        # 1) Manual mappings (before ML)
        if manual_table_exists(client, config):
            manual_mappings = read_manual_mappings(client, config)
            if manual_mappings:
                log.info("Applying manual mappings before ML")
                good_manual, matched_manual = apply_manual_mappings_pre_ml(
                    client, config, manual_mappings, tree_d_nodes, asset_entities
                )
                good_matches.extend(good_manual)
                matched_node_ids |= matched_manual

        # 2) Rule-based mappings (before ML)
        if rule_table_exists(client, config):
            log.info("Applying rule-based mappings before ML")
            good_rule, matched_rule = apply_rule_mappings(
                client, config, tree_d_nodes, asset_entities, matched_node_ids
            )
            good_matches.extend(good_rule)
            matched_node_ids |= matched_rule

        # 3) ML matching for remaining entities
        remaining_entities = [e for e in three_d_entities if e["id"] not in matched_node_ids]
        if len(remaining_entities) > 0:
            match_results = get_matches(client, asset_entities, remaining_entities, [])
            good_ml, bad_matches = select_and_apply_matches(client, config, match_results)
            good_matches = good_matches + good_ml
        else:
            bad_matches = []

        if len(good_matches) > 0 or len(bad_matches) > 0:
            write_mapping_to_raw(client, config, raw_uploader, good_matches, bad_matches)
        len_good_matches = len(good_matches)
        len_bad_matches = len(bad_matches)

        if len_good_matches > 0 and not config.debug:
            run_apply_dm_cad_contextualization(client, config, model_id, revision_id)

        _asset_ref = (
            config.asset_root_ext_id
            or config.asset_subtree_external_ids
            or config.three_d_model_name
        )
        msg = (
            f"Contextualization of 3D to asset root: {_asset_ref}, "
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
    except CogniteAPIError as e:
        # Transient CDF API failure — write failure run, do not re-raise (expected operational failure)
        _write_failure_run(e)
    except Exception as e:
        # Unexpected / fatal error (bug, bad config, etc.) — write failure run AND re-raise
        # so CDF Functions marks the invocation as failed and monitoring alerts fire
        _write_failure_run(e)
        raise


def select_and_apply_matches(
    client: CogniteClient,
    config: ContextConfig,
    match_results: list[ContextualizationJob],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Classify entity-matching results into good/bad by the score threshold.

    The actual DM links are created downstream by the /3d/contextualization/cad endpoint
    from the RAW good table, so this only builds the good/bad match rows.

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        match_results: list of matches from entity matching

    Returns:
        (good_matches, bad_matches)
    """
    good_matches: list[dict[str, Any]] = []
    bad_matches: list[dict[str, Any]] = []

    try:
        for match in match_results:
            if match["matches"] and match["matches"][0]["score"] >= config.match_threshold:
                good_matches.append(add_to_dict(match))
            else:
                bad_matches.append(add_to_dict(match))

        log.info(f"Got {len(good_matches)} matches with score >= {config.match_threshold}")
        log.info(f"Got {len(bad_matches)} matches with score < {config.match_threshold}")

        return good_matches, bad_matches

    except Exception as e:
        log.error(f"Failed to parse results from entity matching - error: {type(e)}({e})")
        raise


def add_to_dict(match: dict[str, Any]) -> dict[str, Any]:
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
            asset_external_id = mTo["external_id"]
        else:
            score = 0
            asset_name = "_no_match_"
            asset_external_id = None

        return {
            "matchType": "ml",
            "score": score,
            "nodeName": mFrom["org_name"],
            "nodeNameQc": get_qc_friendly_3d_name(mFrom["org_name"]),
            "nodeNameMatched": mFrom["name"],
            "nodeId": mFrom["id"],
            "assetName": asset_name,
            "assetId": asset_external_id,
            "assetExternalId": asset_external_id,
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
