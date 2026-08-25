from __future__ import annotations

import json
import re
import sys
import os
import tempfile
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob
from cognite.client.data_classes.data_modeling import ViewId

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_MATCH_KEY,
    MAX_MODEL_SIZE_TO_CREATE_MODEL,
    ML_MODEL_FEATURE_TYPE,
)
from logger import log


def manual_table_exists(client: CogniteClient, config: ContextConfig) -> bool:
    tables = client.raw.tables.list(config.rawdb, limit=None)
    return any(tbl.name == config.raw_table_manual for tbl in tables)


def read_manual_mappings(client: CogniteClient, config: ContextConfig) -> list[dict[str, Any]]:
    raw_table_manual = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_manual,
                                                          limit=-1)

    manual_entries = [{'targetId': row['assetId'], 'sourceId': row['nodeId']}
                      for index, row in raw_table_manual.iterrows()]
    return manual_entries


def get_3d_model_id_and_revision_id(
    client: CogniteClient, config: ContextConfig, three_d_model_name: str
) -> tuple[int, int]:
    """
    Look up 3D model ID and revision ID from DM only (no classic 3D API).

    Uses DM property filters without `sources` to avoid SDK v7.x deserialization issues.
    Numeric IDs are extracted from Cognite's standard externalId patterns:
      model:    cog_3d_model_{model_id}
      revision: cog_3d_revision_{revision_id}
    """
    try:
        from cognite.client.data_classes.data_modeling import filters as dm_filters

        model_view = ViewId("cdf_cdm", "CogniteCADModel", "v1")
        revision_view = ViewId("cdf_cdm", "CogniteCADRevision", "v1")
        _mv = f"{model_view.external_id}/{model_view.version}"
        _rv = f"{revision_view.external_id}/{revision_view.version}"

        # 1) Find CogniteCADModel by name — filter only, no sources (avoids SDK v7.x deserialization bug)
        model_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[model_view.space, _mv, "name"],
                value=three_d_model_name,
            ),
            limit=100,
        )

        model_matches = [n for n in model_nodes if n.external_id.startswith("cog_3d_model_")]
        if not model_matches:
            raise ValueError(f"No CogniteCADModel with name='{three_d_model_name}' found in DM")
        if len(model_matches) > 1:
            log.warning(
                f"Found {len(model_matches)} CogniteCADModel nodes named '{three_d_model_name}'; "
                f"using the first ({model_matches[0].external_id}). Disambiguate the model name to avoid this."
            )
        model_node = model_matches[0]
        model_ext_id = model_node.external_id
        model_id = int(model_ext_id.split("cog_3d_model_")[1])
        model_space = model_node.space
        log.info(f"Found CogniteCADModel '{three_d_model_name}' space='{model_space}' ext_id={model_ext_id} id={model_id}")

        # 2) Find CogniteCADRevision for this model — filter by model3D direct relation.
        # Pick the latest revision (highest revisionId) rather than an arbitrary first match.
        revision_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[revision_view.space, _rv, "model3D"],
                value={"space": model_space, "externalId": model_ext_id},
            ),
            limit=1000,
        )

        revision_ids = [
            int(n.external_id.split("cog_3d_revision_")[1])
            for n in revision_nodes
            if n.external_id.startswith("cog_3d_revision_")
        ]
        if not revision_ids:
            raise ValueError(
                f"No CogniteCADRevision found for model '{three_d_model_name}' (model externalId={model_ext_id})"
            )
        revision_id = max(revision_ids)
        log.info(
            f"Found {len(revision_ids)} revision(s) for model '{three_d_model_name}'; "
            f"using latest revision_id={revision_id}"
        )

        log.info(f"Resolved: model='{three_d_model_name}' model_id={model_id} revision_id={revision_id}")
        return model_id, revision_id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for 3D nodes in data set: {config.three_d_data_set_ext_id}- error: {e}"
        )


def get_3d_nodes(
    client: CogniteClient,
    config: ContextConfig,
    asset_entities: list[dict[str, Any]],
    model_id: int,
    revision_id: int,
    threed_from_quantum: bool = False
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Read time series based on root ASSET id
    Read all if config property readAll = True, else only read time series not contextualized ( connected to asset)

    Args:
        client: Instance of CogniteClient
        config: Instance of ContextConfig
        manual_matches: list of manual mappings

    Returns:
        list of entities
        list of dict with time series id and metadata
    """
    entities: list[dict[str, Any]] = []
    three_d_nodes: dict[str, list[dict[str, Any]]] = {}
    input_three_d_nodes = None

    three_d_model_name = config.three_d_model_name
    try:
        quantum_three_d_nodes: list[str] = []
        if threed_from_quantum:
            column_three_d_name = "nodeName"
            quantum_df = client.raw.rows.retrieve_dataframe(db_name="ds_qc", table_name="table:quantum_3d_qc1", limit=None, columns=[column_three_d_name])
            quantum_three_d_nodes.extend(quantum_df[column_three_d_name].astype(str).tolist())

        # prep list of asset filters
        # asset_filter = [asset["name"] for asset in asset_entities]
        _ds = client.data_sets.retrieve(external_id=config.three_d_data_set_ext_id) if config.three_d_data_set_ext_id else None
        three_d_data_set_id = _ds.id if _ds else None

        model_file_name = f"3D_nodes_{three_d_model_name}_id_{model_id}_rev_id_{revision_id}.json"
        if not config.run_all:
            three_d_file = client.files.retrieve(external_id=model_file_name)
            if three_d_file:
                file_content = client.files.download_bytes(external_id=model_file_name)
                input_three_d_nodes = json.loads(file_content)

        if not input_three_d_nodes:
            input_three_d_nodes = []

            nodes = client.three_d.revisions.list_nodes(
                model_id=model_id,
                revision_id=revision_id,
                sort_by_node_id=True,
                partitions=500,
                limit=-1,
            )

            for node in nodes:
                if node.name and node.name != "":
                    input_three_d_nodes.append(node.dump())

            fd, tmp_path = tempfile.mkstemp(prefix="3d_nodes_", suffix=".json")
            os.close(fd)
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(input_three_d_nodes, f)
                client.files.upload(
                    path=tmp_path,
                    external_id=model_file_name,
                    name=model_file_name,
                    overwrite=True,
                    data_set_id=three_d_data_set_id,
                )
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

            log.info(f"Uploaded {model_file_name} to CDF.")

        num_nodes = 0
        if input_three_d_nodes:
            # Name normalizer: use config if available, else generic default for 3D paths
            replacements = getattr(config, "name_replacements", None)
            suffixes_to_strip = getattr(config, "suffixes_to_strip", None)
            quantum_node_set = set(quantum_three_d_nodes)
            node_prefixes = getattr(config, "node_name_prefixes", None)
            max_slashes = getattr(config, "node_name_max_slashes", None)

            def clean_name(name: str) -> str:
                original_name = name
                # Generic 3D path: take from first "/", drop trailing "/-suffix", normalize separators
                if "/" in name:
                    name = re.search(r"/.*", name)
                    name = name.group() if name else name
                if isinstance(name, str):
                    name = re.sub(r"/-.+", "", name)
                    name = name.replace("/", "").replace(".", "-")
                else:
                    name = str(original_name)
                name = _normalize_name_generic(name, replacements=replacements, suffixes_to_strip=suffixes_to_strip)
                if not name:
                    name = original_name
                return name

            for node in input_three_d_nodes:
                node_name = str(node.get("name") or "")
                if not node_name:
                    continue
                if quantum_node_set:
                    if node_name not in quantum_node_set:
                        continue
                else:
                    if node_prefixes and not node_name.startswith(tuple(node_prefixes)):
                        continue
                    if max_slashes is not None and node_name.count("/") > int(max_slashes):
                        continue

                num_nodes += 1
                mod_node_name = clean_name(node_name)
                node["mode_node_name"] = mod_node_name

                node_info = {
                    "id": node.get("id"),
                    "subtree_size": node.get("subtreeSize"),
                    "tree_index": node.get("treeIndex"),
                }
                if mod_node_name in three_d_nodes:
                    three_d_nodes[mod_node_name].append(node_info)
                else:
                    three_d_nodes[mod_node_name] = [node_info]
                entities = get_3d_entities(node, mod_node_name, entities)

        log.info(
            f"Total number of 3D Node found: {num_nodes} - unique names to match after asset name filtering: {len(three_d_nodes)}"
        )

        return entities, three_d_nodes

    except Exception as e:
        raise Exception(f"ERROR: Not able to get 3D nodes in data set: {config.three_d_data_set_ext_id} - error: {e}")


def get_3d_entities(node: dict[str, Any], modNodeName: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    process time series metadata and create an entity used as input to contextualization

    Args:
        node: metadata for 3D node
        modNodeName: modified node name
        entities: already processed entities

    Returns:
        list of entities
    """

    # add entities for files used to match between 3D nodes and assets
    entities.append(
        {
            "id": node["id"],
            "name": modNodeName,
            "external_id": node["treeIndex"],
            "org_name": node["name"],
            "type": "3dNode",
        }
    )
    return entities


def _normalize_name_generic(
    name: str,
    replacements: list[dict[str, str]] | None = None,
    suffixes_to_strip: list[str] | None = None,
) -> str:
    """
    Generic name normalizer: apply replacements, strip suffixes, then split on [-_] and rejoin.
    Used for asset and 3D node names when config-driven or default.
    """
    if not name:
        return name
    replacements = replacements or []
    suffixes_to_strip = suffixes_to_strip or []
    for r in replacements:
        from_val = r.get("from") or r.get("from_val")
        to_val = r.get("to") or r.get("to_val") or ""
        if from_val is not None:
            name = name.replace(from_val, to_val)
    for suffix in suffixes_to_strip:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    parts = re.split(r"[-_\s]+", name)
    return "".join(p for p in parts if p)


def get_assets(
    client: CogniteClient,
    config: Any,
    read_limit: int,
) -> list[dict[str, Any]]:
    """
    Get assets from DM (DataModelOnly project — classic Assets API not available).

    Queries AssetExtension nodes from config.asset_dm_space. Optionally restricts to
    nodes whose externalId starts with one of config.asset_subtree_external_ids or
    config.asset_root_ext_id (treats the IDs as path prefixes, e.g. 'CLV/').
    """
    from cognite.client.data_classes.data_modeling import filters as dm_filters

    entities: list[dict[str, Any]] = []
    try:
        instance_space = getattr(config, "asset_dm_space", None) or os.getenv("ASSET_INSTANCE_SPACE")
        asset_view_space = getattr(config, "asset_view_space", None) or instance_space
        asset_view_ext_id = getattr(config, "asset_view_ext_id", None) or os.getenv("ASSET_VIEW_EXT_ID")
        asset_view_version = getattr(config, "asset_view_version", None) or os.getenv("ASSET_VIEW_VERSION")

        missing = [
            label
            for label, value in (
                ("asset_dm_space/ASSET_INSTANCE_SPACE", instance_space),
                ("asset_view_space", asset_view_space),
                ("asset_view_ext_id/ASSET_VIEW_EXT_ID", asset_view_ext_id),
                ("asset_view_version/ASSET_VIEW_VERSION", asset_view_version),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Missing asset view configuration for DM asset query: "
                + ", ".join(missing)
                + ". Set these in the pipeline config or as env vars."
            )

        nodes = client.data_modeling.instances.list(
            instance_type="node",
            space=instance_space,
            filter=dm_filters.HasData(
                views=[(asset_view_space, asset_view_ext_id, asset_view_version)]
            ),
            limit=read_limit if read_limit > 0 else -1,
        )

        # Optional prefix filtering by asset subtree / root
        subtree_ids: list[str] = getattr(config, "asset_subtree_external_ids", None) or []
        root_ext_id: str | None = getattr(config, "asset_root_ext_id", None)
        if not subtree_ids and root_ext_id:
            subtree_ids = [root_ext_id]

        def _in_subtree(ext_id: str) -> bool:
            if not subtree_ids:
                return True
            return any(
                ext_id == prefix or ext_id.startswith(prefix + "/") or ext_id.startswith(prefix + "-")
                for prefix in subtree_ids
            )

        replacements = getattr(config, "name_replacements", None)
        suffixes_to_strip = getattr(config, "suffixes_to_strip", None)

        for node in nodes:
            if not _in_subtree(node.external_id):
                continue

            # Derive display name from externalId (last path segment)
            raw_name = node.external_id.split("/")[-1]
            name = _normalize_name_generic(raw_name, replacements=replacements, suffixes_to_strip=suffixes_to_strip)
            if not name or len(name) <= 3:
                continue

            entities.append(
                {
                    "id": node.external_id,   # DM: use externalId as id (no numeric id)
                    "name": name,
                    "external_id": node.external_id,
                    "org_name": raw_name,
                    "type": "asset",
                }
            )

        log.info(f"Number of DM assets found: {len(entities)} (space='{instance_space}', subtree={subtree_ids or 'all'})")
        return entities

    except Exception as e:
        root = getattr(config, "asset_root_ext_id", "?")
        raise Exception(
            f"ERROR: Not able to get entities for asset extId root: {root}. Error: {type(e)}({e})"
        )


def get_matches(
    client: CogniteClient,
    match_to: list[dict[str, Any]],
    match_from: list[dict[str, Any]],
    manual_mappings: list[dict[str, Any]],
) -> list[ContextualizationJob]:
    """
    Create / Update entity matching model and run job to get matches

    Args:
        client: Instance of CogniteClient
        match_to: list of entities to match to (target)
        match_from: list of entities to match from (source)
        manual_mappings

    Returns:
        list of matches
    """

    more_to_match = True
    all_matches = []
    match_size = MAX_MODEL_SIZE_TO_CREATE_MODEL
    min_match_size = int(MAX_MODEL_SIZE_TO_CREATE_MODEL / 4)
    offset = 0
    retry_num = 3
    match_array = []

    try:
        # limit number input nodes to create model
        if len(match_from) > MAX_MODEL_SIZE_TO_CREATE_MODEL:
            sources = match_from[:MAX_MODEL_SIZE_TO_CREATE_MODEL]
        else:
            sources = match_from

        if len(match_to) > MAX_MODEL_SIZE_TO_CREATE_MODEL:
            raise ValueError(
                f"Too many target assets for entity matching: {len(match_to)} > {MAX_MODEL_SIZE_TO_CREATE_MODEL}. "
                "Narrow asset scope (asset_root_ext_id / asset_subtree_external_ids) before running."
            )
        targets = match_to

        model = client.entity_matching.fit(
            sources=sources,
            targets=targets,
            match_fields=[(COL_MATCH_KEY, COL_MATCH_KEY)],
            feature_type=ML_MODEL_FEATURE_TYPE,
            # true_matches=manual_mappings
        )

        while more_to_match:
            if len(match_from) < offset + match_size:
                more_to_match = False
                match_array = match_from[offset:]
            else:
                match_array = match_from[offset : offset + match_size]

            log.info(f"Run mapping of number of nodes from: {offset} to {offset + len(match_array)}")

            try:
                job = model.predict(sources=match_array, targets=targets, num_matches=1)
                job.wait_for_completion()
                matches = job.result
                all_matches = all_matches + matches["items"]
                offset += match_size
                retry_num = 3  # reset retry
            except Exception as e:
                retry_num -= 1
                if retry_num < 0:
                    raise Exception(f"Not able not run mapping job, giving up after retry - error: {e}") from e
                else:
                    more_to_match = True
                    if int(match_size / 2) > min_match_size:
                        match_size = int(match_size / 2)
                    log.error(f"Not able to run mapping job - error: {e}")
                    pass

        return all_matches

    except Exception as e:
        raise Exception(f"ERROR: Failed to get matching model and run fit / matching. Error: {type(e)}({e})")
