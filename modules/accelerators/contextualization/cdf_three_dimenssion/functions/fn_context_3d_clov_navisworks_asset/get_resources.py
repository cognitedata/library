from __future__ import annotations

import json
import re
import sys
import io
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, ContextualizationJob, Row
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId
from cognite.client.data_classes.three_d import ThreeDAssetMapping

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig
from constants import (
    COL_KEY_MAN_CONTEXTUALIZED,
    COL_KEY_MAN_MAPPING_3D_NODE_NAME,
    COL_KEY_MAN_MAPPING_3D_NODE_ID,
    COL_KEY_MAN_MAPPING_ASSET_EXTID,
    COL_KEY_MAN_MAPPING_ASSET_ID,
    COL_MATCH_KEY,
    MAX_MODEL_SIZE_TO_CREATE_MODEL,
    ML_MODEL_FEATURE_TYPE,
)
from logger import log


def manual_table_exists(client: CogniteClient, config: str) -> bool:
    tables = client.raw.tables.list(config.rawdb, limit=None)
    return any(tbl.name == config.raw_table_manual for tbl in tables)


def read_manual_mappings(client: CogniteClient, config: ContextConfig) -> list[dict[str, Any]]:
    raw_table_manual = client.raw.rows.retrieve_dataframe(db_name=config.rawdb, table_name=config.raw_table_manual,
                                                          limit=-1)

    manual_entries = [{'targetId': row['assetId'], 'sourceId': row['3DId']}
                      for index, row in raw_table_manual.iterrows()]
    return manual_entries


def build_cad_node_lookup(client: CogniteClient, instance_space: str) -> dict[int, str]:
    """
    Build a lookup from node3DId (int) to CogniteCADNode external_id in DM.

    Uses filter-only (no sources) to avoid SDK v7.x sources deserialization issue.
    node3DId is extracted from the standard externalId pattern cog_3d_cadnode_{node3d_id}.
    """
    from cognite.client.data_classes.data_modeling import filters as dm_filters

    cad_node_view = ViewId("cdf_cdm", "CogniteCADNode", "v1")
    _cv = f"{cad_node_view.external_id}/{cad_node_view.version}"

    nodes = client.data_modeling.instances.list(
        instance_type="node",
        space=instance_space,
        filter=dm_filters.HasData(views=[(cad_node_view.space, cad_node_view.external_id, cad_node_view.version)]),
        limit=-1,
    )
    lookup: dict[int, str] = {}
    for node in nodes:
        # Extract node3DId from the standard externalId pattern: cog_3d_cadnode_{node3d_id}
        if node.external_id.startswith("cog_3d_cadnode_"):
            try:
                node3d_id = int(node.external_id.split("cog_3d_cadnode_")[1])
                lookup[node3d_id] = node.external_id
            except ValueError:
                pass
    log.info(f"Built CogniteCADNode lookup with {len(lookup)} entries from space '{instance_space}'")
    return lookup


def create_cad_node_mappings(
    client: CogniteClient,
    cad_node_lookup: dict[int, str],
    dm_mappings: list[tuple[int, str, str]],
) -> None:
    """Update CogniteCADNode.asset direct relation for each (node3d_id, asset_ext_id, space) mapping."""
    cad_node_view = ViewId("cdf_cdm", "CogniteCADNode", "v1")
    nodes_to_update: list[NodeApply] = []

    for node3d_id, asset_ext_id, instance_space in dm_mappings:
        cad_node_ext_id = cad_node_lookup.get(node3d_id)
        if not cad_node_ext_id:
            log.warning(f"CogniteCADNode not found for node3DId={node3d_id}, skipping")
            continue
        nodes_to_update.append(
            NodeApply(
                space=instance_space,
                external_id=cad_node_ext_id,
                sources=[NodeOrEdgeData(
                    source=cad_node_view,
                    properties={"asset": {"space": instance_space, "externalId": asset_ext_id}},
                )],
            )
        )

    _BATCH = 1000
    for i in range(0, len(nodes_to_update), _BATCH):
        client.data_modeling.instances.apply(nodes=nodes_to_update[i : i + _BATCH])
    log.info(f"Updated {len(nodes_to_update)} CogniteCADNode asset mappings (DM)")


def get_asset_id_ext_id_mapping(manual_mappings: list[Row]) -> dict[str, Any]:
    """
    Read assets specified in manual mapping input based on external ID and find the corresponding asset internal ID
    Internal ID is used to update time series with asset ID

    Args:
        manual_mappings: list of manual mappings

    Returns:
        dictionary with asset external id as key and asset id as value

    """
    try:
        three_d_node_asset_id = {}
        for mapping in manual_mappings:
            three_d_node_asset_id[mapping[COL_KEY_MAN_MAPPING_3D_NODE_NAME]] = [
                mapping[COL_KEY_MAN_MAPPING_ASSET_ID],
                mapping[COL_KEY_MAN_MAPPING_ASSET_EXTID],
            ]

        return three_d_node_asset_id

    except Exception as e:
        raise Exception(f"ERROR: Not able read list of assets from {manual_mappings}. Error: {type(e)}({e})")


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
            limit=10,
        )

        model_id: int | None = None
        model_ext_id: str | None = None
        model_space: str | None = None

        for node in model_nodes:
            if node.external_id.startswith("cog_3d_model_"):
                model_ext_id = node.external_id
                model_id = int(node.external_id.split("cog_3d_model_")[1])
                model_space = node.space
                log.info(f"Found CogniteCADModel '{three_d_model_name}' space='{model_space}' ext_id={model_ext_id} id={model_id}")
                break

        if model_id is None:
            raise ValueError(f"No CogniteCADModel with name='{three_d_model_name}' found in DM")

        # 2) Find CogniteCADRevision for this model — filter by model3D direct relation
        revision_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[revision_view.space, _rv, "model3D"],
                value={"space": model_space, "externalId": model_ext_id},
            ),
            limit=10,
        )

        revision_id: int | None = None
        for node in revision_nodes:
            if node.external_id.startswith("cog_3d_revision_"):
                revision_id = int(node.external_id.split("cog_3d_revision_")[1])
                log.info(f"Found CogniteCADRevision space='{node.space}' ext_id={node.external_id} revision_id={revision_id}")
                break

        if revision_id is None:
            raise ValueError(
                f"No CogniteCADRevision found for model '{three_d_model_name}' (model externalId={model_ext_id})"
            )

        log.info(f"Resolved: model='{three_d_model_name}' model_id={model_id} revision_id={revision_id}")
        return model_id, revision_id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get entities for 3D nodes in data set: {config.three_d_data_set_ext_id}- error: {e}"
        )


def get_mapping_to_delete(client: CogniteClient, model_id: int, revision_id: int) -> list[ThreeDAssetMapping]:
    mapping_to_delete = client.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id, limit=-1)

    return mapping_to_delete


def get_treed_asset_mappings(
    client: CogniteClient, model_id: int, revision_id: int, existing_matches: dict[str, Any]
) -> dict[str, Any]:
    mappings = client.three_d.asset_mappings.list(model_id=model_id, revision_id=revision_id, limit=-1)

    for mapping in mappings.data:
        if mapping.asset_id in existing_matches:
            existing_matches[mapping.asset_id].append(mapping.node_id)
        else:
            existing_matches[mapping.asset_id] = [mapping.node_id]

    return existing_matches


def filter_3d_nodes(
    client: CogniteClient,
    config: ContextConfig,
    model_id: int,
    revision_id: int,
    manual_mappings: list[Row],
) -> dict[str, Any]:
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
    tree_d_nodes = {}

    node_names = [manual_mappings["3dNodeName"] for manual_mappings in manual_mappings]
    try:
        # read 3D nodes from API with filter on node names
        three_d_nodes = client.three_d.revisions.filter_nodes(
            model_id=model_id,
            revision_id=revision_id,
            properties={"Item": {"Name": node_names}},
            partitions=10,
            limit=-1,
        )

        num_nodes = 0
        for node in three_d_nodes:
            if node.name and node.name != "":
                num_nodes += 1

                if node.name in tree_d_nodes:
                    node_ids = tree_d_nodes[node.name]
                    node_ids.append(
                        {
                            "id": node.id,
                            "subtree_size": node.subtree_size,
                            "tree_index": node.tree_index,
                        }
                    )
                else:
                    node_ids = [
                        {
                            "id": node.id,
                            "subtree_size": node.subtree_size,
                            "tree_index": node.tree_index,
                        }
                    ]
                    tree_d_nodes[node.name] = node_ids

        log.info(
            f"Total number of 3D Node names found for manual mapping: {num_nodes} - unique names : {len(tree_d_nodes)}"
        )

        return tree_d_nodes

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
    numNodes: int = -1,
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
    cdf_3d_nodes = []
    three_d_nodes = {}
    input_three_d_nodes = None

    three_d_model_name = config.three_d_model_name
    try:
        quantum_three_d_nodes = []
        if threed_from_quantum:
            column_three_d_name = "3DName"
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

            # with open("/home/priyanka/Documents/Office/Cognite/Customers/TE/binary_nodes", "rb") as b_file:
            #     file_content = b_file.read()
            #     input_three_d_nodes = json.loads(file_content)

        if not input_three_d_nodes:
            # root_node_ids = [
            #     # 7337885809804409,  # 20210504_CLOV_Part1_#1.nwd
            #     # 8544317359930269,  # Topside.nwd
            #     # 4680504186852549,  # Topside.nwd
            #     7168465427634580,  # M111.nwd
            #     6853894173819691,  # M112.nwd
            #     6902588297621933,  # M113.nwd
            #     597056255964510,  # M114.nwd
            #     5875817696881367,  # M115.nwd
            #     4156786689629169,  # M116.nwd
            #     6728129584161211,  # M121.nwd
            #     2124587024995238,  # M122.nwd
            #     6166097983325138,  # M123.nwd
            #     4785285216113503,  # M124.nwd
            #     7156978479391014,  # M125.nwd
            #     7021601188473505,  # M126.nwd
            #     845230947260427,  # Hull.nwd
            # ]

            input_three_d_nodes_cdf = []

            nodes = client.three_d.revisions.list_nodes(
                model_id=model_id,
                revision_id=revision_id,
                sort_by_node_id=True,
                partitions=500,
                limit=-1,
            )

            input_three_d_nodes_cdf.extend(nodes)

            # for node_id in root_node_ids:
            #     nodes = client.three_d.revisions.list_nodes(
            #         model_id=model_id,
            #         revision_id=revision_id,
            #         sort_by_node_id=True,
            #         partitions=100,
            #         limit=-1,
            #         node_id=node_id
            #     )
            #     input_three_d_nodes_cdf.extend(nodes)

            # Now `all_nodes` contains the nodes retrieved from all the specified root nodes.

            '''import pickle
            # Load the data from the file when needed
            with open('three_d_nodes_data.pkl', 'rb') as file:
                input_three_d_nodes_cdf = pickle.load(file)'''

            for node in input_three_d_nodes_cdf:
                if node.name and node.name != "":
                    # mod_node_name = node.name
                    # if "/" in mod_node_name:
                    #     mod_node_name = mod_node_name.split("/")[-1]

                    # if mod_node_name in asset_filter:
                    cdf_3d_nodes.append(node.dump())

            file_content = json.dumps(cdf_3d_nodes)
            json_bytes = file_content.encode('utf-8')
            binary_io = io.BytesIO(json_bytes)
            client.files.upload_bytes(
                binary_io,
                external_id=model_file_name,
                name=model_file_name,
                overwrite=True,
                data_set_id=three_d_data_set_id,
            )

            log.info(f"Uploaded {model_file_name} to CDF.")

            input_three_d_nodes = json.loads(file_content)

        num_nodes = 0
        if input_three_d_nodes:
            import pandas as pd

            # Convert the list of dictionaries to a DataFrame
            df_nodes = pd.DataFrame(input_three_d_nodes)

            # Name normalizer: use config if available, else generic default for 3D paths
            replacements = getattr(config, "name_replacements", None)
            suffixes_to_strip = getattr(config, "suffixes_to_strip", None)

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
                log.debug(f"{original_name}, {name}")
                return name

            # Filter nodes: quantum list, or config prefixes/slashes, or no filter (default)
            if quantum_three_d_nodes:
                df_nodes = df_nodes[df_nodes["name"].isin(quantum_three_d_nodes)]
            else:
                node_prefixes = getattr(config, "node_name_prefixes", None)
                max_slashes = getattr(config, "node_name_max_slashes", None)
                if node_prefixes:
                    df_nodes = df_nodes[df_nodes["name"].str.startswith(tuple(node_prefixes))]
                if max_slashes is not None:
                    df_nodes = df_nodes[df_nodes["name"].str.count("/") <= max_slashes]

            df_nodes["mode_node_name"] = df_nodes["name"].apply(clean_name)

            # Convert the DataFrame back to a list of dictionaries if needed
            input_three_d_nodes = df_nodes.to_dict(orient='records')

            for node in input_three_d_nodes:
                if node["name"] and node["name"] != "":
                    num_nodes += 1
                    mod_node_name = node["mode_node_name"]

                    if mod_node_name in three_d_nodes:
                        node_ids = three_d_nodes[mod_node_name]
                        node_ids.append(
                            {
                                "id": node["id"],
                                "subtree_size": node["subtreeSize"],
                                "tree_index": node["treeIndex"],
                            }
                        )
                        three_d_nodes[mod_node_name] = node_ids

                        entities = get_3d_entities(node, mod_node_name, entities)
                    else:
                        node_ids = [
                            {
                                "id": node["id"],
                                "subtree_size": node["subtreeSize"],
                                "tree_index": node["treeIndex"],
                            }
                        ]
                        three_d_nodes[mod_node_name] = node_ids

                        entities = get_3d_entities(node, mod_node_name, entities)

                    '''if mod_node_name in three_d_nodes:
                        existing_node = three_d_nodes[mod_node_name]
                        if node["subtreeSize"] > existing_node[0]["subtree_size"]:
                            node_ids = [
                                {
                                    "id": node["id"],
                                    "subtree_size": node["subtreeSize"],
                                    "tree_index": node["treeIndex"],
                                }
                            ]
                            three_d_nodes[mod_node_name] = node_ids
                            entities = replace_3d_entities(node, mod_node_name, entities)
                    else:
                        node_ids = [
                            {
                                "id": node["id"],
                                "subtree_size": node["subtreeSize"],
                                "tree_index": node["treeIndex"],
                            }
                        ]
                        three_d_nodes[mod_node_name] = node_ids

                        entities = get_3d_entities(node, mod_node_name, entities)'''

        log.info(
            f"Total number of 3D Node found: {num_nodes} - unique names to match after asset name filtering: {len(three_d_nodes)}"
        )

        return entities, three_d_nodes

    except Exception as e:
        raise Exception(f"ERROR: Not able to get 3D nodes in data set: {config.three_d_data_set_ext_id} - error: {e}")


def get_3d_entities(node: list[str], modNodeName: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def replace_3d_entities(node: dict[str, Any], modNodeName: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Process time series metadata and replace the entity with the same modNodeName.

    Args:
        node: metadata for 3D node
        modNodeName: modified node name
        entities: already processed entities

    Returns:
        list of entities
    """

    # Create a dictionary for quick lookup by name
    entity_dict = {entity["name"]: entity for entity in entities}

    # Create the new entity
    new_entity = {
        "id": node["id"],
        "name": modNodeName,
        "external_id": node["treeIndex"],
        "org_name": node["name"],
        "type": "3dNode",
    }

    # Replace the entity with the same modNodeName
    entity_dict[modNodeName] = new_entity

    # Convert the dictionary back to a list
    return list(entity_dict.values())


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


def tag_is_dummy(asset: Asset) -> bool:
    custom_description = (asset.metadata or {}).get("Description", "")
    return "DUMMY TAG" in custom_description.upper()


def tag_is_package(asset: Asset) -> bool:
    if not asset.metadata:
        return False

    if "Class" not in asset.metadata.keys():
        return False

    if "PKG" in asset.metadata["Class"]:
        return True


def get_assets(
    client: CogniteClient,
    config: Any,
    existing_matches: list[dict[str, Any]],
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
        instance_space = getattr(config, "asset_dm_space", None) or "sp_enterprise_process_industry"
        asset_view_space = getattr(config, "asset_view_space", instance_space)
        asset_view_ext_id = getattr(config, "asset_view_ext_id", "AssetExtension")
        asset_view_version = getattr(config, "asset_view_version", "v2")

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
    client: CogniteClient, match_to: list[dict[str, Any]], match_from: list[dict[str, Any]], manual_mappings
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
            targets = match_to[:MAX_MODEL_SIZE_TO_CREATE_MODEL]
        else:
            targets = match_to

        def transform_dict_to_list_of_dicts(input_dict):
            result = []
            for key, value in input_dict.items():
                result.append({"targetId": key, "sourceId": int(value[0])})
            return result

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
                    raise Exception("Not able not run mapping job, giving up after retry - error: {e}") from e
                else:
                    more_to_match = True
                    if int(match_size / 2) > min_match_size:
                        match_size = int(match_size / 2)
                    log.error(f"Not able to run mapping job - error: {e}")
                    pass

        return all_matches

    except Exception as e:
        raise Exception(f"ERROR: Failed to get matching model and run fit / matching. Error: {type(e)}({e})")
