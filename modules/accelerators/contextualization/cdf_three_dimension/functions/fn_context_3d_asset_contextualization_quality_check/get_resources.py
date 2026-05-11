from __future__ import annotations

from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as dm_filters

from config import ContextConfig
from logger import log

_CAD_NODE_VIEW = ViewId("cdf_cdm", "CogniteCADNode", "v1")
_CAD_MODEL_VIEW = ViewId("cdf_cdm", "CogniteCADModel", "v1")
_CAD_REVISION_VIEW = ViewId("cdf_cdm", "CogniteCADRevision", "v1")
_MV = f"{_CAD_MODEL_VIEW.external_id}/{_CAD_MODEL_VIEW.version}"
_RV = f"{_CAD_REVISION_VIEW.external_id}/{_CAD_REVISION_VIEW.version}"


def get_treed_asset_mappings(
    client: CogniteClient,
    config: ContextConfig,
) -> dict[str, Any]:
    """
    Build {asset_external_id: [node3d_id, ...]} from DM CogniteCADNode.asset relations.

    Queries all CogniteCADNode nodes in config.cad_node_dm_space that have the
    'asset' direct relation set. node3d_id is extracted from the standard
    externalId pattern: cog_3d_cadnode_{node3d_id}.
    """
    cad_space = config.cad_node_dm_space
    if not cad_space:
        raise ValueError(
            "cad_node_dm_space must be set in pipeline config to run the quality check in DM mode."
        )

    nodes = client.data_modeling.instances.list(
        instance_type="node",
        space=cad_space,
        filter=dm_filters.HasData(
            views=[(_CAD_NODE_VIEW.space, _CAD_NODE_VIEW.external_id, _CAD_NODE_VIEW.version)]
        ),
        sources=[_CAD_NODE_VIEW],
        limit=-1,
    )

    existing_matches: dict[str, list[int]] = {}
    for node in nodes:
        if not node.external_id.startswith("cog_3d_cadnode_"):
            continue
        try:
            node3d_id = int(node.external_id.split("cog_3d_cadnode_")[1])
        except ValueError:
            continue

        props = (
            node.properties
            .get(_CAD_NODE_VIEW.space, {})
            .get(f"{_CAD_NODE_VIEW.external_id}/{_CAD_NODE_VIEW.version}", {})
        )
        asset_rel = props.get("asset")
        if not asset_rel:
            continue

        asset_ext_id = (
            asset_rel.get("externalId")
            if isinstance(asset_rel, dict)
            else getattr(asset_rel, "external_id", None)
        )
        if not asset_ext_id:
            continue

        if asset_ext_id in existing_matches:
            existing_matches[asset_ext_id].append(node3d_id)
        else:
            existing_matches[asset_ext_id] = [node3d_id]

    log.info(f"QC: found {len(existing_matches)} asset→CADNode mappings in DM space '{cad_space}'")
    return existing_matches


def get_3d_model_id_and_revision_id(
    client: CogniteClient, config: ContextConfig, three_d_model_name: str
) -> tuple[int, int]:
    """
    Look up 3D model ID and revision ID from DM only (no classic 3D API).

    Finds CogniteCADModel by name, then CogniteCADRevision by model3D relation.
    Numeric IDs are extracted from standard externalId patterns:
      model:    cog_3d_model_{model_id}
      revision: cog_3d_revision_{revision_id}
    """
    try:
        model_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[_CAD_MODEL_VIEW.space, _MV, "name"],
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
                log.info(
                    f"QC: found CogniteCADModel '{three_d_model_name}' "
                    f"space='{model_space}' ext_id={model_ext_id} id={model_id}"
                )
                break

        if model_id is None:
            raise ValueError(f"No CogniteCADModel with name='{three_d_model_name}' found in DM")

        revision_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[_CAD_REVISION_VIEW.space, _RV, "model3D"],
                value={"space": model_space, "externalId": model_ext_id},
            ),
            limit=10,
        )

        revision_id: int | None = None
        for node in revision_nodes:
            if node.external_id.startswith("cog_3d_revision_"):
                revision_id = int(node.external_id.split("cog_3d_revision_")[1])
                log.info(
                    f"QC: found CogniteCADRevision space='{node.space}' "
                    f"ext_id={node.external_id} revision_id={revision_id}"
                )
                break

        if revision_id is None:
            raise ValueError(
                f"No CogniteCADRevision found for model '{three_d_model_name}' "
                f"(model externalId={model_ext_id})"
            )

        return model_id, revision_id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get 3D model/revision for '{three_d_model_name}' "
            f"(dataset: {config.three_d_data_set_ext_id}) - error: {e}"
        )
