from __future__ import annotations

import re
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling import filters as dm_filters

from config import ContextConfig
from logger import log

_CAD_NODE_VIEW = ViewId("cdf_cdm", "CogniteCADNode", "v1")
_CAD_MODEL_VIEW = ViewId("cdf_cdm", "CogniteCADModel", "v1")
_CAD_REVISION_VIEW = ViewId("cdf_cdm", "CogniteCADRevision", "v1")
_VISUALIZABLE_VIEW = ViewId("cdf_cdm", "CogniteVisualizable", "v1")
_MV = f"{_CAD_MODEL_VIEW.external_id}/{_CAD_MODEL_VIEW.version}"
_RV = f"{_CAD_REVISION_VIEW.external_id}/{_CAD_REVISION_VIEW.version}"


def _rel_ext_id(value: Any) -> str | None:
    """externalId from a direct-relation value (dict or DirectRelationReference)."""
    if not value:
        return None
    if isinstance(value, dict):
        return value.get("externalId") or value.get("external_id")
    return getattr(value, "external_id", None)


def _view_props(node: Any, view: ViewId) -> dict:
    """Read a node's properties for a view, robust across cognite-sdk versions.

    SDK 8.x keys node.properties by ViewId; .dump() yields the nested
    {space: {"extId/version": {...}}} form which we index directly.
    """
    props = getattr(node, "properties", None)
    if props is None:
        return {}
    dumped = props.dump() if hasattr(props, "dump") else props
    key = f"{view.external_id}/{view.version}"
    return (dumped or {}).get(view.space, {}).get(key, {}) or {}


def _node_id_from_cadnode(props: dict, external_id: str) -> int | None:
    """The 3D nodeId for a CADNode: prefer cadNodeReference, else trailing int of externalId."""
    ref = props.get("cadNodeReference")
    if ref is not None:
        try:
            return int(ref)
        except (TypeError, ValueError):
            pass
    m = re.search(r"(\d+)$", external_id or "")
    return int(m.group(1)) if m else None


def get_treed_asset_mappings(
    client: CogniteClient,
    config: ContextConfig,
) -> dict[str, Any]:
    """
    Build {asset_external_id: [nodeId, ...]} reflecting the current DM 3D links.

    Anchored on the asset side through the chain the contextualization endpoint creates:
        Asset.object3D → Cognite3DObject ← CogniteCADNode.object3D
    so it is independent of the CADNode externalId naming scheme and works whether or not
    CogniteCADNode.asset is populated. The per-node id is read from CADNode.cadNodeReference
    (the value stored per node), falling back to the trailing integer of the externalId,
    so it can be compared against the nodeId persisted in the RAW good table.
    """
    cad_space = config.cad_node_dm_space
    if not cad_space:
        raise ValueError(
            "cad_node_dm_space must be set in pipeline config to run the quality check in DM mode."
        )
    asset_space = config.asset_dm_space or cad_space

    # 1) Map Cognite3DObject externalId -> asset externalId via Asset.object3D
    obj3d_to_asset: dict[str, str] = {}
    assets = client.data_modeling.instances.list(
        instance_type="node",
        space=asset_space,
        filter=dm_filters.HasData(
            views=[(_VISUALIZABLE_VIEW.space, _VISUALIZABLE_VIEW.external_id, _VISUALIZABLE_VIEW.version)]
        ),
        sources=[_VISUALIZABLE_VIEW],
        limit=-1,
    )
    for asset in assets:
        obj3d_ext = _rel_ext_id(_view_props(asset, _VISUALIZABLE_VIEW).get("object3D"))
        if obj3d_ext:
            obj3d_to_asset[obj3d_ext] = asset.external_id

    # 2) Walk CADNodes; resolve the asset via the object3D chain (CADNode.asset as fallback)
    existing_matches: dict[str, list[int]] = {}
    spaces = {cad_space, asset_space}
    cad_node_count = 0
    for sp in spaces:
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            space=sp,
            filter=dm_filters.HasData(
                views=[(_CAD_NODE_VIEW.space, _CAD_NODE_VIEW.external_id, _CAD_NODE_VIEW.version)]
            ),
            sources=[_CAD_NODE_VIEW],
            limit=-1,
        )
        for node in nodes:
            props = _view_props(node, _CAD_NODE_VIEW)
            asset_ext_id = _rel_ext_id(props.get("asset"))
            if not asset_ext_id:
                obj3d_ext = _rel_ext_id(props.get("object3D"))
                asset_ext_id = obj3d_to_asset.get(obj3d_ext) if obj3d_ext else None
            if not asset_ext_id:
                continue
            node3d_id = _node_id_from_cadnode(props, node.external_id)
            if node3d_id is None:
                continue
            cad_node_count += 1
            existing_matches.setdefault(asset_ext_id, []).append(node3d_id)

    log.info(
        f"QC: built {len(existing_matches)} asset→node mappings from {cad_node_count} CAD nodes "
        f"(spaces {sorted(spaces)}, asset space '{asset_space}')"
    )
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
            limit=100,
        )

        model_matches = [n for n in model_nodes if n.external_id.startswith("cog_3d_model_")]
        if not model_matches:
            raise ValueError(f"No CogniteCADModel with name='{three_d_model_name}' found in DM")
        if len(model_matches) > 1:
            log.warning(
                f"QC: found {len(model_matches)} CogniteCADModel nodes named '{three_d_model_name}'; "
                f"using the first ({model_matches[0].external_id})."
            )
        model_node = model_matches[0]
        model_ext_id = model_node.external_id
        model_id = int(model_ext_id.split("cog_3d_model_")[1])
        model_space = model_node.space
        log.info(
            f"QC: found CogniteCADModel '{three_d_model_name}' "
            f"space='{model_space}' ext_id={model_ext_id} id={model_id}"
        )

        # Pick the latest revision (highest revisionId) rather than an arbitrary first match.
        revision_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[_CAD_REVISION_VIEW.space, _RV, "model3D"],
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
                f"No CogniteCADRevision found for model '{three_d_model_name}' "
                f"(model externalId={model_ext_id})"
            )
        revision_id = max(revision_ids)
        log.info(
            f"QC: found {len(revision_ids)} revision(s) for model '{three_d_model_name}'; "
            f"using latest revision_id={revision_id}"
        )

        return model_id, revision_id

    except Exception as e:
        raise Exception(
            f"ERROR: Not able to get 3D model/revision for '{three_d_model_name}' "
            f"(dataset: {config.three_d_data_set_ext_id}) - error: {e}"
        )
