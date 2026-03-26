"""
DM-only 3D CAD contextualization via dedicated CDF endpoint.

Uses POST /api/v1/projects/{project}/3d/contextualization/cad to create the full
DM chain: Asset.object3D → Cognite3DObject ← CADNode → CADRevision.

Also ensures CADModel/CADRevision and SceneConfiguration exist.
Called from pipeline after writing good matches to RAW (when use_dm_cad_contextualization is True).
"""
from __future__ import annotations

import time
from typing import Any, Optional

import requests
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DataModelApply,
    EdgeApply,
    NodeApply,
    NodeOrEdgeData,
    ViewId,
)

from config import ContextConfig, resolve_dm_cad_contextualization_config
from get_resources import get_3d_model_id_and_revision_id
from logger import log


def _get_cad_node_view(required_views: list[ViewId]) -> ViewId:
    return next(
        (v for v in required_views if v.external_id == "CogniteCADNode"),
        ViewId("cdf_cdm", "CogniteCADNode", "v1"),
    )


def run(
    client: CogniteClient,
    config: ContextConfig,
    model_id: Optional[int] = None,
    revision_id: Optional[int] = None,
    *,
    ensure_views: bool = True,
) -> None:
    """
    Run DM-only CAD contextualization: ensure CAD revision/scene, then apply
    mappings from config.rawdb / config.raw_table_good via the dedicated API.

    model_id and revision_id can be passed in (e.g. from the pipeline) or
    resolved from config.three_d_model_name when either is None.
    Uses config.asset_dm_space and config.cad_node_dm_space (or defaults).
    """
    if model_id is None or revision_id is None:
        model_id, revision_id = get_3d_model_id_and_revision_id(
            client, config, config.three_d_model_name
        )
        log.info(f"Resolved model_id={model_id}, revision_id={revision_id} from config.three_d_model_name={config.three_d_model_name!r}")

    resolved = resolve_dm_cad_contextualization_config(config)
    asset_space = config.asset_dm_space
    ctx_space = asset_space  # CADNodes in same space as assets for IT location
    raw_db = config.rawdb
    raw_table = config.raw_table_good
    revision_ext_id = f"cog_3d_revision_{revision_id}"
    model_ext_id = f"cog_3d_model_{model_id}"
    scene_model_ext = resolved.scene_model_ext_id or f"clov_3d_model_{model_id}"

    if config.debug:
        log.info("apply_dm_cad_contextualization: debug=True, skipping DM CAD apply")
        return

    # 1) Ensure CADModel + CADRevision
    _ensure_cad_revision(
        client, resolved.cad_space, model_ext_id, revision_ext_id, revision_id,
        resolved.cad_model_name, resolved.cad_model_type,
        resolved.views["cad_model_view"], resolved.views["cad_revision_view"],
    )

    # 2) Optionally add required views to data model
    if ensure_views:
        _ensure_dm_views(client, resolved.dm_space, resolved.dm_ext_id, resolved.dm_version, resolved.required_views)

    # 3) Apply contextualization from RAW via dedicated API
    _apply_contextualization(
        client, asset_space, ctx_space, resolved.cad_space, revision_ext_id,
        raw_db, raw_table, model_id, revision_id,
        batch_size=resolved.batch_size,
        required_views=resolved.required_views,
    )

    # 4) Ensure SceneConfiguration
    _ensure_scene(
        client, resolved.scene_space, resolved.scene_ext_id, scene_model_ext, revision_id, resolved.cad_model_name,
        resolved.views["scene_config_view"], resolved.views["scene_model_view"], resolved.views["rev_props_view"],
    )

    # 5) Cleanup legacy cog_3d_node_* nodes
    cad_node_view = _get_cad_node_view(resolved.required_views)
    _cleanup_legacy_cadnodes(client, resolved.cad_space, ctx_space, cad_node_view, resolved.batch_size)

    log.info("apply_dm_cad_contextualization: completed successfully")


def _ensure_cad_revision(
    client: CogniteClient,
    cad_space: str,
    model_ext_id: str,
    revision_ext_id: str,
    revision_id: int,
    cad_model_name: str,
    cad_model_type: str,
    cad_model_view: ViewId,
    cad_revision_view: ViewId,
) -> None:
    client.data_modeling.instances.apply(
        nodes=[
            NodeApply(
                space=cad_space,
                external_id=model_ext_id,
                sources=[
                    NodeOrEdgeData(
                        source=cad_model_view,
                        properties={"name": cad_model_name, "type": cad_model_type},
                    )
                ],
            )
        ]
    )
    log.info(f"CADModel: {cad_space}/{model_ext_id} (name={cad_model_name!r}, type={cad_model_type!r})")

    client.data_modeling.instances.apply(
        nodes=[
            NodeApply(
                space=cad_space,
                external_id=revision_ext_id,
                sources=[
                    NodeOrEdgeData(
                        source=cad_revision_view,
                        properties={
                            "revisionId": revision_id,
                            "published": True,
                            "status": "Done",
                            "type": cad_model_type,
                            "model3D": {"space": cad_space, "externalId": model_ext_id},
                        },
                    ),
                    NodeOrEdgeData(source=cad_model_view, properties={"type": cad_model_type}),
                ],
            )
        ]
    )
    log.info(f"CADRevision: {cad_space}/{revision_ext_id} (revisionId={revision_id})")


def _ensure_dm_views(
    client: CogniteClient,
    dm_space: str,
    dm_ext_id: str,
    dm_version: str,
    required_views: list[ViewId],
) -> None:
    dms = client.data_modeling.data_models.retrieve(
        (dm_space, dm_ext_id, dm_version), inline_views=False
    )
    if not dms:
        log.warning(f"Data model {dm_space}/{dm_ext_id}/{dm_version} not found — skipping view injection")
        return
    dm = dms[0]
    existing = {(v.space, v.external_id, v.version) for v in dm.views}
    added = [v for v in required_views if (v.space, v.external_id, v.version) not in existing]
    if added:
        dm.views.extend(added)
        client.data_modeling.data_models.apply(
            DataModelApply(
                space=dm_space,
                external_id=dm_ext_id,
                version=dm_version,
                name=dm.name,
                description=dm.description or "",
                views=dm.views,
            )
        )
        log.info(f"Added {len(added)} views to data model {dm_space}/{dm_ext_id}/{dm_version}")
    else:
        log.info(f"All required 3D views already present in data model {dm_space}/{dm_ext_id}/{dm_version}")


def _get_cluster_token(client: CogniteClient) -> str:
    """Return the cluster-scoped Bearer token from the SDK credentials."""
    return client._config.credentials.authorization_header()[1]


def _apply_contextualization(
    client: CogniteClient,
    asset_space: str,
    ctx_space: str,
    cad_space: str,
    revision_ext_id: str,
    raw_db: str,
    raw_table: str,
    model_id: int,
    revision_id: int,
    *,
    batch_size: int = 100,
    required_views: list[ViewId],
) -> None:
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.data_classes.data_modeling import filters as dm_filters

    cad_view = _get_cad_node_view(required_views)
    for sp in {ctx_space, cad_space}:
        existing = client.data_modeling.instances.list(
            instance_type="node",
            space=sp,
            filter=dm_filters.HasData(views=[(cad_view.space, cad_view.external_id, cad_view.version)]),
            limit=-1,
        )
        api_nodes = [
            NodeId(n.space, n.external_id)
            for n in existing
            if n.external_id.startswith("cog_3d_cadnode_")
        ]
        if api_nodes:
            for i in range(0, len(api_nodes), batch_size):
                client.data_modeling.instances.delete(nodes=api_nodes[i : i + batch_size])
            log.info(f"Deleted {len(api_nodes)} existing cog_3d_cadnode_* in {sp}")

    rows = list(client.raw.rows.list(raw_db, raw_table, limit=-1))
    items = []
    seen = set()
    skipped_fake = 0
    for r in rows:
        aid = r.columns.get("assetId") or r.columns.get("assetExternalId")
        nid = r.columns.get("3DId")
        if aid is None or nid is None:
            continue
        nid = int(nid)
        # Skip fake/demo nodeIds — real 3D node IDs in CDF are always large numbers.
        # Small IDs (< 100_000) are placeholder test data that don't exist in the model.
        if nid < 100_000:
            skipped_fake += 1
            log.warning(f"Skipping item assetId={aid!r} nodeId={nid} — nodeId looks like a demo/fake ID (< 100,000). Update contextualization_manual_input with real 3D node IDs.")
            continue
        key = (str(aid), nid)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "asset": {"instanceId": {"space": asset_space, "externalId": str(aid)}},
                "nodeId": nid,
            }
        )

    if skipped_fake:
        log.warning(f"Skipped {skipped_fake} items with fake/demo nodeIds. Populate contextualization_manual_input with real 3D node IDs from the model.")

    if not items:
        log.info("No valid items to contextualize (all nodeIds were fake/demo data)")
        return

    log.info(f"Applying contextualization for {len(items)} valid items from {raw_db}/{raw_table} ({skipped_fake} fake nodeIds skipped)")

    project = client._config.project
    api_config = {
        "object3DSpace": asset_space,
        "contextualizationSpace": ctx_space,
        "revision": {
            "instanceId": {"space": cad_space, "externalId": revision_ext_id},
        },
    }

    cluster_token = _get_cluster_token(client)
    # Extract cluster name from base_url: "https://{cluster}.cognitedata.com"
    cluster = client._config.base_url.rstrip("/").removeprefix("https://").removesuffix(".cognitedata.com")
    url = f"https://{cluster}.cognitedata.com/api/v1/projects/{project}/3d/contextualization/cad"
    headers = {"Authorization": cluster_token, "Content-Type": "application/json"}

    log.info(f"Using contextualization URL: {url}")

    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        resp = requests.post(
            url,
            headers=headers,
            json={"items": batch, "dmsContextualizationConfig": api_config},
            timeout=60,
        )
        if resp.status_code != 200:
            log.error(f"Contextualization API error {resp.status_code}: {resp.text[:500]}")
            raise RuntimeError(f"Contextualization API failed ({resp.status_code}): {resp.text}")
        log.info(f"Batch {i // batch_size + 1}/{(len(items) + batch_size - 1) // batch_size}: {len(batch)} items OK")
        time.sleep(0.1)

    log.info(f"Contextualization done: {len(items)} asset-3D links applied")


def _ensure_scene(
    client: CogniteClient,
    scene_space: str,
    scene_ext_id: str,
    scene_model_ext: str,
    revision_id: int,
    cad_model_name: str,
    scene_config_view: ViewId,
    scene_model_view: ViewId,
    rev_props_view: ViewId,
) -> None:
    client.data_modeling.instances.apply(
        nodes=[
            NodeApply(
                space=scene_space,
                external_id=scene_model_ext,
                sources=[NodeOrEdgeData(source=scene_model_view, properties={"name": cad_model_name})],
            )
        ]
    )
    client.data_modeling.instances.apply(
        nodes=[
            NodeApply(
                space=scene_space,
                external_id=scene_ext_id,
                sources=[
                    NodeOrEdgeData(
                        source=scene_config_view,
                        properties={
                            "name": f"{cad_model_name} Scene",
                            "description": f"3D contextualization scene for {cad_model_name} model",
                            "cameraTranslationX": 0.0,
                            "cameraTranslationY": 0.0,
                            "cameraTranslationZ": 50.0,
                            "cameraEulerRotationX": 0.0,
                            "cameraEulerRotationY": 0.0,
                            "cameraEulerRotationZ": 0.0,
                        },
                    )
                ],
            )
        ]
    )
    edge_ext = f"{scene_ext_id}_to_{scene_model_ext}"
    client.data_modeling.instances.apply(
        edges=[
            EdgeApply(
                space=scene_space,
                external_id=edge_ext,
                type={"space": "scene", "externalId": "SceneConfiguration.model3ds"},
                start_node={"space": scene_space, "externalId": scene_ext_id},
                end_node={"space": scene_space, "externalId": scene_model_ext},
                sources=[
                    NodeOrEdgeData(
                        source=rev_props_view,
                        properties={
                            "revisionId": revision_id,
                            "translationX": 0.0,
                            "translationY": 0.0,
                            "translationZ": 0.0,
                            "eulerRotationX": 0.0,
                            "eulerRotationY": 0.0,
                            "eulerRotationZ": 0.0,
                            "scaleX": 1.0,
                            "scaleY": 1.0,
                            "scaleZ": 1.0,
                            "defaultVisible": True,
                        },
                    )
                ],
            )
        ]
    )
    log.info(f"Scene: {scene_space}/{scene_ext_id}, model {scene_model_ext} (revisionId={revision_id})")


def _cleanup_legacy_cadnodes(
    client: CogniteClient,
    cad_space: str,
    ctx_space: str,
    cad_node_view: ViewId,
    batch_size: int = 100,
) -> None:
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.data_classes.data_modeling import filters as dm_filters

    to_delete = []
    for sp in {cad_space, ctx_space}:
        for n in client.data_modeling.instances.list(
            instance_type="node",
            space=sp,
            filter=dm_filters.HasData(views=[(cad_node_view.space, cad_node_view.external_id, cad_node_view.version)]),
            limit=-1,
        ):
            if n.external_id.startswith("cog_3d_node_"):
                to_delete.append(NodeId(n.space, n.external_id))
    if not to_delete:
        return
    for i in range(0, len(to_delete), batch_size):
        client.data_modeling.instances.delete(nodes=to_delete[i : i + batch_size])
    log.info(f"Cleaned up {len(to_delete)} legacy cog_3d_node_* CADNodes")
