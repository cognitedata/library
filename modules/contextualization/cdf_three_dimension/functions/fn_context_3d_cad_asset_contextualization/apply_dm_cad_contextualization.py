"""
DM-only 3D CAD contextualization via dedicated CDF endpoint.

Uses POST /api/v1/projects/{project}/3d/contextualization/cad to create the full
DM chain: Asset.object3D → Cognite3DObject ← CADNode → CADRevision.

Also ensures CADModel/CADRevision and SceneConfiguration exist.
Called from pipeline after writing good matches to RAW.
"""
from __future__ import annotations

import time
from typing import Any, Optional

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


def _rel_ext_id(value: Any) -> Optional[str]:
    """externalId from a direct-relation value (dict or DirectRelationReference)."""
    if not value:
        return None
    if isinstance(value, dict):
        return value.get("externalId") or value.get("external_id")
    return getattr(value, "external_id", None)


def _view_props(node: Any, view: ViewId) -> dict:
    """Read a node's properties for a view, robust across cognite-sdk versions."""
    props = getattr(node, "properties", None)
    if props is None:
        return {}
    dumped = props.dump() if hasattr(props, "dump") else props
    key = f"{view.external_id}/{view.version}"
    return (dumped or {}).get(view.space, {}).get(key, {}) or {}


def _node_in_revision(node: Any, cad_view: ViewId, revision_ext_id: str) -> bool:
    """True if a CogniteCADNode's `revisions` relation includes the given revision."""
    revs = _view_props(node, cad_view).get("revisions")
    if isinstance(revs, list):
        return any(_rel_ext_id(r) == revision_ext_id for r in revs)
    return _rel_ext_id(revs) == revision_ext_id


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
    # CADNodes live in their own space (cad_node_dm_space); resolved.cad_space already
    # resolves from cad_node_dm_space → cad_space → default_cad_space.
    ctx_space = resolved.cad_space
    raw_db = config.rawdb
    raw_table = config.raw_table_good
    revision_ext_id = f"cog_3d_revision_{revision_id}"
    model_ext_id = f"cog_3d_model_{model_id}"
    scene_model_ext = resolved.scene_model_ext_id or f"cog_3d_model_{model_id}"

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
        skip_demo_nodeids=resolved.skip_demo_nodeids,
        required_views=resolved.required_views,
    )

    # 4) Ensure SceneConfiguration
    _ensure_scene(
        client, resolved.scene_space, resolved.scene_ext_id, scene_model_ext, revision_id, resolved.cad_model_name,
        resolved.views["scene_config_view"], resolved.views["scene_model_view"], resolved.views["rev_props_view"],
    )

    # NOTE: no post-apply node cleanup here. Stale nodes from removed mappings are cleared
    # by the revision-scoped reset at the start of _apply_contextualization (and removals can
    # be done explicitly via POST /3d/contextualization/cad/delete). A blanket prefix-based
    # cleanup here previously risked deleting the nodes the endpoint had just created.

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
    skip_demo_nodeids: bool = False,
    required_views: list[ViewId],
) -> None:
    from cognite.client.data_classes.data_modeling import NodeId
    from cognite.client.data_classes.data_modeling import filters as dm_filters

    cad_view = _get_cad_node_view(required_views)
    # Reset the previous contextualization for THIS revision before re-applying.
    # Scope by the CADNode `revisions` relation (prefix-independent) so we only remove
    # nodes belonging to this revision — not nodes of other models/revisions, and without
    # depending on a particular externalId naming scheme.
    for sp in {ctx_space, cad_space}:
        delete_batch: list[NodeId] = []
        deleted_count = 0
        existing = client.data_modeling.instances.list(
            instance_type="node",
            space=sp,
            filter=dm_filters.HasData(views=[(cad_view.space, cad_view.external_id, cad_view.version)]),
            sources=[cad_view],
            limit=-1,
        )
        for node in existing:
            if not _node_in_revision(node, cad_view, revision_ext_id):
                continue
            delete_batch.append(NodeId(node.space, node.external_id))
            if len(delete_batch) >= batch_size:
                client.data_modeling.instances.delete(nodes=delete_batch)
                deleted_count += len(delete_batch)
                delete_batch = []
        if delete_batch:
            client.data_modeling.instances.delete(nodes=delete_batch)
            deleted_count += len(delete_batch)
        if deleted_count:
            log.info(f"Reset {deleted_count} existing CAD nodes for revision {revision_ext_id} in {sp}")

    raw_rows = client.raw.rows.list(raw_db, raw_table, limit=-1)
    seen: set[tuple[str, int]] = set()
    current_batch: list[dict[str, Any]] = []
    total_items = 0
    batch_count = 0

    if skip_demo_nodeids:
        log.info("Applied skip_demo_nodeids filter: excluded rows with nodeId < 100000")

    endpoint = f"/api/v1/projects/{client.config.project}/3d/contextualization/cad"
    api_config = {
        "object3DSpace": asset_space,
        "contextualizationSpace": ctx_space,
        "revision": {
            "instanceId": {"space": cad_space, "externalId": revision_ext_id},
        },
    }

    def _post_batch(batch_items: list[dict[str, Any]]) -> None:
        nonlocal batch_count, total_items
        response = client.post(
            endpoint,
            json={"items": batch_items, "dmsContextualizationConfig": api_config},
        )
        if response.status_code != 200:
            log.error(f"Contextualization API error {response.status_code}: {response.text[:500]}")
            raise RuntimeError(f"Contextualization API failed ({response.status_code}): {response.text}")
        batch_count += 1
        total_items += len(batch_items)
        log.info(f"Batch {batch_count}: {len(batch_items)} items OK")
        time.sleep(0.1)

    for row in raw_rows:
        aid = row.columns.get("assetExternalId") or row.columns.get("assetId")
        nid = row.columns.get("nodeId")
        if aid is None or nid is None:
            continue
        nid = int(nid)
        if skip_demo_nodeids and nid < 100_000:
            continue
        key = (str(aid), nid)
        if key in seen:
            continue
        seen.add(key)
        current_batch.append(
            {
                "asset": {"instanceId": {"space": asset_space, "externalId": str(aid)}},
                "nodeId": nid,
            }
        )
        if len(current_batch) >= batch_size:
            _post_batch(current_batch)
            current_batch = []

    if current_batch:
        _post_batch(current_batch)

    if total_items == 0:
        log.info(f"No items to contextualize in {raw_db}/{raw_table}")
        return

    log.info(f"Contextualization done: {total_items} asset-3D links applied from {raw_db}/{raw_table}")


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


