#!/usr/bin/env python3
"""
Delete DM 3D contextualization artifacts so the pipeline can be re-tested from a clean state.

In a DataModelOnly (DM-only) project the contextualization creates these instances
(all of them are safe to remove and are recreated on the next run):

  - Cognite3DObject nodes   in ASSET instance space   (externalId prefix: cog_3dobj_, cog_3d_object_)
  - CogniteCADNode nodes    in CAD-node + ASSET space  (externalId prefix: cog_3d_cadnode_, cog_3d_node_)
  - CogniteCADModel node    in CAD space               (externalId: cog_3d_model_<modelId>)
  - CogniteCADRevision node in CAD space               (externalId: cog_3d_revision_<revisionId>)
  - SceneConfiguration + Cdf3dModel + edge  in scene space
  - Asset.object3D direct relations on assets (cleared best-effort)
  - RAW tables: contextualization_good / _bad / _all / _manual_input

Everything is read from the module .env (same file the functions/build use).

SAFETY:
  - Dry-run by default: nothing is deleted unless you pass --apply.
  - Choose scope with flags; default scope is the contextualization links (instances).

Usage (run from anywhere; it finds the .env next to the module):
  python scripts/cleanup_dm_contextualization.py                 # dry-run, instances scope
  python scripts/cleanup_dm_contextualization.py --apply         # delete Cognite3DObject + CADNode
  python scripts/cleanup_dm_contextualization.py --raw --apply   # also clear RAW tables
  python scripts/cleanup_dm_contextualization.py --all --apply   # instances + model + revision + scene + raw
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.data_classes.data_modeling import filters as dm_filters
from cognite.client.exceptions import CogniteAPIError

_DELETE_BATCH = 1000

CAD_NODE_VIEW = ViewId("cdf_cdm", "CogniteCADNode", "v1")
OBJ3D_VIEW = ViewId("cdf_cdm", "Cognite3DObject", "v1")
CAD_MODEL_VIEW = ViewId("cdf_cdm", "CogniteCADModel", "v1")
CAD_REVISION_VIEW = ViewId("cdf_cdm", "CogniteCADRevision", "v1")
COG3D_MODEL_VIEW = ViewId("cdf_cdm", "Cognite3DModel", "v1")
COG3D_REVISION_VIEW = ViewId("cdf_cdm", "Cognite3DRevision", "v1")
VISUALIZABLE_VIEW = ViewId("cdf_cdm", "CogniteVisualizable", "v1")



# --------------------------------------------------------------------------------------
# .env loading + client
# --------------------------------------------------------------------------------------
def load_env() -> dict[str, str]:
    """Load KEY=VALUE pairs from the module .env (../.env relative to this script)."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        sys.exit(f"ERROR: .env not found at {env_path}")
    env: dict[str, str] = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip('"').strip("'")
    # Layer in real environment (so exported vars win over .env)
    env.update({k: v for k, v in os.environ.items() if k in env})
    return env


def _req(env: dict[str, str], *names: str) -> str:
    for n in names:
        if env.get(n):
            return env[n]
    sys.exit(f"ERROR: missing required env var (one of): {', '.join(names)}")


def build_client(env: dict[str, str]) -> CogniteClient:
    project = _req(env, "CDF_PROJECT")
    cluster = _req(env, "CDF_CLUSTER")
    token_url = _req(env, "IDP_TOKEN_URL", "cicd_tokenUri")
    client_id = _req(env, "IDP_CLIENT_ID", "cicd_clientId")
    client_secret = _req(env, "IDP_CLIENT_SECRET", "cicd_clientSecret")
    base_url = f"https://{cluster}.cognitedata.com"
    creds = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{base_url}/.default"],
    )
    cfg = ClientConfig(
        client_name="cleanup-dm-contextualization",
        project=project,
        base_url=base_url,
        credentials=creds,
    )
    return CogniteClient(cfg)


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------
def _rel(value) -> tuple[str | None, str | None]:
    """Extract (space, externalId) from a direct-relation value (dict or DirectRelationReference)."""
    if not value:
        return None, None
    if isinstance(value, dict):
        return value.get("space"), value.get("externalId") or value.get("external_id")
    return getattr(value, "space", None), getattr(value, "external_id", None)


def _view_props(node, view: ViewId) -> dict:
    """Read a node's properties for a given view, robust across cognite-sdk versions.

    In SDK 8.x node.properties is keyed by ViewId; .dump() gives the nested
    {space: {"extId/version": {...}}} form which we index directly.
    """
    props = getattr(node, "properties", None)
    if props is None:
        return {}
    dumped = props.dump() if hasattr(props, "dump") else props
    key = f"{view.external_id}/{view.version}"
    return (dumped or {}).get(view.space, {}).get(key, {}) or {}


def _list_nodes_by_view(client: CogniteClient, space: str, view: ViewId):
    return client.data_modeling.instances.list(
        instance_type="node",
        space=space,
        filter=dm_filters.HasData(views=[(view.space, view.external_id, view.version)]),
        limit=-1,
    )


def _delete_nodes(client: CogniteClient, ids: list[NodeId], apply: bool) -> None:
    if not ids:
        return
    if not apply:
        return
    for i in range(0, len(ids), _DELETE_BATCH):
        client.data_modeling.instances.delete(nodes=ids[i : i + _DELETE_BATCH])


def list_models(client: CogniteClient) -> None:
    """Print all 3D/CAD model nodes with their name/space/externalId for discovery."""
    for view in (COG3D_MODEL_VIEW, CAD_MODEL_VIEW):
        print(f"\n-- {view.external_id} ({view.space}/{view.version}) --")
        try:
            nodes = client.data_modeling.instances.list(
                instance_type="node",
                filter=dm_filters.HasData(views=[(view.space, view.external_id, view.version)]),
                sources=[view],
                limit=-1,
            )
        except CogniteAPIError as e:
            print(f"  [warn] could not list {view.external_id}: {e}")
            continue
        if not nodes:
            print("  (none)")
            continue
        for n in nodes:
            props = _view_props(n, view)
            print(f"  space={n.space!r}  externalId={n.external_id!r}  name={props.get('name')!r}")


def inspect_cadnodes(client: CogniteClient, asset_space: str, cad_space: str, model_name: str, sample: int = 8) -> None:
    """Read-only: print a sample of this model's CogniteCADNode instances so we can see the
    real external-ID scheme, whether `asset` is set, and the object3D/model3D/cadNodeReference."""
    print("\n== Inspect CogniteCADNode sample ==")
    model_node = resolve_model_node(client, model_name)
    model_ext = model_node.external_id

    # Build the asset-side of the chain: Cognite3DObject externalId -> asset externalId,
    # from Asset.object3D (CogniteVisualizable). This is what QC and IT rely on.
    obj3d_to_asset: dict[str, str] = {}
    try:
        assets = client.data_modeling.instances.list(
            instance_type="node",
            space=asset_space,
            filter=dm_filters.HasData(views=[(VISUALIZABLE_VIEW.space, VISUALIZABLE_VIEW.external_id, VISUALIZABLE_VIEW.version)]),
            sources=[VISUALIZABLE_VIEW],
            limit=-1,
        )
        for a in assets:
            _, o_ext = _rel(_view_props(a, VISUALIZABLE_VIEW).get("object3D"))
            if o_ext:
                obj3d_to_asset[o_ext] = a.external_id
    except CogniteAPIError as e:
        print(f"  [warn] could not list assets in '{asset_space}': {e}")
    print(f"  assets with Asset.object3D set in '{asset_space}': {len(obj3d_to_asset)}")

    shown = 0
    with_asset_rel = 0
    total = 0
    object3d_ids: set[str] = set()
    object3d_with_asset: set[str] = set()
    nodes_resolvable = 0
    for sp in {cad_space, asset_space, model_node.space}:
        try:
            nodes = client.data_modeling.instances.list(
                instance_type="node",
                space=sp,
                filter=dm_filters.HasData(views=[(CAD_NODE_VIEW.space, CAD_NODE_VIEW.external_id, CAD_NODE_VIEW.version)]),
                sources=[CAD_NODE_VIEW],
                limit=-1,
            )
        except CogniteAPIError as e:
            print(f"  [warn] could not list CADNodes in '{sp}': {e}")
            continue
        for n in nodes:
            props = _view_props(n, CAD_NODE_VIEW)
            _, m_ext = _rel(props.get("model3D"))
            if m_ext != model_ext:
                continue
            total += 1
            a_space, a_ext = _rel(props.get("asset"))
            if a_ext:
                with_asset_rel += 1
            o_space, o_ext = _rel(props.get("object3D"))
            chain_asset = obj3d_to_asset.get(o_ext) if o_ext else None
            if o_ext:
                object3d_ids.add(o_ext)
                if chain_asset:
                    object3d_with_asset.add(o_ext)
            # An asset is resolvable for this node if the direct relation OR the chain yields one.
            if a_ext or chain_asset:
                nodes_resolvable += 1
            if shown < sample:
                print(
                    f"  externalId={n.external_id!r} space={n.space!r}\n"
                    f"      asset={a_space}/{a_ext}  object3D={o_space}/{o_ext}  "
                    f"cadNodeReference={props.get('cadNodeReference')!r}\n"
                    f"      → asset via object3D chain: {chain_asset!r}"
                )
                shown += 1

    print(
        f"  -- total CADNodes for model '{model_ext}': {total}; "
        f"with CADNode.asset set: {with_asset_rel}"
    )
    print(
        f"  -- unique object3D referenced: {len(object3d_ids)}; "
        f"of those backed by an Asset.object3D: {len(object3d_with_asset)}"
    )
    print(
        f"  -- CADNodes resolvable to an asset (direct or via chain): {nodes_resolvable}/{total}"
    )
    if total and nodes_resolvable == 0:
        print("  [warn] No CADNode resolves to an asset — QC would find 0 mappings. "
              "Check that Asset.object3D is set on assets in the asset space.")


def _revision_sort_key(external_id: str) -> tuple[int, int | str]:
    """Sort revisions: those with the cog_3d_revision_<int> pattern by numeric id (latest last)."""
    if external_id.startswith("cog_3d_revision_"):
        try:
            return (1, int(external_id.split("cog_3d_revision_")[1]))
        except ValueError:
            pass
    return (0, external_id)


def resolve_model_node(client: CogniteClient, model_name: str):
    """Find the model instance node by name across Cognite3DModel / CogniteCADModel."""
    for view in (COG3D_MODEL_VIEW, CAD_MODEL_VIEW):
        key = f"{view.external_id}/{view.version}"
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(property=[view.space, key, "name"], value=model_name),
            limit=100,
        )
        if nodes:
            print(f"  matched model '{model_name}' via {view.external_id}: {nodes[0].space}/{nodes[0].external_id}")
            return nodes[0]
    sys.exit(
        f"ERROR: no Cognite3DModel/CogniteCADModel named '{model_name}' found.\n"
        f"       Check the name (this came from --model-name or .env THREE_D_MODEL_NAME/3d_model_name)."
    )


def resolve_model_revision(client: CogniteClient, model_name: str) -> tuple[str, str]:
    """Resolve the latest revision instance (space, externalId) for a 3D/CAD model by name."""
    model_node = resolve_model_node(client, model_name)
    for rev_view in (CAD_REVISION_VIEW, COG3D_REVISION_VIEW):
        key = f"{rev_view.external_id}/{rev_view.version}"
        rev_nodes = client.data_modeling.instances.list(
            instance_type="node",
            filter=dm_filters.Equals(
                property=[rev_view.space, key, "model3D"],
                value={"space": model_node.space, "externalId": model_node.external_id},
            ),
            limit=1000,
        )
        if rev_nodes:
            latest = sorted(rev_nodes, key=lambda n: _revision_sort_key(n.external_id))[-1]
            print(f"  using revision {latest.space}/{latest.external_id} (from {rev_view.external_id})")
            return latest.space, latest.external_id

    sys.exit(f"ERROR: no Cognite3DRevision/CogniteCADRevision found for model '{model_name}'")


def delete_via_endpoint(
    client: CogniteClient, asset_space: str, cad_space: str, model_name: str,
    raw_db: str, raw_table_good: str, apply: bool,
) -> None:
    """Undo contextualization via the official POST /3d/contextualization/cad/delete endpoint.

    The endpoint needs the CLASSIC nodeId (not treeIndex), which is not stored on the DM
    CADNode instance — so the item list is read from the RAW good table (assetExternalId/
    assetId + nodeId), mirroring how the create flow built its items.
    """
    print("\n== Delete via /3d/contextualization/cad/delete ==")
    revision_space, revision_ext_id = resolve_model_revision(client, model_name)

    try:
        rows = client.raw.rows.list(db_name=raw_db, table_name=raw_table_good, limit=-1)
    except CogniteAPIError as e:
        print(f"  [error] could not read RAW {raw_db}/{raw_table_good}: {e}")
        print("  The endpoint needs the classic nodeId from RAW. If the table is gone,")
        print("  use the direct DM delete instead:  --instances --apply")
        return

    items: list[dict] = []
    seen: set[tuple[str, int]] = set()
    for row in rows:
        cols = row.columns or {}
        aid = cols.get("assetExternalId") or cols.get("assetId")
        nid = cols.get("nodeId")
        if aid is None or nid is None:
            continue
        try:
            nid = int(nid)
        except (TypeError, ValueError):
            continue
        key = (str(aid), nid)
        if key in seen:
            continue
        seen.add(key)
        items.append({
            "asset": {"instanceId": {"space": asset_space, "externalId": str(aid)}},
            "nodeId": nid,
        })

    print(f"  model='{model_name}' revision={revision_ext_id}")
    print(f"  items from RAW {raw_db}/{raw_table_good}: {len(items)}")
    if not items:
        print("  nothing to delete (RAW good empty?) — try --instances for a direct DM delete")
        return
    if not apply:
        print(f"  would POST {len(items)} items to /3d/contextualization/cad/delete (batches of 100)")
        return

    config = {
        "object3DSpace": asset_space,
        "contextualizationSpace": cad_space,
        "revision": {"instanceId": {"space": revision_space, "externalId": revision_ext_id}},
    }
    endpoint = f"/api/v1/projects/{client.config.project}/3d/contextualization/cad/delete"
    deleted = 0
    for i in range(0, len(items), 100):
        batch = items[i : i + 100]
        resp = client.post(endpoint, json={"items": batch, "dmsContextualizationConfig": config})
        if resp.status_code != 200:
            print(f"  [error] batch {i // 100 + 1}: {resp.status_code} {resp.text[:300]}")
            raise RuntimeError(f"cad/delete failed ({resp.status_code})")
        deleted += len(batch)
        print(f"  batch {i // 100 + 1}: deleted {len(batch)} (total {deleted})")
    print(f"  DELETED {deleted} CAD contextualization links via endpoint")


# --------------------------------------------------------------------------------------
# scopes
# --------------------------------------------------------------------------------------
def clean_instances(
    client: CogniteClient, asset_space: str, cad_space: str, model_name: str, apply: bool
) -> None:
    """Model-scoped, prefix-agnostic delete of the DM 3D chain.

    Finds every CogniteCADNode whose model3D points at the resolved model, then deletes
    those CADNodes and the Cognite3DObject nodes they reference (object3D). External-ID
    schemes vary (cog_3d_node_*, UUID object3D, etc.) so we match by relation, not prefix.
    """
    print("\n== Cognite3DObject + CogniteCADNode (model-scoped) ==")
    model_node = resolve_model_node(client, model_name)
    model_ext = model_node.external_id

    cadnode_ids: list[NodeId] = []
    obj3d_ids: set[tuple[str, str]] = set()
    scanned = 0
    for sp in {cad_space, asset_space, model_node.space}:
        try:
            nodes = client.data_modeling.instances.list(
                instance_type="node",
                space=sp,
                filter=dm_filters.HasData(views=[(CAD_NODE_VIEW.space, CAD_NODE_VIEW.external_id, CAD_NODE_VIEW.version)]),
                sources=[CAD_NODE_VIEW],
                limit=-1,
            )
        except CogniteAPIError as e:
            print(f"  [warn] could not list CADNodes in '{sp}': {e}")
            continue
        for n in nodes:
            scanned += 1
            props = _view_props(n, CAD_NODE_VIEW)
            _, m_ext = _rel(props.get("model3D"))
            if m_ext != model_ext:
                continue
            cadnode_ids.append(NodeId(n.space, n.external_id))
            o_space, o_ext = _rel(props.get("object3D"))
            if o_space and o_ext:
                obj3d_ids.add((o_space, o_ext))

    obj3d_node_ids = [NodeId(s, e) for s, e in obj3d_ids]
    print(f"  CADNodes scanned: {scanned}")
    print(f"  CogniteCADNode matching model '{model_ext}': {len(cadnode_ids)}")
    print(f"  Cognite3DObject referenced: {len(obj3d_node_ids)}")
    _delete_nodes(client, cadnode_ids, apply)
    _delete_nodes(client, obj3d_node_ids, apply)
    print(f"  {'DELETED' if apply else 'would delete'} {len(cadnode_ids) + len(obj3d_node_ids)} nodes")


def clear_asset_object3d(client: CogniteClient, asset_space: str, apply: bool) -> None:
    """Best-effort: null Asset.object3D on assets that still point at a 3D object."""
    print("\n== Asset.object3D relations ==")
    from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

    to_clear: list[NodeApply] = []
    try:
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            space=asset_space,
            filter=dm_filters.HasData(views=[(VISUALIZABLE_VIEW.space, VISUALIZABLE_VIEW.external_id, VISUALIZABLE_VIEW.version)]),
            sources=[VISUALIZABLE_VIEW],
            limit=-1,
        )
    except CogniteAPIError as e:
        print(f"  [warn] could not list assets: {e}")
        return
    for n in nodes:
        props = _view_props(n, VISUALIZABLE_VIEW)
        if props.get("object3D"):
            to_clear.append(
                NodeApply(
                    space=n.space,
                    external_id=n.external_id,
                    sources=[NodeOrEdgeData(source=VISUALIZABLE_VIEW, properties={"object3D": None})],
                )
            )
    print(f"  assets with object3D set: {len(to_clear)}")
    if apply and to_clear:
        for i in range(0, len(to_clear), _DELETE_BATCH):
            client.data_modeling.instances.apply(nodes=to_clear[i : i + _DELETE_BATCH])
    print(f"  {'CLEARED' if apply else 'would clear'} {len(to_clear)} Asset.object3D relations")


def clean_model_revision(client: CogniteClient, model_name: str, apply: bool) -> None:
    """Delete ONLY the resolved model node and its revisions (not every model in the space)."""
    print("\n== CogniteCADModel + CogniteCADRevision (model-scoped) ==")
    model_node = resolve_model_node(client, model_name)
    rev_ids: list[NodeId] = []
    for rev_view in (CAD_REVISION_VIEW, COG3D_REVISION_VIEW):
        key = f"{rev_view.external_id}/{rev_view.version}"
        try:
            revs = client.data_modeling.instances.list(
                instance_type="node",
                filter=dm_filters.Equals(
                    property=[rev_view.space, key, "model3D"],
                    value={"space": model_node.space, "externalId": model_node.external_id},
                ),
                limit=1000,
            )
        except CogniteAPIError as e:
            print(f"  [warn] could not list {rev_view.external_id}: {e}")
            continue
        rev_ids += [NodeId(r.space, r.external_id) for r in revs]
    # dedupe
    rev_ids = list({(r.space, r.external_id): r for r in rev_ids}.values())
    print(f"  model: {model_node.space}/{model_node.external_id}")
    print(f"  revisions: {len(rev_ids)}")
    _delete_nodes(client, rev_ids, apply)
    _delete_nodes(client, [NodeId(model_node.space, model_node.external_id)], apply)
    print(f"  {'DELETED' if apply else 'would delete'} {len(rev_ids) + 1} nodes (revisions + model)")


def clean_scene(client: CogniteClient, scene_space: str, apply: bool) -> None:
    print("\n== SceneConfiguration + Cdf3dModel (scene) ==")
    scene_cfg_view = ViewId("scene", "SceneConfiguration", "v1")
    cdf3dmodel_view = ViewId("cdf_3d_schema", "Cdf3dModel", "1")
    ids: list[NodeId] = []
    for view in (scene_cfg_view, cdf3dmodel_view):
        try:
            for n in _list_nodes_by_view(client, scene_space, view):
                ids.append(NodeId(n.space, n.external_id))
        except CogniteAPIError as e:
            print(f"  [warn] could not list {view.external_id} in '{scene_space}': {e}")
    print(f"  scene nodes (SceneConfiguration + Cdf3dModel) in '{scene_space}': {len(ids)}")
    print("  NOTE: this targets ALL scenes in the scene space. If you share this space with")
    print("        other 3D models, delete specific externalIds instead.")
    _delete_nodes(client, ids, apply)
    print(f"  {'DELETED' if apply else 'would delete'} {len(ids)} scene nodes")


def clean_raw(client: CogniteClient, raw_db: str, tables: list[str], apply: bool) -> None:
    print(f"\n== RAW tables in '{raw_db}' ==")
    for tbl in tables:
        if not tbl:
            continue
        if apply:
            try:
                client.raw.tables.delete(raw_db, [tbl])
                print(f"  DELETED table {raw_db}/{tbl}")
            except CogniteAPIError as e:
                if e.code == 404:
                    print(f"  (not found) {raw_db}/{tbl}")
                else:
                    print(f"  [warn] failed to delete {raw_db}/{tbl}: {e}")
        else:
            print(f"  would delete table {raw_db}/{tbl}")


# --------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="actually delete (default is dry-run)")
    parser.add_argument(
        "--endpoint",
        action="store_true",
        help="undo via official POST /3d/contextualization/cad/delete (recommended; uses 3d_model_name)",
    )
    parser.add_argument(
        "--model-name",
        default=None,
        help="override the 3D model name (defaults to .env THREE_D_MODEL_NAME/3d_model_name)",
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="discovery: list all Cognite3DModel/CogniteCADModel nodes (name/space/externalId) and exit",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="discovery: print a sample of the model's CADNodes (externalId scheme, asset/object3D relations) and exit",
    )
    parser.add_argument("--instances", action="store_true", help="model-scoped delete of CogniteCADNode + referenced Cognite3DObject (default scope; reliable, prefix-agnostic)")
    parser.add_argument("--clear-object3d", action="store_true", help="null Asset.object3D relations")
    parser.add_argument("--model", action="store_true", help="delete CogniteCADModel + CogniteCADRevision")
    parser.add_argument("--scene", action="store_true", help="delete SceneConfiguration + Cdf3dModel in scene space")
    parser.add_argument("--raw", action="store_true", help="clear RAW good/bad/all/manual tables")
    parser.add_argument("--all", action="store_true", help="everything: instances + object3d + model + scene + raw")
    args = parser.parse_args()

    env = load_env()
    client = build_client(env)

    if args.list_models:
        print(f"Project: {env.get('CDF_PROJECT')}  ({env.get('CDF_CLUSTER')})")
        list_models(client)
        return

    asset_space = _req(env, "ASSET_INSTANCE_SPACE")
    cad_space_early = env.get("CAD_NODE_INSTANCE_SPACE") or env.get("DEFAULT_CAD_SPACE") or asset_space
    if args.inspect:
        model_name = args.model_name or _req(env, "THREE_D_MODEL_NAME", "3d_model_name")
        print(f"Project: {env.get('CDF_PROJECT')}  ({env.get('CDF_CLUSTER')})")
        inspect_cadnodes(client, asset_space, cad_space_early, model_name)
        return

    cad_space = env.get("CAD_NODE_INSTANCE_SPACE") or env.get("DEFAULT_CAD_SPACE") or asset_space
    scene_space = env.get("DEFAULT_SCENE_SPACE") or "scene"
    raw_db = _req(env, "RAW_DB")
    raw_tables = [
        env.get("RAW_TABLE_GOOD", "contextualization_good"),
        env.get("RAW_TABLE_BAD", "contextualization_bad"),
        env.get("RAW_TABLE_ALL", "contextualization_all"),
        env.get("RAW_TABLE_MANUAL", "contextualization_manual_input"),
    ]

    do_instances = args.instances or args.all or not (
        args.model or args.scene or args.raw or args.clear_object3d or args.endpoint
    )
    do_object3d = args.clear_object3d or args.all
    do_model = args.model or args.all
    do_scene = args.scene or args.all
    do_raw = args.raw or args.all

    print("=" * 70)
    print(f"Project        : {env.get('CDF_PROJECT')}  ({env.get('CDF_CLUSTER')})")
    print(f"Asset space    : {asset_space}")
    print(f"CAD-node space : {cad_space}")
    print(f"Scene space    : {scene_space}")
    print(f"RAW db         : {raw_db}")
    print(f"Mode           : {'APPLY (will delete)' if args.apply else 'DRY-RUN (no changes)'}")
    print("=" * 70)

    # Model name is needed for endpoint / instances / model scopes.
    model_name = None
    if args.endpoint or do_instances or do_model:
        model_name = args.model_name or _req(env, "THREE_D_MODEL_NAME", "3d_model_name")

    raw_table_good = raw_tables[0]

    if args.endpoint:
        delete_via_endpoint(client, asset_space, cad_space, model_name, raw_db, raw_table_good, args.apply)
    if do_instances:
        clean_instances(client, asset_space, cad_space, model_name, args.apply)
    if do_object3d:
        clear_asset_object3d(client, asset_space, args.apply)
    if do_model:
        clean_model_revision(client, model_name, args.apply)
    if do_scene:
        clean_scene(client, scene_space, args.apply)
    if do_raw:
        clean_raw(client, raw_db, raw_tables, args.apply)

    print("\nDone." + ("" if args.apply else "  (dry-run — re-run with --apply to delete)"))


if __name__ == "__main__":
    main()
