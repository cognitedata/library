# 3D Contextualization in a DM-Only CDF Project — Setup Guide

> **Project reference:** `cdf-shivam-test` (bluefield cluster)  
> **Data model:** `upstream-value-chain / upstream_value_chain / v1`  
> **Asset instance space:** `instance_upstream_value_chain`  
> **3D instance space:** `rmdm`  

---

## Table of Contents

1. [Confirm Project Type](#1-confirm-project-type)
2. [Full DM 3D Chain](#2-full-dm-3d-chain)
3. [Required Data Model Views](#3-required-data-model-views)
4. [Create the DM 3D Instances](#4-create-the-dm-3d-instances)
5. [Create the Scene Configuration](#5-create-the-scene-configuration)
6. [Space Assignment Rules](#6-space-assignment-rules)
7. [Industrial Tools Location Setup](#7-industrial-tools-location-setup-ui)
8. [Verification Checklist](#8-verification-checklist)
9. [Common Mistakes](#9-common-mistakes)

---

## 1. Confirm Project Type

Before starting, confirm whether the project is **DM-only** or **Hybrid**.

```python
try:
    client.three_d.asset_mappings.list(model_id=MODEL_ID, revision_id=REVISION_ID, limit=1)
    print("HYBRID project — classic asset mappings available")
except Exception as e:
    if "Not supported in DMS Only project" in str(e):
        print("DM-ONLY project — must use DM chain, classic mappings will NEVER work")
```

| Project Type | Classic 3D Asset Mappings | DM 3D Chain | IT 3D Preview |
|---|---|---|---|
| DM-only | Not supported | Required | Works via DM |
| Hybrid | Supported | Optional | Works via classic |

---

## 2. Full DM 3D Chain

All 5 node types must exist and be linked correctly:

```
Asset (instance_upstream_value_chain)
  └─ object3D ──→ Cognite3DObject (instance_upstream_value_chain)  ← SAME space as Asset!
                    └─ cadNodes (reverse relation) ──→ CADNode (rmdm)
                                                         ├─ treeIndexes = [int]   ← NOT node_id
                                                         ├─ revisions   = [→ CADRevision]
                                                         └─ object3D    = → Cognite3DObject

CADRevision (rmdm)
  ├─ status    = "Done"
  ├─ published = True
  ├─ type      = "CAD"
  └─ model3D ──→ CADModel (rmdm)
                  └─ type = "CAD"
```

### Critical Rules

- `Cognite3DObject` **must be in the same instance space as Asset** — Industrial Tools scopes
  queries by the asset's instance space. If it is in the 3D space (`rmdm`), IT will not find it.
- `cadNodeReference` must use `str(treeIndex)` — **not** `node_id`. They are different values.
- `treeIndexes` must be `[int]` (a list of integers). The 3D viewer uses this list, not `cadNodeReference`, to match clicked nodes.
- `revisions` must be a direct-relation list pointing to the `CADRevision` node.
- `CADRevision` must have data written to **both** `cdf_cdm_3d/Cognite3DRevision` (for status/published)
  **and** to `cdf_cdm_3d/Cognite3DModel` on the revision node itself (for `type="CAD"`) to satisfy the view filter.

---

## 3. Required Data Model Views

When you add `Cognite3DObject/v1` to your data model you **must also add every view type
that its relation fields reference**. Missing target view types cause GraphQL `_UnknownType`
errors which crash Industrial Tools.

Add **all of the following views** to your data model as a single operation:

```python
from cognite.client.data_classes.data_modeling import ViewId, DataModelApply

REQUIRED_3D_VIEWS = [
    # Core 3D chain
    ViewId("cdf_cdm",       "Cognite3DObject",           "v1"),
    ViewId("cdf_cdm",       "CogniteCADNode",             "v1"),
    ViewId("cdf_cdm",       "CogniteCADRevision",         "v1"),
    ViewId("cdf_cdm",       "CogniteCADModel",            "v1"),
    ViewId("cdf_cdm",       "Cognite3DRevision",          "v1"),
    ViewId("cdf_cdm",       "Cognite3DModel",             "v1"),
    ViewId("cdf_cdm",       "Cognite3DTransformation",    "v1"),
    ViewId("cdf_cdm",       "CogniteVisualizable",        "v1"),
    # Required by Cognite3DObject.images360
    # (multi-edge connection — needs BOTH target view AND edgeSource view)
    ViewId("cdf_cdm",       "Cognite360Image",            "v1"),
    ViewId("cdf_cdm",       "Cognite360ImageAnnotation",  "v1"),  # ← edgeSource!
    ViewId("cdf_cdm",       "CogniteAnnotation",          "v1"),  # parent of above
    ViewId("cdf_cdm",       "Cognite360ImageCollection",  "v1"),
    ViewId("cdf_cdm",       "Cognite360ImageStation",     "v1"),
    # Required by Cognite3DObject.pointCloudVolumes
    ViewId("cdf_cdm",       "CognitePointCloudVolume",    "v1"),
    ViewId("cdf_cdm",       "CognitePointCloudRevision",  "v1"),
    # Required by Industrial Tools 3D scene preview
    ViewId("scene",         "SceneConfiguration",         "v1"),
    ViewId("scene",         "RevisionProperties",         "v1"),
    ViewId("cdf_3d_schema", "Cdf3dModel",                 "1"),
]

# Add all to the data model
dm = client.data_modeling.data_models.retrieve(
    (DM_SPACE, DM_EXT_ID, DM_VERSION), inline_views=False)[0]
existing = {(v.space, v.external_id, v.version) for v in dm.views}
for v in REQUIRED_3D_VIEWS:
    if (v.space, v.external_id, v.version) not in existing:
        dm.views.append(v)
client.data_modeling.data_models.apply(DataModelApply(
    space=DM_SPACE, external_id=DM_EXT_ID, version=DM_VERSION,
    name=dm.name, description=dm.description, views=dm.views))
```

### Scanning for Missing View Types

After adding views, run this scanner to ensure no further types are missing:

```python
dm = client.data_modeling.data_models.retrieve(
    (DM_SPACE, DM_EXT_ID, DM_VERSION), inline_views=True)[0]

dm_view_ids = {(v.space, v.external_id, v.version) for v in dm.views}
missing = set()
for view in dm.views:
    for prop_name, prop in view.properties.items():
        prop_dict = prop.dump() if hasattr(prop, 'dump') else {}
        for field in ["source", "edgeSource"]:
            src = prop_dict.get(field) or {}
            if isinstance(src, dict) and src.get("type") == "view":
                key = (src.get("space"), src.get("externalId"), src.get("version"))
                if key not in dm_view_ids:
                    missing.add((key, f"{view.external_id}.{prop_name}[{field}]"))

if missing:
    print("MISSING referenced view types:")
    for (space, ext, ver), context in sorted(missing):
        print(f"  {space}/{ext}/{ver}  ← from {context}")
else:
    print("All referenced view types are present.")
```

---

## 4. Create the DM 3D Instances

### Recommended: Use the Dedicated Endpoint (DM-Only Projects Only)

CDF provides a dedicated endpoint specifically for DM-only projects that handles the full
DM chain creation internally — no need to manually build `Cognite3DObject`, `CADNode`, or
update `Asset.object3D`.

```
POST /api/v1/projects/{project}/3d/contextualization/cad
```

**NOTE: This endpoint is only available for DataModelOnly projects.**

```python
import requests

# Read matches from RAW
matches = list(client.raw.rows.list(RAW_DB, RAW_TABLE, limit=-1))

items = [
    {
        "asset":  {"instanceId": {"space": ASSET_SPACE, "externalId": row.columns["assetId"]}},
        "nodeId": int(row.columns["3DId"])   # classic 3D node_id — API resolves treeIndex internally
    }
    for row in matches
]

config = {
    "object3DSpace":          ASSET_SPACE,   # where Cognite3DObject instances are created
    "contextualizationSpace": CAD_SPACE,     # where CADNode instances are created
    "revision": {
        "instanceId": {"space": CAD_SPACE, "externalId": REVISION_EXT_ID}
    }
}

# Send in batches of 100 (API limit per request)
url = f"https://{cluster}.cognitedata.com/api/v1/projects/{project}/3d/contextualization/cad"
token = client._config.credentials.authorization_header()[1]
headers = {"Authorization": token, "Content-Type": "application/json"}

for i in range(0, len(items), 100):
    batch = items[i:i+100]
    resp = requests.post(url, headers=headers,
                         json={"items": batch, "dmsContextualizationConfig": config})
    resp.raise_for_status()
    print(f"  Batch {i//100 + 1}: {len(batch)} items → {resp.status_code}")
```

This replaces the entire manual `create_cad_node_mappings.py` script (~300 lines) with
~20 lines. The API internally handles:
- `nodeId` → `treeIndex` lookup
- `Cognite3DObject` instance creation in `object3DSpace`
- `CADNode` instance creation with `treeIndexes`, `revisions`, `object3D` properties
- `Asset.object3D` property update

### Alternative: Manual DM Instance Creation (Legacy)

If the endpoint is unavailable, use `scripts/create_cad_node_mappings.py` to create all instances.

The script reads from the `contextualization_good` RAW table (output of the entity-matching
pipeline) and creates:

| What | Where | Count (this project) |
|---|---|---|
| `Cognite3DObject` nodes | `instance_upstream_value_chain` | 523 |
| `CADNode` nodes | `rmdm` | 523 |
| `Asset.object3D` updates | `instance_upstream_value_chain` | 14 |

```bash
python scripts/create_cad_node_mappings.py
```

The script is fully idempotent — all operations are upserts, safe to re-run.

### Key Variables in the Script

```python
CAD_INSTANCE_SPACE   = "rmdm"
OBJ3D_INSTANCE_SPACE = "instance_upstream_value_chain"   # MUST match ASSET_INSTANCE_SPACE
ASSET_INSTANCE_SPACE = "instance_upstream_value_chain"

CLASSIC_MODEL_ID    = 1872804187104968
CLASSIC_REVISION_ID = 3857338668041659
```

---

## 5. Create the Scene Configuration

Industrial Tools 3D preview requires a `SceneConfiguration` node. **Without it the 3D
preview panel shows "No data available".**

Run `scripts/create_scene_config.py` (or paste the snippet below):

```python
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId, EdgeApply

SCENE_SPACE         = "scene"
CLASSIC_MODEL_ID    = 1872804187104968
CLASSIC_REVISION_ID = 3857338668041659

MODEL_EXT_ID = f"clov_3d_model_{CLASSIC_MODEL_ID}"
SCENE_EXT_ID = "clov_navisworks_scene"

# 1. Cdf3dModel node — represents the physical 3D model
client.data_modeling.instances.apply(nodes=[NodeApply(
    space=SCENE_SPACE,
    external_id=MODEL_EXT_ID,
    sources=[NodeOrEdgeData(
        source=ViewId("cdf_3d_schema", "Cdf3dModel", "1"),
        properties={"name": "deployment_pack"},
    )],
)])

# 2. SceneConfiguration node — the scene entry point
client.data_modeling.instances.apply(nodes=[NodeApply(
    space=SCENE_SPACE,
    external_id=SCENE_EXT_ID,
    sources=[NodeOrEdgeData(
        source=ViewId("scene", "SceneConfiguration", "v1"),
        properties={
            "name":               "CLOV Navisworks Scene",
            "description":        "3D contextualization scene for CLOV Navisworks model",
            "cameraTranslationX": 0.0,
            "cameraTranslationY": 0.0,
            "cameraTranslationZ": 50.0,
            "cameraEulerRotationX": 0.0,
            "cameraEulerRotationY": 0.0,
            "cameraEulerRotationZ": 0.0,
        },
    )],
)])

# 3. Edge: SceneConfiguration ──(model3ds)──→ Cdf3dModel
#    Edge carries RevisionProperties (which revision to load + transform)
client.data_modeling.instances.apply(edges=[EdgeApply(
    space=SCENE_SPACE,
    external_id=f"{SCENE_EXT_ID}_to_{MODEL_EXT_ID}",
    type={"space": "scene", "externalId": "SceneConfiguration.model3ds"},
    start_node={"space": SCENE_SPACE, "externalId": SCENE_EXT_ID},
    end_node={"space":   SCENE_SPACE, "externalId": MODEL_EXT_ID},
    sources=[NodeOrEdgeData(
        source=ViewId("scene", "RevisionProperties", "v1"),
        properties={
            "revisionId":     CLASSIC_REVISION_ID,
            "translationX":   0.0,
            "translationY":   0.0,
            "translationZ":   0.0,
            "eulerRotationX": 0.0,
            "eulerRotationY": 0.0,
            "eulerRotationZ": 0.0,
            "scaleX":         1.0,
            "scaleY":         1.0,
            "scaleZ":         1.0,
            "defaultVisible": True,
        },
    )],
)])
```

---

## 6. Space Assignment Rules

| Resource | Instance Space | Reason |
|---|---|---|
| Assets | `instance_<project>` | Main project instance space |
| `Cognite3DObject` | **Same as Assets** | IT queries 3D objects scoped to the asset's space |
| `CADNode` | `rmdm` or a dedicated 3D space | Keeps 3D geometry data separate |
| `CADRevision` / `CADModel` | Same as `CADNode` | Same 3D scope |
| `SceneConfiguration` / `Cdf3dModel` | `scene` | Fixed — IT always reads scenes from this space |

> **Never** put `Cognite3DObject` in `rmdm`. Industrial Tools will not find it.

---

## 7. Industrial Tools Location Setup (UI)

This step **cannot be done via API** — it must be done through the browser once per project.

1. Open Industrial Tools in CDF Fusion
2. Go to **Settings / Admin → Locations**
3. Create a new location (or edit the existing one):
   - **Name:** e.g. `upstream-value-chain`
   - **Data model space:** `upstream-value-chain`
   - **Data model external ID:** `upstream_value_chain`
   - **Data model version:** `v1`
   - **Instance space:** `instance_upstream_value_chain`
4. Save → the **"incompatible data model"** banner should disappear

If you see the banner, click **"Reset to preset location"** to apply the configured preset.

---

## 8. Verification Checklist

Run this after completing setup to confirm the full chain is intact:

```python
from cognite.client.data_classes.data_modeling import ViewId, NodeId

ASSET_SPACE = "instance_upstream_value_chain"
CAD_SPACE   = "rmdm"
TEST_ASSET  = "EN0110-BA-201-001"   # an asset known to be contextualized

VISUAL_VIEW  = ViewId("cdf_cdm", "CogniteVisualizable", "v1")
OBJ3D_VIEW   = ViewId("cdf_cdm", "Cognite3DObject",     "v1")
CAD_VIEW     = ViewId("sp_enterprise_process_industry", "CADNode", "v1")
REV_VIEW     = ViewId("cdf_cdm", "CogniteCADRevision",  "v1")
MODEL_VIEW   = ViewId("cdf_cdm", "CogniteCADModel",     "v1")

def pdump(node, view_id):
    d = node.properties.dump()
    return d.get(view_id.space, {}).get(f"{view_id.external_id}/{view_id.version}", {})

checks = []

# 1. Asset has object3D
r = client.data_modeling.instances.retrieve(nodes=[NodeId(ASSET_SPACE, TEST_ASSET)], sources=[VISUAL_VIEW])
obj3d = pdump(r.nodes[0], VISUAL_VIEW).get("object3D")
checks.append(("Asset.object3D set",            obj3d is not None))

# 2. Cognite3DObject exists
obj3d_ext = obj3d.get("externalId") if isinstance(obj3d, dict) else obj3d.external_id
r2 = client.data_modeling.instances.retrieve(nodes=[NodeId(ASSET_SPACE, obj3d_ext)], sources=[OBJ3D_VIEW])
checks.append(("Cognite3DObject exists",         bool(r2.nodes)))

# 3. CADNode exists with treeIndexes + revisions
tree_idx = obj3d_ext.replace("cog_3dobj_", "")
r3 = client.data_modeling.instances.retrieve(nodes=[NodeId(CAD_SPACE, f"cog_3d_node_{tree_idx}")], sources=[CAD_VIEW])
if r3.nodes:
    cp = pdump(r3.nodes[0], CAD_VIEW)
    checks.append(("CADNode.treeIndexes set",    bool(cp.get("treeIndexes"))))
    checks.append(("CADNode.revisions set",      bool(cp.get("revisions"))))
    rev = cp.get("revisions", [None])[0]
else:
    checks += [("CADNode exists", False), ("CADNode.treeIndexes", False), ("CADNode.revisions", False)]
    rev = None

# 4. CADRevision exists
if rev:
    rev_ext = rev.external_id if hasattr(rev, "external_id") else rev.get("externalId")
    r4 = client.data_modeling.instances.retrieve(nodes=[NodeId(CAD_SPACE, rev_ext)], sources=[REV_VIEW])
    if r4.nodes:
        rp = pdump(r4.nodes[0], REV_VIEW)
        checks.append(("CADRevision.published=True", rp.get("published") is True))
        checks.append(("CADRevision.status=Done",    rp.get("status") == "Done"))
        model = rp.get("model3D")
    else:
        checks += [("CADRevision exists", False)]; model = None
else:
    model = None

# 5. SceneConfiguration exists
r5 = client.data_modeling.instances.retrieve(nodes=[NodeId("scene", "clov_navisworks_scene")])
checks.append(("SceneConfiguration exists",     bool(r5.nodes)))

print("\n=== 3D Chain Verification ===")
all_ok = True
for label, ok in checks:
    status = "OK" if ok else "FAIL"
    print(f"  [{status}] {label}")
    if not ok:
        all_ok = False
print(f"\n  {'All checks passed!' if all_ok else 'Some checks FAILED — review above.'}")
```

---

## 9. Common Mistakes

| Mistake | Impact | Correct Approach |
|---|---|---|
| Using `node_id` for `cadNodeReference` | 3D viewer cannot highlight nodes | Use `str(treeIndex)` |
| Using `node_id` in external IDs (`cog_3dobj_{node_id}`) | Mismatched IDs when tree_index ≠ node_id | Use `cog_3dobj_{treeIndex}` |
| Putting `Cognite3DObject` in `rmdm` | IT 3D preview shows "No data" | Put it in the asset instance space |
| Adding only `Cognite3DObject` to data model | App crashes with `_UnknownType` GraphQL error | Add all 18 views listed in Section 3 |
| Omitting `edgeSource` view for edge connections | `_UnknownType` on `.edges` in GraphQL | Check `edgeSource` field in each multi-edge view |
| Skipping `SceneConfiguration` | IT 3D preview shows "No data" | Always create the scene (Section 5) |
| Not configuring IT Location | "Incompatible data model" banner permanently | Configure Location in IT admin (Section 7) |
| Running create script once and assuming success | Silent partial failures leave chain broken | Always run the verification script (Section 8) |

---

## Quick Reference — Script Execution Order

For a new project, run these scripts in order:

```
1. python scripts/upload_asset_hierarchy_dm.py          # upload asset hierarchy to DM
2. (run entity matching pipeline / function)            # populate RAW contextualization_good
3. python scripts/add_3d_views_to_datamodel.py          # add all required views to DM  ← do BEFORE step 4
4. POST /3d/contextualization/cad  (see Section 4)      # create 3D DM chain via dedicated endpoint
   OR python scripts/create_cad_node_mappings.py        # manual fallback
5. python scripts/create_scene_config.py                # create SceneConfiguration
6. (browser) Configure IT Location                      # one-time UI step
7. python scripts/verify_3d_chain.py                    # confirm everything is wired up
```
