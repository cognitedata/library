# Configuration verification guide (instance spaces, views, pipeline config)

This module reads **runtime settings from CDF extraction pipeline config** (and function **env vars** set at deploy). Local `default.config.yaml` / project `config.dev.yaml` / `.env` are inputs to **`cdf build`** only—they must be **deployed** so CDF stores **resolved** values (no literal `${...}` or `{{...}}` left in the live pipeline config).

Use this checklist when you point the module at **your** data model, **your** instance spaces, and **your** view external ids and versions.

---

## 1. Concepts (DM)

| Concept | What it is | Example |
|--------|------------|--------|
| **Instance space** | Where **node instances** live (`space` + `externalId` on `NodeApply` / list filters). | `sp_inst_domain_whiptail` |
| **View** | Schema slice for properties: **`ViewId(view_space, external_id, version)`**. The **view space** is usually the **data model / schema** space where the view is defined—not always the same string as the instance space. | `sp_dm_domain_epc`, `Tag`, `0.0.5` |
| **Asset DM space (`assetDmSpace`)** | Used by the main contextualization function to **list asset nodes** in DM (`get_resources.get_assets`). Must match where your **Asset** (or asset view) instances actually exist. | Same as instance space if all assets are in one space |
| **CAD node space (`cadNodeDmSpace`)** | Instance space for **CAD** nodes (`CogniteCADNode`, etc.). | Often separate from assets |
| **RAW DB** | `rawdb` must match the deployed RAW database name, typically `3d_<default_location>_<source_name>`. |

See also **[3d_contextualization_dm_only_guide.md](./3d_contextualization_dm_only_guide.md)** (`Cognite3DObject` same space as `Asset` for IT, CAD nodes may differ).

---

## 2. Where configuration is read

| Component | Source at runtime |
|-----------|-------------------|
| **Main contextualization** (`fn_context_3d_cad_asset_contextualization`) | `client.extraction_pipelines.config.retrieve(<annotation pipeline ext id>)` → `config.data.parameters` (camelCase in API). Optional env fallbacks where implemented (e.g. `ASSET_INSTANCE_SPACE`, `ASSET_VIEW_*`). |
| **Upload asset hierarchy** | Same pattern for `ep_ctx_*_upload_asset_hierarchy`; **requires** `assetInstanceSpace` + `assetViewSpace` / `assetViewExternalId` / `assetViewVersion` (or matching env vars). **No** hardcoded defaults for view/instance space. |
| **Upload manual mappings** | Pipeline config for `rawdb`, `rawTableManual`, `fileExternalId`. |
| **Function.yaml `envVars`** | Injected at deploy; must align with `default.config` / `.env` so values are correct **after** `cdf build`. |

---

## 3. Main annotation pipeline (`ctx_3d_oid_fileshare_annotation.config.Config.yaml`)

Verify these **`parameters`** in the **deployed** pipeline config in CDF (names are **camelCase** in JSON/YAML):

| Parameter | Purpose |
|-----------|---------|
| `assetDmSpace` | Instance space for listing **asset** nodes used in matching (`get_assets`). |
| `assetViewSpace` | View **space** for `HasData` filter (often data model space; can match `asset_view_space` in your project). |
| `assetViewExtId` | View **external id** (e.g. `AssetExtension`, `Tag`). |
| `assetViewVersion` | View **version** string (must match the view in your data model). |
| `cadNodeDmSpace` | Instance space for CAD / 3D node side. |
| `rawdb` | RAW database name. |
| `threeDModelName` | 3D model name in CDF. |
| `threeDDataSetExtId` | Dataset for 3D resources. |

**Code reference:** `get_resources.get_assets` uses `asset_dm_space`, `asset_view_space`, `asset_view_ext_id`, `asset_view_version` (from pipeline config / env).

---

## 4. Upload asset hierarchy pipeline (`ctx_3d_upload_asset_hierarchy.config.Config.yaml`)

| Parameter | Purpose |
|-----------|---------|
| `assetInstanceSpace` | **Instance space** for upserted asset nodes. |
| `assetViewSpace` | View space for `NodeApply.sources` (template may use `data_model_space`). |
| `assetViewExternalId` | View external id. |
| `assetViewVersion` | View version. |

**Code reference:** `fn_context_3d_upload_asset_hierarchy/handler.py` — `_resolve_asset_instance_space`, `_resolve_asset_view`.

---

## 5. Verification checklist (before relying on production runs)

1. **Build and deploy**  
   Run `cdf build` and `cdf deploy` after changing `variables` / `.env`.

2. **Inspect live pipeline config in CDF**  
   Open the extraction pipeline → configuration. Confirm:
   - No unresolved **`${VAR}`** or **`{{var}}`** strings.
   - **`assetDmSpace` / `assetView*` / `cadNodeDmSpace`** match your DM project.
   - **`rawdb`** matches your RAW database.

3. **Views exist and versions match**  
   In **Data modeling**, confirm the view **`(space, externalId, version)`** used in config is **exactly** what your instances implement. A wrong version returns **no rows** in `get_assets` or fails applies.

4. **Instance space vs view space**  
   If assets live in `sp_inst_*` but the view is published under `sp_dm_*`, set **`assetViewSpace`** to the **view’s** space and **`assetDmSpace`** to the **instance** space where nodes exist (per your model).

5. **Functions**  
   Confirm the annotation function’s schedule/trigger uses the **same** pipeline external id as in config. After renaming **`default_location` / `source_name`**, pipeline external ids change—update schedules, workflows, and any hardcoded ids in scripts.

6. **Smoke test**  
   - Trigger **upload asset hierarchy** with a tiny CSV and confirm instances appear under the expected **space** and **view**.  
   - Run **annotation** with a small `readLimit` / debug if available and confirm `get_assets` returns entities (check logs for `Number of DM assets found`).

---

## 6. Common failures

| Symptom | Likely cause |
|--------|----------------|
| Literal `${3d_model_name}` in config | Pipeline config in CDF not substituted—redeploy from a correct build or patch config in CDF. |
| `0` assets found | Wrong `assetDmSpace`, or view triple wrong, or subtree filters exclude everything. |
| `ValueError` on upload hierarchy | Missing `assetInstanceSpace` or view fields—set pipeline parameters or `ASSET_INSTANCE_SPACE` / `ASSET_VIEW_*` env. |
| Permission errors | Auth group `3d.Group.yaml` scopes must include your spaces and datasets. |

---

## 7. Changing `default_location` or `source_name`

These drive **many** external ids (`ep_*`, `fn_*` patterns, `rawdb`, etc.). Changing them creates a **new** naming slice in CDF; existing pipelines and RAW DBs under the old names are **not** renamed automatically. Plan migration or accept parallel resources.

---

## Related files (reference)

- `extraction_pipelines/ctx_3d_oid_fileshare_annotation.config.Config.yaml` — main parameters  
- `extraction_pipelines/ctx_3d_upload_asset_hierarchy.config.Config.yaml` — upload hierarchy  
- `extraction_pipelines/ctx_3d_upload_manual_mappings.config.Config.yaml` — manual CSV → RAW  
- `functions/fn_context_3d_cad_asset_contextualization/get_resources.py` — `get_assets`  
- `functions/fn_context_3d_upload_asset_hierarchy/handler.py` — instance space + view resolution  
- `env.template` — env var names for local/project `.env`  
