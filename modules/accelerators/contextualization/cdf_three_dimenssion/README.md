# 3D contextualization (CDF Toolkit)

This CDF Toolkit module deploys a **data modeling (DM)–first** pipeline that contextualizes 3D CAD nodes with Cognite Asset instances: manual and rule-based input, ML matching, quality review, and RAW outputs for iterative tuning.

It is parameterized by **`default_location`** and **`source_name`** (for example `clov` / `navisworks`), so resource external IDs follow the pattern `…_{{default_location}}_{{source_name}}` after build.

## Prerequisites

- A **DM-only or hybrid** CDF project with the 3D chain and views required for Industrial Tools / CAD contextualization. Follow **[docs/3d_contextualization_dm_only_guide.md](docs/3d_contextualization_dm_only_guide.md)** for spaces, `Cognite3DObject` / `CADNode` linking, scene configuration, and common pitfalls.
- After configuring **`asset_instance_space`**, **view** (`assetView*` / `asset_view_*`), and pipeline IDs for your project, use **[docs/CONFIGURATION_VERIFICATION.md](docs/CONFIGURATION_VERIFICATION.md)** to verify live CDF pipeline config and instance vs view spaces.
- A **3D model** ingested and processed in CDF (file upload via UI or the file extractor pattern in this module).
- Toolkit **`variables`** set in your environment config (see `default.config.dev.yaml` and `env.template`).

Optional reference data (for demos): small asset / OID-style examples can be adapted; the module itself is not tied to a single field like Valhall.

## Processing workflow

High-level contextualization flow (manual input → ML → good/bad RAW → retune):

![Processing workflow](https://github.com/cognitedata/toolkit/assets/31886431/b29522f8-7f4b-4e23-b06a-f3ffffde103c)

## Managed resources

Names below use toolkit variables; **deployed** external IDs use your configured `default_location` and `source_name`.

### 1. Auth groups

| Pattern | Purpose |
|--------|---------|
| `gp_3d_{{default_location}}_extractor` | Extractor: RAW, files, extraction runs for the 3D dataset and source pipeline |
| `gp_3d_{{default_location}}_processing` | Runs contextualization functions: RAW, DM instances, files, 3D, sessions, pipelines |
| `gp_3d_{{default_location}}_read` | Read access to 3D / files for consumers |

Source IDs for each group come from your IdP (`3d_location_*_group_source_id` in config).

### 2. Data set

| External ID | Role |
|-------------|------|
| `ds_3d_{{default_location}}` | Lineage for extraction pipelines, functions, RAW, and files |

### 3. Extraction pipelines

| External ID pattern | Role |
|---------------------|------|
| `ep_src_3d_{{default_location}}_{{source_name}}` | File extractor: upload 3D files from local disk, SharePoint, etc. |
| `ep_ctx_3d_{{default_location}}_{{source_name}}_annotation` | Main **3D ↔ asset** contextualization (CDF Function) |
| `ep_ctx_3d_{{default_location}}_{{source_name}}_annotation_quality_check` | Quality check on contextualization results |
| `ep_ctx_3d_{{default_location}}_{{source_name}}_upload_manual_mappings` | Load **CSV** mappings into RAW (`3DId`, `assetId`, …) |
| `ep_ctx_3d_{{default_location}}_{{source_name}}_upload_asset_hierarchy` | Load **asset hierarchy** CSV into DM asset instances |

Pipeline documentation and **runtime parameters** (RAW DB, DM asset/CAD spaces, model name, thresholds) live in the corresponding `*.config.Config.yaml` next to each pipeline.

### 4. Functions

| External ID pattern | Role |
|---------------------|------|
| `fn_context_3d_{{default_location}}_{{source_name}}_asset` | Main annotation / contextualization |
| `fn_context_3d_{{default_location}}_{{source_name}}_quality_check` | Post-run quality checks |
| `fn_context_3d_{{default_location}}_{{source_name}}_upload_manual_mappings` | CSV → `contextualization_manual_input` |
| `fn_context_3d_{{default_location}}_{{source_name}}_upload_asset_hierarchy` | CSV → DM assets |

The main contextualization function, in normal operation, follows the pipeline described on the annotation extraction pipeline (read manual RAW, apply overrides, match assets using DM configuration, write **good** / **bad** tables for workflow and tuning).

### 5. RAW database and tables

| Database | Tables |
|----------|--------|
| `3d_{{default_location}}_{{source_name}}` | `contextualization_good`, `contextualization_bad`, `contextualization_manual_input`, `contextualization_rule` (optional rule-based mapping before ML) |

### Illustrations

**3D data pipeline (time series / context):**

![3D data pipeline](https://github.com/cognitedata/toolkit/assets/31886431/f1129181-bab0-42cb-8366-860e8fb30d7e)

**Contextualization workflow** (good/bad tables, manual and rule modules):

![Contextualization workflow](https://github.com/cognitedata/toolkit/assets/31886431/0e990b47-0c06-4040-b680-7e2dddcdccee)

## Variables

Set module variables in **`default.config.yaml`** (merged by the Toolkit). Values such as DM spaces, model names, and `function_space` are read from **environment variables** (see **`env.template`**): `FUNCTION_SPACE`, `THREE_D_MODEL_NAME`, `CAD_MODEL_NAME`, `DEFAULT_CAD_SPACE`, `DEFAULT_DM_SPACE`, `DATA_MODEL_SPACE`, `DM_EXT_ID`, `DM_VERSION`, `ASSET_INSTANCE_SPACE`, `CAD_NODE_INSTANCE_SPACE`, plus `CDF_PROJECT`, `CDF_CLUSTER`, IDP and CI/CD vars as already referenced in `default.config.yaml`. Copy `env.template` → `.env` and fill in real values before `cdf build`.

| Variable | Description |
|----------|-------------|
| `default_location` | Short location key used in resource names (e.g. `clov`) |
| `source_name` | Source system key (e.g. `navisworks`) |
| `3d_model_name` | 3D / CAD model identifier in CDF (from `.env`: `THREE_D_MODEL_NAME`) |
| `3d_dataset` | Data set external ID for 3D resources (typically `ds_3d_<location>`) |
| `raw_db`, `raw_table_manual` | RAW database and manual-input table (for scripts / docs) |
| `asset_instance_space`, `cad_node_instance_space` | DM instance spaces (from `.env`: `ASSET_INSTANCE_SPACE`, `CAD_NODE_INSTANCE_SPACE`) |
| `function_space` | Space for Cognite Function code artifacts (from `.env`: `FUNCTION_SPACE`) |
| `default_dm_space`, `dm_ext_id`, `dm_version` | Data model reference (spaces / ext id from `.env` via `${DM_EXT_ID}` / `${DM_VERSION}`) |
| `default_cad_space`, `default_scene_space` | CAD and scene configuration spaces (`DEFAULT_CAD_SPACE` in `.env`) |
| `required_views`, `cad_model_view`, … | Optional overrides; defaults are in `fn_context_3d_cad_asset_contextualization/config.py` (`_DEFAULT_*`) |
| `file_extractor_watch_path` | Local path for file extractor config (if used) |
| `3d_location_extractor_group_source_id`, `3d_location_processing_group_source_id`, `3d_location_read_group_source_id` | IdP object IDs for the three auth groups |
| `cicd_clientId`, `cicd_clientSecret` | Optional: schedules / automation |

Legacy variable **`external_root_id_asset`** may appear in older templates; DM deployments use **`asset_instance_space`** / **`cad_node_instance_space`** in pipeline config instead.

## Usage

Copy this module into your project’s `custom_modules` (or reference it from `modules/`), adjust **`variables`** for your project and IdP groups, then add it under **`selected_modules_and_packages`** in your `config.<env>.yaml` and deploy with the CDF Toolkit.

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates).

**Note:** Cognite Functions have **time and memory limits**. Very large asset or node volumes may need batching or a different runtime than a single function invocation.
