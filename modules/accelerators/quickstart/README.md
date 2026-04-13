# Quickstart Deployment Pack

This module provides a consolidated deployment and validation path for the Quickstart Deployment Pack (`dp:quickstart`), combining ingestion, contextualization, search, model, and quality reporting modules into one end-to-end setup.

## Why Use This Package?

**Deploy a complete CDF demo/reference pipeline with one package selection**

Configuring all dependent Quickstart modules manually can be time-consuming and error-prone. This package provides a **single, guided path** to initialize, configure, and test a full data-to-context flow.

**Key Benefits:**

- ⚡ **Single Package Selection**: Install the entire Quickstart module set from `quickstartdp`
- 🔗 **End-to-End Scope**: Covers source ingestion, contextualization, search, and quality reporting
- 🧭 **Guided Setup**: Includes setup wizard for required config and SQL mode updates
- 🧪 **Synthetic Data Testing**: Validate workflows without live source integrations
- 📚 **Centralized Docs**: One index with links to each module README

## 🎯 Overview

Package metadata from `modules/packages.toml`:

- `id`: `dp:quickstart`
- `title`: `Quickstart Deployment Pack`
- `canCherryPick`: `false`

Included modules:

- `accelerators/cdf_common`
- `accelerators/cdf_ingestion`
- `accelerators/contextualization/cdf_file_annotation`
- `accelerators/contextualization/cdf_entity_matching`
- `accelerators/contextualization/cdf_connection_sql`
- `accelerators/industrial_tools/cdf_search`
- `accelerators/open_industrial_data_sync`
- `accelerators/quickstart`
- `sourcesystem/cdf_pi`
- `sourcesystem/cdf_sap_assets`
- `sourcesystem/cdf_sap_events`
- `sourcesystem/cdf_sharepoint`
- `dashboards/rpt_quality`
- `models/qs_enterprise_dm`

## 🏗️ Package Structure

```text
modules/
├── accelerators/
│   ├── cdf_common/
│   ├── cdf_ingestion/
│   ├── contextualization/
│   │   ├── cdf_file_annotation/
│   │   ├── cdf_entity_matching/
│   │   └── cdf_connection_sql/
│   ├── industrial_tools/cdf_search/
│   ├── open_industrial_data_sync/
│   └── quickstart/
├── sourcesystem/
│   ├── cdf_pi/
│   ├── cdf_sap_assets/
│   ├── cdf_sap_events/
│   └── cdf_sharepoint/
├── dashboards/rpt_quality/
└── models/qs_enterprise_dm/
```

## 📚 Module Documentation Index

### Foundation


| Module             | Purpose                                                        | Documentation                                                                          |
| ------------------ | -------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `cdf_common`       | Shared spaces, datasets, RAW, and common runtime resources     | `[modules/accelerators/cdf_common/README.md](../cdf_common/README.md)`                 |
| `cdf_ingestion`    | Ingestion workflow orchestration and transformation sequencing | `[modules/accelerators/cdf_ingestion/README.md](../cdf_ingestion/README.md)`           |
| `qs_enterprise_dm` | Quickstart enterprise data model                               | `[modules/models/qs_enterprise_dm/README.md](../../models/qs_enterprise_dm/README.md)` |


### Source system + data simulation


| Module                      | Purpose                                  | Documentation                                                                                        |
| --------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `cdf_pi`                    | PI sample ingestion and timeseries setup | `[modules/sourcesystem/cdf_pi/README.md](../../sourcesystem/cdf_pi/README.md)`                       |
| `cdf_sap_assets`            | SAP asset/functional location ingestion  | `[modules/sourcesystem/cdf_sap_assets/README.md](../../sourcesystem/cdf_sap_assets/README.md)`       |
| `cdf_sap_events`            | SAP maintenance event ingestion          | `[modules/sourcesystem/cdf_sap_events/README.md](../../sourcesystem/cdf_sap_events/README.md)`       |
| `cdf_sharepoint`            | File ingestion for annotation testing    | `[modules/sourcesystem/cdf_sharepoint/README.md](../../sourcesystem/cdf_sharepoint/README.md)`       |
| `open_industrial_data_sync` | Time-shifted OID replay and sync         | `[modules/accelerators/open_industrial_data_sync/README.md](../open_industrial_data_sync/README.md)` |


### Contextualization


| Module                | Purpose                                                    | Documentation                                                                                                                |
| --------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `cdf_connection_sql`  | SQL-based relationship creation                            | `[modules/accelerators/contextualization/cdf_connection_sql/README.md](../contextualization/cdf_connection_sql/README.md)`   |
| `cdf_entity_matching` | Entity matching and metadata optimization                  | `[modules/accelerators/contextualization/cdf_entity_matching/README.md](../contextualization/cdf_entity_matching/README.md)` |
| `cdf_file_annotation` | File annotation workflow (prepare/launch/finalize/promote) | `[modules/accelerators/contextualization/cdf_file_annotation/README.md](../contextualization/cdf_file_annotation/README.md)` |


### Monitoring and industrial tools


| Module        | Purpose                                        | Documentation                                                                                            |
| ------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `cdf_search`  | Search/location resources for Industrial Tools | `[modules/accelerators/industrial_tools/cdf_search/README.md](../industrial_tools/cdf_search/README.md)` |
| `rpt_quality` | Contextualization quality KPI reporting        | `[modules/dashboards/rpt_quality/README.md](../../dashboards/rpt_quality/README.md)`                     |


## 🔧 Configuration

### Prerequisites

- Cognite Toolkit `0.7.33` or newer
- `cdf.toml` present in project root
- Auth initialized and verified (`cdf auth init`, `cdf auth verify`)
- Data plugin enabled:

```toml
[plugins]
data = true
```

- Library source configured:

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

## 🏃 Getting Started

### 1. Initialize package modules

```bash
cdf modules init . --clean
```

Select **Quickstart Deployment Pack** in the module picker.

> `--clean` can overwrite existing module folders.

### 2. Run setup wizard (recommended)

```bash
python3 modules/accelerators/quickstart/scripts/qs_dp_setup_wizard.py --env <dev|prod|staging>
```

The wizard updates required Quickstart settings in:

- `config.<env>.yaml`
- `.env`
- `modules/sourcesystem/cdf_sap_assets/transformations/population/asset.Transformation.sql`

It also creates `.bak` backups on first write.

### 3. Verify generated changes

Confirm:

- `environment.project` is correct
- Entity matching defaults are updated for Quickstart
- `ApplicationOwner` is set
- group source and secret values exist in `.env`
- `FILE_ANNOTATION MODE` is active in `asset.Transformation.sql`

> If you run `cdf auth init` after the wizard, re-check `.env` values before deploy.

## 🧪 Testing the Quickstart Package

QS DP includes synthetic data for full validation without live source connectors.

### 1. Build and deploy

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

### 2. Upload synthetic data

```bash
cdf data upload dir modules/sourcesystem/cdf_pi/upload_data
cdf data upload dir modules/sourcesystem/cdf_sap_assets/upload_data
cdf data upload dir modules/sourcesystem/cdf_sap_events/upload_data
cdf data upload dir modules/sourcesystem/cdf_sharepoint/upload_data
cdf data upload dir modules/accelerators/contextualization/cdf_entity_matching/upload_data
cdf data upload dir modules/accelerators/contextualization/cdf_file_annotation/upload_data
```

If needed in test environments, add `--skip-verify-cdf-project` to upload commands.

### 3. Trigger workflows in order

Run from Data Workflows in UI:

1. `ingestion`
2. `wf_file_annotation`
3. `EntityMatching`

## ✅ Post-Deployment Verification

- Verify file links in Industrial Tools Search (`Files`)
- Validate entity matching runs for `dm:context:timeseries:entity_matching`
- Confirm workflow runs complete successfully
- Run `wf_contextualization_rate` and review `tbl_contextualization_rate_report` in `db_quality_reports`

## 📚 References

- [Toolkit setup](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/setup)
- [Toolkit authentication](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth)
- [Entity matching module guide](https://hub.cognite.com/deployment-packs-472/how-to-cdf-entity-matching-module-cognite-official-5317)
- [Quickstart enterprise DM guide](https://hub.cognite.com/deployment-packs-472/how-to-get-started-with-quick-start-enterprise-data-model-5997)

## 🆘 Support

- Refer to Cognite docs for Toolkit/deployment guidance
- Contact Cognite support for environment-specific issues
- Deployment packs channel: `#topic-deployment-packs`

