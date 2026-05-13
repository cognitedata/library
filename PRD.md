# PRD: Foundation Deployment Pack (`dp:foundation`)

## Overview

The **Foundation Deployment Pack** (`dp:foundation`) is a scalable, modular CDF deployment package that gives industrial projects a near zero-configuration starting point. It covers the full stack from source system extraction through contextualization: production-grade extractor configuration templates for PI, OPC-UA, SAP, DB, and file sources; a choice between ISA Manufacturing Extension and CFIHOS Oil & Gas data models; a modular ingestion orchestration workflow; and contextualization capabilities including file annotation and entity matching.

Every module is independently deployable. The package supports `canCherryPick = true`, so teams select only the source systems and capabilities they need. Adding a new source system or contextualization step means adding a module — not modifying existing ones.

All resource naming strictly follows `.cursor/rules/cdf-naming-conventions.mdc`. Each module is fully self-contained — no shared foundation module is required. Auth, datasets, and spaces that a module needs are defined within that module itself.

---

## Background

### Who is this for?

The primary users of `dp:foundation` are **Data Engineers (DEs)** and **Cognite partners** who are responsible for standing up CDF environments for industrial customers. Secondary users are **Solutions Architects** who define the project structure before a DE takes over.

### What is the pain today?

When a DE starts a new CDF project, there is no authoritative, reusable starting point that covers the full stack. The options available today each have gaps:

- **`dp:quickstart`** is demo-oriented — it ships synthetic data and pre-configured connections that are tightly coupled to a fictional "Springfield" site. Removing synthetic data, renaming locations, or swapping a source system requires touching files across multiple modules. It is not designed to evolve into a production deployment.
- **Blank-slate projects** (no DP) require DEs to assemble spaces, datasets, groups, extractor configs, transformations, and orchestration from scratch on every engagement. This leads to inconsistent project structures, configuration drift, and undocumented decisions.
- **gss-knowledge-base** contains mature extractor configuration templates (PI, OPC-UA, SAP, DB, Files) but they are not wired into any deployable DP — DEs must manually locate, copy, and adapt them per project.
- **Lack of standardisation**: a simple research showed that out of eight delivered quickstarts there was no consistency in naming conventions, access control, or data model alignment.

### What does this enable?

`dp:foundation` gives DEs and partners a single, composable starting point they can deploy to a real customer project on day one:

- A near **zero-configuration deployment**: the DP deploys and runs without significant initial configuration beyond filling in credentials and a `location` variable.
- Extractor configuration templates for the most common industrial source systems are bundled — no gss-knowledge-base lookup required.
- Transformations are provided as **generalized examples and AI/Cursor scaffolds**, guided by cursor rules (`.cursor/rules/cdf-transformations.mdc`) so DEs can use them as input to AI tools to generate site-specific SQL rapidly.
- The project structure is standardised and parameterised by `location` — a new site is a new variable value, not a new set of files.
- Contextualization is a first-class, deployable capability, not an afterthought.
- The architecture is open for extension (add a module, set a variable) rather than modification (fork and edit).

---

## Goals

- Provide a **near zero-configuration** CDF project foundation: deploy and run with minimal initial setup beyond credentials and a `location` identifier.
- Ship production-grade extractor configuration templates for **PI**, **OPC-UA**, **SAP**, **DB**, and **file sources** from gss-knowledge-base, so field engineers get a real starting point with all required parameters documented.
- Ship transformation SQL as **generalized examples and AI/Cursor scaffolds**, guided by cursor rules (`.cursor/rules/cdf-transformations.mdc`), that DEs can adapt rapidly for site-specific data.
- Support a choice between **ISA Manufacturing Extension** and **CFIHOS Oil & Gas** data models — selectable via a single config variable.
- Each source system module is self-contained and independently deployable — no module fails to deploy because another is absent.
- Ingestion orchestration is source-agnostic: configuring which transformations run in which phase is the only customization required when adding or removing a source system.
- Include a **CLI-based configuration wizard (P1)** to guide users through auth, source system selection, and initial variable setup.

---

## Non-Goals (v1 Scope Exclusions)

| Excluded | Rationale |
|---|---|
| **Maximo, Meridium, other CMMS** | SAP is the most common asset/maintenance source in the target segment. Additional CMMS sources are P2+ work. |
| **OSIsoft PI Asset Framework (AF)** | `cdf_pi_foundation` covers the PI Data Archive (timeseries). PI AF hierarchy ingestion requires a separate extractor; not in v1. |
| **SAP PM via IDoc / RFC** | The SAP OData extractor pattern is the primary integration. IDoc/RFC-based extractions require different tooling. |
| **OPC-UA Historical Access (HDA)** | The OPC-UA module covers live/subscribed data. HDA backfill is a documented extension pattern for P2+. |
| **OOTB CDF project setup** | `dp:foundation` does not configure IDP, project creation, or network connectivity. It assumes a provisioned CDF project. |
| **Atlas AI / OOTB Agents** | AI agent deployment (`dp:atlas_ai`) is a P2 concern layered on top of a working foundation. |
| **Automated transformation unit tests** | A testing framework for verifying SQL output is a P2 concern. |
| **Multi-tenant or multi-project federation** | Each `dp:foundation` deployment targets a single CDF project. |
| **Japanese / multi-language localization** | Future consideration. Not in v1 scope. |
| **CI/CD pipeline templates for ADO** | A separate initiative around SOPs and branching strategy templates. |
| **Module dependency auto-resolution** | Automatically resolving dependent modules is a desirable toolkit-level enhancement (P1). |

---

## Target Users

| Persona | How this DP helps |
|---|---|
| **Field Engineers / DEs** setting up a new customer project | Drop-in extractor config templates with all parameters documented; near zero-config start; transformations as AI scaffolds rather than manual SQL |
| **Solutions Architects** designing a scalable CDF project | Modular structure grows by adding modules, not by forking; shared variable contract means location-specific config lives in one place |
| **Partners** deploying CDF for the first time | Clear, layered architecture from data model → source systems → orchestration → contextualization; cherry-pick only what applies |

---

## Step 0 — Prerequisites: Auth Setup

`dp:foundation` does not prescribe or deploy IDP groups, app registrations, or project-level auth configuration. These are set up outside the DP according to the standard Cognite access management process. Refer to the [Cognite Access Management documentation](https://docs.cognite.com/cdf/access/) and your organisation's IDP configuration guide before deploying any module.

Each source system module that requires a CDF group (e.g. the workflow execution group in `cdf_ingestion_foundation`) ships its own Group YAML. The DE populates the `sourceId` field in that YAML with the corresponding IDP group object ID after the IDP group has been created.

---

## Module Architecture

### Package Composition

```toml
[packages.foundation]
id = "dp:foundation"
title = "Foundation Deployment Pack"
description = "A scalable, modular foundation for industrial CDF projects. Near zero-config start with ISA Manufacturing Extension or CFIHOS Oil & Gas DM, production-grade extractor config templates for PI, OPC-UA, SAP, DB, and file sources, modular ingestion orchestration, and contextualization."
canCherryPick = true
modules = [
    # Data models — choose one or both
    "models/isa_manufacturing_extension",
    "models/cfihos_oil_and_gas_extension",
    # Source systems — deploy the ones matching your site
    "sourcesystem/cdf_pi_foundation",
    "sourcesystem/cdf_opcua_foundation",
    "sourcesystem/cdf_sap_foundation",
    "sourcesystem/cdf_db_foundation",
    "sourcesystem/cdf_files_foundation",
    # Ingestion orchestration
    "foundation/cdf_ingestion_foundation",
    # Contextualization
    "accelerators/contextualization/cdf_file_annotation",
    "accelerators/contextualization/cdf_entity_matching",
    # Quality tooling
    "tools/apps/qualitizer",
]
```

New modules created as part of this DP:

- `sourcesystem/cdf_pi_foundation`
- `sourcesystem/cdf_opcua_foundation`
- `sourcesystem/cdf_sap_foundation`
- `sourcesystem/cdf_db_foundation`
- `sourcesystem/cdf_files_foundation`
- `foundation/cdf_ingestion_foundation`

Existing modules referenced without modification:

- `models/isa_manufacturing_extension`
- `models/cfihos_oil_and_gas_extension`
- `accelerators/contextualization/cdf_file_annotation`
- `accelerators/contextualization/cdf_entity_matching`
- `tools/apps/qualitizer`

---

### Module Dependency Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 0 — DATA MODEL                                                        │
│                                                                              │
│  ┌─────────────────────────────────┐  ┌──────────────────────────────────┐  │
│  │  models/isa_manufacturing_      │  │  models/cfihos_oil_and_gas_      │  │
│  │  extension  [existing]          │  │  extension  [existing]           │  │
│  │  Equipment · Asset · TS ·       │  │  CFIHOS-aligned equipment,       │  │
│  │  WorkOrder · Operation          │  │  document, and tag hierarchy     │  │
│  └─────────────────────────────────┘  └──────────────────────────────────┘  │
│                 (choose one or both; set dataModelVariant in config)         │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — SOURCE SYSTEMS  (independently deployable; each self-contained)   │
│                                                                              │
│  cdf_pi_foundation   cdf_opcua_foundation   cdf_sap_foundation               │
│  [new]               [new]                 [new]                             │
│  EP config tmpl      EP config tmpl        EP config tmpl                    │
│  Direct → DM TS      Direct → DM TS        RAW → 6 transformations           │
│                                                                              │
│  cdf_db_foundation   cdf_files_foundation                                    │
│  [new]               [new]                                                   │
│  EP config tmpl      EP config tmpl                                          │
│  DB → RAW/DM         Files → RAW/DM                                          │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 2 — INGESTION ORCHESTRATION                                           │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │  foundation/cdf_ingestion_foundation  [new]                           │   │
│  │  Two-phase workflow: Population → Contextualization                   │   │
│  │  Phase tasks configured via variable flags — no hardcoded sources     │   │
│  │  Parallel execution within each phase · Abort on failure              │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — CONTEXTUALIZATION  (existing modules, no changes)                 │
│                                                                              │
│  ┌──────────────────────────────┐  ┌────────────────────────────────────┐   │
│  │  cdf_file_annotation         │  │  cdf_entity_matching               │   │
│  │  [existing]                  │  │  [existing]                        │   │
│  │  P&ID annotation             │  │  TS→Asset AI matching              │   │
│  │  4 functions · 1 workflow    │  │  2 functions · 1 workflow          │   │
│  └──────────────────────────────┘  └────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 4 — QUALITY TOOLING  (existing module, no changes)                    │
│                                                                              │
│  tools/apps/qualitizer                                                       │
│  Interactive data quality inspection and remediation tool for DEs/partners   │
│  [existing]                                                                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Specifications

### 1. `models/isa_manufacturing_extension` *(Existing — no changes)*

**Purpose**: Extends the CDF core data model with manufacturing-specific views covering Asset, Equipment, TimeSeries, and Maintenance entities. v1 default data model.

**Key implementation note**: Read `module.toml` and resource YAMLs to confirm exact `schemaSpace`, view external IDs, and property names before authoring transformation SQL.

**Dependencies**: None

---

### 2. `models/cfihos_oil_and_gas_extension` *(Existing — no changes)*

**Purpose**: CFIHOS-aligned data model for oil & gas projects. Alternative to ISA Manufacturing Extension; selected via `dataModelVariant: cfihos_oil_and_gas` in each module's `default.config.yaml`.

**Dependencies**: None

---

### 3. `sourcesystem/cdf_pi_foundation` *(New)*

**Purpose**: Ingest PI timeseries directly from a PI server into CDF via the PI .NET Extractor. The extractor writes timeseries data and metadata **directly to CDF** (no RAW landing step required). Ships with a complete, parameterized extractor configuration template sourced from gss-knowledge-base.

**PI writes directly to CDF**: The PI .NET Extractor creates CDF TimeSeries resources and writes values directly. A lightweight DM-enrichment transformation maps CDF TimeSeries into the DM view (`ISATimeSeries` or equivalent). No RAW database or landing zone is needed.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_timeseries_{{location}}_pi` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` (remote) | *(attached to pipeline)* | PI .NET extractor remote config template |
| `Transformation` | `tr_pi_timeseries_{{location}}_to_isa` | CDF TimeSeries metadata → DM `ISATimeSeries` instances |

**Extractor config — remote vs local split**:

The PI extractor config is split into two files:
- **Remote config** (`ep_pi.ExtractionPipeline.Config.yaml`): Pushed to CDF via ExtractionPipeline Config. Contains source-system-specific settings: PI server connection, extraction pipeline ID, data model destination, id-prefix, and tag filter. Managed by the DP.
- **Local config** (`config_local.yaml` — stays on extractor host, not committed to repo): Contains logger settings and Cognite connection (project, base URL, token URL, client ID/secret). These values are environment-specific and should not be pushed to CDF.

> Cross-reference: [PI .NET Extractor documentation](https://docs.cognite.com/cdf/integration/guides/extraction/pi/) · gss-knowledge-base: `extractors/toolkit_examples/pi_net_extractor/`

Key remote config parameters:

```yaml
# Remote (pushed to CDF ExtractionPipeline Config):
#   extraction-pipeline.external-id: ep_timeseries_{{location}}_pi
#   data-modeling.space:             {{instanceSpace}}
#   destination.dataset-external-id: {{dataset}}
#   id-prefix:                       pi:
#
# Local (stays on extractor host — local config file):
#   logger.console.level: info
#   cognite.project:      ${COGNITE_PROJECT}
#   cognite.base-url:     ${COGNITE_BASE_URL}
#   cognite.idp-authentication.client-id:     ${COGNITE_CLIENT_ID}
#   cognite.idp-authentication.client-secret: ${COGNITE_CLIENT_SECRET}
#
# Source credentials (environment variables, not in config):
#   PI_HOST, PI_USER, PI_PASSWORD
```

**Transformation notes**:

- SQL scaffold maps PI timeseries metadata into the selected DM view
- Maps PI tag name → `externalId` with `pi:` prefix
- `sysTagsFound` populated by default (opt-out via `populateSysTagsFound: false`)
- Use with `.cursor/rules/cdf-transformations.mdc` to adapt to site-specific tag naming

**File structure**:

```
sourcesystem/cdf_pi_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_pi.ExtractionPipeline.yaml
│   └── ep_pi.ExtractionPipeline.Config.yaml   # remote config only
└── transformations/
    ├── tr_pi_timeseries.Transformation.yaml
    └── tr_pi_timeseries.Transformation.sql
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `piIdPrefix` | `pi:` | External ID prefix for PI timeseries |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required** (on extractor host):
- `PI_HOST`, `PI_USER`, `PI_PASSWORD`
- `COGNITE_PROJECT`, `COGNITE_BASE_URL`, `COGNITE_TOKEN_URL`, `COGNITE_CLIENT_ID`, `COGNITE_CLIENT_SECRET`

**Dependencies**: None (self-contained)

---

### 4. `sourcesystem/cdf_opcua_foundation` *(New)*

**Purpose**: Ingest OPC-UA node data via the OPC-UA Extractor directly into CDF. Like PI, the OPC-UA extractor writes timeseries **directly to CDF** — no RAW landing step. A DM-enrichment transformation maps CDF TimeSeries into the selected DM view. Ships with a parameterized extractor configuration template sourced from gss-knowledge-base.

> **Production warning**: OPC-UA node structures are highly site-specific. Node filter lists (allow/deny by node ID pattern) **must** be configured before production use. The extractor template ships with commented-out example filters. Without filters, the extractor browses the full server tree, which may be very large.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_timeseries_{{location}}_opcua` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` (remote) | *(attached to pipeline)* | OPC-UA extractor remote config template |
| `Transformation` | `tr_opcua_timeseries_{{location}}_to_isa` | CDF TimeSeries metadata → DM `ISATimeSeries` instances |

**Extractor config — remote vs local split**:

- **Remote config** (`ep_opcua.ExtractionPipeline.Config.yaml`): OPC-UA endpoint, node filters, publishing/sampling intervals, extraction pipeline ID.
- **Local config** (stays on extractor host): Logger settings, Cognite connection settings, auth credentials.

> Cross-reference: [OPC-UA Extractor documentation](https://docs.cognite.com/cdf/integration/guides/extraction/opc_ua/) · gss-knowledge-base: `extractors/toolkit_examples/opcua_extractor_speira_quickstart/`

Key remote config parameters:

```yaml
# Remote:
#   endpoint.url:                    ${OPCUA_ENDPOINT_URL}
#   extraction-pipeline.external-id: ep_timeseries_{{location}}_opcua
#   id-prefix:                       opcua:
#   source.node-filter:              # site-specific allow/deny lists — MUST configure
#   source.publishing-interval:      {{opcuaPublishingInterval}}
#   source.sampling-interval:        {{opcuaSamplingInterval}}
#
# Local (stays on extractor host):
#   logger.console.level: info
#   cognite.project, base-url, idp-authentication — from env vars
#   OPCUA_USER, OPCUA_PASSWORD if server requires auth
```

**File structure**:

```
sourcesystem/cdf_opcua_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_opcua.ExtractionPipeline.yaml
│   └── ep_opcua.ExtractionPipeline.Config.yaml   # remote config only
└── transformations/
    ├── tr_opcua_timeseries.Transformation.yaml
    └── tr_opcua_timeseries.Transformation.sql
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `opcuaIdPrefix` | `opcua:` | External ID prefix for OPC-UA timeseries |
| `opcuaPublishingInterval` | `5000` | OPC-UA publishing interval in ms |
| `opcuaSamplingInterval` | `5000` | OPC-UA sampling interval in ms |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required** (on extractor host):
- `OPCUA_ENDPOINT_URL` (and optionally `OPCUA_USER`, `OPCUA_PASSWORD`)
- Standard Cognite auth vars

**Dependencies**: None (self-contained)

---

### 5. `sourcesystem/cdf_sap_foundation` *(New)*

**Purpose**: Ingest SAP functional locations, equipment, maintenance orders, and operations into CDF via a **single SAP OData extraction pipeline** writing to RAW, then transform into DM instances. Single-plant by default; multi-plant via `sapPlants` list variable.

> SAP OData service names and entity keys vary across SAP versions and NW Gateway configurations. Verify all service names and field names before deploying.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_assets_{{location}}_sap` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` (remote) | *(attached to pipeline)* | SAP OData extractor remote config template |
| `RAW Database` | `assets_{{location}}_sap` | SAP data landing zone |
| `Transformation` | `tr_sap_floc_{{location}}_to_isa_asset` | Functional locations → `ISAAsset` DM instances |
| `Transformation` | `tr_sap_equip_{{location}}_to_equipment` | Equipment master → `Equipment` DM instances |
| `Transformation` | `tr_sap_equip_{{location}}_to_asset_rel` | Equipment → Asset edge relationships |
| `Transformation` | `tr_sap_order_{{location}}_to_workorder` | Work orders → `WorkOrder` DM instances |
| `Transformation` | `tr_sap_oper_{{location}}_to_operation` | Work tasks → `Operation` DM instances |
| `Transformation` | `tr_sap_oper_{{location}}_to_order_rel` | Operation → WorkOrder edge relationships |

**Extractor config — remote vs local split**:

- **Remote config** (`ep_sap.ExtractionPipeline.Config.yaml`): SAP Gateway URL, client number, endpoint list (FunclocListSet, EquipmentListSet, ExHeaderSet, ExOlistSet, ExOperationsSet, ExNotifheader), RAW database and table targets, state-store config.
- **Local config** (stays on extractor host): Logger, Cognite connection, SAP credentials (`SAP_USERNAME`, `SAP_PASSWORD`).

> Cross-reference: [SAP OData Extractor documentation](https://docs.cognite.com/cdf/integration/guides/extraction/sap/) · gss-knowledge-base: `extractors/toolkit_examples/sap_odata_extractor_remote/`

**Transformation notes**:

- SQL is provided as **generalized scaffolds** targeting the selected DM. Use with `.cursor/rules/cdf-transformations.mdc` to adapt to site-specific SAP entity naming.
- Key columns to verify before production: `Functlocation`, `Descript`, `Supfloc`, `Fltyp`, `Equipment`, `OrderId`, `Activity`
- `sysTagsFound` populated on `WorkOrder` for downstream contextualization compatibility (opt-out via `populateSysTagsFound: false`)

**File structure**:

```
sourcesystem/cdf_sap_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_sap.ExtractionPipeline.yaml
│   └── ep_sap.ExtractionPipeline.Config.yaml   # remote config only
├── raw/
│   └── db_sap.Database.yaml
└── transformations/
    ├── tr_sap_assets.Transformation.yaml + .sql
    ├── tr_sap_equipment.Transformation.yaml + .sql
    ├── tr_sap_equipment_to_asset.Transformation.yaml + .sql
    ├── tr_sap_maintenance_orders.Transformation.yaml + .sql
    ├── tr_sap_operations.Transformation.yaml + .sql
    └── tr_sap_operation_to_order.Transformation.yaml + .sql
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `sapSystem` | `s4hana` | SAP system label, used in external IDs |
| `sapPlant` | `1000` | Default single-plant code |
| `sapPlants` | `[]` | Override for multi-plant, e.g. `["1000","2000"]` |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required** (on extractor host):
- `SAP_GATEWAY_URL`, `SAP_CLIENT`, `SAP_USERNAME`, `SAP_PASSWORD`
- Standard Cognite auth vars

**Dependencies**: None (self-contained)

---

### 6. `sourcesystem/cdf_db_foundation` *(New)*

**Purpose**: Ingest data from relational databases (MSSQL, PostgreSQL, Oracle, etc.) via the DB Extractor into CDF RAW, then transform into DM instances. The DB Extractor is configured with SQL queries that pull from source database tables on a schedule. Template sourced from gss-knowledge-base.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_db_{{location}}_{{dbSystem}}` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` (remote) | *(attached to pipeline)* | DB extractor remote config template |
| `RAW Database` | `db_{{location}}_{{dbSystem}}` | DB source data landing zone |

**Extractor config — remote vs local split**:

- **Remote config**: Query list, RAW database/table targets, extraction pipeline ID, schedule.
- **Local config** (stays on extractor host): Logger, Cognite connection, database connection string (`DB_CONNECTION_STRING`).

> Cross-reference: [DB Extractor documentation](https://docs.cognite.com/cdf/integration/guides/extraction/db_extractor/) · gss-knowledge-base: `extractors/toolkit_examples/db_extractor/`

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `dbSystem` | `db` | Source database system label (e.g. `mssql`, `postgres`) |

**Dependencies**: None (self-contained)

---

### 7. `sourcesystem/cdf_files_foundation` *(New)*

**Purpose**: Ingest files and documents (from SharePoint, network shares, or other file stores) into CDF Files via the File Extractor. Enables downstream P&ID annotation via `cdf_file_annotation`. Template sourced from gss-knowledge-base.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_files_{{location}}_{{fileSource}}` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` (remote) | *(attached to pipeline)* | File extractor remote config template |

**Extractor config — remote vs local split**:

- **Remote config**: File source paths/SharePoint site URL, file type filters, extraction pipeline ID.
- **Local config** (stays on extractor host): Logger, Cognite connection, source credentials (`SHAREPOINT_CLIENT_ID`, `SHAREPOINT_CLIENT_SECRET`, or file share credentials).

> Cross-reference: [File Extractor documentation](https://docs.cognite.com/cdf/integration/guides/extraction/file/) · gss-knowledge-base: `extractors/toolkit_examples/file_extractor/`

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `fileSource` | `sharepoint` | Source label (e.g. `sharepoint`, `fileshare`) |

**Dependencies**: None (self-contained)

---

### 8. `foundation/cdf_ingestion_foundation` *(New)*

**Purpose**: A source-agnostic, two-phase ingestion workflow that orchestrates population and contextualization transformations. `WorkflowVersion.yaml` is **generated at build time** from per-task snippet templates by `scripts/build_workflow.py`. A DE edits only `default.config.yaml` to declare which sources are enabled and which DM variant is in use — then runs the script to produce the final workflow YAML before deploying.

**Design**:

1. **Population phase** — loads data into DM instances (source system transformations). Tasks within the phase run in parallel where no `dependsOn` is declared.
2. **Contextualization phase** — builds relationships between DM instances. Runs only after population phase succeeds. Tasks run in parallel within the phase.

Contextualization task snippets included depend on `dataModelVariant`:
- `isa_manufacturing_extension` → ISA relationship transformations (v1 default)
- `cfihos_oil_and_gas` → CFIHOS relationship task snippets (v1)

**Build-time generation flow**:

```
default.config.yaml              workflow_template/tasks/
(enabledSources, DM variant)  +  (one YAML snippet per task)
            │                              │
            └──────────┬───────────────────┘
                       ▼
            scripts/build_workflow.py
                       │
                       ▼
            workflows/wf_ingestion_v1.WorkflowVersion.yaml   ← committed, deployed
```

**DE workflow**:

```bash
# 1. Edit default.config.yaml — set enabledSources and dataModelVariant
# 2. Regenerate the workflow YAML
python scripts/build_workflow.py

# 3. Build and deploy
cdf build
cdf deploy
```

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `Workflow` | `wf_{{location}}_ingestion` | Orchestrates population → contextualization |
| `WorkflowVersion` | `wf_{{location}}_ingestion/v1` | Generated version with task graph for enabled sources |
| `WorkflowTrigger` | `wf_{{location}}_ingestion_trigger` | Scheduled execution (configurable cron) |
| `Group` | `gp_cdf_{{location}}_workflow` | Service account group for workflow execution |
| `Group` | `gp_cdf_{{location}}_workflow_user` | User group for manual workflow triggering/monitoring |

**File structure**:

```
foundation/cdf_ingestion_foundation/
├── module.toml
├── default.config.yaml
├── scripts/
│   └── build_workflow.py
├── workflow_template/
│   └── tasks/
│       ├── task.pi_timeseries.yaml
│       ├── task.opcua_timeseries.yaml
│       ├── task.sap_assets.yaml
│       ├── task.sap_equipment.yaml
│       ├── task.sap_maintenance_orders.yaml
│       ├── task.sap_operations.yaml
│       ├── task.db_ingest.yaml
│       ├── task.files_ingest.yaml
│       ├── ctx.isa_manufacturing_extension.equipment_to_asset.yaml
│       ├── ctx.isa_manufacturing_extension.operation_to_order.yaml
│       ├── ctx.cfihos.tag_to_document.yaml        # CFIHOS ctx tasks (v1)
│       └── ctx.cfihos.tag_to_equipment.yaml
├── auth/
│   ├── grp_workflow.Group.yaml
│   └── grp_workflow_user.Group.yaml
└── workflows/
    ├── wf_ingestion.Workflow.yaml
    ├── wf_ingestion_v1.WorkflowVersion.yaml    # GENERATED — do not edit by hand
    └── wf_ingestion_trigger.WorkflowTrigger.yaml
```

**Dependencies**: None (self-contained). Source system modules must be deployed before the workflow is triggered.

---

### 9. `accelerators/contextualization/cdf_file_annotation` *(Existing — no changes)*

**Purpose**: P&ID and engineering document annotation using the Diagram Detect API.

**Resources** (existing): 4 CDF Functions, 1 Workflow, 6 RAW tables, 1 Extraction Pipeline, 1 Dataset.

**Dependencies**: Requires `instanceSpace`, `functionSpace`, and `dataset` variables to be set in the deployment's variable file.

---

### 10. `accelerators/contextualization/cdf_entity_matching` *(Existing — no changes)*

**Purpose**: AI-powered and rule-based timeseries-to-asset matching.

**Resources** (existing): 2 CDF Functions, 1 Workflow, 5 RAW tables.

**Dependencies**: Requires `instanceSpace` and `dataset` variables to be set in the deployment's variable file.

---

### 11. `tools/apps/qualitizer` *(Existing — no changes)*

**Purpose**: Interactive data quality inspection and remediation tool. DEs and partners can inspect and act on quality issues across assets, timeseries, and files — record-level review with actionable controls. Complements contextualization coverage metrics with hands-on quality work.

**Dependencies**: None structural; requires DM instances to be present.

---

## Configuration Variables — Consolidated Reference

Each module defines its own `default.config.yaml`. The following variables appear across multiple modules and **must be set consistently** in the deployment's environment variable file (or toolkit config). There is no shared foundation module that centralises them.

| Variable | Defined in | Consumed by |
|---|---|---|
| `location` | Every module | External IDs, RAW database names, space names across all modules |
| `organization` | `cdf_sap_foundation` | SAP transformation SQL view references |
| `schemaSpace` | Each source module | All source system transformations (DM view lookup) |
| `instanceSpace` | Each source module | All source system transformations (DM instance write target) |
| `dataset` | Each source module | Extraction pipelines and transformations |
| `dataModelVersion` | Each source module | Transformation view references |
| `dataModelVariant` | `cdf_ingestion_foundation` | Workflow task selection (ISA or CFIHOS ctx snippets) |
| `populateSysTagsFound` | Per source module | Source system transformations; `false` to omit the field |
| `sapPlant` / `sapPlants` | `cdf_sap_foundation` | SAP extractor config and transformation UNION logic |

> **Note on variable defaults**: Default values in `default.config.yaml` must be concrete strings — not variable references (e.g. `location: oslo`, not computed cross-references). The `{{location}}` substitution is applied in resource YAML files by the toolkit, not within `default.config.yaml` itself. Verify chained variable resolution behaviour against the minimum supported toolkit version before using cross-variable defaults.

---

## Resource Summary

| Module | Status | Datasets | RAW DBs | Transformations | Workflows | Pipelines | Groups |
|---|---|---|---|---|---|---|---|
| `models/isa_manufacturing_extension` | Existing | — | — | — | — | — | — |
| `models/cfihos_oil_and_gas_extension` | Existing | — | — | — | — | — | — |
| `sourcesystem/cdf_pi_foundation` | New | 1 | 0 | 1 | 0 | 1 | 0 |
| `sourcesystem/cdf_opcua_foundation` | New | 1 | 0 | 1 | 0 | 1 | 0 |
| `sourcesystem/cdf_sap_foundation` | New | 1 | 1 | 6 | 0 | 1 | 0 |
| `sourcesystem/cdf_db_foundation` | New | 1 | 1 | 0 | 0 | 1 | 0 |
| `sourcesystem/cdf_files_foundation` | New | 1 | 0 | 0 | 0 | 1 | 0 |
| `foundation/cdf_ingestion_foundation` | New | 0 | 0 | 0 | 1 | 0 | 2 |
| `cdf_file_annotation` | Existing | 1 | 6 | 0 | 1 | 1 | 0 |
| `cdf_entity_matching` | Existing | 0 | 0 | 0 | 1 | 1 | 0 |
| `tools/apps/qualitizer` | Existing | 0 | 0 | 0 | 0 | 0 | 0 |
| **Total (new modules only)** | | **5** | **2** | **8** | **1** | **5** | **2** |

---

## Release Phases

### P0 — v1: Foundational Deployment

**Goal**: A DE can deploy a complete, real industrial CDF project from scratch using this DP — source system extraction, data model, orchestration, and contextualization — without consulting gss-knowledge-base or assembling boilerplate manually.

| Deliverable | Modules |
|---|---|
| ISA Manufacturing Extension data model | `models/isa_manufacturing_extension` |
| CFIHOS Oil & Gas data model (alternative) | `models/cfihos_oil_and_gas_extension` |
| PI timeseries ingestion (direct write) | `sourcesystem/cdf_pi_foundation` |
| OPC-UA timeseries ingestion (direct write) | `sourcesystem/cdf_opcua_foundation` |
| SAP asset + maintenance ingestion (RAW → DM) | `sourcesystem/cdf_sap_foundation` |
| DB extractor ingestion | `sourcesystem/cdf_db_foundation` |
| File/document ingestion | `sourcesystem/cdf_files_foundation` |
| Modular ingestion orchestration | `foundation/cdf_ingestion_foundation` |
| P&ID file annotation | `accelerators/contextualization/cdf_file_annotation` |
| Entity matching | `accelerators/contextualization/cdf_entity_matching` |
| Data quality tooling | `tools/apps/qualitizer` |


### P1 — v2: Hardening, Security, and Usability

**Goal**: The v1 DP is hardened for production: security posture is tightened, feedback from first-wave deployments is incorporated, transformation scaffolds are testable, and new users can be guided through setup without reading documentation.

| Deliverable | Notes |
|---|---|
| **Security hardening** | Review and tighten group ACL scopes; add per-resource scope options; document least-privilege patterns for each persona; add secret rotation guidance |
| **Feedback-driven improvements** | Incorporate feedback from first v1 deployments: naming adjustments, config ergonomics, extractor config corrections, README gaps |
| **Transformation unit test framework** | Test harness that validates SQL transformation output against known-good fixtures; enables DEs to verify scaffolds before handover |
| **Improved usability** | Improve `default.config.yaml` inline documentation; add validation helpers; reduce time-to-first-deploy |
| **CLI configuration wizard** | Interactive `cdf init` style wizard that guides users through auth setup, source system selection, and initial variable population |
| **Module dependency auto-resolution** | When a package depends on another, automatically resolve and prompt the user to include it (toolkit-level enhancement) |
| **Multi-plant SAP validation** | Verified multi-plant expansion via `sapPlants` list variable |
| **CFIHOS ctx task validation** | End-to-end validation of CFIHOS relationship task snippets in the ingestion workflow |


### P2 — v3: AI, Extended Source Systems, and Operational Tooling

**Goal**: The foundation DP becomes the entry point for AI-augmented industrial workflows, a broader set of source systems, and richer operational visibility.

| Deliverable | Module | Notes |
|---|---|---|
| Atlas AI integration | `dp:atlas_ai` | OOTB agents layered on top of the foundation data model |
| Additional source systems | `sourcesystem/cdf_<system>_foundation` | Maximo, Meridium, or other CMMS |
| PI Asset Framework (AF) support | `sourcesystem/cdf_pi_af_foundation` | Hierarchy ingestion from PI AF |
| Project health dashboard | `dashboards/project_health` | Extraction pipeline uptime, transformation error rates |
| Automated quality assertions | *(new module)* | Scheduled transformation-based checks (null rates, referential integrity, coverage thresholds) |

---

## Risks and Dependencies

### Team and Repository Dependencies

| Dependency | Risk | Mitigation |
|---|---|---|
| `library` repo | All new modules land here; merge conflicts if other teams are actively developing in `modules/` | Coordinate with module owners; use feature branches per module |
| `gss-knowledge-base` repo | Extractor config templates sourced from here; changes there may need to be reflected in the DP | Document the source commit/tag in module READMEs; re-sync on major extractor version bumps |
| **`isa_manufacturing_extension` / `cfihos_oil_and_gas_extension` schema evolution** | View or property changes will break source system transformations without a coordinated update | Pin `dataModelVersion` variable; reference all view IDs and spaces via variables — a version bump stays a config change |
| **CDF Toolkit version** | Module YAML syntax and variable substitution are Toolkit-version-dependent | Document minimum supported Toolkit version in `module.toml`; test in CI |
| **CDF Workflows API** | Behaviour changes to `concurrencyPolicy`, `onFailure`, or task types would require workflow YAML updates | Monitor CDF release notes; the two-phase task graph is simple and low-risk |

### Known Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **PI extractor config drift across PI versions** | Medium | Medium | Document which extractor version the template targets; field engineers verify compatibility before deploying |
| **SAP OData endpoint variability** | Medium | High | Template documents common entity sets; customers adjust service names and field names for their SAP landscape |
| **OPC-UA node filter requirements** | High | Low | Config template ships with commented-out example filters; documented as required before production use |
| **Transformation generalization limits** | High | Medium | SQL scaffolds are generalized examples. Cursor rules (`.cursor/rules/cdf-transformations.mdc`) guide AI-assisted adaptation, but DEs must validate against real source data before handover |
| **Workflow generation script correctness** | Low | Medium | `build_workflow.py` validates `dependsOn` integrity at generation time; CI `--check` flag prevents drift between config and committed YAML |
| **PI/OPC-UA direct-write DM enrichment timing** | Low | Medium | Transformation enriching DM instances from CDF TimeSeries must run after extractors have written timeseries. Workflow ordering ensures this via `dependsOn` |

---

## Open Questions

### Q1 — Optional source system tasks in the ingestion workflow *(resolved)*

**Decision**: Build-time YAML generation via `scripts/build_workflow.py`. Script reads `enabledSources` flags and DM variant, assembles relevant task snippets, validates `dependsOn` references, and writes `wf_ingestion_v1.WorkflowVersion.yaml`. CI `--check` flag prevents drift.

### Q2 — `cdf_sap_foundation`: merged vs split module *(resolved)*

**Decision**: Single merged module. In practice, assets and maintenance orders are always needed together for meaningful contextualization; the SAP OData extractor uses a single NW Gateway connection.

### Q3 — `cdf_file_annotation` variable compatibility *(verify before v1)*

`cdf_file_annotation` was written against the `cdf_common` variable contract. Verify that the variable names it expects (`instanceSpace`, `functionSpace`, `dataset`) are populated correctly by the deployment's variable file when deploying alongside the foundation source system modules. No structural dependency on `cdf_common` should be assumed.

### Q4 — Minimum supported CDF Toolkit version *(must document before v1)*

The minimum Toolkit version that supports all resource types used in this DP must be identified and pinned in each module's `module.toml`.

### Q5 — Transformation scaffold validation approach *(open)*

- **Option A**: Manual validation checklist — DE runs transformation in preview mode against real RAW data
- **Option B**: Transformation unit test framework (P1) with known-good RAW fixtures
- **Option C**: Validation notebook that runs scaffolds against sample data and reports mismatches

Decision needed before P1.

### Q6 — CFIHOS ctx task snippets for the ingestion workflow *(open)*

The CFIHOS Oil & Gas DM relationship transformations (tag-to-document, tag-to-equipment) need to be authored and validated as task snippets for `cdf_ingestion_foundation`. This is a v1 blocker if CFIHOS DM support is required at launch.

---

## Success Metrics

### Near zero-config (usability check)

- A DE deploys a working environment (foundation + one source system + workflow) by filling in ≤ 10 variables and running `cdf deploy` — without consulting additional documentation or requesting help.

### Deployment health (binary checks)

- `dp:foundation` deploys cleanly to a brand-new CDF project with no prior resources and no errors.
- Each source system module (`cdf_pi_foundation`, `cdf_opcua_foundation`, `cdf_sap_foundation`, `cdf_db_foundation`, `cdf_files_foundation`) deploys independently when the others are absent.
- All resource external IDs contain zero hardcoded site names or space identifiers — every location-specific value is a template variable.
- Existing contextualization modules (`cdf_file_annotation`, `cdf_entity_matching`) deploy alongside the new foundation modules with no changes to their files.

### Field engineer experience (outcome-based)

- **Extractor readiness**: A field engineer with CDF credentials and source system credentials can configure and run the PI, OPC-UA, or SAP extractor using only the config template shipped in the module — without consulting gss-knowledge-base or requesting help.
- **Time to first data in CDF**: A DE starting from a blank, provisioned CDF project can have timeseries and asset data flowing into the data model within **one working day** of deploying `dp:foundation` with a single source system.
- **Time to end-to-end deployment**: A DE deploying all source systems plus contextualization can complete the deployment within **two working days**.

### Scalability (structural checks)

- Adding a new source system requires: (a) deploying one new module and (b) updating one variable in `cdf_ingestion_foundation/default.config.yaml` — no existing module files are modified.
- Deploying the same DP to a second site (different `location`) requires only a new variable file — all module YAML is reused as-is.
