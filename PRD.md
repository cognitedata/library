# PRD: Foundation Deployment Pack (`dp:foundation`)

## Overview

The **Foundation Deployment Pack** (`dp:foundation`) is a scalable, modular CDF deployment package that gives industrial projects a well-structured starting point. It covers the full stack from source system extraction through contextualization: production-grade extractor configuration templates for PI, OPC-UA, and SAP; an enterprise data model; a modular ingestion orchestration workflow; and contextualization capabilities including file annotation, entity matching, and SQL-based connections.

Every module is independently deployable. The package supports `canCherryPick = true`, so teams select only the source systems and capabilities they need. Adding a new source system or contextualization step means adding a module — not modifying existing ones.

---

## Background

### Who is this for?

The primary users of `dp:foundation` are **Deployment Engineers (DEs)** and **Cognite partners** who are responsible for standing up CDF environments for industrial customers. Secondary users are **Solutions Architects** who define the project structure before a DE takes over, and — in more self-serve engagements — **technically capable customers** who own their own CDF configuration.

### What is the pain today?

When a DE starts a new CDF project, there is no authoritative, reusable starting point that covers the full stack. The options available today each have gaps:

- **`dp:quickstart`** is demo-oriented — it ships synthetic data and pre-configured connections that are tightly coupled to a fictional "Springfield" site. Removing synthetic data, renaming locations, or swapping a source system requires touching files across multiple modules. It is not designed to evolve into a production deployment.
- **Blank-slate projects** (no DP) require DEs to assemble spaces, datasets, groups, extractor configs, transformations, and orchestration from scratch on every engagement. This leads to inconsistent project structures, configuration drift, and undocumented decisions.
- **gss-knowledge-base** contains mature extractor configuration templates (PI, OPC-UA, SAP) but they are not wired into any deployable DP — DEs must manually locate, copy, and adapt them per project.

The result is that each new deployment is partially reinvented, slowing delivery and introducing variability that makes cross-project support harder.

### What does this enable?

`dp:foundation` gives DEs and partners a single, composable starting point they can deploy to a real customer project on day one:

- Extractor configuration templates for the three most common industrial source systems are bundled and ready — no gss-knowledge-base lookup required.
- The project structure (spaces, datasets, groups, data model, orchestration) is standardised and parameterised by `location` — a new site is a new variable value, not a new set of files.
- Contextualization is a first-class, deployable capability, not an afterthought.
- The architecture is open for extension (add a module, set a variable) rather than modification (fork and edit).

---

## Goals

- Provide a complete, deployable CDF project structure: spaces, datasets, auth groups, data model, extraction pipelines, transformations, ingestion orchestration, and contextualization.
- Ship production-grade extractor configuration templates for **PI**, **OPC-UA**, and **SAP** sourced from gss-knowledge-base, so field engineers get a real starting point with all required parameters documented.
- Each source system module is self-contained and independently deployable — no module fails to deploy because another is absent.
- Ingestion orchestration is source-agnostic: configuring which transformations run in which phase is the only customization required when adding or removing a source system.
- Contextualization modules (file annotation, entity matching, SQL connections) and a contextualization quality dashboard are first-class parts of this DP.

---

## Non-Goals (v1 Scope Exclusions)

The following are deliberately out of scope for the initial release. Excluding them keeps v1 focused and deliverable; they are candidates for later phases.

| Excluded | Rationale |
|---|---|
| **Maximo, Meridium, other CMMS** | SAP is the most common asset/maintenance source system in the target segment. Additional CMMS sources are P2+ work. |
| **OSIsoft PI Asset Framework (AF)** | `cdf_pi_foundation` covers the PI Data Archive (timeseries) via the PI .NET Extractor. PI AF hierarchy ingestion requires a separate extractor and transformation pattern; not in v1. |
| **SAP PM via IDoc / RFC** | The SAP OData extractor pattern is the primary integration. IDoc and RFC-based extractions require different tooling and are not covered. |
| **OPC-UA Historical Access (HDA)** | The OPC-UA module covers live/subscribed data. Historical data backfill via HDA is a separate configuration concern; left as a documented extension pattern. |
| **OOTB (Out-of-the-Box) Cognite project setup** | `dp:foundation` does not configure IDP, project creation, or network connectivity. It assumes a provisioned CDF project with IDP authentication already in place. |
| **Atlas AI / OOTB Agents** | AI agent deployment (`dp:atlas_ai`) is a P2 concern layered on top of a working foundation. |
| **Automated transformation unit tests** | A testing framework for verifying transformation SQL output is a P1 concern. |
| **Multi-tenant or multi-project federation** | Each deployment of `dp:foundation` targets a single CDF project. Cross-project data federation is out of scope. |
| **Sharepoint / document source system** | File ingestion from SharePoint is not included in v1. A generic `cdf_documents_foundation` module is a P1 candidate. |

---

## Target Users

| Persona | How this DP helps |
|---|---|
| **Field Engineers** setting up a new customer project | Drop-in extractor config templates (PI, OPC-UA, SAP) with all required parameters pre-documented; fill in credentials and run |
| **Solutions Architects** designing a scalable CDF project | Modular structure grows by adding modules, not by forking; shared variable contract means location-specific config lives in one place |
| **Customers** deploying CDF for the first time | Clear, layered architecture from data model → source systems → orchestration → contextualization; cherry-pick only what applies |

---

## Module Architecture

### Package Composition

```toml
[packages.foundation]
id = "dp:foundation"
title = "Foundation Deployment Pack"
description = "A scalable, modular foundation for industrial CDF projects. Includes an enterprise data model, production-grade extractor config templates for PI, OPC-UA, and SAP, modular ingestion orchestration, and contextualization with quality dashboards."
canCherryPick = true
modules = [
    # Core infrastructure
    "foundation/cdf_foundation",
    "models/qs_enterprise_dm",
    # Source systems — deploy the ones matching your site
    "sourcesystem/cdf_pi_foundation",
    "sourcesystem/cdf_opcua_foundation",
    "sourcesystem/cdf_sap_foundation",
    # Ingestion orchestration
    "foundation/cdf_ingestion_foundation",
    # Contextualization
    "accelerators/contextualization/cdf_file_annotation",
    "accelerators/contextualization/cdf_entity_matching",
    "accelerators/contextualization/cdf_connection_sql",
    # Search
    "accelerators/industrial_tools/cdf_search",
    # Contextualization quality
    "dashboards/context_quality",
]
```

New modules created as part of this DP:
- `foundation/cdf_foundation`
- `sourcesystem/cdf_pi_foundation`
- `sourcesystem/cdf_opcua_foundation`
- `sourcesystem/cdf_sap_foundation`
- `foundation/cdf_ingestion_foundation`

Existing modules referenced without modification:
- `models/qs_enterprise_dm`
- `accelerators/contextualization/cdf_file_annotation`
- `accelerators/contextualization/cdf_entity_matching`
- `accelerators/contextualization/cdf_connection_sql`
- `accelerators/industrial_tools/cdf_search`
- `dashboards/context_quality`

---

### Module Dependency Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 0 — DATA MODEL                                                        │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │  models/qs_enterprise_dm  [existing]                                  │   │
│  │  39 views · 46 containers · 3 spaces · 2 data models                 │   │
│  │  sp_enterprise_process_industry · sp_enterprise_instance             │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — PROJECT FOUNDATION                                                │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │  foundation/cdf_foundation  [new]                                     │   │
│  │  Spaces · Datasets · Auth groups · Base RAW database                 │   │
│  │  Defines: location, organization, schemaSpace, instanceSpace vars    │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                    │                  │                   │
          ┌─────────┘          ┌───────┘          ┌───────┘
          ▼                    ▼                   ▼
┌──────────────────┐  ┌─────────────────┐  ┌────────────────────┐
│  LAYER 2 — SOURCE SYSTEMS  (independently deployable)          │
│                                                                │
│  sourcesystem/   │  sourcesystem/    │  sourcesystem/         │
│  cdf_pi_         │  cdf_opcua_       │  cdf_sap_              │
│  foundation      │  foundation       │  foundation            │
│  [new]           │  [new]            │  [new]                 │
│                  │                   │                        │
│  EP config tmpl  │  EP config tmpl   │  EP config tmpl        │
│  (PI .NET)       │  (OPC-UA)         │  (SAP OData)           │
│  RAW DB + table  │  RAW DB + table   │  RAW DB + 5 tables     │
│  1 transformation│  1 transformation │  6 transformations     │
│  TS → DM views   │  TS → DM views    │  Assets+Events→DM      │
└──────────────────┘  └─────────────────┘  └────────────────────┘
          │                    │                   │
          └────────────────────┴───────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — INGESTION ORCHESTRATION                                           │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │  foundation/cdf_ingestion_foundation  [new]                           │   │
│  │  Two-phase workflow: Population → Contextualization                   │   │
│  │  Phase tasks are configured via variable lists — no hardcoded sources │   │
│  │  Parallel execution within each phase · Abort on failure              │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
         ┌─────────────────────┼──────────────────────┐
         ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 4 — CONTEXTUALIZATION  (existing modules, no changes)                 │
│                                                                              │
│  ┌──────────────────┐  ┌───────────────────┐  ┌─────────────────────────┐   │
│  │  cdf_file_       │  │  cdf_entity_      │  │  cdf_connection_sql     │   │
│  │  annotation      │  │  matching         │  │                         │   │
│  │  [existing]      │  │  [existing]       │  │  [existing]             │   │
│  │  P&ID annotation │  │  TS→Asset AI      │  │  SQL relation builder   │   │
│  │  4 functions     │  │  matching         │  │  TS→Equipment           │   │
│  │  1 workflow      │  │  2 functions      │  │  Order→Asset            │   │
│  └──────────────────┘  └───────────────────┘  └─────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                               │
         ┌─────────────────────┴──────────────────────┐
         ▼                                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 5 — SEARCH & QUALITY  (existing modules, no changes)                  │
│                                                                              │
│  accelerators/industrial_tools/cdf_search        dashboards/context_quality  │
│  Location filters for scoped search              Contextualization coverage  │
│  [existing]                                      metrics and quality reports │
│                                                  [existing]                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Specifications

### 1. `foundation/cdf_foundation` *(New)*

**Purpose**: Project-scoped infrastructure shared by all modules in this DP. Creates the spaces, datasets, auth groups, and RAW databases that every source system and contextualization module references. Defines the canonical set of template variables used across the entire DP.

`cdf_foundation` is designed as a strict superset of `accelerators/cdf_common`. It creates all the same resources that `cdf_common` creates — the instance space, function space, ingestion dataset, RAW source database, RAW state database, and state table — using the same variable names so that existing modules (`cdf_file_annotation`, `cdf_entity_matching`, `cdf_connection_sql`, `cdf_ingestion_foundation`) that were written against the `cdf_common` variable contract work without changes. The annotation-specific CDF Function (`contextualization_connection_writer`) and ExtractionPipeline (`ctx_files_direct_relation_write`) from `cdf_common` are not included — those belong to `cdf_file_annotation`.

On top of what `cdf_common` provides, `cdf_foundation` adds auth groups and a foundation dataset scoped to the deployment.

**Resources**:

| Resource | Variable / External ID | Mirrors `cdf_common`? | Purpose |
|---|---|---|---|
| `Space` | `{{ instanceSpace }}` | Yes | Primary instance space for DM data |
| `Space` | `{{ functionSpace }}` | Yes | Space for CDF Function instances |
| `DataSet` | `{{ dataset }}` | Yes | Shared dataset for ingestion resources |
| `DataSet` | `ds_{{location}}_foundation` | No (new) | Dataset for foundational resources |
| `RAW Database` | `{{ rawSourceDatabase }}` | Yes | Source data landing zone |
| `RAW Database` | `{{ rawStateDatabase }}` | Yes | Contextualization state database |
| `RAW Table` | `{{ rawStateDatabase }}.{{ rawStateTable }}` | Yes | State cursor table for annotation/sync jobs |
| `Group` | `grp_{{location}}_admins` | No (new) | Full project access for administrators |
| `Group` | `grp_{{location}}_readonly` | No (new) | Read-only access for viewers |
| `Group` | `grp_{{location}}_extractors` | No (new) | Write access for extractor service accounts |
| `Group` | `grp_{{location}}_functions` | No (new) | Access for CDF Function service accounts |

**File structure**:
```
foundation/cdf_foundation/
├── module.toml
├── default.config.yaml
├── auth/
│   ├── grp_admins.Group.yaml
│   ├── grp_readonly.Group.yaml
│   ├── grp_extractors.Group.yaml
│   └── grp_functions.Group.yaml
├── data_model/
│   ├── instances.Space.yaml           # {{ instanceSpace }}
│   └── functions.Space.yaml           # {{ functionSpace }}
├── data_sets/
│   ├── ingestion.DataSet.yaml         # {{ dataset }}
│   └── foundation.DataSet.yaml        # ds_{{location}}_foundation
└── raw/
    ├── source.Database.yaml           # {{ rawSourceDatabase }}
    ├── state.Database.yaml            # {{ rawStateDatabase }}
    └── state.Table.yaml               # {{ rawStateDatabase }}.{{ rawStateTable }}
```

**Configuration variables** (`default.config.yaml`) — variable names intentionally match `cdf_common` defaults for compatibility:

| Variable | Default | Description |
|---|---|---|
| `location` | *(required)* | Site identifier, e.g. `oslo`, `stavanger` |
| `organization` | *(required)* | Org prefix for view/transformation references |
| `instanceSpace` | `{{location}}_instances` | Instance space for this site's DM data |
| `functionSpace` | `{{location}}_functions` | Space for CDF Function code nodes |
| `dataset` | `ingestion` | Shared dataset for transformations, functions, workflows |
| `rawSourceDatabase` | `ingestion` | RAW DB for source system landed data |
| `rawStateDatabase` | `contextualizationState` | RAW DB for state cursors |
| `rawStateTable` | `diagramParsing` | State table used by annotation and sync jobs |
| `schemaSpace` | `sp_enterprise_process_industry` | Schema space from `qs_enterprise_dm` |
| `annotationSpace` | `{{location}}_instances` | Space where annotation edges are stored |
| `dataModelVersion` | `v1.0` | Data model version for transformation view references |

**Dependencies**: None

---

### 2. `models/qs_enterprise_dm` *(Existing — no changes)*

**Purpose**: Enterprise data model providing 39 views across Asset, Equipment, Maintenance, TimeSeries, Files, and 3D. All source system transformations in this DP write into views defined here.

**Resources** (existing, unchanged):
- 3 Spaces (`sp_enterprise_process_industry`, `sp_enterprise_instance`, `sp_site_instance`)
- 46 Containers, 39 Views
- 2 Data Models (`qs-enterprise`, `qs-enterprise-search`)

**Dependencies**: None

---

### 3. `sourcesystem/cdf_pi_foundation` *(New)*

**Purpose**: Ingest PI timeseries metadata from a PI server into CDF RAW via the PI .NET Extractor, then transform the RAW data into `CogniteTimeSeries` data model instances. Ships with a complete, parameterized extractor configuration template sourced from gss-knowledge-base so a field engineer needs only to supply credentials.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_{{location}}_pi` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` | *(attached to pipeline)* | PI .NET extractor config template |
| `RAW Database` | `db_{{location}}_pi` | PI tag metadata landing zone |
| `RAW Table` | `db_{{location}}_pi:timeseries` | PI tag name, description, unit, engineering range |
| `Transformation` | `tr_{{location}}_pi_timeseries` | RAW PI tags → `CogniteTimeSeries` DM instances |

**Extractor config template** (from `gss-knowledge-base/extractors/toolkit_examples/pi_net_extractor/`):

Key customer-configured parameters:
```yaml
# Supplied via environment variables:
#   PI_HOST           — PI server hostname or IP
#   PI_USER           — PI server username
#   PI_PASSWORD       — PI server password
#   COGNITE_BASE_URL, COGNITE_PROJECT, COGNITE_TOKEN_URL
#   COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET
#
# Template-resolved at deploy time:
#   extraction-pipeline.external-id: ep_{{location}}_pi
#   data-modeling.space:             {{instanceSpace}}
#   destination.dataset-external-id: {{dataSet}}
#   id-prefix:                       pi:
```

**Transformation notes**:
- Maps PI tag name → `externalId` with `pi:` prefix
- Writes to `CogniteTimeSeries` view in `{{instanceSpace}}`
- `sysTagsFound` is populated by default (opt-out via `populateSysTagsFound: false` in `default.config.yaml`) to maintain downstream contextualization compatibility

**File structure**:
```
sourcesystem/cdf_pi_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_pi.ExtractionPipeline.yaml
│   └── ep_pi.ExtractionPipeline.Config.yaml
├── raw/
│   └── db_pi.Database.yaml
└── transformations/
    └── tr_pi_timeseries.Transformation.yaml
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `piIdPrefix` | `pi:` | External ID prefix for PI timeseries |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required**:
- `PI_HOST`, `PI_USER`, `PI_PASSWORD`
- Standard CDF auth vars

**Dependencies**: `foundation/cdf_foundation`

---

### 4. `sourcesystem/cdf_opcua_foundation` *(New)*

**Purpose**: Ingest OPC-UA node metadata via the OPC-UA Extractor into CDF RAW, then transform into `CogniteTimeSeries` data model instances. Follows the same RAW → Transformation pattern as PI and SAP for consistency and auditability. Ships with a complete, parameterized extractor configuration template sourced from gss-knowledge-base.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_{{location}}_opcua` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` | *(attached to pipeline)* | OPC-UA extractor config template |
| `RAW Database` | `db_{{location}}_opcua` | OPC-UA node metadata landing zone |
| `RAW Table` | `db_{{location}}_opcua:nodes` | OPC-UA variable nodes: name, description, data type, EU range |
| `Transformation` | `tr_{{location}}_opcua_timeseries` | RAW OPC-UA nodes → `CogniteTimeSeries` DM instances |

**Extractor config template** (from `gss-knowledge-base/extractors/toolkit_examples/opcua_extractor_speira_quickstart/`):

Key customer-configured parameters:
```yaml
# Supplied via environment variables:
#   OPCUA_ENDPOINT_URL  — e.g. opc.tcp://192.168.1.10:4840
#   OPCUA_USER          — OPC-UA server username
#   OPCUA_PASSWORD      — OPC-UA server password
#   COGNITE_BASE_URL, COGNITE_PROJECT, COGNITE_TOKEN_URL
#   COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET
#
# Template-resolved at deploy time:
#   extraction-pipeline.external-id: ep_{{location}}_opcua
#   destination.raw.database:        db_{{location}}_opcua
#   destination.raw.table:           nodes
#   id-prefix:                       opcua:
#
# Site-specific (documented as commented examples in config):
#   source.node-filter.allow / deny lists  — OPC-UA node ID patterns to include/exclude
#   source.browse-throttling               — max-per-minute, max-parallelism
#   source.publishing-interval             — default 5000 ms
#   source.sampling-interval              — default 5000 ms
```

**Transformation notes**:
- OPC-UA extractor writes node metadata to `db_{{location}}_opcua:nodes` (RAW)
- Transformation reads RAW and maps to `CogniteTimeSeries` view in `{{instanceSpace}}`
- External ID: `opcua:<node-id>`
- `sysTagsFound` opt-out behavior same as PI module

**File structure**:
```
sourcesystem/cdf_opcua_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_opcua.ExtractionPipeline.yaml
│   └── ep_opcua.ExtractionPipeline.Config.yaml
├── raw/
│   └── db_opcua.Database.yaml
└── transformations/
    └── tr_opcua_timeseries.Transformation.yaml
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `opcuaIdPrefix` | `opcua:` | External ID prefix for OPC-UA timeseries |
| `opcuaPublishingInterval` | `5000` | OPC-UA publishing interval in ms |
| `opcuaSamplingInterval` | `5000` | OPC-UA sampling interval in ms |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required**:
- `OPCUA_ENDPOINT_URL`, `OPCUA_USER`, `OPCUA_PASSWORD`
- Standard CDF auth vars

**Dependencies**: `foundation/cdf_foundation`

---

### 5. `sourcesystem/cdf_sap_foundation` *(New)*

**Purpose**: Ingest SAP functional locations, equipment master records, maintenance orders, and work operations into CDF RAW via the SAP OData Extractor, then transform into the enterprise data model. Consolidates what would otherwise be separate asset and events modules into a single cohesive SAP source system module — since both share the same extractor, authentication, and data model target.

Ships with a complete SAP OData extractor configuration template sourced from gss-knowledge-base with single-plant default and multi-plant expansion support via a plant list variable.

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `ExtractionPipeline` | `ep_{{location}}_sap` | Pipeline health tracking and config delivery |
| `ExtractionPipeline Config` | *(attached to pipeline)* | SAP OData extractor config template |
| `RAW Database` | `db_{{location}}_sap` | SAP data landing zone |
| `RAW Table` | `db_{{location}}_sap:equipment` | SAP equipment master records |
| `RAW Table` | `db_{{location}}_sap:functional_location` | SAP functional location hierarchy |
| `RAW Table` | `db_{{location}}_sap:workorder` | SAP work orders (PM orders) |
| `RAW Table` | `db_{{location}}_sap:workpackage` | SAP work packages |
| `RAW Table` | `db_{{location}}_sap:worktask` | SAP work tasks (operations) |
| `RAW Table` | `db_{{location}}_sap:workitem` | SAP work items (sub-operations) |
| `RAW Table` | `db_{{location}}_sap:state_store` | OData extractor delta state (cursor tracking) |
| `Transformation` | `tr_{{location}}_sap_assets` | Functional locations → `Asset` DM instances |
| `Transformation` | `tr_{{location}}_sap_equipment` | Equipment master → `Equipment` DM instances |
| `Transformation` | `tr_{{location}}_sap_equipment_to_asset` | Equipment → Asset edge relationships |
| `Transformation` | `tr_{{location}}_sap_maintenance_orders` | Work orders → `MaintenanceOrder` DM instances |
| `Transformation` | `tr_{{location}}_sap_operations` | Work tasks/items → `Operation` DM instances |
| `Transformation` | `tr_{{location}}_sap_operation_to_order` | Operation → MaintenanceOrder edge relationships |

**Extractor config template** (from `gss-knowledge-base/extractors/toolkit_examples/sap_odata_extractor_remote/`):

Key customer-configured parameters:
```yaml
# Supplied via environment variables:
#   SAP_GATEWAY_URL   — SAP NW Gateway base URL
#   SAP_CLIENT        — SAP client number
#   SAP_USERNAME      — SAP OData service user
#   SAP_PASSWORD      — SAP OData service password
#   COGNITE_BASE_URL, COGNITE_PROJECT, COGNITE_TOKEN_URL
#   COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET
#
# Template-resolved at deploy time:
#   extraction-pipeline.external-id: ep_{{location}}_sap
#   state-store.raw.database:        db_{{location}}_sap
#   state-store.raw.table:           state_store
#
# Single-plant default (sapPlant: "1000"):
#   Each endpoint targets db_{{location}}_sap with table names as above.
#
# Multi-plant expansion (sapPlants: ["1000", "2000", "3000"]):
#   Additional per-plant RAW databases are generated: db_{{location}}_sap_{{plant}}
#   Each plant gets its own endpoint entries for Equipment, FunctionalLocation, etc.
#   Transformations union across plants using the sapPlants list variable.
#
# Endpoints configured (schedules are per-endpoint, default weekly cron):
#   EquipmentListSet, FunclocListSet, ExHeaderSet (work orders),
#   ExOlistSet (operations), ExOperationsSet (items), ExNotifheader (notifications)
```

**Multi-plant behavior**:
- Default: `sapPlant: "1000"` → single RAW database, standard table names
- Multi-plant: `sapPlants: ["1000", "2000", "3000"]` → extractor config expands per plant, transformations use a `UNION ALL` pattern across plant-prefixed tables

**Transformation notes**:
- All SQL uses `{{location}}`, `{{organization}}`, `{{sapSystem}}`, `{{instanceSpace}}`, `{{schemaSpace}}` variables — no hardcoded site names
- `sysTagsFound` populated on `MaintenanceOrder` for downstream connection compatibility (opt-out via `populateSysTagsFound: false`)

**File structure**:
```
sourcesystem/cdf_sap_foundation/
├── module.toml
├── default.config.yaml
├── extraction_pipelines/
│   ├── ep_sap.ExtractionPipeline.yaml
│   └── ep_sap.ExtractionPipeline.Config.yaml
├── raw/
│   └── db_sap.Database.yaml
└── transformations/
    ├── tr_sap_assets.Transformation.yaml
    ├── tr_sap_equipment.Transformation.yaml
    ├── tr_sap_equipment_to_asset.Transformation.yaml
    ├── tr_sap_maintenance_orders.Transformation.yaml
    ├── tr_sap_operations.Transformation.yaml
    └── tr_sap_operation_to_order.Transformation.yaml
```

**Configuration variables**:

| Variable | Default | Description |
|---|---|---|
| `location` | *(inherited)* | Site identifier |
| `organization` | *(inherited)* | Org prefix for view references |
| `sapSystem` | `s4hana` | SAP system label, used in external IDs |
| `sapPlant` | `1000` | Default single-plant code |
| `sapPlants` | `[]` | Override with a list for multi-plant expansion, e.g. `["1000","2000"]` |
| `populateSysTagsFound` | `true` | Set to `false` to omit `sysTagsFound` population |

**Environment variables required**:
- `SAP_GATEWAY_URL`, `SAP_CLIENT`, `SAP_USERNAME`, `SAP_PASSWORD`
- Standard CDF auth vars

**Dependencies**: `foundation/cdf_foundation`

---

### 6. `foundation/cdf_ingestion_foundation` *(New)*

**Purpose**: A source-agnostic, two-phase ingestion workflow that orchestrates population and contextualization transformations. Each transformation task in the workflow is referenced via a named variable in `default.config.yaml`. Deploying a new source system or wiring in a new contextualization transformation means updating a variable value — the workflow YAML structure does not change.

**Design**:

The CDF Toolkit does not support foreach/list iteration in YAML — variables are simple value substitutions, not list expansions. The workflow therefore follows the same pattern as the existing `cdf_ingestion` module: each transformation task is a named variable. The key difference is that all variable defaults use `{{location}}`-prefixed external IDs (matching the foundation source system modules), grouping is clearly separated into population vs contextualization phases in `default.config.yaml`, and OPC-UA is included as a first-class optional task.

The workflow has two sequential phases:

1. **Population phase** — loads data from RAW into DM instances. Tasks run in parallel.
2. **Contextualization phase** — builds relationships between DM instances. Runs only after the entire population phase succeeds. Tasks run in parallel.

Any task failure aborts the workflow (`onFailure: abortWorkflow`).

**Resources**:

| Resource | External ID | Purpose |
|---|---|---|
| `Workflow` | `wf_{{location}}_ingestion` | Orchestrates population → contextualization |
| `WorkflowVersion` | `wf_{{location}}_ingestion/v1` | Active version with task graph |
| `WorkflowTrigger` | `wf_{{location}}_ingestion_trigger` | Scheduled execution (configurable cron) |
| `Group` | `grp_{{location}}_workflow` | Service account group for workflow execution |
| `Group` | `grp_{{location}}_workflow_user` | User group for manual workflow execution |

**`default.config.yaml`**:

```yaml
workflow: "wf_{{location}}_ingestion"
workflowSchedule: "0 2 * * *"     # daily at 02:00 UTC; set to "0 0 29 2 *" to disable auto-run

# -----------------------------------------------------------------
# Population phase — source system transformations
# Set each value to the external ID of the deployed transformation.
# -----------------------------------------------------------------
piTimeseriesTransformationExternalId:    "tr_{{location}}_pi_timeseries"
opcuaTimeseriesTransformationExternalId: "tr_{{location}}_opcua_timeseries"
sapAssetsTransformationExternalId:               "tr_{{location}}_sap_assets"
sapEquipmentTransformationExternalId:            "tr_{{location}}_sap_equipment"
sapEquipmentToAssetTransformationExternalId:     "tr_{{location}}_sap_equipment_to_asset"
sapMaintenanceOrdersTransformationExternalId:    "tr_{{location}}_sap_maintenance_orders"
sapOperationsTransformationExternalId:           "tr_{{location}}_sap_operations"
sapOperationToOrderTransformationExternalId:     "tr_{{location}}_sap_operation_to_order"

# -----------------------------------------------------------------
# Contextualization phase — SQL connection transformations
# Uncomment and set when cdf_connection_sql is deployed.
# -----------------------------------------------------------------
maintenanceOrderToAssetTransformationExternalId: "maintenance_order_to_asset"
operationToAssetTransformationExternalId:        "operation_to_asset"
timeSeriesToEquipmentTransformationExternalId:   "timeseries_to_equipment"
activityToTimeSeriesTransformationExternalId:    "activity_to_timeseries"
```

**`workflows/v1.WorkflowVersion.yaml`** (representative structure):

```yaml
workflowExternalId: "{{ workflow }}"
version: v1
workflowDefinition:
  tasks:
    # ── Population phase ─────────────────────────────────────────
    # PI, OPC-UA, and SAP population tasks run in parallel
    - externalId: "{{ piTimeseriesTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ piTimeseriesTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ opcuaTimeseriesTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ opcuaTimeseriesTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ sapAssetsTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapAssetsTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ sapEquipmentTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapEquipmentTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ sapEquipmentToAssetTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapEquipmentToAssetTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ sapAssetsTransformationExternalId }}"
        - externalId: "{{ sapEquipmentTransformationExternalId }}"

    - externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ sapOperationsTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapOperationsTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow

    - externalId: "{{ sapOperationToOrderTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ sapOperationToOrderTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
        - externalId: "{{ sapOperationsTransformationExternalId }}"

    # ── Contextualization phase ───────────────────────────────────
    # Runs only after all population tasks complete successfully
    - externalId: "{{ maintenanceOrderToAssetTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ maintenanceOrderToAssetTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ sapAssetsTransformationExternalId }}"
        - externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
        - externalId: "{{ sapOperationsTransformationExternalId }}"
        - externalId: "{{ sapOperationToOrderTransformationExternalId }}"

    - externalId: "{{ timeSeriesToEquipmentTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ timeSeriesToEquipmentTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ piTimeseriesTransformationExternalId }}"
        - externalId: "{{ opcuaTimeseriesTransformationExternalId }}"
        - externalId: "{{ sapEquipmentTransformationExternalId }}"
        - externalId: "{{ sapEquipmentToAssetTransformationExternalId }}"

    - externalId: "{{ activityToTimeSeriesTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ activityToTimeSeriesTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ piTimeseriesTransformationExternalId }}"
        - externalId: "{{ opcuaTimeseriesTransformationExternalId }}"
        - externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
        - externalId: "{{ sapOperationToOrderTransformationExternalId }}"

    - externalId: "{{ operationToAssetTransformationExternalId }}"
      type: transformation
      parameters:
        transformation:
          externalId: "{{ operationToAssetTransformationExternalId }}"
          concurrencyPolicy: fail
      onFailure: abortWorkflow
      dependsOn:
        - externalId: "{{ sapAssetsTransformationExternalId }}"
        - externalId: "{{ sapEquipmentTransformationExternalId }}"
        - externalId: "{{ sapMaintenanceOrdersTransformationExternalId }}"
        - externalId: "{{ sapOperationsTransformationExternalId }}"
        - externalId: "{{ sapEquipmentToAssetTransformationExternalId }}"
        - externalId: "{{ sapOperationToOrderTransformationExternalId }}"
```

> **Adding a new source system**: Add the new transformation external ID as a new named variable in `default.config.yaml`, add a new task entry in the WorkflowVersion YAML referencing that variable, and set any `dependsOn` entries needed. The existing task graph is unchanged.

**File structure**:
```
foundation/cdf_ingestion_foundation/
├── module.toml
├── default.config.yaml
├── auth/
│   ├── grp_workflow.Group.yaml
│   └── grp_workflow_user.Group.yaml
└── workflows/
    ├── wf_ingestion.Workflow.yaml
    ├── wf_ingestion_v1.WorkflowVersion.yaml
    └── wf_ingestion_trigger.WorkflowTrigger.yaml
```

**Dependencies**: `foundation/cdf_foundation`. Source system and contextualization modules must be deployed before the workflow is triggered.

---

### 7. `accelerators/contextualization/cdf_file_annotation` *(Existing — no changes)*

**Purpose**: P&ID and engineering document annotation using the Diagram Detect API. Discovers files, submits annotation jobs, processes results, and builds CogniteFile → CogniteAsset/Equipment edges. Includes pattern-based promotion and a Streamlit dashboard.

**Resources** (existing): 4 CDF Functions, 1 Workflow, 6 RAW tables, 1 Extraction Pipeline, 1 Dataset, 1 data model for annotation state.

**Dependencies**: `accelerators/cdf_common` (spaces, datasets) — this dependency is met by deploying `foundation/cdf_foundation` which provides equivalent infrastructure under the same variable contract.

---

### 8. `accelerators/contextualization/cdf_entity_matching` *(Existing — no changes)*

**Purpose**: AI-powered and rule-based timeseries-to-asset matching. Uses multi-method matching (rule-based + ML + manual expert mappings) to link `CogniteTimeSeries` instances to `Asset`/`Equipment` DM instances.

**Resources** (existing): 2 CDF Functions, 1 Workflow, 5 RAW tables, 1 Extraction Pipeline.

**Dependencies**: `accelerators/cdf_common` (spaces, datasets).

---

### 9. `accelerators/contextualization/cdf_connection_sql` *(Existing — no changes)*

**Purpose**: SQL-based relationship builder that creates edges between DM instances using `sysTagsFound` populated by source system transformations. Links timeseries to equipment, maintenance orders to assets, and operations to assets.

**Resources** (existing): 4 SQL Transformations.

**Dependencies**: Source system transformations must have run first and populated `sysTagsFound`. Relies on `{{instanceSpace}}` and `{{schemaSpace}}` variables from `cdf_foundation`.

---

### 10. `accelerators/industrial_tools/cdf_search` *(Existing — no changes)*

**Purpose**: Configures location-scoped filters for CDF Search, scoped to the site's instance space and linked to the process industry data model.

**Resources** (existing): 1 Location Filter.

**Dependencies**: `models/qs_enterprise_dm` (data model space).

---

### 11. `dashboards/context_quality` *(Existing — no changes)*

**Purpose**: Contextualization quality dashboard showing coverage metrics: how many timeseries, assets, equipment, files, and maintenance orders have been successfully contextualized.

**Resources** (existing):
- `DataSet`: `context_quality_apps` — created by this module
- `Function`: `context_quality_handler` — computes contextualization metrics by querying the CDF API at runtime
- `Streamlit`: `context_quality_dashboard` — interactive quality dashboard

**Dependency analysis**: The module has no hard structural dependencies on specific RAW tables or data model spaces from other modules. The CDF Function queries the live CDF API (DM instances, timeseries, assets) to compute metrics — it does not join against a predetermined set of RAW tables. The function YAML references `space: context_quality`, which is the module's own space for storing the function code node (a `DATA_MODELING_ONLY` mode requirement), not a runtime data dependency on another module's space.

This means the module deploys and operates independently. It will display meaningful metrics as soon as source system transformations have run and DM instances exist. No structural changes are needed to use it in this DP.

---

## Configuration Variables — Consolidated Reference

Variables are defined once — in `foundation/cdf_foundation/default.config.yaml` — and flow to every other module via the shared variable contract. No module hardcodes a site name or space identifier.

| Variable | Owner | Consumed by |
|---|---|---|
| `location` | `cdf_foundation` | All modules — used in every external ID and RAW database name |
| `organization` | `cdf_foundation` | `cdf_sap_foundation`, all transformation SQL view references |
| `schemaSpace` | `cdf_foundation` | All source system transformations (DM view lookup) |
| `instanceSpace` | `cdf_foundation` | All source system transformations (DM instance write target) |
| `dataSet` | `cdf_foundation` | All source system extraction pipelines and transformations |
| `dataModelVersion` | `cdf_foundation` | All transformation view references |
| `populateSysTagsFound` | Per source module | Source system transformations; set `false` to omit the field |
| `populationTasks` | `cdf_ingestion_foundation` | Workflow task graph generation |
| `contextualizationTasks` | `cdf_ingestion_foundation` | Workflow task graph generation |
| `sapPlant` / `sapPlants` | `cdf_sap_foundation` | SAP extractor config and transformation UNION logic |

---

## Resource Summary

| Module | Status | Spaces | Datasets | RAW DBs | RAW Tables | Transformations | Workflows | Pipelines | Groups |
|---|---|---|---|---|---|---|---|---|---|
| `foundation/cdf_foundation` | New | 2 | 2 | 1 | 0 | 0 | 0 | 0 | 4 |
| `models/qs_enterprise_dm` | Existing | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `sourcesystem/cdf_pi_foundation` | New | 0 | 0 | 1 | 1 | 1 | 0 | 1 | 0 |
| `sourcesystem/cdf_opcua_foundation` | New | 0 | 0 | 1 | 1 | 1 | 0 | 1 | 0 |
| `sourcesystem/cdf_sap_foundation` | New | 0 | 0 | 1 | 7 | 6 | 0 | 1 | 0 |
| `foundation/cdf_ingestion_foundation` | New | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| `cdf_file_annotation` | Existing | 0 | 1 | 6 | 6 | 0 | 1 | 1 | 0 |
| `cdf_entity_matching` | Existing | 0 | 0 | 0 | 5 | 0 | 1 | 1 | 0 |
| `cdf_connection_sql` | Existing | 0 | 0 | 0 | 0 | 4 | 0 | 0 | 0 |
| `cdf_search` | Existing | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `dashboards/context_quality` | Existing | 0 | 1 | 1 | — | — | 1 | 0 | 0 |
| **Total (new modules only)** | | **5** | **2** | **4** | **9** | **8** | **1** | **3** | **5** |

---

## Release Phases

### P0 — v1: Foundational Deployment

**Goal**: A DE can deploy a complete, real industrial CDF project from scratch using this DP — source system extraction, data model, orchestration, and contextualization — without consulting gss-knowledge-base or assembling boilerplate manually.

| Deliverable | Modules |
|---|---|
| Core project infrastructure | `foundation/cdf_foundation` |
| Enterprise data model | `models/qs_enterprise_dm` |
| PI timeseries ingestion | `sourcesystem/cdf_pi_foundation` |
| OPC-UA timeseries ingestion | `sourcesystem/cdf_opcua_foundation` |
| SAP asset + maintenance ingestion | `sourcesystem/cdf_sap_foundation` |
| Modular ingestion orchestration | `foundation/cdf_ingestion_foundation` |
| P&ID file annotation | `accelerators/contextualization/cdf_file_annotation` *(existing)* |
| Entity matching | `accelerators/contextualization/cdf_entity_matching` *(existing)* |
| SQL-based connections | `accelerators/contextualization/cdf_connection_sql` *(existing)* |
| Contextualization quality dashboard | `dashboards/context_quality` *(existing)* |
| Scoped search filters | `accelerators/industrial_tools/cdf_search` *(existing)* |

### P1 — v2: Quality and Testing

**Goal**: Deployments are verifiable. A DE can confirm that transformations produce correct output before handing a project to a customer.

| Deliverable | Notes |
|---|---|
| Transformation unit test framework | Test harness that validates SQL transformation output against known-good fixtures |
| Document source module (`cdf_documents_foundation`) | Generic file ingestion replacing the SharePoint-specific module |
| Multi-plant SAP validation | Verified multi-plant expansion via `sapPlants` list variable |

### P2 — v3: AI and Extended Source Systems

**Goal**: The foundation DP becomes the entry point for AI-augmented industrial workflows and a broader set of source systems.

| Deliverable | Notes |
|---|---|
| Atlas AI integration (`dp:atlas_ai`) | OOTB agents layered on top of the foundation data model |
| Additional source systems | Maximo, Meridium, or other CMMS as `sourcesystem/cdf_<system>_foundation` modules |
| Automated quality checks | Scheduled data quality assertions beyond contextualization coverage |
| PI Asset Framework (AF) support | Hierarchy ingestion from PI AF as an extension module |

---

## Risks and Dependencies

### Team and Repository Dependencies

| Dependency | Risk | Mitigation |
|---|---|---|
| **`library` repo** (this repo) | All new modules land here; merge conflicts if other teams are actively developing in `modules/` | Coordinate with module owners; use feature branches per module |
| **`gss-knowledge-base` repo** | Extractor config templates are sourced from here; changes there may need to be reflected in the DP | Pin to a specific commit or tag at release time; document the source commit in module READMEs |
| **`qs_enterprise_dm` data model** | All source system transformations reference views in this data model. A view rename or property removal in a future `qs_enterprise_dm` version will break the foundation transformations without a coordinated update | Transformation SQL should reference the model via the `{{dataModelVersion}}` variable; bump the variable when upgrading; pin the model version used in v1 explicitly |
| **CDF Toolkit version** | Module YAML syntax, WorkflowVersion schema, and variable substitution behaviour are Toolkit-version-dependent. A Toolkit major version bump may require YAML updates | Document the minimum supported Toolkit version in `module.toml`; test against the pinned version in CI |
| **CDF Workflows API** | The ingestion workflow uses the Workflows API (GA). Behaviour changes to `concurrencyPolicy`, `onFailure`, or task types would require workflow YAML updates | Monitor CDF release notes; the two-phase task graph is relatively simple and low-risk |

### Known Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **PI extractor config drift across PI versions** | Medium | Medium | The PI .NET Extractor config schema changes between major versions (e.g., v2.x → v3.x renamed several keys). The config template must document which extractor version it targets; field engineers must verify compatibility before deploying against older PI servers. |
| **SAP OData endpoint variability** | Medium | High | SAP NW Gateway services and entity names differ between SAP versions and customer configurations. The extractor config template provides the common entity set (equipment, functional locations, work orders) but customers may need to adjust service names and key fields for their specific SAP landscape. |
| **OPC-UA node filter requirements** | High | Low | OPC-UA node structure is highly site-specific. The extractor config template ships with commented-out example filters. Without correct filters, the extractor will browse the full OPC-UA server tree, which may be very large and slow. DEs must configure filters before production use — this should be explicitly documented. |
| **`cdf_foundation` vs `cdf_common` variable conflict** | Low | High | If an existing module is deployed with `cdf_common` and then `cdf_foundation` is introduced in the same project, and the variable values differ (e.g., different `instanceSpace` defaults), resources may be duplicated or point to the wrong space. The two modules should not be deployed together in the same project. |
| **Optional workflow tasks referencing non-existent transformations** | High | Medium | CDF Workflows will fail at runtime if a task references a transformation external ID that does not exist. If a customer deploys only PI (no SAP), the workflow will fail on SAP task references. This must be resolved before v1 ships — see Open Questions. |

---

## Open Questions

### Q1 — Optional source system tasks in the ingestion workflow *(must resolve before v1)*

If a customer deploys only PI (no OPC-UA, no SAP), the WorkflowVersion YAML will still reference OPC-UA and SAP transformation external IDs. CDF Workflows fails at runtime if any referenced transformation does not exist.

**Options**:

| Option | Pros | Cons |
|---|---|---|
| **(a) Require all referenced transformations** — document as a hard deploy prerequisite | Simple, no additional tooling | Forces customers to deploy unused source system modules just to satisfy the workflow |
| **(b) Dynamic task generation** — a CDF Function checks which transformations exist at runtime and emits only those as dynamic tasks | True modularity; workflow adapts to what's deployed | Adds a Function dependency to the orchestration layer; more complex to debug |
| **(c) Multiple WorkflowVersion templates** — ship templates for common combinations (PI-only, SAP-only, PI+SAP, PI+OPC-UA+SAP) | Simple YAML, no runtime logic | Combinatorial explosion as source systems increase; violates the "add a variable, not a file" principle |

**Recommendation**: Option (b) is the right long-term answer but adds scope. For v1, implement option (c) with the full combination as the default and document option (b) as the v2 target. Needs a decision before implementation starts.

---

### Q2 — `cdf_sap_foundation`: merged module vs separate assets and events *(decided, documenting trade-off)*

**Decision**: Merge `cdf_sap_assets` and `cdf_sap_events` concerns into a single `cdf_sap_foundation` module.

**Trade-off**:

| | Merged (`cdf_sap_foundation`) | Split (`cdf_sap_assets_foundation` + `cdf_sap_events_foundation`) |
|---|---|---|
| **Extractor config** | One extractor config, one extraction pipeline — SAP OData targets all entity types | Two configs — possible if different schedules or auth are needed per entity group |
| **Deployment atomicity** | Deploy SAP as one unit — assets and events always go together | Can deploy asset hierarchy without maintenance data (useful in early-stage projects) |
| **Module count** | Simpler; fewer modules to manage | More granular control |
| **Auth** | Single service account for all SAP entities | Could scope credentials separately per entity group |

The merged approach is preferred because: (a) in practice, assets and maintenance orders are always needed together for meaningful contextualization; (b) the SAP OData extractor is typically configured with a single connection to the NW Gateway covering all entity types; (c) fewer modules reduces cognitive overhead for field engineers. If a customer needs to separate schedules, they can configure per-endpoint cron expressions within the single extractor config.

---

### Q3 — `cdf_file_annotation` dependency on `cdf_common` *(verify before v1)*

`cdf_file_annotation` was written against the `cdf_common` variable contract. Verify that `cdf_foundation`'s variable defaults (particularly `functionSpace: {{location}}_functions`) are compatible with the values `cdf_file_annotation` expects. If `cdf_file_annotation`'s `module.toml` declares a hard dependency on `cdf_common`, a compatibility shim or a `module.toml` update in `cdf_file_annotation` may be needed.

---

### Q4 — Minimum supported CDF Toolkit version *(must document before v1)*

The WorkflowVersion YAML schema and variable substitution syntax are Toolkit-version-dependent. The minimum Toolkit version that supports all resource types used in this DP (Workflows, WorkflowTrigger, ExtractionPipeline Config, Space, etc.) must be identified and pinned in each module's `module.toml`. This is a blocking prerequisite for CI validation.

---

## Success Metrics

### Deployment health (binary checks)

- `dp:foundation` deploys cleanly to a brand-new CDF project with no prior resources and no errors.
- Each source system module (`cdf_pi_foundation`, `cdf_opcua_foundation`, `cdf_sap_foundation`) deploys independently when the others are absent.
- All transformation SQL and resource external IDs contain zero hardcoded site names or space identifiers — every location-specific value is a template variable.
- Existing contextualization modules (`cdf_file_annotation`, `cdf_entity_matching`, `cdf_connection_sql`) deploy alongside the new foundation modules with no changes to their files.

### Field engineer experience (outcome-based)

- **Extractor readiness**: A field engineer with CDF credentials and source system credentials can configure and run the PI, OPC-UA, or SAP extractor using only the config template shipped in the module — without consulting gss-knowledge-base or requesting help from another team member.
- **Time to first data in CDF**: A DE starting from a blank, provisioned CDF project can have timeseries and asset data flowing into the data model within **one working day** of deploying `dp:foundation` with a single source system.
- **Time to end-to-end deployment**: A DE deploying all three source systems (PI + OPC-UA + SAP) plus contextualization on a new customer project can complete the deployment within **two working days**.

### Scalability (structural checks)

- Adding a fourth source system to an existing deployment requires: (a) deploying one new module and (b) updating one variable in `cdf_ingestion_foundation/default.config.yaml` — no existing module files are modified.
- Deploying the same DP to a second site (different `location` value) requires only a new variable file — all module YAML is reused as-is.
