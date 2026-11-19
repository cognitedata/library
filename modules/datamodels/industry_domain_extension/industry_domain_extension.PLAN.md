# Simplified Industry Domain Data Model Template — Plan & Spec

> **Status:** Plan for review and build in this repository (`cognitedata/library`).
> **Audience:** Developers and agents scaffolding and adapting the modules below.
> **Standards:** cog-vd `cdf-data-modeling`, `cdf-transformation`, `cdf-workflow`, `cdf-naming-check` skills,
> `cdf-naming-conventions` rule, and the Cognite docs (core data model, process industry data model, Toolkit
> resource library).
> **Layout patterned after:** `cfihos_oil_and_gas_extension` + its `sourcesystem/*_data_dump` and
> `common/cdf_ingestion` companions (structure, Toolkit variables, sample data — **no CFIHOS or ISA content**).

---

## 1. Goal

Ship a **small, industry-neutral** reusable domain data model template that works for any asset-heavy industry
(process, manufacturing, utilities, energy, mining, infrastructure):

- Same Toolkit folder shape as the existing `datamodels/*_extension` modules.
- **Only** CDM / IDM extensions (`implements` on domain views) plus two label-only custom types.
- Reuse CDM/IDM properties and type nodes wherever they exist; custom properties only for what CDM/IDM lack.
- Custom containers/views **must not** implement or require `CogniteDescribable`.
- Scaffold properties clearly marked so an agent can rename, replace or delete them when wiring real source systems
  (§8.0).
- Companion Search `_SOL` data product: a **schema only** (views + data model) that maps enterprise containers and
  adds reverse relations — no containers, no transformations, no data of its own.
- One high-volume event type as **records** (`AlarmEvent`, `usedFor: record`) in a stream, linked to the graph by
  direct relations (§5.11).
- Sample data, transformations and a workflow so the template runs end to end out of the box.
- Every rule in §9 (validation gate) passes before the template ships.

---

## 2. Decisions locked

| Decision | Choice |
|----------|--------|
| Home | `modules/datamodels/` (pack `dp:models`), sample data in `modules/sourcesystem/`, workflow in `modules/common/` |
| Layers | Enterprise `_DOM` + companion Search `_SOL`; no `_SRC` model (RAW → enterprise directly) |
| Asset hub | `Tag` implements `cdf_cdm:CogniteAsset` (one asset view per model) |
| Industry standards | **Excluded** — no CFIHOS, ISA-95/88, RDS or other standard-specific classes or coded properties |
| CDM/IDM reuse | IDM `status` / `type` / `priority`, CDM `equipmentType`, `category`, `manufacturer`, `serialNumber` instead of custom duplicates |
| Custom types | Local `name` only — **no** `CogniteDescribable` |
| Location | Canonical on `Tag` in enterprise; search reaches it by navigating the asset relation (no copies) |
| Search `_SOL` | Schema-only data product: views + data model in `{{ search_space }}` mapping enterprise / CDM / IDM containers; **no** containers, transformations, instance writes, or search instance space |
| Reverse directs | On Search `_SOL` only — **not** on domain views |
| Event records | `AlarmEvent` record container + record view in the enterprise space, stream `{{ alarm_stream_external_id }}` (immutable); records stored in `{{ instance_space }}`; loaded by a CDF Function (Records cannot be onboarded through RAW + Transformations) |
| Access | Five persona-led groups (cog-vd `<persona>_industry_<type>_<env>` with type vocabulary `all`/`dom`/`sol`/`pp`/`wf`): one schema admin, DOM producer, SOL consumer, pipeline producer, pipeline operator (§11.1). Alarms via DOM producer; Search UI via SOL consumer |
| Instance spaces | One shared space `inst_location` (`{{ instance_space }}`) for every sample source — **no** `_SRC` schema layer, **no** search instance space (§11.3). Split or rename only when the model is installed and extended, based on derived-domain usage. Function code in separate `{{ function_space }}` |
| Tag labels | CogniteDescribable `tags` exposed as view property `labels` on `Tag` / `SearchTag` (§5.1.1) |
| Versions | `{{dm_enterprise_version}}` and `{{dm_search_version}}` — never shared; AlarmEvent record view uses enterprise version |
| Variables | Same snake_case variable names in all six modules (§3.2) |

### 2.1 Intentional divergences from full skill / SOP defaults (do not “correct”)

These are **locked pack decisions**. Agents and reviewers must **not** change them solely because a skill, SOP
checklist, or architecture diagram prefers a fuller pattern. Documented adaptations (§8.0 / §11.3 / §12.6) remain
valid for a *delivery project* that needs more — they are out of scope for this template’s OOTB ship.

| Locked decision | What skill / SOP / diagram may prefer | Why this pack keeps the simpler choice |
|-----------------|----------------------------------------|----------------------------------------|
| **No `_SRC` schema layer** | Cognite Docs / `cdf-data-modeling`: separate source vs enterprise models and instance spaces | Demo pack: RAW → `Industry_DOM` directly, into one instance space (`inst_location`). Do **not** reintroduce `_SRC`, and do **not** split that space per source system. Extra instance spaces are a delivery change when the derived domain needs them (§11.3). |
| **No per-source human admin groups** | Finer Access contracts per Source / Derived / Product, or per-extractor human admins | OOTB: one `admin_industry_all_*`, one `producer_industry_dom_*`, one `consumer_industry_sol_*` (§11.1). Write into CDF for extractors stays use-case / SP-specific (`ep_*`), not more human admin slices. |
| **No extraction ACLs on OOTB `pp` / `wf`** | Foundation `producer` / `cdf_ingestion` workflow groups often include extraction pipeline/config/run ACLs | This pack ships sample dumps + workflow only — no Extraction Pipelines. Prefer `producer.ep_*` when real extractors are added (§12.6). Do **not** put `extractionPipelinesAcl` / `extractionConfigsAcl` / `extractionRunsAcl` on `producer_industry_pp_*` or `producer_industry_wf_*` by default. |

---

## 3. Modules to create

| Module | Path | Role |
|--------|------|------|
| Enterprise | `modules/datamodels/industry_domain_extension` | Node containers + domain views (`implements` CDM/IDM), `AlarmEvent` record container + record view + stream, auth groups, data set, enterprise location filter |
| Search | `modules/datamodels/industry_domain_extension_search` | Schema only: Search views mapping enterprise/CDM/IDM containers, reverses, DataModel, search location filter |
| SAP sample data | `modules/sourcesystem/cdf_sap_industry_data_dump` | RAW db + tables, sample rows, SAP transformations into `Industry_DOM`, workflow `wf_sap_all_to_industry` |
| PI sample data | `modules/sourcesystem/cdf_pi_industry_data_dump` | RAW db + tables, sample rows, time series + operating limit transformations, alarm event record ingestion Function, workflow `wf_pi_all_to_industry` |
| SharePoint sample data | `modules/sourcesystem/cdf_sharepoint_industry_data_dump` | RAW db + tables, sample rows, document + diagram annotation transformations, workflow `wf_sharepoint_all_to_industry` |

Pipeline `pp` / `wf` auth groups live on the enterprise module. Each dump owns its load workflow (run SAP
before PI / SharePoint). Unlike CFIHOS (`common/cdf_ingestion`), there is no central industry ingestion module.

### 3.1 Target tree (data model modules)

```
industry_domain_extension/
  module.toml                    # id = dp:models:industry_domain_extension
  default.config.yaml
  README.md                      # incl. "How to extend" (§8.0), "Stable contract vs scaffolding", "Adapting this template"
  SPEC.md                        # full agent contract from this plan §8 (lead with §8.0)
  auth/
    admin.Group.yaml             # name {{ admin_group_name }} — schema WRITE on enterprise + search
    dom_producer.Group.yaml      # name {{ dom_producer_group_name }}
    ingestion.Group.yaml         # name {{ ingestion_group_name }} — pipeline pp
    operator.Group.yaml          # name {{ operator_group_name }} — pipeline wf
  data_sets/
    dataset.DataSet.yaml         # externalId {{ dataset_external_id }}
  locations/
    enterprise.LocationFilter.yaml   # externalId {{ enterprise_location_filter_external_id }}
  streams/
    alarm.Streams.yaml           # externalId {{ alarm_stream_external_id }}, template {{ alarm_stream_template }}
  data_modeling/
    enterprise.Space.yaml        # {{ enterprise_space }}
    instance.Space.yaml          # {{ instance_space }} — default inst_location
    enterprise.DataModel.yaml    # {{ enterprise_data_model_external_id }}
    containers/
      Tag.Container.yaml
      Equipment.Container.yaml
      Document.Container.yaml
      TimeSeriesData.Container.yaml
      WorkOrder.Container.yaml
      Notification.Container.yaml
      Material.Container.yaml
      OperatingLimit.Container.yaml
      AlarmEvent.Container.yaml      # usedFor: record — no constraints, no indexes
    views/
      Tag.View.yaml
      Equipment.View.yaml
      Document.View.yaml
      TimeSeriesData.View.yaml
      WorkOrder.View.yaml
      WorkOrderOperation.View.yaml   # no own container — IDM properties only
      Notification.View.yaml
      Material.View.yaml
      OperatingLimit.View.yaml
      AlarmEvent.View.yaml           # record view (streamId + version); not listed in the DataModel

industry_domain_extension_search/
  module.toml                    # id = dp:models:industry_domain_extension_search
  default.config.yaml
  README.md
  auth/
    sol_consumer.Group.yaml      # name {{ sol_consumer_group_name }}
  locations/
    search.LocationFilter.yaml   # externalId {{ search_location_filter_external_id }}
  data_modeling/
    search.Space.yaml            # {{ search_space }} — schema only (views + model); no instance space
    search.DataModel.yaml        # {{ search_data_model_external_id }}
    views/                       # no containers/ and no transformations/ in this module
      SearchTag.View.yaml
      SearchEquipment.View.yaml
      SearchDocument.View.yaml
      SearchTimeSeriesData.View.yaml
      SearchWorkOrder.View.yaml
      SearchWorkOrderOperation.View.yaml
      SearchNotification.View.yaml
      SearchMaterial.View.yaml
      SearchOperatingLimit.View.yaml
```

Resource files are named by **role**, not by identifier, so renaming a space, data model, data set or location filter
is a config-only change. Container and view files keep their PascalCase `externalId` (Toolkit convention).
Source-system and ingestion trees are in §12.1.

### 3.2 Shared variables (identical names in all six modules)

Every module declares only the variables it uses, but **the same concept always has the same variable name**. A
project overrides them once at the `variables.modules` level of `config.<env>.yaml`, and every module picks them up.

```yaml
# Data model identifiers
enterprise_space: dm_dom_industry
search_space: dm_sol_industry_search
instance_space: inst_location                 # all nodes, edges and records; extend per derived domain (§11.3)
function_space: sp_function_industry          # function code only — never an instance space (cdf_common pattern)
enterprise_data_model_external_id: Industry_DOM
enterprise_data_model_name: Industry DM DOM
search_data_model_external_id: IndustrySearch_SOL
search_data_model_name: Industry search DM SOL
dm_enterprise_version: v1
dm_search_version: v1

# Governance
dataset_external_id: ds_industry_all
dataset_name: Industry domain model
enterprise_location_filter_external_id: loc_industry_all
enterprise_location_filter_name: Industry all locations
search_location_filter_external_id: loc_industry_search_all
search_location_filter_name: Industry search all locations

# Access groups — persona-led, the only identifiers that carry the environment token (override per config.<env>.yaml)
# Type tokens for this template: all | dom | sol | pp | wf (§8.8 / §11.1 vocabulary)
admin_group_name: admin_industry_all_dev
dom_producer_group_name: producer_industry_dom_dev
sol_consumer_group_name: consumer_industry_sol_dev
ingestion_group_name: producer_industry_pp_dev
operator_group_name: producer_industry_wf_dev
admin_source_id: ${ADMIN_GROUP_SOURCE_ID}
dom_producer_source_id: ${DOM_PRODUCER_GROUP_SOURCE_ID}
sol_consumer_source_id: ${SOL_CONSUMER_GROUP_SOURCE_ID}
ingestion_source_id: ${INGESTION_GROUP_SOURCE_ID}
operator_source_id: ${OPERATOR_GROUP_SOURCE_ID}

# Records
alarm_stream_external_id: pi_all_alarm_event
# Sample/dev default only. Template is fixed at stream creation — set BasicArchive before any non-dev deploy (§5.11).
alarm_stream_template: ImmutableTestStream
alarm_function_external_id: fn_pi_all_ingest_alarm_event

# Sample data and processing
raw_database_sap: raw_sample_all_sap
raw_database_pi: raw_sample_all_pi
raw_database_sharepoint: raw_sample_all_sharepoint
# Per-dump workflows (each *_industry_data_dump module sets its own workflow_external_id)
# wf_sap_all_to_industry | wf_pi_all_to_industry | wf_sharepoint_all_to_industry
ingestion_client_id: ${IDP_CLIENT_ID}
ingestion_client_secret: ${IDP_CLIENT_SECRET}
run_workflow_schedule: "0 0 29 2 *"   # intentionally never; sample workflows are run manually
```


Naming checks applied (cog-vd `cdf-naming-conventions` + `cdf-data-model-limits`):

- Data models: PascalCase + layer suffix; names in sentence case (`Industry DM DOM`, `Industry search DM SOL`).
- DM spaces: `dm_{dom|sol}_{snake_case}`; instance space: `inst_{snake_case}` (template default `inst_location`); function space: `sp_function_*`
  (code upload only — separate from instances).
- Location filters: `loc_{location}` with `all` because the template is not scoped to one site.
- Data set: `ds_{data_type}_{location}`, shared by the model, transformations and workflow.
- Streams: snake_case `{source}_{location}_{concept}`; functions: `fn_{source}_{location}_{intent}_{target}`;
  record external IDs: `{source}_{location}_{machine}_{id}` (e.g. `pi_all_<tag>_<eventTime>_<eventType>`).
- Views and containers: PascalCase, **singular**, and not on the reserved list (`File`, `TimeSeries`, … are
  rejected by CDF — hence `Document` and `TimeSeriesData`).
- Display names: sentence case — spaces (`Industry DM DOM`, `Industry search DM SOL`, `Industry instance space`,
  `Industry function space`), views/containers (`Time series data`, `Work order operation`, `Equipment`),
  location filters, data set.
- No environment tokens (`dev` / `test` / `prod`) and no customer or site names in any identifier — except the
  required `<environment>` suffix on persona-led access group names (set per `config.<env>.yaml`).
- Access group type tokens: §8.8 — standard `all` / `pp` plus template-specific `dom` / `sol` / `wf` (docs Tip on
  alternative schemes; this PLAN is the governance record).
- Template targets **CDM `v1` and IDM `v1`**; CDM/IDM view versions are hard-coded `v1`.

### 3.3 module.toml

```toml
# enterprise
[module]
title = "Industry Domain Data Model template"
id = "dp:models:industry_domain_extension"
package_id = "dp:models"

# search
[module]
title = "Industry Domain Search Data Model template"
id = "dp:models:industry_domain_extension_search"
package_id = "dp:models"
```

Registry changes for all six modules are in §13.

---

## 4. Architecture

```mermaid
flowchart TB
  subgraph cdm [cdf_cdm and cdf_idm]
    CogniteAsset
    CogniteEquipment
    CogniteEquipmentType
    CogniteFile
    CogniteFileCategory
    CogniteTimeSeries
    CogniteMaintenanceOrder
    CogniteOperation
    CogniteNotification
  end

  subgraph dom [Industry_DOM]
    Tag
    Equipment
    Document
    TimeSeriesData
    WorkOrder
    WorkOrderOperation
    Notification
    Material
    OperatingLimit
  end

  subgraph rec ["Stream pi_all_alarm_event (records)"]
    AlarmEvent["AlarmEvent record view"]
  end

  subgraph sol [IndustrySearch_SOL]
    SearchTag
    SearchEquipment
    SearchDocument
    SearchTimeSeriesData
    SearchWorkOrder
    SearchWorkOrderOperation
    SearchNotification
    SearchMaterial
    SearchOperatingLimit
  end

  Tag -->|implements| CogniteAsset
  Equipment -->|implements| CogniteEquipment
  Document -->|implements| CogniteFile
  TimeSeriesData -->|implements| CogniteTimeSeries
  WorkOrder -->|implements| CogniteMaintenanceOrder
  WorkOrderOperation -->|implements| CogniteOperation
  Notification -->|implements| CogniteNotification
  Equipment -->|equipmentType| CogniteEquipmentType
  Document -->|category| CogniteFileCategory

  SearchTag -->|maps containers| Tag
  SearchWorkOrder -->|"maps IDM + WorkOrder containers"| WorkOrder

  AlarmEvent -->|asset| Tag
  AlarmEvent -->|timeSeries| TimeSeriesData
  AlarmEvent -->|operatingLimit| OperatingLimit
```

`AlarmEvent` lives in `{{ enterprise_space }}` but outside the `Industry_DOM` data model (record views can't be
members of a data model yet).

---

## 5. Enterprise content

### 5.1 CDM/IDM extensions — CONTRACT wiring + SOURCE_SCAFFOLD props

Mark every custom property description with a leading tag agents can grep:

- `CONTRACT:` — do not remove without understanding CDM/IDM / app impact
- `SOURCE_SCAFFOLD:` — safe to rename/replace/delete before first production deploy

| View | implements | Own container (requires) | SOURCE_SCAFFOLD properties | CDM/IDM properties used instead of custom ones |
|------|------------|--------------------------|----------------------------|-----------------------------------------------|
| **Tag** | `CogniteAsset` | `Tag` (`cdf_cdm:CogniteAsset`) | `site`, `area`, `system`, `tagClass` | `name`, `description`, `aliases`, `labels` (alias of CogniteDescribable `tags` — see §5.1.1), `parent`, `sourceId` |
| **Equipment** | `CogniteEquipment` | `Equipment` (`cdf_cdm:CogniteEquipment`) | `modelNumber` | `equipmentType` (→ CogniteEquipmentType: `code`, `equipmentClass`, `standard`, `standardReference`), `manufacturer`, `serialNumber`, `asset`, `files` |
| **Document** | `CogniteFile` | `Document` (`cdf_cdm:CogniteFile`) | `documentNumber`, `disciplineCode`, `revision`; source-native `site`, `area`, `system` | `category` (→ CogniteFileCategory for the document type), `mimeType`, `assets` |
| **TimeSeriesData** | `CogniteTimeSeries` | `TimeSeriesData` (`cdf_cdm:CogniteTimeSeries`) | `timeSeriesSource` enum (`Historian` / `OpcUa` / `Other`), `historianTagName`, `historianPointSource` | `type`, `isStep`, `sourceUnit`, `assets`, `equipment` |
| **WorkOrder** | `CogniteMaintenanceOrder` | `WorkOrder` (`cdf_idm:CogniteMaintenanceOrder`) | `workOrderNumber` | `status`, `type`, `priority`, `mainAsset`, `assets`, `scheduledStartTime`, `scheduledEndTime`, `startTime`, `endTime` |
| **WorkOrderOperation** | `CogniteOperation` | **none** — no custom properties | — | `status`, `phase`, `sequence`, `mainDiscipline`, `personHours`, `maintenanceOrder`, `mainAsset`, `assets` |
| **Notification** | `CogniteNotification` | `Notification` (`cdf_idm:CogniteNotification`) | `failureModeCode`, `detectionMethodCode` | `status`, `type`, `priority`, `priorityDescription`, `asset`, `maintenanceOrder`, `sourceId` |

Rules:

- Never add a custom property for a field CDM/IDM already has (§8.4); no container is created for a type with no
  custom properties (WorkOrderOperation).
- **Do not** re-require Describable / Sourceable / Schedulable / Activity when the parent CDM/IDM container already
  requires them.
- `workOrderNumber` is kept only because the source `order` number differs from the instance key; if the source
  order number equals `sourceId`, delete it.

#### 5.1.1 Property alias — `labels` ← CogniteDescribable `tags` (required on Tag)

Because the CogniteAsset view in this model is named `Tag`, the inherited Describable property `tags` (text-based
labels) collides with the view name and its direct relations (cog-vd *Property Aliasing — labels vs tags*; same
pattern as CFIHOS). **Both** the enterprise `Tag` view and `SearchTag` must expose it as `labels`:

```yaml
# Tag.View.yaml and SearchTag.View.yaml
labels:
  name: Labels
  description: "CONTRACT: Text-based labels (CogniteDescribable.tags). Aliased to labels because the view is named Tag."
  container:
    space: cdf_cdm
    externalId: CogniteDescribable
    type: container
  containerPropertyIdentifier: tags
```

- Transformations and queries that target `Tag` / `SearchTag` use the view property id `labels` (e.g. SQL
  `AS labels`), never `tags`.
- Other views that `implements` CDM/IDM may keep the inherited property name `tags` (no collision).
- Enterprise and Search READMEs include the same naming note as CFIHOS.

### 5.2 Typed relation overrides (domain views)

Every inherited CDM/IDM direct relation a domain view exposes gets `source` pointing at the **most specific** view in
`Industry_DOM` (cog-vd relation checklist item 9):

| View | Property | source view |
|------|----------|-------------|
| Tag | `parent`, `root` | Tag |
| Equipment | `asset` | Tag |
| Equipment | `files` | Document |
| Equipment | `equipmentType` | `cdf_cdm:CogniteEquipmentType` |
| Document | `assets` | Tag |
| Document | `category` | `cdf_cdm:CogniteFileCategory` |
| TimeSeriesData | `assets` | Tag |
| TimeSeriesData | `equipment` | Equipment |
| WorkOrder | `mainAsset`, `assets` | Tag |
| WorkOrder | `equipment` / `timeSeries` | Equipment / TimeSeriesData |
| WorkOrderOperation | `mainAsset`, `assets` | Tag |
| WorkOrderOperation | `maintenanceOrder` | WorkOrder |
| WorkOrderOperation | `equipment` / `timeSeries` | Equipment / TimeSeriesData |
| Notification | `asset` | Tag |
| Notification | `maintenanceOrder` | WorkOrder |
| Notification | `equipment` / `timeSeries` | Equipment / TimeSeriesData |

`path` stays a CDM-managed list; `TimeSeriesData.unit` stays sourced to `cdf_cdm:CogniteUnit`.
**No reverse directs** on these domain views.

### 5.3 CDM type nodes

`CogniteEquipmentType` and `CogniteFileCategory` are used as-is from `cdf_cdm` (no domain view, no container). Both
views are listed in `Industry_DOM` and `IndustrySearch_SOL` so transformations and apps can reach them. Type nodes
live in `{{ instance_space }}`; their external IDs are the source codes.

### 5.4 Custom types — no CogniteDescribable

| View / Container | Pattern | Properties |
|------------------|---------|------------|
| **Material** | Local `name` text; `usedFor: node`; **no** CDM requires | `name`, `materialType`, `equipment` → Equipment, `asset` → Tag |
| **OperatingLimit** | Local `name`; operating limits for a measured value | `name`, `timeSeries` → TimeSeriesData, `limitHigh`, `limitLow` (float64), `limitHighConsequence`, `limitLowConsequence`, `limitHighCorrectiveAction`, `limitLowCorrectiveAction` |

Domain views: **no** `implements`. Every navigated direct has view `source` + container btree index.

### 5.5 Explicitly excluded

- Industry-standard class hierarchies and coded property keys (CFIHOS, ISA-95/88, RDS, …)
- Functional-location class views, failure-mode / reliability models
- More record types (PI event frames, OPC UA events, batch logs) — add them with the §5.11 pattern
- Custom duplicates of CDM/IDM fields (§8.4)

### 5.6 Location (no denormalization)

| Layer | Rule |
|-------|------|
| Enterprise | Location context on **Tag** (`site`, `area`, `system`) + CogniteAsset hierarchy. Do **not** copy it onto other enterprise containers. |
| Enterprise – Document (exception) | The `Document` container (implements CogniteFile) may keep **source-native** `site` / `area` / `system` because the document register owns them (`SOURCE_SCAFFOLD`) — not copies of Tag. |
| Search `_SOL` | No copies. `SearchTag` maps the Tag location; other search views reach it through their typed asset direct (`asset` / `mainAsset` / `assets` → `SearchTag`). `SearchDocument` maps the source-native Document location. |

### 5.7 Indexes and descriptions (every container)

- Set `usedFor` explicitly on every container: `node` for the eight entity containers, `record` for `AlarmEvent`
  (§5.11)
- `usedFor: node`: include `constraints` and `indexes` keys; stay under 100 properties / 10 indexes per container
- `usedFor: record`: **no** `constraints` and **no** `indexes` keys; up to 1,000 properties; the rules below on
  indexes do not apply
- btree on every owned scalar direct that is filtered or traversed
- Before btree on `direct[]`, set `maxListSize` ≤ 300
- btree on lookup keys: `workOrderNumber`, `documentNumber`, `site`, `area`, `system` — with `maxTextSize` ≤ 2400
- Property descriptions: domain sentence + example value; every view property has a description
- Closed value sets → container `enum` (e.g. `timeSeriesSource`); no reserved enum values (`true`, `false`, `null`)

### 5.8 YAML pattern — CDM extension container

```yaml
space: "{{ enterprise_space }}"
externalId: Tag
name: Tag
description: Enterprise tag / functional location hub. Extends CogniteAsset.
usedFor: node
properties:
  site:
    type: { list: false, collation: ucs_basic, type: text, maxTextSize: 2400 }
    nullable: true
    autoIncrement: false
    immutable: false
    name: Site
    description: "SOURCE_SCAFFOLD: Site or plant code. Example: PLANT01."
  # ... area, system, tagClass ...
constraints:
  requiresCogniteAsset:
    constraintType: requires
    require:
      space: cdf_cdm
      externalId: CogniteAsset
      type: container
indexes:
  site:
    indexType: btree
    properties: [site]
    cursorable: false
```

### 5.9 YAML pattern — CDM extension view

```yaml
space: "{{ enterprise_space }}"
externalId: Tag
name: Tag
description: Enterprise tag view. CONTRACT: implements CogniteAsset; one asset view in this model.
implements:
  - space: cdf_cdm
    externalId: CogniteAsset
    version: v1
    type: view
version: "{{ dm_enterprise_version }}"
properties:
  site:
    name: Site
    description: "SOURCE_SCAFFOLD: Site or plant code. Example: PLANT01."
    container:
      space: "{{ enterprise_space }}"
      externalId: Tag
      type: container
    containerPropertyIdentifier: site
  parent:
    name: Parent
    description: "CONTRACT: Hierarchy parent; sourced to enterprise Tag."
    container:
      space: cdf_cdm
      externalId: CogniteAsset
      type: container
    containerPropertyIdentifier: parent
    source:
      space: "{{ enterprise_space }}"
      externalId: Tag
      version: "{{ dm_enterprise_version }}"
      type: view
```

### 5.10 YAML pattern — custom type without Describable

```yaml
# Material.Container.yaml — no requires CogniteDescribable
space: "{{ enterprise_space }}"
externalId: Material
name: Material
description: Spare part or bill-of-materials item linked to equipment or tag. Label-only; no CogniteDescribable.
usedFor: node
properties:
  name:
    type: { list: false, collation: ucs_basic, type: text }
    nullable: true
    name: Name
    description: "SOURCE_SCAFFOLD: Display label for the material. Example: Gasket DN50."
  equipment:
    type: { list: false, type: direct }
    nullable: true
    name: Equipment
    description: "CONTRACT: Direct relation to Equipment."
# No constraints key unless there is a real requires — omit empty constraints: {} (anti-pattern §10)
indexes:
  equipment:
    indexType: btree
    properties: [equipment]
    cursorable: true
```

```yaml
# Material.View.yaml — no implements
space: "{{ enterprise_space }}"
externalId: Material
name: Material
description: Material access view. No CogniteDescribable.
version: "{{ dm_enterprise_version }}"
properties:
  equipment:
    name: Equipment
    description: "CONTRACT: Equipment this material belongs to."
    container:
      space: "{{ enterprise_space }}"
      externalId: Material
      type: container
    containerPropertyIdentifier: equipment
    source:
      space: "{{ enterprise_space }}"
      externalId: Equipment
      version: "{{ dm_enterprise_version }}"
      type: view
```

### 5.11 Event records — `AlarmEvent` (`usedFor: record`)

Alarms are high-volume, write-once events, so they are **records in a stream**, not nodes (Cognite docs *About
records and streams*: storing each event as a node degrades graph performance and consumes instance budget). Each
state change (raised, acknowledged, cleared) is a new record — this fits an immutable stream.

| Resource | File | Rule |
|----------|------|------|
| Stream | `streams/alarm.Streams.yaml` | `externalId: {{ alarm_stream_external_id }}`; `settings.template.name: {{ alarm_stream_template }}`. Default in sample config is `ImmutableTestStream` (**dev / experimentation only**, short retention). The template **cannot be changed** after creation — set `BasicArchive` in `config.<env>.yaml` **before the first non-dev deploy** (and before any deploy that must retain sample alarms). Stream limits per project are small (1 `BasicLiveData`, 2 `BasicArchive`, 3 `ImmutableTestStream`). §9.2 **[FAIL]** if any non-dev config still uses `ImmutableTestStream` |
| Record container | `containers/AlarmEvent.Container.yaml` | `space: {{ enterprise_space }}`, `usedFor: record`; keys `space`, `externalId`, `name`, `description`, `properties`, `usedFor` only — **no** `constraints`, **no** `indexes`; no `immutable`, `autoIncrement`, `collation`, or `timeseries` / `file` / `sequence` types |
| Record view | `views/AlarmEvent.View.yaml` | `version: "{{ dm_enterprise_version }}"`; `streamId: ['{{ alarm_stream_external_id }}']` (Toolkit ≥ 0.9); maps only `AlarmEvent`; directs carry `source` to the enterprise views. **Not** listed in `Industry_DOM` — data models can't include record views yet. Cognite docs *Record views* require this view; do **not** delete it because an older cog-vd one-liner said "record has no view" (§9.2) |
| Records | written by `{{ alarm_function_external_id }}` (§12.5) | `space: {{ instance_space }}`; every direct (`asset`, `timeSeries`, `operatingLimit`) uses `{{ instance_space }}`; `externalId` `pi_all_<historianTagName>_<eventTime>_<eventType>` |

`AlarmEvent` properties (all `SOURCE_SCAFFOLD` except the directs):

| Property | Type | Notes |
|----------|------|-------|
| `eventTime` | timestamp, required | When the state change happened |
| `eventType` | enum `Raised` / `Acknowledged` / `Cleared` | One record per state change |
| `severity` | enum `Low` / `Medium` / `High` / `Critical` | |
| `limitType` | enum `High` / `Low` | Which operating limit was crossed |
| `value` | float64 | Measured value at `eventTime` |
| `message` | text | Alarm text from the source |
| `asset` | direct → Tag (`CONTRACT:`) | Main filter key for "alarms for asset X" |
| `timeSeries` | direct → TimeSeriesData (`CONTRACT:`) | Measured signal |
| `operatingLimit` | direct → OperatingLimit (`CONTRACT:`) | Limit that was crossed |

Rules:

- Records validate only that the target **space** exists, not the target node — the ingestion Function runs after
  the tag, time series and operating limit loads (§12.4).
- No reverse directs to records on domain views; graph ↔ records traversal is the documented two-step query
  (filter nodes, then filter records with `in` on the direct, max 100 ids per request).
- Immutable streams require a `lastUpdatedTime` range on every filter / aggregate (`maxFilteringInterval`, 365 days
  for `BasicArchive`); the README shows a sample query.
- **Deleting the container makes its records permanently inaccessible** — `AlarmEvent` is never removed or
  recreated after records are ingested (§8.7).

---

## 6. Search `_SOL` content

The search model is a **data product schema**: it owns no data and no storage. It reads the same nodes the
enterprise transformations write, through different views. One `Search*` view per enterprise type. Rules:

0. **Schema only** — the module contains a space, views, the data model, a location filter and the SOL consumer
   access group (§11.1). Schema admin for this space lives in the enterprise module (`admin_industry_all_*`).
   **No** containers, **no** transformations, **no** workflow tasks, **no** search instance space; every view
   property maps a container in `{{ enterprise_space }}`, `cdf_cdm` or `cdf_idm`, or is a reverse direct.
1. **No** `implements` — map minimum CDM/IDM container properties so apps classify the type (cog-vd *Minimum CDM/IDM
   Mapping Without `implements`*).
2. Map enterprise extension props via `container:` + `containerPropertyIdentifier:` using
   `space: "{{ enterprise_space }}"`.
3. Host reverse directs here, each paired with a `source`-typed direct on the target **search** view:
   - `SearchTag.workOrders` ← through `SearchWorkOrder.mainAsset`
   - `SearchTag.documents` ← through `SearchDocument.assets`
   - `SearchTag.timeSeries` ← through `SearchTimeSeriesData.assets`
   - `SearchTag.equipment` ← through `SearchEquipment.asset`
   - `SearchTag.notifications` ← through `SearchNotification.asset`
   - `SearchWorkOrder.operations` ← through `SearchWorkOrderOperation.maintenanceOrder`
   - `SearchEquipment.materials` ← through `SearchMaterial.equipment`
   - `SearchTimeSeriesData.operatingLimits` ← through `SearchOperatingLimit.timeSeries`
4. Show related context (work order number on operations, tag location on equipment / work items) by navigating
   the typed directs (`maintenanceOrder`, `asset`, `mainAsset`, `assets`) — never by copying values.
5. Version with `{{ dm_search_version }}` only.
6. **No search view for `AlarmEvent`** — a regular view can't map a `usedFor: record` container and data models
   can't include record views. Consumers read alarms through the enterprise `AlarmEvent` record view and the
   Records API.

### 6.1 Location in search

Search views can only show values stored on the node they read, and the search model stores nothing. Location
therefore comes from:

- `SearchTag` — maps `Tag.site` / `area` / `system` from the enterprise `Tag` container.
- `SearchDocument` — maps the source-native `Document.site` / `area` / `system`.
- Every other search view — the typed asset direct to `SearchTag`; apps and queries filter through the relation
  (nested filter / `SearchTag` reverse such as `workOrders`, `equipment`, `timeSeries`).

If a customer needs location as a flat filter on non-tag nodes, add it as a `SOURCE_SCAFFOLD` property on the
**enterprise** container and populate it in the enterprise transformation — never as a search container.

### 6.2 Minimum CDM/IDM maps (Search)

**Classification minimum** (cog-vd *Minimum CDM/IDM Mapping Without `implements`*) — required for app icons /
type recognition. **Search UX maps** — also required for this template so Search returns labeled, usable nodes
(not classified-but-blank rows). Map Describable from `cdf_cdm:CogniteDescribable`; do not invent custom copies.

| Search view | Classification (required) | Search UX (required for this template) |
|-------------|---------------------------|----------------------------------------|
| SearchTag | `CogniteAsset.type`, `parent` (with `source` → `SearchTag`) | `name`, `description`; `labels` ← Describable `tags` (§5.1.1); enterprise `site` / `area` / `system` |
| SearchTimeSeriesData | `CogniteTimeSeries.isStep`, `type` | `name`, `description`; `assets` → `SearchTag`; enterprise historian scaffolds as kept |
| SearchDocument | `CogniteFile.mimeType`, `isUploaded`, `uploadedTime`; `category` | `name`, `description`; `assets` → `SearchTag`; enterprise document scaffolds as kept |
| SearchEquipment | `CogniteEquipment.asset` → `SearchTag`; `equipmentType` with `source` → `cdf_cdm:CogniteEquipmentType` | `name`, `description` |
| SearchWorkOrder | IDM `mainAsset` → `SearchTag`; `status`, `type`, `priority` | `name`, `description` (from IDM/CDM Describable maps) |
| SearchNotification | IDM `asset` → `SearchTag`; `maintenanceOrder`, `status`, `priority` | `name`, `description` |
| SearchWorkOrderOperation | IDM `maintenanceOrder` → `SearchWorkOrder`; `status`, `sequence` | `name` when present on the IDM/CDM maps |
| SearchMaterial / SearchOperatingLimit | Local enterprise containers only (no Describable `implements`) | Local `name` (+ other enterprise props); no CDM Describable map |

Search README "do not copy" box: CFIHOS `_search` still `implements` CDM/IDM — that is legacy **[WARN]**; this
module maps containers only.

---

## 7. Data model files

- List **every** node `*.View.yaml` as a plain view reference: `space`, `externalId`, `version`, `type: view`.
- Do **not** list the `AlarmEvent` record view (data models can't include record views yet).
- Also list `cdf_cdm:CogniteEquipmentType` and `cdf_cdm:CogniteFileCategory` (`v1`) in both data models (§5.3).
- Do **not** list commented CDM interface views.
- Enterprise uses `{{ dm_enterprise_version }}`; search uses `{{ dm_search_version }}`.

---

## 8. Agent adaptation contract (`SPEC.md`, summarized in README)

The cog-vd `cdf-data-modeling` skill (W1a) tells agents to read the **README** first, so each data model README
includes *How to extend* (§8.0 condensed), *Stable contract vs scaffolding* (8.1 condensed) and *Adapting this
template* linking `SPEC.md` and the commands in §9. `SPEC.md` holds the full contract below. **Start at §8.0**
when wiring this template to a real project — everything else in §8 is reference for that procedure.

### 8.0 How to extend — replace demo with real sources

This template ships **demo** RAW tables, sample rows, scaffold property names and example values so the pack runs
end to end. Adopting it means swapping that demo surface for the project's real extractors and field names
**before the first production deploy** (after that, container properties cannot be removed — §8.7).

#### Markers agents must grep

| Marker | Where | Meaning |
|--------|-------|---------|
| `SOURCE_SCAFFOLD:` | Property `description` on containers and views | Demo / illustrative property — rename, replace or delete to match the source |
| `CONTRACT:` | Property / view descriptions | Keep; change only with model-owner approval |
| `raw_sample_all_*` / `*_industry_data_dump` | Config, RAW, transformations, workflow | Demo source modules — replace with project RAW / extractors |
| `Example:` in descriptions | Same properties as `SOURCE_SCAFFOLD` | Synthetic illustration only — not a real code list |

```bash
# Inventory every scaffold property (from a Toolkit / library checkout)
rg -n "SOURCE_SCAFFOLD:" modules/datamodels/industry_domain_extension modules/datamodels/industry_domain_extension_search
rg -n "raw_sample_all_|industry_data_dump" modules/sourcesystem
```

#### Ordered procedure (do not skip)

1. **Inventory the demo surface.** Run the greps above. List every `SOURCE_SCAFFOLD` property (§5.1 table) and every
   demo RAW table / transformation / workflow task (§8.9 touch map, §12).
2. **Set instance spaces from the derived domain.** The template ships one instance space, `{{ instance_space }}`
   (`inst_location`). All sample sources write into it. When the model is installed and extended, change that
   space — or add spaces — to match how the derived domain is used (site, plant, or access boundary). Do not
   split by source system. Update location filters, group scopes, transformation `instanceSpace`, and every
   `node_reference` so each direct points at the space that holds the target node. Keep the ids in config.
   Spaces are permanent; decide this before the first production deploy (§8.8, §11.3).
3. **Map real sources → template.** For each real source table or API entity, fill the worksheet (§8.2):
   source system → source field → target view → container property → action (`keep` / `replace` / `drop`).
   Confirm the table with the developer before editing YAML (cog-vd W1a).
4. **Trim unused types first.** If a demo entity has no real source and no consumer, remove it with §8.6 + the
   touch-map row (§8.9) — container, views, DataModel entries, Search reverse, transformation, workflow task, RAW
   table. Free now; hard later.
5. **Swap scaffold properties for source fields.** For each `replace` / `drop` row:
   - **drop** — delete the property from the enterprise container + view, the Search view map, indexes, and the
     SQL / Function that wrote it.
   - **replace (rename)** — use §8.10: same touch-map row; keep one camelCase id everywhere (container, view,
     search, SQL alias, `through.identifier`).
   - **replace (new meaning)** — prefer renaming the scaffold id to the source name when it is still pre-prod;
     do not leave a dead demo property beside the real one.
   - Never invent a custom property that duplicates CDM/IDM (§8.4); map to the CDM/IDM field instead.
6. **Point ingestion at real RAW.** Change transformation `source` databases/tables and column lists to the
   project extractors. Update `config.<env>.yaml` RAW database variables. Remove or stop deploying the
   `*_industry_data_dump` sample modules when real data is connected (§8.1 Sample data row).
7. **Extend only for gaps.** If the source has fields with no template home and no CDM/IDM equivalent:
   - Prefer a new property on the **existing adopter-owned** enterprise container (still pre-prod), tagged
     without `SOURCE_SCAFFOLD` once it is production-shaped.
   - After first prod deploy, add properties only on a **new extension container** mapped into the view
     (cog-vd W1a step 5 / W2) — never edit CDM/IDM containers.
   - New entity type → §8.5; new high-volume event type → §5.11 (records), not nodes.
8. **Align Search `_SOL`.** Update or remove Search views and reverses for every container you kept, renamed,
   trimmed or added. Search stays schema-only (no containers, no transforms). Point the search location filter
   at the same instance spaces as the enterprise module.
9. **Drop demo values from docs and examples.** Rewrite property descriptions: remove `SOURCE_SCAFFOLD:` and
   synthetic `Example: PLANT01`-style values; replace with **AI-friendly** domain wording (what the field means;
   Cognite Docs scalable-models checklist + naming skill) and a non-sensitive example from the source. Update
   README *Adapting this template* with the filled §8.2 worksheet.
10. **Validate.** Run §9 (naming, structure, relations, build, dry-run). Confirm instance / record counts against
    the source after the first non-demo deploy.

#### Done when

- [ ] `rg SOURCE_SCAFFOLD:` returns no hits in the adopted modules (or only properties still intentionally
      temporary, listed in the README); remaining custom properties have AI-friendly descriptions (§9.2)
- [ ] No transformation or Function still reads `raw_sample_all_*` in the target environment
- [ ] §8.2 worksheet is filled and checked into the project (README or `SPEC.md`)
- [ ] Instance spaces match the derived domain. The template default `inst_location` stays unless the domain
      needs more; no space is named after a source system
- [ ] §9 gate is green

#### Do not

- Rename or remove `CONTRACT` wiring (`implements`, typed `source`, CDM/IDM maps) without approval
- Leave demo RAW modules enabled beside production extractors writing the same nodes
- Add source-system or site names into permanent identifiers (view ids, property ids) — put those in
  config / instance data only (§8.8, §9.1)
- Split instance spaces by source system. Extra spaces come from derived-domain usage (§8.0 step 2)

### 8.1 Stable vs scaffolding

| Kind | Examples | Agent action |
|------|----------|--------------|
| CONTRACT | `implements`, `requires`, typed `source` on CDM/IDM directs, DataModel membership of core types, search module = schema only | Keep; change only with model-owner approval |
| SOURCE_SCAFFOLD | `site`, `documentNumber`, `historianTagName`, `workOrderNumber`, `failureModeCode`, … | Rename/replace/delete to match the source before first prod deploy |
| CUSTOM type | Material, OperatingLimit | Keep pattern (no Describable); replace props freely |
| RECORD type | `AlarmEvent` container + record view + stream | Keep `usedFor: record`, no constraints/indexes, `streamId`; rename/replace scaffold props **before** the first record is ingested |
| Sample data | `*_industry_data_dump` modules (each with its own workflow) | Replace with real extractors/RAW before production |

The enterprise containers are **adopter-owned scaffolding**, not an upstream contract: adapting them in place before
the first production deploy satisfies W1a step 5. After that, new properties go on a new extension container.

### 8.2 Mapping worksheet (fill per adoption)

| Source system | Source field | Target view | Container property | Action (keep / replace / drop) |
|---------------|--------------|-------------|--------------------|--------------------------------|
| | | | | |

Fill one row per source field during §8.0 step 3. §12.3 is a filled-in example for the **demo** sample data —
replace those rows with the real mapping; do not ship the demo worksheet as the project contract.

### 8.3 Replacement recipes

| Entity | Typical sources | What to edit |
|--------|-----------------|--------------|
| Tag | ERP functional locations, tag register, asset registry | `Tag` scaffold props; keep CogniteAsset hierarchy (`parent` written by ingest) |
| Equipment | ERP / EAM equipment master | `modelNumber`; map class/code to CogniteEquipmentType nodes; use CDM `manufacturer` / `serialNumber` |
| Document | Document management, SharePoint, engineering systems | Document scaffolds; document type → CogniteFileCategory nodes; keep CogniteFile maps |
| TimeSeriesData | Historian, OPC UA, SCADA, IoT platform | Replace/remove `historian*`; add source-family props (e.g. `opcUaNodeId`); keep CogniteTimeSeries maps; leave CDM `unit` for UoM |
| WorkOrder / Operation / Notification | ERP / EAM work management | Keep IDM implements; map status/type/priority/schedule to IDM properties; custom props only for what IDM lacks |
| Material / OperatingLimit | MRO catalog, operating envelopes | Custom props only; never add Describable |

### 8.4 Collision blacklist (views that `implements` CDM/IDM)

Never use these as **custom** property ids on a view that `implements` a CDM/IDM type (or a container that type
requires) — they already exist there. Label-only custom types without `implements` (Material, OperatingLimit) may
use `name`, `asset`, `equipment` and `timeSeries` as their own direct relations.

`name`, `description`, `tags`, `aliases`, `sourceId`, `source`, `sourceContext`, `sourceCreatedTime`,
`sourceUpdatedTime`, `sourceCreatedUser`, `sourceUpdatedUser`, `unit`, `sourceUnit`, `status`, `type`, `priority`,
`priorityDescription`, `phase`, `sequence`, `mainDiscipline`, `numberOfMainDiscipline`, `personHours`, `parent`,
`root`, `path`, `assets`, `asset`, `mainAsset`, `maintenanceOrder`, `equipment`, `equipmentType`, `manufacturer`,
`serialNumber`, `files`, `timeSeries`, `category`, `mimeType`, `directory`, `isUploaded`, `uploadedTime`, `isStep`,
`stateSet`, `startTime`, `endTime`, `scheduledStartTime`, `scheduledEndTime`, …

Also never use reserved container property names (`space`, `externalId`, `createdTime`, `lastUpdatedTime`,
`deletedTime`, `version`, …) or reserved view/container ids (`File`, `TimeSeries`, `String`, `Query`, …).

If the source has a conflicting concept, pick a distinct id (`locationUnit`, `siteStatus`, …).

### 8.5 Add custom type checklist

1. New `*.Container.yaml` with `usedFor: node`, description, properties, indexes; **no** CogniteDescribable require
   (prefer local `name`).
2. New `*.View.yaml` — no `implements` for label-only types; `source` on every direct.
3. Add view to `enterprise.DataModel.yaml`.
4. Matching `Search*.View.yaml` (maps the new enterprise container only) + `search.DataModel.yaml` entry + reverses.
5. New transformation + workflow task + ingestion variable (§8.9 row pattern).
6. Update README `consumers:` if already deployed.

**Add a record type** (events, logs) — follow §5.11 instead: `usedFor: record` container without constraints or
indexes, record view with `streamId` (not in a data model, no search view), reuse `{{ alarm_stream_external_id }}`
or add a stream only if retention/mutability differ (project stream limits are small), ingestion by extractor or
Function — never a transformation.

### 8.6 Trim checklist (W1a — before prod)

Use the touch map (§8.9) for the type being removed:

1. Remove the container, view and DataModel entry.
2. Remove the matching Search view, its DataModel entry and its reverses on other search views.
3. Remove the transformation, its workflow task and ingestion variable, and the RAW table + sample data.
4. Remove unused scaffold properties (containers are additive after deploy — trim **now**).

### 8.7 Permanence and versioning (cog-vd W2)

- **Container and property identifiers are permanent once deployed.** Rename them only before the first production
  deploy. After that, add a new property, migrate data, then retire the old one (expand-and-contract).
- Containers: unversioned; additive after deploy.
- **Record containers are stricter:** deleting `AlarmEvent` makes every record written through it permanently
  inaccessible, even if it is recreated with the same id. Never delete or recreate it once records exist; add
  properties only. The stream template is fixed at creation — choose `BasicArchive` before going to production.
- Every property **written** to a stream counts against the stream's max unique properties budget; don't create
  throw-away properties on record containers.
- Breaking view change → bump `dm_enterprise_version` or `dm_search_version` and every reference (`source`,
  `through`, `implements`, data model `views:`). Also bump the `AlarmEvent` record view `version` (it uses
  `{{ dm_enterprise_version }}` even though it is not listed in the DataModel) and any Function / client that
  references `AlarmEvent/{{ dm_enterprise_version }}`.
- Keep previous view versions through the agreed deprecation window (90 days default); mark deprecated views in
  their `description`.
- README `consumers:` lists apps and pinned versions from day one (stubs are fine for the template — Search UI,
  InField-shaped work views, sample workflow).

### 8.8 Naming

| Resource | Pattern | Example |
|----------|---------|---------|
| Data model | `PascalCase_DOM` / `_SOL` | `Industry_DOM` |
| DM space | `dm_dom_*` / `dm_sol_*` | `dm_dom_industry` |
| Instance space | `inst_{snake_case}` — one space in the template; add spaces only for derived-domain usage (site, plant, access boundary), not per source | `inst_location` |
| View / container | PascalCase, singular, not reserved | `TimeSeriesData`, `Document` |
| Property | camelCase | `documentNumber` |
| Location filter | `loc_{location}` | `loc_industry_all` |
| Data set | `ds_{data_type}_{location}` | `ds_industry_all` |
| RAW database | `raw_{data_type}_{location}_{source}` | `raw_sample_all_sap` |
| Transformation | `tr_{source}_{location}_to_{target}` | `tr_sap_all_to_industry_tag` |
| Stream | `{source}_{location}_{concept}` (snake_case) | `pi_all_alarm_event` |
| Record | `{source}_{location}_{machine}_{id}` (snake_case) | `pi_all_<tag>_<eventTime>_<eventType>` |
| Function | `fn_{source}_{location}_{intent}_{target}` | `fn_pi_all_ingest_alarm_event` |
| Workflow | `wf_{source}_{location}_to_{target}` | `wf_sap_all_to_industry`, `wf_pi_all_to_industry`, `wf_sharepoint_all_to_industry` |
| Workflow trigger | `trigger_` + workflow id (cog-vd workflow skill) | `trigger_wf_sap_all_to_industry`, … |
| Access group / service principal | persona-led `<persona>_[segment_]<type>_<environment>`; persona `admin` / `producer` / `consumer`; segment `industry`; **type** from the vocabulary below | `consumer_industry_sol_prod` |
| Function space | `sp_function_{snake_case}` — code upload only; never reuse `instance_space` | `sp_function_industry` |

**Access group type vocabulary (this template).** Cognite’s default Access Management type tokens are building-block
scopes such as `ep`, `pp`, and `all` (see Cognite Docs *Resource naming conventions* → Access management). The same
page allows **alternative schemes** (ACL-, data-domain-, or source-system-based) when they better match the IdP —
**provided the chosen scheme is documented in project governance**. This PLAN **is** that documentation for the
industry pack.

| Type token | Kind | Meaning | Used by |
|------------|------|---------|---------|
| `all` | Standard (cog-vd / docs) | Cross-layer schema admin (enterprise + search spaces) | `admin_industry_all_*` |
| `pp` | Standard (cog-vd / docs) | Processing pipeline (transformations + function runtime) | `producer_industry_pp_*` |
| `dom` | **Template-specific** | Enterprise / domain data scope (human producer) | `producer_industry_dom_*` |
| `sol` | **Template-specific** | Search / solution data product scope (consumer) | `consumer_industry_sol_*` |
| `wf` | **Template-specific** | Workflow operator (run / monitor only) | `producer_industry_wf_*` |

Do **not** invent further type tokens without updating this section. `cdf-naming-check` / `cog-vd-audit` must treat
these five tokens as approved for this pack — do not flag `dom` / `sol` / `wf` as unknown types. Delivery projects
that prefer only the default `ep` / `pp` / `all` list may map IdP groups differently; keep this vocabulary when
shipping the pack as documented.

**Spaces:** `{{ search_space }}` is schema-only (views + data model). There is **no** search instance space. Nodes, edges and records live in `{{ instance_space }}` (default `inst_location`). Directs use that same space. When the model is installed and extended, replace or add instance spaces to match the derived domain (§8.0 step 2); do not introduce one space per source system.

### 8.9 Touch map — every file per entity

| Entity | Enterprise | Search | Processing |
|--------|-----------|--------|------------|
| Tag | `Tag.Container`, `Tag.View`, DataModel | `SearchTag.View` (+ all reverses), DataModel | `tr_sap_all_to_industry_tag`, `raw_sample_all_sap.tag`, workflow task |
| Equipment | `Equipment.Container`, `Equipment.View`, DataModel | `SearchEquipment.View`, `SearchTag.equipment`, `SearchEquipment.materials`, DataModel | `tr_sap_all_to_industry_equipment_type`, `tr_sap_all_to_industry_equipment`, `tr_sharepoint_all_to_industry_equipment_document_connection`, `raw_sample_all_sap.equipment`, workflow tasks |
| Document | `Document.Container`, `Document.View`, DataModel | `SearchDocument.View`, `SearchTag.documents`, DataModel | `tr_sharepoint_all_to_industry_file_category`, `tr_sharepoint_all_to_industry_document`, `tr_sharepoint_all_to_industry_diagram_annotation`, `raw_sample_all_sharepoint.file` / `.diagram_annotation`, workflow tasks |
| TimeSeriesData | `TimeSeriesData.Container`, `TimeSeriesData.View`, DataModel | `SearchTimeSeriesData.View`, `SearchTag.timeSeries`, `SearchTimeSeriesData.operatingLimits`, DataModel | `tr_pi_all_to_industry_time_series`, `raw_sample_all_pi.timeseries`, workflow task |
| WorkOrder | `WorkOrder.Container`, `WorkOrder.View`, DataModel | `SearchWorkOrder.View`, `SearchTag.workOrders`, `SearchWorkOrder.operations`, DataModel | `tr_sap_all_to_industry_work_order`, `raw_sample_all_sap.work_order`, workflow task |
| WorkOrderOperation | `WorkOrderOperation.View`, DataModel | `SearchWorkOrderOperation.View`, DataModel | `tr_sap_all_to_industry_work_order_operation`, `raw_sample_all_sap.work_order_operation`, workflow task |
| Notification | `Notification.Container`, `Notification.View`, DataModel | `SearchNotification.View`, `SearchTag.notifications`, DataModel | `tr_sap_all_to_industry_notification`, `raw_sample_all_sap.notification`, workflow task |
| Material | `Material.Container`, `Material.View`, DataModel | `SearchMaterial.View`, `SearchEquipment.materials`, DataModel | `tr_sap_all_to_industry_material`, `raw_sample_all_sap.material`, workflow task |
| OperatingLimit | `OperatingLimit.Container`, `OperatingLimit.View`, DataModel | `SearchOperatingLimit.View`, `SearchTimeSeriesData.operatingLimits`, DataModel | `tr_pi_all_to_industry_operating_limit`, `raw_sample_all_pi.timeseries` + `raw_sample_all_sap.instrument_equipment`, workflow task |

| AlarmEvent (record) | `AlarmEvent.Container` (`usedFor: record`), `AlarmEvent.View` (record view), `alarm.Streams.yaml` — **no** DataModel entry | none | `fn_pi_all_ingest_alarm_event` (+ `pyproject.toml`, `requirements.txt`, `PACKAGE_SPECS` entry), `raw_sample_all_pi.alarm_event`, workflow task |

Each dump module owns its workflow; transformation and function ids are referenced from that module's WorkflowVersion.

### 8.10 Rename recipes

| Change | Where | How |
|--------|-------|-----|
| Spaces, data model ids/names, versions, data set, location filters, group names, RAW databases, workflow | `config.<env>.yaml` → `variables.modules` | Config only (§3.2) — no YAML edits |
| Rename a property | Touch-map row of the entity | Container property key + index, enterprise view property + `containerPropertyIdentifier`, search view `containerPropertyIdentifier`, transformation SQL alias, any `through.identifier` |
| Rename a type (e.g. `Tag` → `FunctionalLocation`) | Touch-map row + every `source:` / `through:` | Grep `externalId: Tag`, `destinationType: Tag`, `'Tag'` in SQL; rename container/view files; update DataModel lists; rename search view (`SearchTag` → `SearchFunctionalLocation`) |
| Remove a type | §8.6 | Follow the touch-map row |

Before the first production deploy all of the above are free; after it, follow §8.7.

---

## 9. Validation gate (cog-vd — must pass before merge)

Run each check and report findings as **[FAIL]** / **[WARN]** / **[INFO]** with `file:line`. This gate is the
template's equivalent of the NEAT validation notebook (cog-vd *Templates and blueprints*); the README lists it with
the commands `uv run cdf build`, `uv run cdf deploy --dry-run` and the `cog-vd-audit` skill.

### 9.1 Naming (`cdf-naming-check`)

- [ ] Every identifier matches §8.8; display names in sentence case
- [ ] No reserved view/container ids, property names, enum values or space ids
- [ ] No environment tokens, customer, site, or industry-specific names in identifiers (env suffix only on group
      names, always via variable)
- [ ] Group names match `<persona>_industry_<type>_<environment>` with persona `admin` / `producer` / `consumer`
      and type from the §8.8 vocabulary (`all`, `dom`, `sol`, `pp`, `wf`)
- [ ] No two identifiers differ only by capitalization; same concept uses the same property id everywhere

### 9.2 Structure (`cdf-data-modeling` W3 — structural checklist)

- [ ] `usedFor` set on every container; every `node` container has a view; every node view is in a data model
- [ ] `AlarmEvent`: `usedFor: record`, no `constraints` / `indexes` keys, no unsupported property settings
- [ ] `AlarmEvent` **record view is required** (Cognite docs *Record views*; Toolkit ≥ 0.9): `streamId` bound to
      `{{ alarm_stream_external_id }}`, `version: "{{ dm_enterprise_version }}"`, maps only record containers,
      directs have `source` to enterprise node views; record view **not** in any data model. **Exception to stale
      cog-vd Quick rule** ("`record` has no view") — that one-liner predates record views; do **not** strip the
      view during W3. Marketplace follow-up updates the skill (§13)
- [ ] Stream file named `*.Streams.yaml` under `streams/`; template from `{{ alarm_stream_template }}`; README warns
      that `ImmutableTestStream` is sample/dev only; **[FAIL]** if any non-dev `config.*.yaml` still uses
      `ImmutableTestStream` (§5.11)
- [ ] ≤ 100 properties and ≤ 10 indexes per container
- [ ] No EAV / key-value property bags
- [ ] Exactly one `CogniteAsset` view (`Tag`) in `Industry_DOM`; ingest writes `parent`
- [ ] `Tag` and `SearchTag` expose CogniteDescribable `tags` as view property `labels` (`containerPropertyIdentifier:
      tags`); no view property named `tags` on those two views (§5.1.1)
- [ ] No custom property duplicates a CDM/IDM field (§8.4); no container without custom properties
- [ ] No `CogniteDescribable` on Material / OperatingLimit
- [ ] `requires` only on the parent CDM/IDM container
- [ ] Search module has **no** `*.Container.yaml`, `*.Transformation.*` or workflow tasks; every search view property
      maps an `{{ enterprise_space }}` / `cdf_cdm` / `cdf_idm` container or is a reverse
- [ ] One instance space `{{ instance_space }}` (`inst_location`) documented in both READMEs; Search README states
      **no** search instance space — schema only. Extra spaces only for derived-domain usage (§11.3)
- [ ] **After** `SOURCE_SCAFFOLD:` is removed (adaptation §8.0 step 9 / gate): every remaining custom property has an
      AI-friendly description (human-readable, what the field means; Cognite Docs *Designing scalable data models*
      checklist + naming skill). Empty or placeholder-only descriptions are a fail once scaffolds are gone

### 9.3 Relations (`cdf-data-modeling` W3 — relation checklist)

- [ ] Every direct on a view has `source`, pointing at the most specific view (§5.2)
- [ ] Every owned direct on a `node` container has a btree index; `direct[]` has `maxListSize` ≤ 300 before
      indexing (record containers: no indexes)
- [ ] Every reverse's `through` points at a forward property on a **search** view whose `source` is the reverse's view
- [ ] No reverses on domain views

### 9.4 Layers (`cdf-data-modeling` W3 — layer checklist)

- [ ] `_DOM` views may `implements` CDM/IDM; `_SOL` views never `implements`
- [ ] `_SOL` maps the classification minimum **and** Search UX Describable maps per type (§6.2); `SearchTag.labels`
      aliases Describable `tags`; `SearchEquipment.equipmentType` has `source` → `CogniteEquipmentType`
- [ ] Separate version variables per data model; no hard-coded versions besides CDM/IDM `v1`
- [ ] README lists layer, `consumers:` stubs (Search UI, InField-shaped work views, sample workflow), CDM/IDM
      versions, and the stable-contract section; Search README has the "do not copy CFIHOS `implements`" box

### 9.5 Access (least privilege)

- [ ] Five groups exist (§11.1): admin, DOM producer, SOL consumer, pipeline producer, pipeline operator; each in
      the module that owns its scope; `sourceId` from env vars
- [ ] No group has WRITE on `cdf_cdm`, `cdf_idm` or `cdf_cdm_units`
- [ ] Only `{{ ingestion_group_name }}` and `{{ dom_producer_group_name }}` write instances or records; only
      `{{ admin_group_name }}` writes schema (enterprise + search)
- [ ] `{{ sol_consumer_group_name }}` has READ only — no RAW, records, transformation or workflow access; READMEs
      document alarms via DOM producer vs Search via SOL consumer (§11.1)
- [ ] Dev check with a `{{ sol_consumer_group_name }}` member: every `Search*` view returns data and a write is
      rejected; same for `{{ dom_producer_group_name }}` on `Industry_DOM` and an `AlarmEvent` records filter
- [ ] Workflow run succeeds with **only** `{{ ingestion_group_name }}` on the client (no extra admin rights)

### 9.6 Build and registry

- [ ] `uv run cdf build` with all six modules — fix "not optimized for querying" warnings only with semantically
      valid `requires` / indexes
- [ ] All six modules registered in `modules/packages.toml`; `python validate_packages.py` passes
- [ ] Each module has a `README.md` in the CONTRIBUTING.md standard structure

### 9.7 Transformations (`cdf-transformation`)

- [ ] `externalId` = file stem; source-to-target naming (§8.8)
- [ ] Destination is always an enterprise view (or CDM type view) with `{{ dm_enterprise_version }}` — never a
      search view; `dataSetExternalId: {{ dataset_external_id }}`; `conflictMode: upsert`
- [ ] Each dump’s `destination.instanceSpace` is `{{ instance_space }}`, including the equipment↔document overlay
      (§12.3)
- [ ] `ignoreNullFields: false` except overlay transformations that share nodes (§12.3 marks them)
- [ ] Each transformation writes only containers it owns; no two transformations write the same property
- [ ] Every direct built with `node_reference('{{ instance_space }}', …)`, including cross-source asset/Tag directs
      and diagram edge ends; null/empty keys yield `NULL`. After a delivery splits spaces, each direct points at
      the space that holds the target node (§11.3)
- [ ] Tag transform writes Describable labels as view property `labels` (SQL `AS labels`), never `AS tags` (§5.1.1)
- [ ] Document transform populates CogniteFile `mimeType`, `isUploaded`, and `uploadedTime` (file syncer + §6.2)
- [ ] JOINs only for real lookups (asset → equipment, annotation → tag), never just to resolve a direct relation
- [ ] DM reads (`cdf_nodes`), if any, alias-qualified with `alias.space` equal to the space being read
      (`{{ instance_space }}` in the template) and
      `` alias.`node.deletedTime` = 0 ``
- [ ] SQL style: uppercase keywords, lowercase CDF functions, leading commas, CTEs, explicit columns, no `SELECT *`
- [ ] Full load + `upsert` (no `is_new()`) — documented in the README as a sample-data choice
- [ ] `authentication` from environment variables; no hard-coded spaces, versions, databases or data set ids

### 9.8 Workflow (`cdf-workflow`)

- [ ] `.Workflow.yaml` sets `dataSetExternalId: {{ dataset_external_id }}`; version `v1`
- [ ] `dependsOn` only for real data dependencies (§12.4); each transformation invoked by exactly one task
- [ ] `onFailure: abortWorkflow` by default; alarm Function task uses `abortWorkflow` (§12.4); `skipTask` only for
      optional enrichment (diagram annotation, equipment–document overlay)
- [ ] Every task: `concurrencyPolicy: fail`, `retries: 3`, `timeout: 3600`
- [ ] Trigger credentials from environment variables; never-firing schedule documented as intentional
- [ ] Dev deploy + manual workflow run: instance counts per view match the RAW row counts in §12.2
- [ ] Second workflow run adds **no** duplicate `AlarmEvent` records (immutable streams don't enforce uniqueness)

### 9.9 Records Function (`cdf-function`)

- [ ] `externalId` `{{ alarm_function_external_id }}` = folder name `fn_pi_all_ingest_alarm_event/`;
      `runtime: py311` (CDF Functions runtime — not the repo's 3.13 tooling target); `pyproject.toml` +
      `requirements.txt` (direct deploy deps only); registered in `scripts/generate_uv_member_projects.py`
      `PACKAGE_SPECS`; `uv lock` + `python scripts/export_deploy_requirements.py` run
- [ ] Function YAML `space: {{ function_space }}` (dedicated code space — **never** `inst_location` or any other
      instance space); `dataSetExternalId: {{ dataset_external_id }}`; no secrets in YAML (client from runtime /
      workflow auth)
- [ ] Handler `data` contract documented (§12.5): stream id, `instance_space` (records and every direct),
      enterprise space, RAW db/table, optional `lastUpdatedTime` window — no hard-coded identifiers
- [ ] Idempotent: before ingesting, filters existing records by `externalId` (with the required `lastUpdatedTime`
      range) and ingests only missing ones
- [ ] Unit tests in the function folder for row → record mapping and the duplicate filter (`uv run pytest`); when
      adapting the function, also scaffold / keep `run_local()` (+ `.env`) and iterate locally before deploy
      (`cdf-function` skill — §12.5)
- [ ] Typed (pydantic models for rows/records), specific exceptions, logging with context (repo style guide)

---

## 10. Anti-patterns (do not ship)

- Empty or duplicate YAML keys (e.g. repeated `container:`, empty `constraints: {}` with no `requires`)
- View direct without `source`, or `source` pointing at a CDM ancestor when a domain view exists
- Custom property duplicating a CDM/IDM field (`statusCodes` beside IDM `status`, `techId` beside `equipmentType.code`)
- View property named `unit` for a plant/location code (use `locationUnit`; leave CDM `unit` for UoM)
- View property named `tags` on `Tag` or `SearchTag` (must be aliased to `labels` — §5.1.1); SQL `AS tags` into those views
- Reserved identifiers (`File`, `TimeSeries`, …) as view/container ids
- `implements CogniteDescribable` on Material / OperatingLimit / other label-only types
- Search views that `implements` CDM/IDM (use mapping only) — do **not** copy CFIHOS `_search` `implements`
- Containers, transformations or instance writes in the search module (it is a schema-only data product)
- Search views mapping enterprise properties that are not stored on the node they read (navigate the direct instead)
- Location context copied onto every enterprise container
- Two transformations writing the same property on the same node
- Alarms / events modeled as nodes, or records loaded through RAW + Transformations (not supported)
- Stripping the `AlarmEvent` record view because an outdated skill said "record has no view" (§9.2)
- `constraints` / `indexes` on a `usedFor: record` container; record view listed in a data model
- Function `space:` set to `inst_location` or any other instance space (use `{{ function_space }}`)
- One instance space per source system in the template — use `{{ instance_space }}` (`inst_location`); extra
  spaces only when the derived domain needs them (§11.3)
- Reintroducing an OOTB `_SRC` schema “to match the diagram / skill” without a delivery need (§2.1 / §11.3)
- Per-source (or per-layer) human **admin** groups beyond `admin_industry_all_*` (§2.1 / §11.1)
- Extraction pipeline/config/run ACLs on OOTB `pp` / `wf` groups (§2.1 / §12.6)
- A `node_reference` or transformation `instanceSpace` that does not match the space the target node is written
  to (in the template, always `{{ instance_space }}`)
- Search module shipping an instance space or writing instances
- Security categories (`sc_*`) used to control Tag / Search / other DM instance access (they do not apply to
  instances linked via `instanceId` — use space-scoped ACLs; §11.1)
- Shipping production-shaped models still tagged `SOURCE_SCAFFOLD:` with no AI-friendly descriptions (§9.2)
- `ImmutableTestStream` in any non-dev `config.*.yaml`, or used for production records; deleting / recreating a
  record container that holds records
- Re-running an immutable-stream ingest without a duplicate check
- Commented CDM views inside the DataModel file
- Shared `{{dm_version}}` across enterprise and search
- Hard-coded instance space ids in sample CSVs or SQL (use variables)
- Industry-standard class trees or coded properties baked into the generic template

---

## 11. Auth groups, data set and spaces (enterprise module)

### 11.1 Access model — one group per persona, owned by the module that owns the scope

Persona-led names (Cognite Docs / cog-vd *Access Management*): `<persona>_industry_<type>_<environment>`. The
environment token is allowed **only** in group and service principal names; every name is a variable (§3.2) so
`config.<env>.yaml` sets `_dev` / `_test` / `_prod`. Group `sourceId`s come from env vars; never hard-code IdP ids.

**Type tokens:** §8.8 — standard `all` / `pp` plus template-specific `dom` / `sol` / `wf`, documented here as the
governance alternative allowed by Cognite Docs (*Resource naming conventions* → Access management Tip). `all` is
used for the single cross-layer schema admin.

**OOTB human groups are intentionally simple:** one schema admin (both spaces), a DOM producer (instance/record
write), and a SOL consumer (Search UI read). Pipeline `pp` / `wf` remain service identities. Do **not** ship
separate `dom`/`sol` admin groups, a DOM consumer, or **per-source human admin** groups — locked in §2.1 even when
a diagram or SOP shows finer Access boxes.

**Additive to foundation / SOP — not a replacement.** The Foundation deployment pack / SOP access-management
checklist ships project-wide personas `admin_all_*`, `producer_all_*`, and `consumer_all_*` (Cognite Docs
*Foundation* → Access groups deployed; *Configure access management*). Industry groups
(`admin_industry_all_*`, `producer_industry_dom_*`, `consumer_industry_sol_*`, `producer_industry_pp_*`,
`producer_industry_wf_*`) are **scoped add-ons for this pack**. Delivery IdP mapping normally keeps **both**
foundation personas (org-wide / project default) and these industry groups (least-privilege for Industry DOM /
Search / ingestion). Do not delete foundation groups when installing this pack, and do not treat
`admin_industry_all_*` as a substitute for `admin_all_*` (the latter typically includes `groups:write` and broader
project admin).

**Alarms vs Search:** `sol_consumer` has no `streamRecordsAcl` (alarms are not in the search model). Alarm access is
via `{{ dom_producer_group_name }}` (or admin); Search UI via `{{ sol_consumer_group_name }}`. Map both IdP groups
only when a user needs both products. Enterprise README shows a sample two-step graph→records query (filter nodes,
then records `in` on the direct, chunk ≤ 100 ids per request — Cognite docs *Traversing between records and the
graph*; `cdf-dms-queries` pagination).

**Dual ACL for CDM time series / files (locked pattern):** Cognite Docs (*Designing scalable data models* →
Security and governance) require **both** (1) `dataModelsAcl` READ on `cdf_cdm` (schema — what a
`CogniteTimeSeries` / `CogniteFile` is) **and** (2) `dataModelInstancesAcl` READ on the **instance space(s)** that
hold the nodes (and thus the linked datapoints / file bytes). Do not ship Search/app consumers with only one of
these. This pack already grants both on `admin`, `dom_producer`, and `sol_consumer` (instance READ on
`{{ instance_space }}` + `cdf_cdm` / `cdf_cdm_units` as needed). Adapters that add groups must keep the pair,
including every instance space added when the domain is extended.

**Security categories (`sc_*`) — do not use for Tag / Search DM access.** Cognite Docs (*Resource naming
conventions* → Access management) note that security categories apply only to classic **time series and files**,
not to data-modeling instances linked via `instanceId`. Fine-grained Tag / Search access is via **space-scoped**
`dataModelInstancesAcl` (and schema `dataModelsAcl`) — not `sc_producer_*` / `sc_consumer_*` categories. Do not
add security categories expecting them to gate DM nodes or Search views.

```mermaid
flowchart LR
  deployer["CI/CD deployer (cdf_project_foundation)"] -->|deploys| schema
  admin[admin_industry_all] -->|"schema WRITE"| dom[enterprise_space]
  admin -->|"schema WRITE"| sol[search_space]
  ingestion[producer_industry_pp] -->|"RAW READ"| raw[raw_sample_all_*]
  ingestion -->|"WRITE + records"| inst[instance_space]
  domProducer[producer_industry_dom] -->|WRITE| inst
  domProducer -->|READ| dom
  operator[producer_industry_wf] -->|"run / monitor"| wf[wf_*_all_to_industry]
  solConsumer[consumer_industry_sol] -->|READ| sol
  solConsumer -->|"READ (mapped containers)"| dom
  solConsumer -->|READ| inst
  schema[all schema resources]
```

| Group (module) | Persona | Capabilities | Never |
|----------------|---------|--------------|-------|
| `admin.Group.yaml` → `{{ admin_group_name }}` (enterprise) | Schema admin (enterprise + search) | `dataModelsAcl` READ/WRITE on `{{ enterprise_space }}` and `{{ search_space }}`, READ on `cdf_cdm`, `cdf_idm`, `cdf_cdm_units`; `dataModelInstancesAcl` READ on `{{ instance_space }}`; `datasetsAcl` OWNER/READ/WRITE on `{{ dataset_external_id }}`; `streamsAcl` READ/CREATE/DELETE; `streamRecordsAcl` READ on `{{ instance_space }}`; `locationFiltersAcl` READ/WRITE | WRITE on any `cdf_*` space; instance or record WRITE |
| `dom_producer.Group.yaml` → `{{ dom_producer_group_name }}` (enterprise) | Domain producer (interactive / domain data work on `Industry_DOM`) | `dataModelsAcl` READ on `{{ enterprise_space }}`, `cdf_cdm`, `cdf_idm`, `cdf_cdm_units`; `dataModelInstancesAcl` READ/WRITE/WRITE_PROPERTIES on `{{ instance_space }}`, READ on `cdf_cdm_units`; `streamsAcl` READ; `streamRecordsAcl` READ/WRITE on `{{ instance_space }}` (alarms); `locationFiltersAcl` READ | Schema WRITE; search-space WRITE; RAW |
| `sol_consumer.Group.yaml` → `{{ sol_consumer_group_name }}` (search) | Search data product consumer (Search UI, apps) | `dataModelsAcl` READ on `{{ search_space }}` **and** `{{ enterprise_space }}`, `cdf_cdm`, `cdf_idm`, `cdf_cdm_units` (search views map containers there); `dataModelInstancesAcl` READ on `{{ instance_space }}`, `cdf_cdm_units`; `locationFiltersAcl` READ | Any WRITE; RAW; records (alarms via DOM producer); transformations / workflows |
| `ingestion.Group.yaml` → `{{ ingestion_group_name }}` (enterprise) | Pipeline runtime — the service principal `{{ ingestion_client_id }}` used by dump transformations, workflow triggers and the function task | see §12.6 | `dataModelsAcl` WRITE; search space; `datasetsAcl` OWNER |
| `operator.Group.yaml` → `{{ operator_group_name }}` (enterprise) | Pipeline operator (runs / monitors dump workflows) | `workflowOrchestrationAcl` READ/WRITE, `transformationsAcl` READ, on `{{ dataset_external_id }}`; `functionsAcl` READ; `rawAcl` READ on the three databases; `sessionsAcl` CREATE; `projectsAcl` READ/LIST; `groupsAcl` LIST | Instance / record WRITE; schema WRITE |

Scope rules: data models, instances and records are scoped by **space** (`spaceIdScope`); transformations and
workflows by **data set** (`datasetScope`); RAW by **database** (`tableScope.dbsToTables`); functions, streams,
projects, groups and sessions are `all`. Deployment of every resource (spaces, containers, views, stream, function
code, data set, groups) is done by the CI/CD deployer identity from `common/cdf_project_foundation` — not by these
groups; the README states this dependency.

### 11.2 Data set

`dataset.DataSet.yaml`: `externalId: {{ dataset_external_id }}`, `name: {{ dataset_name }}`; labels `INDUSTRY`,
`DOMAIN MODEL`; no industry-standard labels. Used by the data model, all transformations and the workflow.

### 11.3 Spaces — one instance space (no Source schema layer)

| File | Identifier | Name | Role |
|------|------------|------|------|
| `enterprise.Space.yaml` | `{{ enterprise_space }}` | Industry DM DOM | Enterprise **schema** (containers, `_DOM` views, data model) |
| `instance.Space.yaml` | `{{ instance_space }}` | Industry instance space | All nodes, edges and alarm records. Default `inst_location` |
| `search.Space.yaml` (search module) | `{{ search_space }}` | Industry search DM SOL | Search **schema only** (views + data model) — **not** an instance space |
| `function.Space.yaml` (PI data dump) | `{{ function_space }}` | Industry function space | Function code upload only |

#### Instance strategy

Cognite Docs (*Designing scalable data models* → Source and enterprise layer instance strategy) recommend
**separate instance spaces for the source and enterprise layers**, each owned by the domain that writes them, so
deleting a source instance cannot wipe the enterprise copy. Schema spaces stay separate from instance spaces;
solution (`_SOL`) models generally **map** enterprise (and CDM/IDM) containers and do not own instances.

**This template keeps a deliberate hybrid** — locked in §2.1. Do **not** reintroduce a `_SRC` schema layer only to
match Cognite’s default diagram or a Source→Derived→Product sketch. Do **not** split the instance space by sample
source. A delivery changes instance spaces when the model is installed and extended (§8.0 step 2).

| Cognite default | This pack (locked) |
|-----------------|--------------------|
| `_SRC` schema + source-owned instance space(s) | **No `_SRC` schema** — sample RAW → transformations write straight into `Industry_DOM` containers |
| Separate enterprise-owned instance space | **One** enterprise instance space (`inst_location`) shared by every sample source |
| `_SOL` maps enterprise instances | Same — Search is schema-only; **no** search instance space |
| Per-site instance spaces for site AC | **Adaptation** — when the model is installed and extended, change instance spaces to match derived-domain usage (site, plant, access boundary). Do not split by source system |

**Why one space:** the pack is a demo / starting template without source data models. One instance space keeps
every sample node, edge and record addressable without baking source-system names into permanent space ids.
How many instance spaces a project needs follows how the derived domain is used, and that decision belongs to
the install — not to the template.

**Rules that follow:**

1. **No search instance space.** Search views map enterprise / CDM / IDM containers; once loads finish, data is
   visible through `IndustrySearch_SOL` with no search-side instance writes.
2. **One write space in the template.** Every dump’s `instanceSpace` and every `node_reference` (including
   cross-source Tag/asset directs and diagram edge ends) uses `{{ instance_space }}`.
3. **Extend spaces from the derived domain, not the source.** On install, keep `inst_location` or replace it.
   Add further instance spaces only when the derived domain needs them (a plant, a site, an access boundary).
   Then update location filters, group scopes, transformation `instanceSpace`, and each `node_reference` so the
   direct points at the space that holds the target node. Spaces are permanent — decide before the first
   production deploy (§8.0).
4. **Adding a real `_SRC` layer later** is a project adaptation: introduce `_SRC` models and source-owned
   instance spaces, and do not treat `inst_location` as if it were already a source space. Re-plan write targets
   so source and enterprise instances stay separable.

### 11.4 Location filters

| Module | File | Data model | Instance spaces |
|--------|------|------------|-----------------|
| Enterprise | `enterprise.LocationFilter.yaml` | `{{ enterprise_data_model_external_id }}` @ `{{ dm_enterprise_version }}` | `{{ instance_space }}` |
| Search | `search.LocationFilter.yaml` | `{{ search_data_model_external_id }}` @ `{{ dm_search_version }}` | `{{ instance_space }}` |

Both use `dataModelingType: DATA_MODELING_ONLY` (Toolkit resource library, *Locations*), mirroring the CFIHOS and
ISA search modules.

---

## 12. Data and processing (same setup as CFIHOS)

The industry model reuses the **same synthetic sample data** that the CFIHOS quickstart ships in
`cdf_sap_data_dump`, `cdf_pi_data_dump` and `cdf_sharepoint_data_dump`. The industry modules copy only the tables the
generic model needs into one RAW database per source module, so the two setups can be deployed side by side without
sharing or overwriting transformations.

| RAW database | Owned by | Tables |
|--------------|----------|--------|
| `raw_sample_all_sap` | `cdf_sap_industry_data_dump` | `tag`, `equipment`, `instrument_equipment`, `material`, `work_order`, `work_order_operation`, `notification` |
| `raw_sample_all_pi` | `cdf_pi_industry_data_dump` | `timeseries`, `alarm_event` (read by the records Function, not by a transformation) |
| `raw_sample_all_sharepoint` | `cdf_sharepoint_industry_data_dump` | `file`, `diagram_annotation` |

Cross-source joins (time series → equipment, operating limit → instrument + time series, equipment → document)
read across databases, so the PI and SharePoint modules depend on the SAP module being deployed first.

```mermaid
flowchart LR
  subgraph sapRaw [raw_sample_all_sap]
    tagRaw[tag]
    equipmentRaw[equipment]
    instrumentRaw[instrument_equipment]
    materialRaw[material]
    workOrderRaw[work_order]
    operationRaw[work_order_operation]
    notificationRaw[notification]
  end
  subgraph piRaw [raw_sample_all_pi]
    timeseriesRaw[timeseries]
    alarmRaw[alarm_event]
  end
  subgraph spRaw [raw_sample_all_sharepoint]
    fileRaw[file]
    annotationRaw[diagram_annotation]
  end
  subgraph sapWf [wf_sap_all_to_industry]
    sapLoads["SAP base loads"]
  end
  subgraph piWf [wf_pi_all_to_industry]
    piLoads["PI time series + limits"]
    alarmFn["fn_pi_all_ingest_alarm_event"]
  end
  subgraph spWf [wf_sharepoint_all_to_industry]
    spLoads["SharePoint documents"]
    annotations["diagram annotations"]
    overlays["equipment document links"]
  end
  sapRaw --> sapLoads
  timeseriesRaw --> piLoads
  spRaw --> spLoads
  alarmRaw --> alarmFn
  sapLoads -->|"run first (manual)"| piLoads
  sapLoads -->|"run first (manual)"| spLoads
  piLoads --> alarmFn
  spLoads --> annotations --> overlays
  sapLoads --> dom[Industry_DOM]
  piLoads --> dom
  overlays --> dom
  alarmFn --> stream["Stream pi_all_alarm_event"]
  dom -.->|"same containers, read through views"| sol[IndustrySearch_SOL]
```


### 12.1 Module trees

```
sourcesystem/cdf_sap_industry_data_dump/
  module.toml                  # id = dp:sourcesystem:cdf_sap_industry_data_dump
  default.config.yaml
  README.md
  raw/
    sap.Database.yaml          # {{ raw_database_sap }}
    tag.Table.yaml  equipment.Table.yaml  instrument_equipment.Table.yaml  material.Table.yaml
    work_order.Table.yaml  work_order_operation.Table.yaml  notification.Table.yaml
  upload_data/                 # <table>.Manifest.yaml + <table>.RawRows.csv per table
  transformations/             # tr_*.Transformation.yaml + .sql pairs (§12.3)
  workflows/                   # wf_sap_all_to_industry (+ Version, Trigger)

sourcesystem/cdf_pi_industry_data_dump/
  module.toml  default.config.yaml  README.md
  raw/         pi.Database.yaml  timeseries.Table.yaml  alarm_event.Table.yaml
  upload_data/ timeseries + alarm_event manifests/CSVs
  transformations/
  data_modeling/
    function.Space.yaml                  # {{ function_space }} — code upload only
  functions/
    ingest_alarm_event.Function.yaml     # externalId {{ alarm_function_external_id }}; space {{ function_space }}; runtime py311
    fn_pi_all_ingest_alarm_event/        # folder name MUST equal externalId
      handler.py  pyproject.toml  requirements.txt  test_handler.py
  workflows/                   # wf_pi_all_to_industry (+ Version, Trigger)

sourcesystem/cdf_sharepoint_industry_data_dump/
  module.toml  default.config.yaml  README.md
  raw/         sharepoint.Database.yaml  file.Table.yaml  diagram_annotation.Table.yaml
  upload_data/ file + diagram_annotation manifests/CSVs, file content manifest
  transformations/
  workflows/                   # wf_sharepoint_all_to_industry (+ Version, Trigger)

# Pipeline pp/wf auth lives on the enterprise module (not a separate ingestion module):
# industry_domain_extension/auth/ingestion.Group.yaml + operator.Group.yaml
```

No separate ingestion data set: each dump workflow and every transformation use `{{ dataset_external_id }}`, so the
pipeline group's transformation scope matches the transformations it runs.

### 12.2 Sample data (copied from the CFIHOS data dumps)

| RAW table | Copied from | Rows | Notes |
|-----------|-------------|------|-------|
| `tag` | `cdf_sap_data_dump/upload_data/tag.RawRows.csv` | 1154 | As-is |
| `equipment` | `cdf_sap_data_dump/upload_data/equipment.RawRows.csv` | 10 | Also source of CogniteEquipmentType nodes (`code`, `equipmentClass`, `standard`, `standardReference`) |
| `instrument_equipment` | `cdf_sap_data_dump/upload_data/instrument_equipment.RawRows.csv` | 5 with set points | Source of OperatingLimit |
| `work_order` | `cdf_sap_data_dump/upload_data/work_order.RawRows.csv` | 11 | As-is |
| `work_order_operation` | `cdf_sap_data_dump/upload_data/work_order_operation.RawRows.csv` | 17 | As-is |
| `notification` | `cdf_sap_data_dump/upload_data/notification.RawRows.csv` | 10 | As-is |
| `timeseries` | `cdf_pi_data_dump/upload_data/timeseries.RawRows.csv` | 375 | As-is |
| `file` | `cdf_sharepoint_data_dump/upload_data/file.RawRows.csv` | 11 | As-is, plus file content manifest; also source of CogniteFileCategory nodes |
| `diagram_annotation` | `cdf_sharepoint_data_dump/upload_data/diagram_annotation.RawRows.csv` | as source | As-is; transformation sets the edge, start (document) and end (Tag) from `{{ instance_space }}`; CSV space columns ignored |
| `material` | **new** synthetic CSV | 10 | One spare part per equipment row; columns `key,name,materialType,equipment_externalId,asset_externalId` |
| `alarm_event` | **new** synthetic CSV | 30 | Raised → Acknowledged → Cleared for 2 crossings on each of the 5 instruments with set points; columns `key,historianTagName,asset_externalId,timeseries_externalId,operatingLimit_externalId,eventTime,eventType,severity,limitType,value,message`; ids match the tag, time series and operating limit samples |

CFIHOS class tables (pump, valve, compressor, …), `functional_location`, `maintenance_integrity` and
`failure_mode` are **not** copied — the generic model has no views for them.

### 12.3 Transformations

Source-to-target naming: `tr_{source}_all_to_industry_{target}`; the `industry` token keeps the ids distinct from the
CFIHOS transformations that may run in the same project. **Overlay** = writes properties on nodes another
transformation creates → `ignoreNullFields: true`; all others `false`.

| Module | Transformation | RAW → view | Key mappings |
|--------|----------------|------------|--------------|
| SAP | `tr_sap_all_to_industry_tag` | `tag` → Tag | `key`→externalId; `name`, `description`; `facility`→`site`; `area`, `system`; `className`→`tagClass`; `parentTag`→`parent`; `aliases` from `name`/`tagNumber`; `labels` (view alias of CogniteDescribable `tags`) incl. `DetectInDiagrams` — SQL `AS labels`, never `AS tags` (§5.1.1) |
| SAP | `tr_sap_all_to_industry_equipment_type` | `equipment` → `cdf_cdm:CogniteEquipmentType` | distinct `code`→externalId and `code`; `equipmentClass`, `standard`, `standardReference`; `class`→`name` |
| SAP | `tr_sap_all_to_industry_equipment` | `equipment` → Equipment | `code`→`equipmentType`; `model`→`modelNumber`; `asset_externalId`→`asset`; `sourceId` |
| SAP | `tr_sap_all_to_industry_material` | `material` → Material | `name`, `materialType`; `equipment_externalId`→`equipment`; `asset_externalId`→`asset` |
| SAP | `tr_sap_all_to_industry_work_order` | `work_order` → WorkOrder | `order`→`workOrderNumber`; `systemStatusCode`→`status`; `maintActivityType`→`type`; `SPNPriority`→`priority`; `scheduledStart/FinishDateTime`→`scheduledStart/EndTime`; `actualOrderStart/FinishDateTime`→`startTime`/`endTime`; `mainAsset_externalId`→`mainAsset` and `assets` |
| SAP | `tr_sap_all_to_industry_work_order_operation` | `work_order_operation` → WorkOrderOperation | `userStatusCode`→`status`; `operationCounter`→`sequence`; `wctrDiscipline`→`mainDiscipline`; `plannedWork`→`personHours`; `maintenanceOrder_externalId`→`maintenanceOrder`; `mainAsset_externalId`→`mainAsset`/`assets` |
| SAP | `tr_sap_all_to_industry_notification` | `notification` → Notification | `systemStatusCode`→`status`; `notificationPriorityType`→`priority`; `failureModeCode`, `detectionMethodCode`; `sourceId`; `asset_externalId`→`asset`; `maintenanceOrder_externalId`→`maintenanceOrder` |
| PI | `tr_pi_all_to_industry_time_series` | `timeseries` → TimeSeriesData | `type`/`isStep` from `isString`/`isStep`; `timeSeriesType` `PI`→`timeSeriesSource: Historian`; `pi_tag`→`historianTagName`; `pi_pointSource`→`historianPointSource`; `engunits`→`sourceUnit`; `asset_externalId`→`assets`; `equipment` via lookup join to `equipment.asset_externalId` |
| PI | `tr_pi_all_to_industry_operating_limit` | `instrument_equipment` ⋈ `timeseries` → OperatingLimit | rows with `setPointHigh`/`setPointLow`; `limitHigh`/`limitLow`; `timeSeries` = first time series whose `asset_externalId` = instrument `key` |
| SharePoint | `tr_sharepoint_all_to_industry_file_category` | `file` → `cdf_cdm:CogniteFileCategory` | distinct `documentTypeCode`→externalId and `code` |
| SharePoint | `tr_sharepoint_all_to_industry_document` | `file` ⋈ `diagram_annotation` → Document | `documentNumber`, `disciplineCode`; `currentRevision`→`revision`; `documentTypeCode`→`category`; `facility`→`site`; `area`, `system`; `name` / `description` from file metadata; `mimeType`; **`isUploaded`** and **`uploadedTime`** populated so the CogniteFile syncer and Search (§6.2) work (same pattern as CFIHOS file load — confirm against the SharePoint file-content manifest); `assets` = distinct union of `asset_externalId` and annotated tags (single owner of `CogniteFile.assets`) |
| SharePoint | `tr_sharepoint_all_to_industry_diagram_annotation` | `diagram_annotation` → `cdf_cdm:CogniteDiagramAnnotation` edges | Edge, start (document) and end (Tag) in `{{ instance_space }}` |
| SharePoint | `tr_sharepoint_all_to_industry_equipment_document_connection` | `equipment` ⋈ `diagram_annotation` → Equipment.`files` | **Overlay**; documents whose annotated tag = equipment asset |
Each `.yaml` follows the CFIHOS pattern, pointed at the industry model. Every dump, the enterprise module and
the search module set `instance_space` to `inst_location` (§11.3).

```yaml
externalId: tr_sap_all_to_industry_tag
name: Industry tag from SAP
destination:
  dataModel:
    space: '{{ enterprise_space }}'
    externalId: '{{ enterprise_data_model_external_id }}'
    version: '{{ dm_enterprise_version }}'
    destinationType: Tag
  instanceSpace: '{{ instance_space }}'
  type: instances
dataSetExternalId: '{{ dataset_external_id }}'
ignoreNullFields: false
isPublic: true
conflictMode: upsert
authentication:
  clientId: '{{ ingestion_client_id }}'
  clientSecret: '{{ ingestion_client_secret }}'
```

SQL follows the `cdf-transformation` style guide, not the CFIHOS SQL style. Every direct, including
cross-source asset/Tag directs, uses `{{ instance_space }}`.

```sql
WITH source AS (
  SELECT
      cast(`key` AS STRING)       AS tag_key
    , cast(name AS STRING)        AS name
    , cast(facility AS STRING)    AS site
    , cast(parentTag AS STRING)   AS parent_key
  FROM `{{ raw_database_sap }}`.`tag`
)
SELECT
    tag_key                                                       AS externalId
  , name                                                          AS name
  , site                                                          AS site
  , array('DetectInDiagrams')                                     AS labels   -- view alias of Describable.tags (§5.1.1)
  , CASE
      WHEN parent_key IS NULL OR parent_key = '' THEN NULL
      ELSE node_reference('{{ instance_space }}', parent_key)
    END                                                           AS parent
FROM source
WHERE tag_key IS NOT NULL
```

Loads are full loads with `upsert` (no `is_new()`); the README states this is a sample-data choice and points to
the `cdf-transformation` skill for incremental loads on real sources.

### 12.4 Workflows (per dump module)

Each `*_industry_data_dump` owns its workflow (`wf_{source}_all_to_industry`), version `v1`, and a schedule
trigger that never fires by default (`run_workflow_schedule: "0 0 29 2 *"`). Run order across modules is manual:
**SAP first**, then PI and SharePoint (cross-source Tag/Equipment nodes are not expressed as `dependsOn` across
workflows).

| Module | Workflow | Tasks | Notes |
|--------|----------|-------|-------|
| SAP | `wf_sap_all_to_industry` | 7 SAP transforms in parallel | `abortWorkflow` |
| PI | `wf_pi_all_to_industry` | time series + operating limit → alarm Function | Prefer SAP first; Function `abortWorkflow` |
| SharePoint | `wf_sharepoint_all_to_industry` | file category + document → diagram annotation / equipment–document | Prefer SAP first; enrichment tasks `skipTask` |

No search tasks: once the enterprise loads finish, the data is visible through `IndustrySearch_SOL`.

Transformation tasks: `type: transformation`, `concurrencyPolicy: fail`. Function task: `type: function`,
`isAsyncComplete: false`, `data` per §12.5 contract. Every task: `retries: 3`, `timeout: 3600`.

### 12.5 Records Function (`fn_pi_all_ingest_alarm_event`)

The Records service does **not** support onboarding through RAW + Transformations; production uses an extractor
that writes directly to the stream (PI / OPC UA events, hosted extractor). In the sample, a small CDF Function
stands in for that extractor.

**Handler `data` contract** (passed by the workflow function task; also usable for local runs):

| Key | Type | Required | Meaning |
|-----|------|----------|---------|
| `stream_external_id` | string | yes | `{{ alarm_stream_external_id }}` |
| `instance_space` | string | yes | Space of the record and of every direct (`asset`, `timeSeries`, `operatingLimit`) |
| `enterprise_space` | string | yes | Space of the `AlarmEvent` container / record view |
| `raw_database` | string | yes | `{{ raw_database_pi }}` |
| `raw_table` | string | yes | `alarm_event` |
| `last_updated_time_gt` | ISO-8601 string | no | Lower bound for immutable-stream filters; default = sample window |

No secrets in Function YAML or `data` — the workflow session / injected `client` carries credentials
(`cdf-function` skill).

**Local-first (`cdf-function` skill):** when creating or adapting this function, keep unit tests for pure mapping /
duplicate-filter logic (`test_handler.py`, `uv run pytest`) and scaffold a `run_local()` entry point (plus `.env`
for a dev project) so agents iterate locally before `cdf deploy` — do not treat a deploy cycle as the first test.

Steps:

1. Read `data["raw_database"].data["raw_table"]` rows; validate each into a pydantic model.
2. Build records in `data["instance_space"]` (`inst_location` in the template) with source = `AlarmEvent` record
   view (`{{ enterprise_space }}` / `AlarmEvent` / `{{ dm_enterprise_version }}`). Directs `asset`, `timeSeries`
   and `operatingLimit` → `{space: data["instance_space"], externalId}`.
3. Filter the stream for existing `externalId`s (immutable streams need a `lastUpdatedTime` range — use
   `last_updated_time_gt` or the sample window) and ingest only missing records in batches
   (`client.data_modeling.records.ingest`).
4. Log counts (read / skipped / ingested) and return them.

Packaging follows AGENTS.md + `cdf-function` / `cdf_common`:

- Folder name == `externalId`: `functions/fn_pi_all_ingest_alarm_event/` (+ top-level
  `ingest_alarm_event.Function.yaml` or equivalent role-named file whose `externalId` matches).
- `runtime: py311` (CDF Functions; do not use the repo's Python 3.13 target).
- `space: {{ function_space }}` — dedicated function code space (same pattern as
  `common/cdf_common` `{{ functionSpace }}`); **never** upload function code into `inst_location` or any other
  instance space.
- PI module owns `data_modeling/function.Space.yaml` for `{{ function_space }}` (or reuses a project function
  space via config).
- `pyproject.toml` in the function folder, entry in `PACKAGE_SPECS`, `uv lock`,
  `python scripts/export_deploy_requirements.py`; `requirements.txt` lists `cognite-sdk` (version with Records
  support) and `pydantic` only.
- Function is in `{{ dataset_external_id }}`.

### 12.6 Ingestion and operator groups (`industry_domain_extension/auth`)

One runtime identity runs the dump pipelines: each dump trigger uses `{{ ingestion_client_id }}`, the workflow
session runs the transformations and the function task, and every transformation's `authentication` uses the same
client. Its IdP group is mapped to `{{ ingestion_group_name }}` via `{{ ingestion_source_id }}`. The `pp` / `wf`
group YAML lives on the enterprise module so the dump modules stay sample-data + workflow only.

`ingestion.Group.yaml` → `{{ ingestion_group_name }}` (`producer_industry_pp_<env>`):

| Capability | Actions | Scope | Why |
|------------|---------|-------|-----|
| `projectsAcl` | READ, LIST | all | Runtime minimum |
| `groupsAcl` | LIST | all | Runtime minimum |
| `sessionsAcl` | CREATE | all | Workflow trigger / function sessions |
| `rawAcl` | READ | `tableScope` on `{{ raw_database_sap }}`, `{{ raw_database_pi }}`, `{{ raw_database_sharepoint }}` | Sample sources (transformations + function) |
| `transformationsAcl` | READ, WRITE | `datasetScope` `{{ dataset_external_id }}` | Run transformations |
| `workflowOrchestrationAcl` | READ, WRITE | `datasetScope` `{{ dataset_external_id }}` | Workflow execution |
| `functionsAcl` | READ, WRITE | all | Call the function task |
| `dataModelsAcl` | READ | `{{ enterprise_space }}`, `cdf_cdm`, `cdf_idm`, `cdf_cdm_units` | Resolve views/containers; **no** search space, **no** WRITE |
| `dataModelInstancesAcl` | READ, WRITE, WRITE_PROPERTIES | `{{ instance_space }}` | Write nodes, edges and type nodes |
| `dataModelInstancesAcl` | READ | `cdf_cdm`, `cdf_cdm_units` | `CogniteAsset.parent` writes; `TimeSeriesData.unit` |
| `streamsAcl` | READ | all | Resolve `{{ alarm_stream_external_id }}` |
| `streamRecordsAcl` | READ, WRITE | `{{ instance_space }}` | Duplicate check + ingest `AlarmEvent` records |

`operator.Group.yaml` → `{{ operator_group_name }}` (`producer_industry_wf_<env>`): run and monitor only — see §11.1.

These `pp` / `wf` groups are pack-scoped service identities (§11.1 *Additive to foundation / SOP*); they do not
replace foundation `producer_all_*` / `admin_all_*`.

**Extraction pipelines — not in OOTB groups (locked §2.1).** This module ships sample RAW → transformations →
workflow → alarm function only; it does **not** deploy Extraction Pipelines. Do **not** add
`extractionPipelinesAcl` / `extractionConfigsAcl` / `extractionRunsAcl` to `ingestion` (`pp`) or `operator`
(`wf`) by default — even though foundation / `cdf_ingestion` workflow groups often include them. That would
over-permission a pack that has no EPs.

**Adaptation when replacing sample dumps with real extractors** (delivery only — not an OOTB change request):

- Prefer the existing per-extractor pattern (`producer.ep_*` groups in `sourcesystem/cdf_*_extractor`) — READ
  pipelines/configs, WRITE runs, dataset-scoped — and dual-map the service principal if it also runs the industry
  workflow.
- Or extend `{{ ingestion_group_name }}` with dataset-scoped extraction ACLs if the **same** SP must report
  extractor runs and run transformations.
- Optionally give `{{ operator_group_name }}` READ on pipelines/runs (dataset-scoped) so humans can monitor
  extractors alongside the workflow; leave WRITE runs on the extractor SP.

---

## 13. Registry and docs

`modules/packages.toml`:

| Package | Add |
|---------|-----|
| `dp:models` | `datamodels/industry_domain_extension`, `datamodels/industry_domain_extension_search` |
| `dp:sourcesystem` | the three `*_industry_data_dump` modules (synthetic data-dump group) |
| `dp:common` | *(no industry ingestion module — workflows live on each dump)* |
| new `dp:industry_quickstart` | all six industry modules + `common/cdf_project_foundation`; `canCherryPick = false`; title "Industry Domain Deployment Pack Demo" |

Docs:

- README per module (CONTRIBUTING.md structure) with layer, consumers, CDM/IDM versions, deploy order, the
  single instance-space strategy (§11.3), the stable-contract and adapting sections (§8), and the validation commands (§9)
- `modules/README.md` / root `README.md`: list the industry model next to ISA and CFIHOS
- Cognite docs: a package reference page next to `/cdf/deploy/cdf_toolkit/references/packages/oil_gas_data_model`
  with the configuration reference (§3.2)
- `cognitedata/cognite-ai-tooling-marketplace` (separate PR): add `datamodels/industry_domain_extension` (+ `_search`)
  to the `cdf-data-modeling` W1 template list, and `dp:industry_quickstart` to the `cdf-deployment-packs` pack
  registry as the generic counterpart of the CFIHOS-locked `dp:quickstart`; **also** update `cdf-data-modeling`
  Quick rules / `cdf-data-model-structure.md` *`usedFor` table*: `usedFor: record` containers **do** have record
  views with `streamId` (no DataModel membership, no `constraints` / `indexes`) — replace the outdated
  "record has no view" line

---

## 14. Build sequence (one PR each, under ~500 lines where possible)

Split the enterprise delivery so PRs stay reviewable (~500 lines):

1. **Enterprise schema + auth (no SPEC):** spaces, 8 node containers + `AlarmEvent` record container, 9 views +
   `AlarmEvent` record view (`version` + `streamId`), stream, DataModel, auth (`admin` + `dom_producer`), data set,
   location filter, `default.config.yaml`, README with layer / `consumers:` stubs / labels note /
   one instance space (`inst_location`; no search instance space) / stream-template warning / alarms vs Search access +
   sample two-step records query. Point *Adapting this template* at this plan until SPEC lands.
2. **Enterprise SPEC + adaptation docs:** `SPEC.md` (full §8 contract) and README *How to extend* / stable-contract
   sections condensed from §8.0–§8.10.
3. Search module (schema only): space, Search views (classification + UX maps + reverses + `labels` alias),
   DataModel, location filter, `sol_consumer` group, config, README (incl. "do not copy CFIHOS `implements`" box and
   no search instance space).
4. `cdf_sap_industry_data_dump`: RAW, sample data, 7 transformations (Tag SQL uses `AS labels`).
5. `cdf_pi_industry_data_dump` + `cdf_sharepoint_industry_data_dump`: RAW, sample data, 6 transformations
   (Document populates `isUploaded` / `uploadedTime`); PI module includes `function.Space.yaml`.
6. `fn_pi_all_ingest_alarm_event`: failing tests first, then handler (`data` contract §12.5), `runtime: py311`,
   `space: {{ function_space }}`, `alarm_event` sample CSV, uv workspace registration (`PACKAGE_SPECS`, `uv lock`,
   export requirements).
7. Move `ingestion` / `operator` groups onto `industry_domain_extension`; add per-dump workflows (SAP first, then PI with alarm `abortWorkflow`, then SharePoint).
8. `packages.toml` + repo docs; `python validate_packages.py`.
9. Run the §9 validation gate; dev deploy order: enterprise → search → SAP data dump → PI and SharePoint data
   dumps → ingestion; run the workflow twice and compare instance and record counts with §12.2.
10. Cognite docs package page and marketplace PR (incl. record-view skill fix, §13).

---

## 15. Out of scope

- Industry-standard extensions (CFIHOS, ISA) — they stay in their own modules and are not changed
- Live extractor modules (`cdf_*_extractor`) — the industry template uses the synthetic data dumps only
- Contextualization modules (entity matching, file annotation) beyond the sample diagram annotation load
- Populating `equipment` / `timeSeries` on work orders, operations and notifications (typed in §5.2; no sample data)
- Mutable alarm working set (`BasicLiveData` stream + archive pipeline) — only one such stream per project
- Live alarm extractors (PI / OPC UA events, hosted extractors) writing to the stream

---

## 16. Implementation todos

- [ ] Enterprise schema + auth + README (no SPEC yet) — §14 step 1
- [ ] Enterprise `SPEC.md` + adaptation README sections — §14 step 2
- [ ] Search module (schema only: views, DataModel, location filter, UX maps)
- [ ] SAP industry data dump (RAW, sample data incl. synthetic `material`, transformations; `AS labels`)
- [ ] PI and SharePoint industry data dumps (incl. `function_space`, file sync fields)
- [ ] `fn_pi_all_ingest_alarm_event` records Function + `alarm_event` sample data (test-first, py311)
- [ ] Per-dump workflows (SAP / PI alarm `abortWorkflow` / SharePoint), pp/wf auth on enterprise module
- [ ] `packages.toml` (`dp:models`, `dp:sourcesystem`, `dp:common`, `dp:industry_quickstart`) and repo docs
- [ ] Pass the §9 validation gate and dev workflow run
- [ ] Cognite docs package page and cog-vd marketplace PR (record-view skill fix)

---

## 17. Reference modules and docs

| Source | Use as |
|--------|--------|
| `isa_manufacturing_extension` / `_search` | Folder layout, DOM + search pairing, search location filter |
| `cfihos_oil_and_gas_extension` / `_search` | Tag-centric hub idea; auth/data set/location layout (new search must be map-only, no `implements`) |
| `sourcesystem/cdf_sap_data_dump`, `cdf_pi_data_dump`, `cdf_sharepoint_data_dump` | Sample CSVs to copy; RAW/manifest/transformation file layout |
| `common/cdf_ingestion` | Workflow, trigger, and auth group shape |
| Docs `/cdf/dm/dm_reference/dm_core_data_model` | CDM properties (CogniteEquipment, CogniteEquipmentType, CogniteFile, CogniteFileCategory, CogniteActivity, CogniteTimeSeries) |
| Docs `/cdf/dm/dm_reference/dm_process_industry_data_model` | IDM properties (CogniteMaintenanceOrder, CogniteOperation, CogniteNotification) |
| Docs `/cdf/deploy/cdf_toolkit/references/resource_library` | Location filter, `*.Streams.yaml`, record view `streamId` YAML |
| Docs `/cdf/dm/records/concepts/records_and_streams` | Record containers (unsupported features), record views, stream templates, retention, graph traversal |
| Docs `/cdf/dm/records/guides/records_getting_started` | SCADA alarm example; "no RAW + Transformations" ingestion rule; SDK `records.ingest` |
| Docs `/cdf/dm/dm_reference/dm_limits_and_restrictions` | Stream limits per project and per template |
| `modules/common/cdf_common/functions` | Function YAML + uv workspace layout |

Do **not** copy ISA process/batch types, CFIHOS class containers, coded properties, or Describable-only domain views
into the generic template.
