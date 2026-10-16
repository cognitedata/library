# Contextualization Quality Dashboard — Records & Streams Backend

## Overview

This module deploys the storage side of the Context Quality Dashboard's Records & Streams backend: an
**immutable run log** that a metrics function appends to and the dashboard reads. It is a separate module from
`dashboards/context_quality` (the Streamlit dashboard with flat-JSON metric storage), so the two can be developed
and deployed independently.

The module owns two things:

- **The deployment**: the schema space, the instance space, two record containers, two views, the Records stream and
  the metrics function with its data set, all with configurable names.
- **The data contract**: a versioned description of what is written to the log and how it is read. It lives in
  [`contract/`](contract/) and is the source of truth. Producers and consumers, including the dashboard, depend on it
  and do not define it themselves. Start with [`contract/CONTRACT.md`](contract/CONTRACT.md).

The log design is exactly the one that is already deployed and in use in the first (legacy) deployment of this
backend, so that deployment's resources and its history stay valid. Version 1 of the contract is that design.

### Design in one paragraph

A run writes a `run:started` marker, then its `aggregate`, `exclusion`, `drilldown` and `classification` rows (and
`annotationBucket` rows to a second container), and **last** a `run:complete` marker. "Current" means the latest run
with a `complete` marker; earlier complete runs are the history. Rows are raw counts, never percentages: a consumer
sums the rows for the scope the user picked and divides. Nothing is ever updated or deleted by the backend.

### Status

| Part | Status |
|---|---|
| Schema, views, stream and the contract (this module) | Done |
| Metrics function (`fn_context_quality_metrics`) | Ported from the baseline implementation with identical behaviour. Known defects are listed below and are not fixed yet |
| Workflow, schedule trigger and the two access groups | Done in source and validated by `cdf build`; **not deployed**. Needs identity-provider inputs (see "Deployment inputs") |
| Demo data and synthetic history | None. There is no demo module; the function and the workflow carry no demo rules |

## Module Components

| Resource | Where | Notes |
|---|---|---|
| Schema space | `data_modeling/context_quality_metrics.Space.yaml` | Holds only containers and views. Variable `metricsSchemaSpace` |
| Instance space | `data_modeling/context_quality_instances.Space.yaml` | The `space` of every record. Variable `metricsInstanceSpace` |
| `ContextQualityRelationship` container | `data_modeling/containers/` | Record container for the `run`, `aggregate`, `exclusion`, `drilldown` and `classification` rows |
| `ContextQualityFileAnnotation` container | `data_modeling/containers/` | Record container for the `annotationBucket` rows |
| Views of both containers | `data_modeling/views/` | Same external id as the container, bound to the stream. Version `metricsVersion` |
| Metrics stream | `streams/context_quality_metrics.Streams.yaml` | Immutable. Variables `metricsStreamExternalId` and `metricsStreamTemplate` |
| Data set | `data_sets/context_quality.DataSet.yaml` | Holds the function's code. Variable `metricsDatasetExternalId` |
| Metrics function | `functions/context_quality.Function.yaml`, `functions/fn_context_quality_metrics/` | Variable `metricsFunctionExternalId`. The folder is named after the external id |
| Workflow | `workflows/context_quality.Workflow.yaml`, `…WorkflowVersion.yaml` | `wf_context_quality_metrics`, version `v1`. One task: the metrics function |
| Schedule trigger | `workflows/context_quality.WorkflowTrigger.yaml` | `wf_context_quality_metrics_schedule`. Runs the workflow on a cron schedule as the function identity |
| Function group | `auth/context_quality_function.Group.yaml` | `gp_context_quality_function` |
| Dashboard users group | `auth/context_quality_dashboard_users.Group.yaml` | `gp_context_quality_dashboard_users` |
| Contract | `contract/` | `CONTRACT.md`, `contract.json` and the golden `samples.json` |

The two container external ids are part of the contract and are **not** variables.

### Variables

| Variable | Default | Legacy deployment value |
|---|---|---|
| `metricsSchemaSpace` | `sp_context_quality_metrics` | `sp_cq_demo_metrics` |
| `metricsInstanceSpace` | `sp_dat_context_quality_metrics` | `sp_cq_demo_metrics_data` |
| `metricsVersion` | `v1` | `v1` |
| `metricsStreamExternalId` | `context_quality_metrics` | `cq_demo_metrics` |
| `metricsStreamTemplate` | `BasicArchive` | `ImmutableTestStream` |
| `metricsFunctionExternalId` | `fn_context_quality_metrics` | not deployed there yet |
| `metricsDatasetExternalId` | `ds_context_quality` | not deployed there yet |
| `metricsWorkflowExternalId`, `metricsWorkflowVersion` | `wf_context_quality_metrics`, `v1` | not deployed there yet (the legacy `cq_demo` workflow is a different, demo workflow and stays as it is) |
| `metricsTriggerExternalId` | `wf_context_quality_metrics_schedule` | not deployed there yet |
| `metricsWorkflowSchedule`, `metricsWorkflowScheduleTimezone` | `0 4 * * *`, `UTC` | choose per deployment |
| `metricsFunctionGroupName`, `metricsDashboardGroupName` | `gp_context_quality_function`, `gp_context_quality_dashboard_users` | not deployed there yet |
| `metricsSourceSpaces` | `cdf_cdm`, `cdf_idm` | the facility instance spaces, added to those two |
| `metricsFunctionGroupSourceId`, `metricsDashboardGroupSourceId`, `metricsFunctionClientId`, `metricsFunctionClientSecret` | `${CQ_FUNCTION_GROUP_SOURCE_ID}`, `${CQ_DASHBOARD_GROUP_SOURCE_ID}`, `${CQ_FUNCTION_CLIENT_ID}`, `${CQ_FUNCTION_CLIENT_SECRET}` | environment values; **required**, see "Deployment inputs" |
| `metricsFacilityNames`, `metricsTierByTypeCode`, `metricsDisciplineByTypeCode`, `metricsAssetTypeByEquipmentTypeCode`, `metricsDefaultOwners`, `metricsOwnerPrefixRules`, `metricsExclusions` | empty | the project's rules, as JSON text (Toolkit reads a mapping-valued variable as a namespace, so a mapping or list is written as a string such as `'{"pump": 1}'`) |
| `metricsTieringEnabled` | `false` | tiering is off by default; set `true` together with `metricsTierByTypeCode` to turn it on |
| `metricsDivisionTagPrefix`, `metricsRetiredTag`, `metricsFreshnessDays`, `metricsMaxDepth` | `""`, `""`, `30`, `8` | the project's settings |

The legacy at-dev names are supplied by a deployment-specific `config.<env>.yaml` that sets `metricsSchemaSpace`,
`metricsInstanceSpace`, `metricsStreamExternalId` and `metricsStreamTemplate` to the values above. Nothing in the
function or the workflow falls back to them.
### The metrics function

`functions/fn_context_quality_metrics/` reads the contextualization data (Core and maintenance views, diagram
annotations), counts it at the lowest reporting level and appends one run to the stream. Two actions:

| Action | What it does |
|---|---|
| `compute_metrics` | Computes and writes a run: a `started` marker, the rows, then the `complete` marker last. With `"dry_run": true` it computes and returns counts, and writes nothing; add `"return_rows": true` to get the aggregate, exclusion and classification rows back |
| `validate` | Computes and returns totals per metric. Never writes |

`compute_metrics` also takes `extraLinks` in the payload: a list of `{"model": "space:externalId:version", "links":
["View.property", ...]}`. Each link must be a direct relation of a view in that model. The run counts, per facility, the
instances of the view that have it set and writes them as `extraLink` rows (contract section 2.8), next to the built-in
metrics, which do not change. The spaces of the models named in the file are added to the Core facilities, so a model whose instances
live in spaces with no Core entity is still counted (contract section 2.8). A name the model does not have fails the run before anything is written.

Anything under the payload's `config` overrides `cq/config.py`'s `DEFAULT_CONFIG`. An override replaces the default it
names, whatever its type, and is never merged into it: `"tierByTypeCode": {}` clears the tier rules, and a mapping
that is not empty is the whole mapping. Keys the payload does not name keep their defaults. `exclusions` is a mapping
of four lists, so a supplied `exclusions` replaces all of them and any list it does not name is empty. The defaults
carry no project-specific rule: `tierByTypeCode`, `disciplineByTypeCode`, `assetTypeByEquipmentTypeCode`,
`defaultOwners`, `ownerPrefixRules`, `divisionTagPrefix`, `retiredTag` and `exclusions.decommissionedTags` are all
empty, and an empty rule is not applied (an empty required mapping makes the metrics that need it not applicable, with
a `mapping-empty` warning; with no `divisionTagPrefix` no asset gets a division, and rows carry none). The division
convention is the project's: set `divisionTagPrefix` to the prefix of the tags that name a division. `tierByTypeCode`
is validated when the config is built: every value must be the integer 1, 2 or 3 (a string, float, boolean, null or any
other number is a configuration error, raised before anything is read or written). No project's rules ship with
the function: a project supplies its own in the task's `config` (the workflow's `metrics*` variables).

**Resources have no defaults.** The deploying environment names what the function reads and writes, in the task's
`config` (a workflow passes the module's `metricsStreamExternalId`, `metricsSchemaSpace` and `metricsInstanceSpace`):

| Key | Needed by | Why |
|---|---|---|
| `schemaSpace` | every action | the space of the run-log containers; left out of facility discovery |
| `instanceSpace` | every action | the space the run rows are written to; left out of facility discovery |
| `stream` | a real `compute_metrics` run | the Records stream that is written. `validate` and `dry_run` never write it, so they do not need it |

A missing or blank one is a `ValueError` naming it, raised before the client is used, so nothing is read or written.
The two container names (`relationshipContainer`, `fileAnnotationContainer`) are part of the run log contract rather
than of an environment, so they keep their defaults. The legacy at-dev deployment passes its own names explicitly
(`cq_demo_metrics`, `sp_cq_demo_metrics`, `sp_cq_demo_metrics_data`). The function reads no RAW table.

**What is deployed.** Toolkit zips the whole function folder, so the deployed package also contains the tests, their
fixtures (`parity_dataset.py`, `tiny_data.py`, `sample_rules.py`, `conftest.py`). This matches
the library's other functions, which keep `test_*.py` beside the handler (for example `cdf_entity_matching`), so the
packaging convention is unchanged. Nothing in `handler.py` or `cq/` imports any of those files (a test enforces it), so
production cannot pick up any rules without a task `config` that supplies them. `sample_rules.py` is a test fixture:
the rules and resource names that fit the sample dataset in `parity_dataset.py`. A possible packaging step is to move
the test files and fixtures out of the deployed function folder (for example by giving the function its own source
folder); that changes how the repository lays out functions, so it is deliberately not done here.

**Availability and warnings.** A metric that could not be measured for a reporting unit is written as a row with
`eligibleCount` 0 and `notApplicable` true, so it can be told apart from a measured zero (0 of N) and from "nothing was
looked at" (no row). Rows are registered per metric and unit only where the entities the metric is computed from
exist, and only when the unit has no row for that metric. The `complete` marker carries `warnings` (compact JSON, codes
and counts only) for empty required mappings, unmapped asset and equipment types, unavailable optional source views
(the `cdf_idm` maintenance views), populations with no entities, not-applicable metrics and truncated drill-downs. The
rules and codes are in `contract/CONTRACT.md` (sections 3.3 and 6.2). A run whose required source views cannot be read,
or that finds no facility space, writes nothing at all, so the previous run stays current.

**This is a port of the baseline implementation with three deliberate fixes.** It is typed and linted, and the
demo-only `seed_datapoints` and `seed_history` actions are not part of it. Its output was checked against the baseline
implementation's, row for row, except for the changes below, before that comparison was retired.

- **Tiers.** Tiering is off by default (`tieringEnabled`): no asset has a tier and the coverage metrics measure every
  asset. When it is on, an asset's tier comes only from its own type code in `tierByTypeCode`, which is empty by default. An entity
  that is not an asset inherits the tier of the asset it links to. Everything else has no tier; nothing is tier 3 unless
  a project maps a type to 3 (contract section 6.1).
- **Roots.** A root is a parentless asset that has children; a standalone asset is an orphan; an asset whose parent was
  not loaded is a child (contract section 6.6). It no longer depends on how facilities are discovered.
- **Decommissioned equipment** is excluded by `exclusions.decommissionedTags` (a list, empty by default). The old scalar
  `decommissionedTag` is rejected, and `retiredTag` is empty by default.

Drilldown and classification records also get collision-safe external ids, and config mapping overrides replace instead
of merging.

Known limitation: the function reads only the facility spaces, so a parentless asset whose children all live elsewhere
looks standalone (an orphan, not a root).

Tests live beside the function and run with:

```bash
uv run pytest modules/dashboards/context_quality_v2/functions/fn_context_quality_metrics/ --rootdir=. -q
```

They cover the discovery rules, the output against the baseline, the reader and the latest-datapoints lookup using
real Cognite SDK data classes, and the write order. `tests/test_context_quality_function_contract.py` checks that
what the function writes fits the containers in this module and the contract.

### How this reconciles the earlier scaffold with the deployed design

The module first held a different design: four containers (three mutable "current state" node containers and one
immutable snapshot container) and a `BasicArchive` stream named `context_quality_metric_snapshots`. That design was
unmerged and never part of a release, and it is not compatible with the deployed run log, so it is replaced.

| Earlier scaffold resource | Legacy deployment resource | Resource in this module |
|---|---|---|
| `ContextQualityCurrentMetric` (node container) | none | Replaced by `aggregate` rows, read from the latest complete run |
| `ContextQualityCurrentExclusion` (node container) | none | Replaced by `exclusion` rows |
| `ContextQualityDrilldownRow` (node container) | none | Replaced by `drilldown` rows |
| `ContextQualityMetricSnapshot` (record container) | `ContextQualityRelationship` | `ContextQualityRelationship`, one container for every row kind except `annotationBucket` |
| none | `ContextQualityFileAnnotation` | `ContextQualityFileAnnotation` |
| Views of the four containers | Views of the two containers (`v1`) | Views of the two containers |
| `context_quality_metric_snapshots` stream (`BasicArchive`) | `cq_demo_metrics` stream (`ImmutableTestStream`) | One stream, `metricsStreamExternalId`, template `metricsStreamTemplate` |
| Schema space `sp_context_quality_metrics` | `sp_cq_demo_metrics` | `metricsSchemaSpace` |
| Instance space `sp_dat_context_quality_metrics` | `sp_cq_demo_metrics_data` | `metricsInstanceSpace` |

The container and view definitions are the ones deployed in the legacy deployment, with its space, stream and view
version replaced by variables. Building the module with the legacy variable values gives resource definitions
identical to the deployed ones (containers, views and the stream; the two spaces differ only in their descriptions).
`tests/test_context_quality_contract.py` and the fixtures in `tests/fixtures/context_quality/legacy_deployment/`
keep it that way.

## Deployment

### Prerequisites

- Cognite Toolkit 0.8 or later. The module was built and checked with 0.8.191.
- The `record_views` alpha flag enabled in `cdf.toml`, because the containers are `usedFor: record` and the views
  are bound to a stream:

  ```toml
  [alpha_flags]
  record_views = true
  ```

- A deploying principal that may create spaces, containers and views (`dataModelsAcl`) and streams (`streamsAcl`).
  Writing and reading records is done by other identities and is described in the contract, not granted here.

### Adding the module to an existing Toolkit project

```bash
cdf modules add .
```

Select **Dashboards** → **Contextualization Quality Dashboard — Records & Streams Backend**. Then:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

### Deploying where the backend already exists

A stream's template cannot be changed after the stream is created, and existing records must stay readable. When
the project already has this backend, set the variables in `config.<env>.yaml` to the **existing** names and the
template the stream was created with (see the legacy deployment column above), build, and read the dry run: the
containers, views and stream must show as unchanged. Do not delete or recreate the stream to obtain different names.

### The production workflow

`wf_context_quality_metrics` (version `v1`, one execution at a time) has one task, `compute_metrics`, which calls
`fn_context_quality_metrics` with `action: compute_metrics` and `dry_run: false`. The task's `config` carries the three
resource names (`stream`, `schemaSpace`, `instanceSpace`, from the module variables) and the project's rules (from the
`metrics*` variables, empty by default). It is never given demo rules, and no seed or demo task is in it.

Execution path: the schedule trigger (or an operator) starts the workflow, which runs the function as the identity in
the trigger's `authentication`. The function checks its resource settings, reads the facility entities, computes, and
appends the run: a `started` marker, the rows, and the `complete` marker last. Any error fails the task
(`onFailure: abortWorkflow`, one retry), which fails the execution, so a failed computation is never recorded as a
success; a run that fails before the end has no `complete` marker and is never shown.

**Operator run.** Start the workflow from Fusion (Data management, Workflows), or with the Toolkit's `run` plugin
(`run = true` under `[plugins]` in `cdf.toml`; it is deprecated in favour of the `dev` plugin):

```bash
cdf run workflow -x wf_context_quality_metrics -n v1 --env <env>
```

The operator needs `workflowOrchestrationAcl` on the module's data set; this module grants it to nobody. To check the
function's configuration without writing, start the function directly with `{"action": "validate"}`.

### Running with a config file

A person can start the workflow from the dashboard's Configuration page with an uploaded config file (a list of
`space:externalId:version` models, each with `View.property` links). The dashboard passes the links as the execution
input, `{"extraLinks": [...]}`, and the workflow hands them to the function (see "The metrics function"). The schedule
passes `{"extraLinks": []}`, so it never measures extra links.

The run executes as the person who started it (the dashboard opens a session from their own token), so that person
needs, in addition to the read-only dashboard group: `workflowOrchestrationAcl` WRITE, `sessionsAcl` CREATE, and what
the function group (`auth/context_quality_function`) holds, because the run does what that group does. This module does
not create such a group: grant the capabilities to the people who may run it.

**Not yet verified against a deployed project:** that a `${workflow.input.extraLinks}` reference in the function task's
`data` resolves, and that the scheduled trigger's `input` supplies it. Check both on a scratch workflow before deploying.

### Access

| Group | Capabilities | Why |
|---|---|---|
| `gp_context_quality_function` (the identity the workflow runs as) | `projectsAcl` read and list, `groupsAcl` list, `sessionsAcl` create, `functionsAcl` read and write (library convention for a workflow identity); `dataModelsAcl` read on `cdf_cdm`, `cdf_idm` and the schema space; `dataModelInstancesAcl` **read** on `metricsSourceSpaces`; `timeSeriesAcl` read; `streamRecordsAcl` **write** on the instance space | read the entities and datapoints, append records |
| `gp_context_quality_dashboard_users` | `projectsAcl` read and list, `groupsAcl` list, `streamRecordsAcl` **read** on the instance space, `dataModelsAcl` read on the schema space, `streamsAcl` read | read the run log; the stream settings read is optional (the dashboard works without it) |

Neither group can write instances or deploy anything, and the dashboard group cannot write at all. The scopes come from
the module variables, never from `all` except where the capability has no narrower scope (`timeSeriesAcl`, `streamsAcl`).
Not validated against a deployment: that `timeSeriesAcl` read with `all` scope is the narrowest that serves the latest-datapoint
lookup for data-model time series, and that `functionsAcl` write is needed to call a function from a workflow. Both
follow the library's other workflow groups; tighten them after the first dry run if the project allows it.

### Deployment inputs

The scheduled trigger and the groups need values this repository cannot hold. Set them in `.env` (the names are the
defaults of the variables above) before `cdf build` and `cdf deploy`:

| Environment variable | What |
|---|---|
| `CQ_FUNCTION_GROUP_SOURCE_ID` | The identity provider's id of the group whose members get the function group's capabilities |
| `CQ_DASHBOARD_GROUP_SOURCE_ID` | The same, for the people who use the dashboard |
| `CQ_FUNCTION_CLIENT_ID`, `CQ_FUNCTION_CLIENT_SECRET` | The client credentials the schedule runs as. The client must be a member of the function group |

Without them `cdf build` leaves the placeholders in the output and `cdf deploy` cannot create the trigger or the groups.
`cdf build` without CDF credentials also reports `UNVERIFIED-REFERENCE` for `cdf_cdm` and `cdf_idm`, the system spaces
the groups name; the library's `cdf_ingestion` module reports the same, and it clears when the build can reach CDF.

### What this module does not deploy

The dashboard app. Check your project's stream quota before adding a stream.

### Open decisions

- **Contract version on the run marker.** The contract lets the `run` marker declare `contractVersion`, and the
  dashboard reads it when present. The deployed `ContextQualityRelationship` container has no such property.
  Adding an optional property is additive, but whether Toolkit may add a property to a record container that
  already holds data has to be confirmed with `cdf deploy --dry-run` against the legacy deployment before the
  property is added.
- **Default stream template.** The default is `BasicArchive`. Confirm that it is the right production default and
  size it for the run frequency (the retention and the filter window both come from the template).
- **Mutable current-state containers.** The earlier scaffold had them for a "current value" read. The run log does
  that by reading the latest complete run, so they are not carried over. If persisted configuration is needed later,
  it needs its own container.

### Function dependencies

Do not rely on a function's `requirements.txt` alone. In this repository the deployed dependencies come from
`deploy_dependencies` in `PACKAGE_SPECS` in [`scripts/generate_uv_member_projects.py`](../../../scripts/generate_uv_member_projects.py);
`scripts/export_deploy_requirements.py` writes each function's `requirements.txt` from it, the function folder gets a
generated `pyproject.toml`, and the path is registered under `[tool.uv.workspace] members` in the root
`pyproject.toml` and locked in `uv.lock`. This function's entry is `cognite-sdk>=7.89.0,<8` (the floor the existing
`dashboards/context_quality` function uses) with Python `>=3.11,<3.14`, and its tests have been run against SDK 7.94
(locked) and 7.89.0 on Python 3.12 and 3.13. CDF Functions install the listed packages on top of the Functions
runtime. See [`scripts/README.md`](../../../scripts/README.md).

## Module Structure

```
context_quality_v2/
├── contract/
│   ├── CONTRACT.md          # the contract: rules and semantics
│   ├── contract.json        # identifiers, row kinds and properties, machine-readable
│   └── samples.json         # golden records, one set per row kind
├── data_sets/
│   └── context_quality.DataSet.yaml
├── functions/
│   ├── context_quality.Function.yaml
│   └── fn_context_quality_metrics/   # handler.py, cq/, tests, pyproject.toml, requirements.txt
├── workflows/
│   ├── context_quality.Workflow.yaml
│   ├── context_quality.WorkflowVersion.yaml
│   └── context_quality.WorkflowTrigger.yaml
├── auth/
│   ├── context_quality_function.Group.yaml
│   └── context_quality_dashboard_users.Group.yaml
├── data_modeling/
│   ├── context_quality_metrics.Space.yaml
│   ├── context_quality_instances.Space.yaml
│   ├── containers/
│   │   ├── ContextQualityRelationship.Container.yaml
│   │   └── ContextQualityFileAnnotation.Container.yaml
│   └── views/
│       ├── ContextQualityRelationship.View.yaml
│       └── ContextQualityFileAnnotation.View.yaml
├── streams/
│   └── context_quality_metrics.Streams.yaml
├── default.config.yaml
├── module.toml              # id: dp:dashboards:context_quality_v2
└── README.md
```

Tests: `tests/test_context_quality_contract.py`, with the legacy definitions in
`tests/fixtures/context_quality/legacy_deployment/`; `tests/test_context_quality_deployment.py` for the workflow, trigger,
groups and variables.

## Support

- The contract is in [`contract/CONTRACT.md`](contract/CONTRACT.md). Change it, `contract.json` and `samples.json`
  together, and run `uv run pytest tests/test_context_quality_contract.py`.
- For troubleshooting Toolkit deployment, see [docs.cognite.com](https://docs.cognite.com) or
  `#topic-deployment-packs` on Slack.
