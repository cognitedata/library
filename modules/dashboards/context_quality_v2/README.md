# Contextualization Quality Dashboard — Records & Streams Backend

## Overview

This module deploys the storage side of the Context Quality Dashboard's Records & Streams backend: an
**immutable run log** that a metrics function appends to and the dashboard reads. It is a separate module from
`dashboards/context_quality` (the Streamlit dashboard with flat-JSON metric storage), so the two can be developed
and deployed independently.

The module owns two things:

- **The deployment**: the schema space, the instance space, two record containers, two views, the Records stream and
  the metrics function with its data set, all with configurable names.
- **The run log**: what the function writes to the stream and the dashboard reads (see "The run log" below). The two
  container definitions in `data_modeling/` are the source of truth for its properties.

### The run log

Every record is a row of the container `ContextQualityRelationship` (or, for `annotationBucket`, of
`ContextQualityFileAnnotation`). Its `externalId` is `{runId}:{rowKey}` in the instance space, and `rowKind` says what it
is:

| `rowKind` | What it holds | Row key |
|---|---|---|
| `run` | The run's `started` marker and, last, its `complete` marker (with `rowsWritten` and `warnings`) | `run:started`, `run:complete` |
| `aggregate` | Counts of one metric for one facility, tier, division, discipline, owner and sub-key | `agg:` |
| `exclusion` | How many entities were excluded, per category | `excl:` |
| `drilldown` | One entity that has a gap, capped per metric and space | `drill:` |
| `classification` | Counts per type, class or category | `cls:` |
| `annotationBucket` | File annotation counts by status and confidence | `anno:` |
| `extraLink`, `extraLinkGap` | Counts for the links of a configuration file, and the instances without the link | `xlink:`, `xgap:` |
| `location` | A location filter that can be used as a scope: its name, parent and the facilities it covers | `loc:` |
| `runPart` | What one parallel piece of a run reports to `finalize`; read only by the function | `part:` |

Rows are raw counts, never percentages. Nothing is ever updated or deleted by the backend, and new properties are only
ever added as optional ones, so an older run stays readable.

### Design in one paragraph

A run writes a `run:started` marker, then its `aggregate`, `exclusion`, `drilldown` and `classification` rows (and
`annotationBucket` rows to a second container), and **last** a `run:complete` marker. "Current" means the latest run
with a `complete` marker; earlier complete runs are the history. Rows are raw counts, never percentages: a consumer
sums the rows for the scope the user picked and divides. Nothing is ever updated or deleted by the backend.

### Status

| Part | Status |
|---|---|
| Schema, views and stream (this module) | Done; deployed and run on a test deployment |
| Metrics function (`fn_context_quality_metrics`) | Done: plan, parallel shards and batches, finalize, the workbook of unlinked items and location filters, run end to end on a test deployment |
| Workflow | Done; run on a test deployment |
| Schedule trigger and the two access groups | Done in source and validated by `cdf build`; **not yet deployed**. They need identity-provider inputs (see "Deployment inputs") |

The two container external ids are fixed by the run log and are **not** variables.

### Variables

| Variable | Default | What |
|---|---|---|
| `metricsSchemaSpace` | `sp_context_quality_metrics` | the space of the run-log containers and views |
| `metricsInstanceSpace` | `sp_dat_context_quality_metrics` | the space the run records are written to |
| `metricsVersion` | `v1` | the view version |
| `metricsStreamExternalId` | `context_quality_metrics` | the Records stream |
| `metricsStreamTemplate` | `BasicArchive` | the stream template; it cannot be changed after the stream is created |
| `metricsFunctionExternalId` | `fn_context_quality_metrics` | the function; its folder under `functions/` is named after it |
| `metricsDatasetExternalId` | `ds_context_quality` | the data set of the function's code and of the workbook files |
| `metricsWorkflowExternalId`, `metricsWorkflowVersion` | `wf_context_quality_metrics`, `v1` | the workflow |
| `metricsTriggerExternalId` | `wf_context_quality_metrics_schedule` | the schedule trigger |
| `metricsWorkflowSchedule`, `metricsWorkflowScheduleTimezone` | `0 4 * * *`, `UTC` | when the trigger starts a run |
| `metricsFunctionGroupName`, `metricsDashboardGroupName` | `gp_context_quality_function`, `gp_context_quality_dashboard_users` | the access groups |
| `metricsSourceSpaces` | `cdf_cdm`, `cdf_idm` | the spaces the function may read; add the facility instance spaces |
| `metricsFunctionGroupSourceId`, `metricsDashboardGroupSourceId`, `metricsFunctionClientId`, `metricsFunctionClientSecret` | `${CQ_FUNCTION_GROUP_SOURCE_ID}`, `${CQ_DASHBOARD_GROUP_SOURCE_ID}`, `${CQ_FUNCTION_CLIENT_ID}`, `${CQ_FUNCTION_CLIENT_SECRET}` | environment values; **required**, see "Deployment inputs" |
| `metricsFacilityNames`, `metricsTierByTypeCode`, `metricsDefaultOwners`, `metricsOwnerPrefixRules`, `metricsExclusions` | empty | the project's rules, as JSON text (Toolkit reads a mapping-valued variable as a namespace, so a mapping or list is written as a string such as `'{"pump": 1}'`) |
| `metricsTieringEnabled` | `false` | tiering is off by default; set `true` together with `metricsTierByTypeCode` to turn it on |
| `metricsDivisionTagPrefix`, `metricsRetiredTag`, `metricsFreshnessDays`, `metricsMaxDepth` | `""`, `""`, `30`, `8` | the project's settings |
| `metricsExportRetentionDays` | `30` | how many days the workbook of unlinked items of a run stays in Files |
| `metricsMaxShards`, `metricsShardTargetEntities`, `metricsMaxConcurrentShards` | `150`, `250000`, `0` | how a run is divided into parallel function calls, and how many run at once (see "Running in parallel shards") |

A deployment that needs other names sets them in its own `config.<env>.yaml`. Nothing in the function or the workflow
falls back to a name that is not passed to it.

### The metrics function

`functions/fn_context_quality_metrics/` reads the contextualization data (Core and maintenance views, diagram
annotations), counts it at the lowest reporting level and appends one run to the stream. Its actions:

| Action | What it does |
|---|---|
| `plan`, `compute_shard`, `compute_batch`, `finalize` | A run in parallel pieces, as the workflow runs it (see "Running in parallel shards") |
| `export` | Builds the Excel workbook of every unlinked item of a run (see "The workbook of unlinked items") |
| `compute_metrics` | Computes and writes a whole run in one call, for small projects and comparison: a `started` marker, the rows, then the `complete` marker last. With `"dry_run": true` it computes and returns counts, and writes nothing; add `"return_rows": true` to get the aggregate, exclusion and classification rows back |
| `validate` | Computes and returns totals per metric. Never writes |

`compute_metrics` also takes `extraLinks` in the payload: a list of `{"model": "space:externalId:version", "links":
["View.property", ...]}`. Each link must be a direct relation of a view in that model. The run counts, per facility, the
instances of the view that have it set and writes them as `extraLink` rows, next to the built-in
metrics, which do not change. The spaces of the models named in the file are added to the Core facilities, so a model whose instances
live in spaces with no Core entity is still counted. A name the model does not have fails the run before anything is written.

Anything under the payload's `config` overrides `cq/config.py`'s `DEFAULT_CONFIG`. An override replaces the default it
names, whatever its type, and is never merged into it: `"tierByTypeCode": {}` clears the tier rules, and a mapping
that is not empty is the whole mapping. Keys the payload does not name keep their defaults. `exclusions` is a mapping
of four lists, so a supplied `exclusions` replaces all of them and any list it does not name is empty. The defaults
carry no project-specific rule: `tierByTypeCode`,
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
The two container names (`relationshipContainer`, `fileAnnotationContainer`) are part of the run log rather
than of an environment, so they keep their defaults. The function reads no RAW table.

**What is deployed.** Toolkit zips the whole function folder, so the deployed package also contains the tests, their
fixtures (`parity_dataset.py`, `tiny_data.py`, `sample_rules.py`, all in the function's `tests/` folder, and `conftest.py`). This matches
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
(the `cdf_idm` maintenance views), populations with no entities, not-applicable metrics and truncated drill-downs. A run whose required source views cannot be read,
or that finds no facility space, writes nothing at all, so the previous run stays current.

**This is a port of the baseline implementation with three deliberate fixes.** It is typed and linted, and the
demo-only `seed_datapoints` and `seed_history` actions are not part of it. Its output was checked against the baseline
implementation's, row for row, except for the changes below, before that comparison was retired.

- **Tiers.** Tiering is off by default (`tieringEnabled`): no asset has a tier and the coverage metrics measure every
  asset. When it is on, an asset's tier comes only from its own type code in `tierByTypeCode`, which is empty by default. An entity
  that is not an asset inherits the tier of the asset it links to. Everything else has no tier; nothing is tier 3 unless
  a project maps a type to 3.
- **Roots.** A root is a parentless asset that has children; a standalone asset is an orphan; an asset whose parent was
  not loaded is a child. It no longer depends on how facilities are discovered.
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
real Cognite SDK data classes, and the write order. `tests/test_context_quality_function_records.py` checks that
what the function writes fits the containers in this module.

### Compatibility

The container and view definitions are those of the first deployment of this backend, with its space, stream and view
version replaced by variables, so a deployment that already holds runs keeps reading them. Change them only
additively: a property that is deployed keeps its definition, and a new one is optional.

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
  Writing and reading records is done by other identities and is not granted here.

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
template the stream was created with, build, and read the dry run: the
containers, views and stream must show as unchanged. Do not delete or recreate the stream to obtain different names.

### The production workflow

`wf_context_quality_metrics` (version `v1`, one execution at a time) has three steps, all calling
`fn_context_quality_metrics`: `plan`, a dynamic task `shards`, and `finalize` (see "Running in parallel shards"). The
`plan` task's `config` carries the three resource names (`stream`, `schemaSpace`, `instanceSpace`, from the module
variables) and the project's rules (from the `metrics*` variables, empty by default), and hands them to every shard. It
is never given demo rules, and no seed or demo task is in it.

Execution path: the schedule trigger (or an operator) starts the workflow, which runs the function as the identity in
the trigger's `authentication`. The plan finds the facilities and appends the `started` marker, the shards append the
rows, and `finalize` appends the `complete` marker last. Any error fails its task (`onFailure: abortWorkflow`), which
fails the execution, so a failed computation is never recorded as a success; a run that fails before the end has no
`complete` marker and is never shown.

### Running in parallel shards

A project with millions of instances cannot be counted by one function call: it has ten minutes and limited memory. The
workflow therefore uses the *dynamic task* of Cognite Workflows, which runs tasks that a previous task returns, in
parallel:

1. **`plan`** (function action `plan`) counts the instances per space and kind with aggregate queries, so nothing is
   loaded, applies the same facility rule as a single call, decides what each call counts, writes `run:started`, and
   returns the run id and one task definition per call.
2. **`shards`** (dynamic task, `tasks: ${plan.output.response.tasks}`) runs those tasks, each a call of this function.
3. **`finalize`** (function action `finalize`) runs after every call has finished. It reads the `runPart` records
   (this is why the function group has `streamRecordsAcl` READ), checks that every call reported, and appends
   `run:complete` last with the summed row count and the calls' warnings merged. If one is missing it writes nothing,
   so the run is never shown.

**How the work is divided.** Each call counts one of two things:

- **Whole spaces** (`compute_shard`). A space with no more than `metricsShardTargetEntities` entities (default 250,000,
  configured views included) is counted whole, and small spaces share a call.
- **Part of a space** (`compute_batch`). A bigger space is split by kind (assets, equipment, time series, files, 3D
  objects, work orders, notifications, annotations, and each configured view). Kinds that are small enough share a
  batch. A kind that is still bigger than the target is cut into slices by the start of its instances' external ids:
  the plan counts how many instances start with each character, cuts further where a slice is still too big, and
  packs the slices into batches of about the target size. It only counts; it does not read the instances. A batch
  looks up what its entities link to outside it (the assets up a parent chain, whether a parentless asset has a child,
  whether an asset has a time series, and so on) with targeted queries, so a space's batches add up to the numbers of
  counting the space whole.

The number of calls follows the size of the project, at most `metricsMaxShards` (default 150). A workflow execution
may hold 200 tasks, generated ones included, and the plan, the dynamic task and `finalize` are three of them, so keep
it below 197; when the calls would be more, the target is made larger. At most 50 tasks run at the same time in a
project; the rest wait. The tasks carry the configuration, and a task's input and output may not exceed 0.2 MB, so the
plan also makes the target larger when a large config file would not fit, and rejects a configuration too large for
even one call.

**Running the calls one after the other.** `metricsMaxConcurrentShards` sets how many calls run at once: `0` (the
default) all at once, `1` one after the other, `N` in N lanes. The calls are listed heaviest first and a call waits for
the one N places before it (`dependsOn`), so with `1` a run is a chain; a call that fails stops the ones after it. Use
it to go easy on the project's data modeling and time series services, or to follow one run in order.

**What differs from one call (`compute_metrics`)**, because a call sees only part of the project:

- *Several spaces.* An entity that links to one in another space has no such entity loaded: its tier, division and
  discipline are unknown (whether the link is set is not affected), a parent in another space counts as not loaded
  (`parents-not-loaded`), and a work order is matched only with notifications of its own space. A facility is an
  instance space, so these are the exception; where they are common, call `compute_metrics`, which counts them exactly.
- *Batches.* A space may have several rows of one grain instead of one; the dashboard sums rows, so the totals are
  the same. The cap on a drill-down list is per space and kind, so the batches of a kind share it. A metric with
  nothing to measure in a batched kind is still shown as not applicable, but without the reason. The depth of an asset
  beyond `maxDepth` levels is a lower bound.
- *No retry.* A call writes immutable records, so it is not retried (`retries: 0`): a call that fails fails the run,
  and the run is started again.
- *Limits that remain.* Slices are cut by the start of the external ids: instances whose ids cannot be told apart by
  their first characters stay together in one slice, bigger than the target. A call has ten minutes, so a slice of
  millions of such instances can still be too much. Lookups for an asset batch ask about its ancestors level by level
  and about every asset, in chunks, so an asset batch is slower than a batch of another kind of the same size.

`compute_metrics` still exists for small projects, dry runs and comparison. `validate` is unchanged.

**Confirmed on a test deployment**: the dynamic task accepts the generated tasks and `${plan.output.response.…}`
resolves. **Still to confirm**: that `dependsOn` between the generated tasks makes them wait (start a run with
`metricsMaxConcurrentShards: 1` and read the tasks' start and end times).

### The workbook of unlinked items

The drill-down lists in the stream are capped (500 rows per list, space and kind), so the dashboard can show only a
preview. The full lists are an **Excel workbook in Cognite Files**, one per run, so that the append-only stream does not
grow with them (records cannot be deleted; files can).

- While a run is counted, each piece of it (a shard or a batch) uploads its items as a small CSV part,
  `cq-unlinked-part-{runId}-{piece}.csv`, into the module's data set (`metricsDatasetExternalId`).
- After the run is complete, a fourth workflow task, `export`, reads the parts one at a time, writes
  `cq-unlinked-{runId}.xlsx` in streaming mode (a `Summary` sheet with the numbers of the metric cards, and a sheet for
  every list that has items; a configured link has a sheet named `View.property`), uploads it, deletes the parts and the
  workbooks older than `metricsExportRetentionDays` (30). A sheet holds at most 1,048,575 items.
- The task is separate from `finalize` and set to `skipTask` on failure, so a slow or failed export never keeps a run
  from completing. A part that cannot be uploaded adds an `export-failed` warning to the run.
- **Access.** The function group has `filesAcl` READ and WRITE and `datasetsAcl` READ on the data set, and the dashboard
  users group `filesAcl` READ on it. Without them the export is skipped with the warning above.
- The function needs the `xlsxwriter` package (its `requirements.txt`).

### Running with a config file

A person can start the workflow from the dashboard's Configuration page with an uploaded config file (a list of
`space:externalId:version` models, each with `View.property` links). The dashboard passes the links as the execution
input, `{"extraLinks": [...]}`, and the workflow hands them to the function (see "The metrics function"). The schedule
passes `{"extraLinks": []}`, so it never measures extra links.

The run executes as the person who started it (the dashboard opens a session from their own token), so that person
needs, in addition to the read-only dashboard group: `workflowOrchestrationAcl` WRITE, `sessionsAcl` CREATE, and what
the function group (`auth/context_quality_function`) holds, because the run does what that group does. This module does
not create such a group: grant the capabilities to the people who may run it.

A `${workflow.input.extraLinks}` reference in the function task's `data` resolves for a run started with an input
(confirmed on a test deployment). **Not yet verified:** that the scheduled trigger's `input` supplies it.

### Location filters as a scope

The project's location filters (Locations in Fusion, `locations/*.LocationFilter.yaml` in Toolkit) can be used as a scope next
to facilities. The `plan` step (or the single-call run) reads them from the Locations API and writes one `location` row for
each one that lists instance spaces and is defined by nothing else: the row names the facilities those spaces are, so the
dashboard offers "Location" next to "Facility" and sums the rows of those facilities. Locations defined by a data set, an
asset sub-tree or an external id prefix, without instance spaces, or whose spaces are no facility of the run cannot be told
from rows that are counted per space; they are left out and a `location-unsupported` warning counts them by reason.
Locations never fail a run: if they cannot be read, none are offered and `locations-unavailable` is warned. The function's
group needs `locationFiltersAcl` read. 

### Access

| Group | Capabilities | Why |
|---|---|---|
| `gp_context_quality_function` (the identity the workflow runs as) | `projectsAcl` read and list, `groupsAcl` list, `sessionsAcl` create, `functionsAcl` read and write (library convention for a workflow identity); `dataModelsAcl` read on `cdf_cdm`, `cdf_idm` and the schema space; `dataModelInstancesAcl` **read** on `metricsSourceSpaces`; `timeSeriesAcl` read; `locationFiltersAcl` read; `streamRecordsAcl` **write** on the instance space | read the entities, datapoints and location filters, append records |
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

- **Version on the run marker.** The `run` marker may declare a `contractVersion`, and the dashboard reads it when
  present. The deployed `ContextQualityRelationship` container has no such property.
  Adding an optional property is additive, but whether Toolkit may add a property to a record container that
  already holds data has to be confirmed with `cdf deploy --dry-run` against a deployment that holds data before
  the property is added.
- **Default stream template.** The default is `BasicArchive`. Confirm that it is the right production default and
  size it for the run frequency (the retention and the filter window both come from the template).

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

Tests: `tests/test_context_quality_module.py` for the containers, views, stream and variables,
`tests/test_context_quality_deployment.py` for the workflow, trigger, groups and variables, and
`tests/test_context_quality_function_records.py` for what the function writes.

## Support

- Change the containers and views only additively, and run `uv run pytest tests/test_context_quality_module.py`.
- For troubleshooting Toolkit deployment, see [docs.cognite.com](https://docs.cognite.com) or
  `#topic-deployment-packs` on Slack.
