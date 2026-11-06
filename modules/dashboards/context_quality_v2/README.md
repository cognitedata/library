# Contextualization Quality Dashboard: backend

The backend of the Contextualization Quality Dashboard: a scheduled workflow that measures how well a CDF
project's data is contextualised and stores the results as an immutable run log.

## Overview

The dashboard needs numbers: for each dimension (asset hierarchy, equipment, time series, maintenance, files,
annotations, 3D), how many items are linked and how many are not. Counting that over millions of instances takes
more than one function call, so this module counts in parallel, writes raw counts to a Records stream, and lets the
dashboard divide them.

**This module is half of the solution.** The app that shows the results is
[`tools/apps/context_quality_dashboard`](../../tools/apps/context_quality_dashboard/README.md). It also holds the
description of what the dashboard shows and what every metric means. **Deploy this module first, then the app.**

```
workflow (daily) -> function (plan, shards, finalize, export) -> Records stream -> dashboard app
```

## Module Components

| Resource | What it is |
|---|---|
| Function `fn_context_quality_metrics` | Reads the Core and maintenance views, counts them, writes the run |
| Workflow `wf_context_quality_metrics` + schedule trigger | `plan`, then one task per shard in parallel, `finalize`, then `export`. Runs daily at 04:00 UTC |
| Stream `context_quality_metrics` | The Records stream that holds the run log (append-only) |
| Container and view `ContextQualityRelationship`, two spaces | Schema space for the definitions, instance space for the rows |
| Data model `ContextQualityMetrics` | Lists the view |
| Data set `ds_context_quality` | Holds the function code and the Excel workbook of unlinked items |
| Groups `gp_context_quality_function`, `gp_context_quality_dashboard_users` | The workflow identity (reads data, writes records) and the dashboard readers (read only) |

How it works, the run log format and every option are in [`REFERENCE.md`](REFERENCE.md).

## Deployment

### Prerequisites

- Cognite Toolkit 0.8 or later, and a Toolkit project with a `cdf.toml`.
- The `record_views` alpha flag in `cdf.toml` (the containers are record containers):

  ```toml
  [alpha_flags]
  record_views = true
  ```

- A deploying identity with `dataModelsAcl` and `streamsAcl` (plus the usual Toolkit access to groups, data sets,
  functions and workflows).
- Two identity-provider groups (function and dashboard users) and a client in the function group, for the schedule.

### Step 1: Add the module

```bash
cdf modules init .      # in a new project; otherwise: cdf modules add .
```

Select **Dashboards** > **Contextualization Quality Dashboard — Records & Streams Backend**. Toolkit adds `context_quality_v2` to
your `config.<env>.yaml`.

### Step 2: Set the environment values

Add to `.env` (nothing secret is stored in the module):

| Variable | What |
|---|---|
| `CQ_FUNCTION_GROUP_SOURCE_ID` | IdP id of the group whose members get the function group's access |
| `CQ_DASHBOARD_GROUP_SOURCE_ID` | IdP id of the group of dashboard users |
| `CQ_FUNCTION_CLIENT_ID`, `CQ_FUNCTION_CLIENT_SECRET` | Client the schedule runs as. It must be a member of the function group |

### Step 3: Configure `config.<env>.yaml`

Only these need attention; the rest have working defaults (full list in [`REFERENCE.md`](REFERENCE.md#variables)).

| Variable | Set it to |
|---|---|
| `metricsSourceSpaces` | `cdf_cdm`, `cdf_idm` **plus the instance spaces that hold your assets, equipment, time series, files and so on**. The function can only read these |
| `metricsFacilityNames` | Optional display names, `'{"<space>": "<name>"}'` |
| `metricsTieringEnabled`, `metricsTierByTypeCode` | Optional importance tiers, off by default |
| `metricsWorkflowSchedule` | Cron for the daily run (default `0 4 * * *`, UTC) |
| `metricsStreamTemplate` | Stream template, default `BasicArchive`. **Cannot be changed after the stream exists** |

The names the dashboard needs to know are `metricsStreamExternalId`, `metricsSchemaSpace`, `metricsWorkflowExternalId` and
`metricsWorkflowVersion`. Keep the defaults, or copy your values into the dashboard's `.env.local`.

> **Data-modeling-only project?** Function code is then stored in a space, so add `space: <your function space>` to
> `functions/context_quality.Function.yaml`. See [`REFERENCE.md`](REFERENCE.md#data-modeling-only-projects-name-the-function-space).

> **Backend already deployed?** Set the variables to the existing names and stream template, and check that the dry
> run shows the containers, views and stream as unchanged. Never recreate the stream.

### Step 4: Build and deploy

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

### Step 5: Run it once and check

The first scheduled run follows the next trigger time. To get data now, start the workflow in Fusion (**Data
management > Data workflows > `wf_context_quality_metrics` > Run**) with the input `{"extraLinks": [], "scope": "all"}`.

A run is done when its last task, `finalize`, succeeds. Only then does the dashboard show it; a failed run is never
shown and the previous one stays current. Then deploy the
[dashboard app](../../tools/apps/context_quality_dashboard/README.md) and give your users the dashboard group.

## Module Structure

```
context_quality_v2/
├── auth/                  # function group and dashboard-users group
├── data_modeling/         # spaces, container, view, data model
├── data_sets/
├── functions/
│   ├── context_quality.Function.yaml
│   └── fn_context_quality_metrics/   # handler.py, cq/, tests
├── streams/
├── workflows/             # workflow, version, schedule trigger
├── default.config.yaml    # all variables and their defaults
├── module.toml
├── README.md
└── REFERENCE.md           # how it works, run log format, every option
```

Function tests: `uv run pytest modules/dashboards/context_quality_v2/functions/fn_context_quality_metrics/ --rootdir=. -q`

## Support

- The dashboard that reads this backend: [`tools/apps/context_quality_dashboard`](../../tools/apps/context_quality_dashboard/README.md).
- Change the container and view only additively (a deployed property keeps its definition; new ones are optional).
- Toolkit help: [docs.cognite.com](https://docs.cognite.com) or `#topic-deployment-packs` on Slack.
