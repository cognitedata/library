# Contextualization Quality Dashboard

A Cognite Fusion app that shows how well a CDF project's data is **contextualised**: how completely assets,
equipment, time series, maintenance records, files, P&ID annotations and 3D objects are linked to each other.

## Overview

Contextualisation gaps are hard to see across millions of instances. This app turns them into scores per
dimension, trends over time, and lists of the exact items to fix.

**It is only half of the solution.** The app reads results; it does not compute them. The numbers come from the
backend module [`dashboards/context_quality_v2`](../../../dashboards/context_quality_v2/README.md), a function and
workflow that write a run log to a Records stream. **Deploy the backend first, then this app.**

```
backend (context_quality_v2)  --writes-->  Records stream  --read by-->  this app (in Fusion)
```

## What it shows

| Page | What you get |
|---|---|
| **Overview** | One card per dimension with its headline score and trend, plus your own *configured metrics* |
| **Dimensions** | The metrics of one dimension; open a metric to list the items behind it (first 200) |
| **Issues** | Patterns behind the gaps, for example the same missing link on many items |
| **Configuration** | Upload a YAML or JSON file naming your own data-model links and run the backend with it |
| **Download unlinked (Excel)** | Every unlinked item of the latest run, one sheet per metric |

Filter by **facility** (an instance space) or **location** (a Fusion location filter). A score is always
*items that pass ÷ items that apply*, shown as a percentage.

## What each metric means

| Dimension | Measures | Headline metric |
|---|---|---|
| **Asset Hierarchy** | Is every asset placed in the tree? | Hierarchy completion: assets with a parent ÷ assets that are not roots |
| **Equipment-Asset** | Is equipment tied to an asset, with usable metadata? | Association rate: equipment linked to an asset ÷ all equipment |
| **Time Series** | Is each time series tied to an asset and still alive? | Time series to asset rate: linked ÷ all |
| **Maintenance Workflow** | Do work orders and notifications point to each other and to assets? | Work order to asset rate: linked ÷ all work orders |
| **File Annotation** | Are diagram annotations confident and approved? | Average confidence, approval mix |
| **3D Model** | Are 3D objects tied to assets? | 3D to asset rate: linked ÷ all objects |
| **Files** | Are files tied to assets and described? | File to asset rate: linked ÷ all files |

Each dimension also has **coverage** metrics (assets that have at least one linked item, for example time series),
**completeness** metrics (a field is filled in, for example serial number or bounding box) and **freshness**
(updated within `metricsFreshnessDays`, 30 by default).

Terms:

- **Run**: one measurement of the project. "Current" is the latest *completed* run; unfinished runs are never shown.
- **Facility**: an instance space that holds entities to measure.
- **Tier**: an optional 1-3 importance grade for assets, from the project's own type-to-tier mapping. Off by default.
- **Excluded**: items left out before counting (synthetic time series, decommissioned equipment, rules you add). Each
  exclusion is counted and shown, so nothing disappears silently.
- **Configured metric**: a link in *your* data model (`space:externalId:version` + `View.property`), counted as set or not set.
- **Not applicable**: the metric could not be measured for that scope. It is shown as N/A, not as 0%.

## Deployment

### Prerequisites

- The backend [`context_quality_v2`](../../../dashboards/context_quality_v2/README.md) is deployed and has completed
  at least one run. Until then the app shows a first-run screen where you start one (see "Start a run").
- Node.js 20+ and npm.
- A Fusion user in the target project with the backend's `gp_context_quality_dashboard_users` group (read access
  to the stream, schema and the workbook files). To start runs from the app, the user also needs permission to run workflows
  (see the backend README, "Running with a config file").

### 1. Point the app at your project

The app has no project hard-coded. Two files decide where it runs:

**`app.json`**: your Fusion organisation and CDF project. The first entry in `deployments` is used for local
development; `apps deploy` uses it too unless you pick another with `-d`.

```json
"deployments": [{
  "org": "<your Fusion organisation>",
  "project": "<your CDF project>",
  "baseUrl": "https://<your-cluster>.cognitedata.com",
  "deployClientId": "",
  "deploySecretName": "<ENV_VAR holding the deploy client secret, only for non-interactive deploys>"
}]
```

**`.env.local`**: which backend to read. Copy `.env.example` and keep the defaults unless you renamed resources
in the backend's `config.<env>.yaml`.

| Variable | Default (backend defaults) | Meaning |
|---|---|---|
| `VITE_QUALITY_BACKEND` | `cdf` | `cdf` reads the backend; `mock` shows sample data with a "Demo mode" banner |
| `VITE_CQ_STREAM_ID` | `context_quality_metrics` | the Records stream (`metricsStreamExternalId`) |
| `VITE_CQ_SCHEMA_SPACE` | `sp_context_quality_metrics` | the schema space (`metricsSchemaSpace`) |
| `VITE_CQ_WORKFLOW_ID`, `VITE_CQ_WORKFLOW_VERSION` | `wf_context_quality_metrics`, `v1` | the workflow behind **Run with this file**. Leave both out for a read-only app |
| `VITE_CQ_WINDOW_DAYS` | stream limit, max 90 | most days of history to read |
| `VITE_CQ_AUTO_START_FIRST_RUN` | off | `true` starts the built-in checks by itself on the first visit |

Vite inlines these at build time, so they must be set when you run `npm run dev` or deploy. For several
environments, create `.env.<name>` and run `vite --mode <name>`.

### 2. Run locally in Fusion

```bash
npm install
npx @cognite/cli apps setup-https   # once: trusted local HTTPS certificate (mkcert)
npx @cognite/cli auth login         # sign in to your project
npm run dev                         # https://localhost:3004, opens your project in Fusion
```

`npm run dev` prints a **Fusion** URL built from `app.json` and opens it. Always use that URL (not bare
`localhost`); Fusion loads your local server inside the project, with your own sign-in. No backend yet? Run
`npm run dev:mock` for sample data.

| Problem | Fix |
|---|---|
| "Connection timed out" in Fusion | Keep `npm run dev` running; run `setup-https`; use the URL it printed |
| Port 3004 in use | Stop the other process; the port is fixed because Fusion embeds it in the URL |
| Configuration screen instead of the app | `.env.local` is missing or `VITE_CQ_*` are empty |
| "No run yet" | No completed run exists. Start one from the dashboard (see "Start a run") or in CDF |
| A run failed right after deploying the backend | The function was still building. Wait until it shows as ready in Fusion, then run again |

### Start a run

The dashboard never starts a run by itself. The app needs `VITE_CQ_WORKFLOW_ID` and `VITE_CQ_WORKFLOW_VERSION`
(set in `.env.example`) and a user who may run workflows.

- **First run:** on a project with no completed run, the Configuration tab opens a first-run screen. Press
  **Run built-in checks**. The status shows *Running*, and the results appear in Overview when it completes.
- **Later runs:** on the Overview and Dimensions pages, **Refresh** re-reads the latest results; use the run button on a
  dimension page to re-measure that dimension. In **Configuration**, upload a YAML or JSON file of your own
  `space:externalId:version` models and `View.property` links, then press **Run with this file**.
- **If it fails:** open **Run notes** for the reason. A run started right after deploying the backend can fail
  because the function is still building; wait until it is ready and press **Run built-in checks** again.

The run executes as you, so you need `workflowOrchestrationAcl` write and `sessionsAcl` create, plus
what the backend's function group holds (see the backend README, "Running with a config file").
Set `VITE_CQ_AUTO_START_FIRST_RUN=true` only if you want the first visit to start the built-in checks itself.

### 3. Deploy and publish to a CDF project

```bash
npm test && npm run build                 # checks, with the same .env.local
npx @cognite/cli apps deploy --interactive   # build and upload as a DRAFT version
npx @cognite/cli apps publish . --interactive   # DRAFT -> PUBLISHED
npx @cognite/cli apps activate . --interactive  # make it the active version users open
```

Without `--interactive`, `deploy` uses the client credentials named in `app.json` (`deployClientId`,
and the environment variable named by `deploySecretName`). Raise `versionTag` in `app.json` for each release.
`apps status` shows the state of the current version. In CI, give the build step the `VITE_*` variables: a build
without them only shows the configuration screen.

## Module Structure

```
context_quality_dashboard/
├── src/
│   ├── features/       # overview, dimension, issues, configuration, runs, shell
│   ├── services/       # CDF Records, workflow and export clients
│   ├── i18n/           # English and Japanese text
│   └── mock/           # sample data for dev:mock and tests
├── app.json            # Fusion app + deployment targets
├── manifest.json       # network permissions
├── .env.example        # backend configuration
├── module.toml
└── CLAUDE.md           # coding standards for this app
```

Develop with `npm test`, `npm run lint` and `npm run build`.

## Support

- Backend, run log and access groups: [`dashboards/context_quality_v2`](../../../dashboards/context_quality_v2/README.md).
- Fusion apps CLI: `npx @cognite/cli apps --help`.
- Questions: `#topic-deployment-packs` on Slack.
