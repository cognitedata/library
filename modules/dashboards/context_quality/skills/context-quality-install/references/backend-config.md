# Backend configuration

All variables live under `variables.modules.dashboards.context_quality` in `<org_dir>/config.<env>.yaml`.
`cdf modules add` copies them there from the module's `default.config.yaml`, which explains each one in a comment.
Full behaviour: the backend `REFERENCE.md`.

## `.env` (never in config files, never in chat)

| Variable | What it is | Who has it |
|---|---|---|
| `CQ_FUNCTION_GROUP_SOURCE_ID` | IdP object ID of the group whose members get the function group's access | IdP / Entra ID admin |
| `CQ_DASHBOARD_GROUP_SOURCE_ID` | IdP object ID of the group of dashboard users (may be the same group as above) | IdP / Entra ID admin |
| `CQ_FUNCTION_CLIENT_ID` | App registration the daily schedule runs as; must be a member of the function group | IdP / Entra ID admin |
| `CQ_FUNCTION_CLIENT_SECRET` | Its secret. The user pastes it into `.env`; check presence only | IdP / Entra ID admin |

Without them `cdf deploy` cannot create the groups or the schedule trigger. Toolkit 0.8 keeps the `${CQ_*}` placeholders
at build time, so an offline `cdf build` does not catch a missing value; check presence with `grep -c` instead.

## Variables to ask about

| Variable | Default | Change it when |
|---|---|---|
| `metricsSourceSpaces` | `[cdf_cdm, cdf_idm]` | Always on a new install: add the instance spaces holding assets, equipment, time series, files, maintenance. The function group can read only these |
| `metricsFacilityNames` | `'{}'` | The user wants display names: `'{"<space>": "<name>"}'` (JSON text) |
| `metricsTieringEnabled` | `false` | The user has an asset type-to-tier mapping |
| `metricsTierByTypeCode` | `'{}'` | Only with tiering on: `'{"<type code>": 1}'` (1, 2 or 3) |
| `metricsWorkflowSchedule` | `"0 4 * * *"` | Another daily time (cron) |
| `metricsWorkflowScheduleTimezone` | `UTC` | Cron in another time zone |
| `metricsStreamTemplate` | `BasicArchive` | Only on the first deploy. **Cannot change after the stream exists**; on an upgrade set it to the template the stream was created with |

## Resource names (keep the defaults unless upgrading or the project's naming rules say otherwise)

If you change any of the four marked with *, copy the value into the app's `.env.local` (`references/app-config.md`).

| Variable | Default |
|---|---|
| `metricsSchemaSpace` * | `sp_context_quality_metrics` |
| `metricsInstanceSpace` | `sp_dat_context_quality_metrics` |
| `metricsVersion` | `v1` |
| `metricsStreamExternalId` * | `context_quality_metrics` |
| `metricsFunctionExternalId` | `fn_context_quality_metrics` (the function folder is named after it; rename both together) |
| `metricsDatasetExternalId` | `ds_context_quality` |
| `metricsWorkflowExternalId` * | `wf_context_quality_metrics` |
| `metricsWorkflowVersion` * | `v1` |
| `metricsTriggerExternalId` | `wf_context_quality_metrics_schedule` |
| `metricsFunctionGroupName` | `producer_context_quality_function_dev` |
| `metricsDashboardGroupName` | `consumer_context_quality_dashboard_dev` |
| `creator_email` | `name@company.com` |

## Tuning (keep the defaults on a first install)

| Variable | Default | Change it when |
|---|---|---|
| `metricsShardTargetEntities` | `100000` | Shards time out: lower it (for one run, pass `"planning": {"shardTargetEntities": 25000}` instead) |
| `metricsShardTargetTimeSeries` | `20000` | Time series shards time out |
| `metricsMaxShards` | `150` | Rarely; the function rejects more than 190 |
| `metricsMaxConcurrentShards` | `2` | The project can run more calls at once (0 = all) |
| `metricsFreshnessDays` | `30` | "Fresh" means another window |
| `metricsMaxDepth` | `8` | Asset hierarchies are deeper |
| `metricsExportRetentionDays` | `30` | Keep the Excel workbook of unlinked items longer or shorter |
| `metricsDivisionTagPrefix`, `metricsRetiredTag` | `""` | The project tags divisions or retired assets |
| `metricsDefaultOwners`, `metricsOwnerPrefixRules` | `'{}'`, `'[]'` | The user wants owners on issues |
| `metricsExclusions` | all lists empty | The user wants synthetic time series, file categories or decommissioned equipment left out |

Mappings are JSON **text** in single quotes, because Toolkit reads a YAML mapping as a nested namespace.

## Upgrade (backend already deployed)

1. Set the resource names and `metricsStreamTemplate` to the deployed values. Ask the user; do not guess.
2. Build and dry run. Containers, views and the stream must show as **unchanged**. A stream create or delete means
   the names or template differ: stop and fix the variables. Never delete or recreate the stream.
3. Container and view changes must be additive only.

## Data-modeling-only projects

The function code is stored in a space. Add `space: <function space>` under `externalId` in
`<org_dir>/modules/dashboards/context_quality/functions/context_quality.Function.yaml`. The space must exist and the
deploying identity must be able to write to it (other function modules in the project show which space). A module
update overwrites the file, so re-add the line after updating.

## List candidate spaces

Read-only, prints space IDs only, and needs working Toolkit credentials in `.env`. It uses a private Toolkit API
(checked to import on 0.8.191): if it fails, do not debug it, ask the user to look in Fusion
(**Data management > Data modeling**) instead.

```bash
<PY> - <<'EOF'
from dotenv import load_dotenv
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

load_dotenv()
client = EnvironmentVariables.create_from_environment().get_client()
for space in client.data_modeling.spaces.list(limit=-1):
    print(space.space)
EOF
```

`<PY>` is `uv run python` in a uv project, otherwise the Python that has `cognite-toolkit` installed. Propose the spaces
that look like asset, equipment, time series or file spaces, and let the user pick. Never add a space the user did not
confirm.
