# Operations Summarizer

Fusion app that turns CDF operational data into short written summaries and optional podcasts, with a config node, a Cognite Function, and a helper agent.

## Overview

Operations teams use this module to summarize assets, files, work orders, time series, and equipment over a chosen period, keep that history in CDF, and compare a new run with a saved baseline.

Project settings live on one config node. The app bundle does not contain spaces, prompts, or limits. `cdf deploy` overwrites that node, including edits made in Fusion. Two installs of this app in one project are not supported.

Product background is in [PRD.md](./PRD.md), [SPEC.md](./SPEC.md), and [App-Brief.md](./App-Brief.md).

## Module Components

- **Config data model** — schema space `dm_sol_ops_summarizer_app`, data model `OpsSummarizerConfig_SOL`, view and container `OpsSummarizerConfig`, and the config node `ops_summarizer_config` in `inst_ops_summarizer_app_config`.
- **Summary space** — `ops_summaries`, for summary files, test reports, podcast files, and the function code upload. This space is shared with the Operations Dashboard. Do not `cdf clean` this module or `cdf deploy --drop` it. The space holds user data.
- **Function** — `Operations_Summarizer_Function_v2`. The code directory name matches that externalId. `handler.py` generates summaries. A call with `task: podcast` runs `podcast_handler.py` in the same directory.
- **Agent** — backup Atlas AI agent. Its externalId is `{{cdf_project}}_operation_summarizer_helper_agent`. The app (`src/hooks/useComparisonTest.ts`) and the function (`agent_utils.py`) build that same id from the CDF project name, so the prefix stays `cdf_project`.
- **Fusion app** — Vite/React app in `src/`, deployed with `@cognite/cli`.

These identifiers are literals in the YAML, in `src/config/configLocation.ts`, and in `app_config.py`. Do not rename them without a matching app and function release.

| Resource | Identifier |
| --- | --- |
| Schema space | `dm_sol_ops_summarizer_app` |
| Instance space | `inst_ops_summarizer_app_config` |
| Data model | `OpsSummarizerConfig_SOL` (version is `ops_config_version`) |
| View and container | `OpsSummarizerConfig` |
| Config node | `inst_ops_summarizer_app_config` / `ops_summarizer_config` |
| Summary space | `ops_summaries` |
| Function | `Operations_Summarizer_Function_v2` |

`schemaVersion` on the node is the content shape (currently `1`). Renaming or removing a key inside a section needs a `schemaVersion` bump and a matching app and function release. Bump `ops_config_version` only when a top-level property is added or changed, and bump the view and data model together.

Prompts, agent instructions, timezone offset, default period, and persona presets live on the config node. The function YAML `space: ops_summaries` is a deploy-time literal. Keep it equal to `ops_summaries_space`.

Likely-to-change values are in [default.config.yaml](./default.config.yaml). Set `ops_data_instances_space` to the instance space that holds the source data to summarize. `ops_agent_model` and `ops_agent_name` are shared by the config node and the agent. `limits.instanceQueryLimit` must stay from 1 to 1000.

This module ships a placeholder auth group so Toolkit accepts it. It does not grant summary or config access. Before production use, replace `auth/apps.operations-summarizer.Group.yaml` with groups that match how you scope read and write:

- **App users** need `dataModels:read` on `dm_sol_ops_summarizer_app` and `dataModelInstances:read` on `inst_ops_summarizer_app_config`.
- **Config editors** need `dataModelInstances:write` on `inst_ops_summarizer_app_config`. Summary writers must not receive write on that space.
- **The function caller** needs read on the config model and read/write on `ops_summaries`, plus `functions:read` and `functions:run` on `Operations_Summarizer_Function_v2`. Podcast generation uses the `ops_podcast_` external-ID prefix in that space.
- **The deploy principal** needs write on the schema space and the config instance space, and rights to create or update the function and the agent.

The config node holds no secrets.

## Deployment

### Prerequisites

- [Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/) 0.9.37 or later. Agents still need the alpha flag:

```toml
[alpha_flags]
agents = true
```

- Node.js 20.19+ and npm, for the Fusion app.
- A CDF project and a principal that can deploy data models, functions, and agents.

### Add to an existing Toolkit project

From the project directory:

```bash
cdf modules add tool
```

Select **Operations Summarizer**. Toolkit copies [default.config.yaml](./default.config.yaml) into your `config.<env>.yaml`. Set `ops_data_instances_space` to the source-data space, and set `cdf_project` to the CDF project name. The agent externalId is `{{cdf_project}}_operation_summarizer_helper_agent`, and the app builds that same id from the project name. Then build and deploy:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

Deploy this module before the Fusion app. The same app bundle can run in every project. Each project has its own config node.

### Start from scratch

```bash
cdf modules init .
cdf modules add tool
```

`cdf modules init` overwrites modules in the current directory. Run it in an empty project directory, or commit your project first. Enable the agents alpha flag, set `ops_data_instances_space`, then `cdf build` and `cdf deploy` as above.

### Deploy the Fusion app

Edit [app.json](./app.json). Set `deployments[].org`, `project`, and `baseUrl` for the target project, and provide the deploy client id and secret name that `@cognite/cli` expects. Then, from this directory:

```bash
npm ci
npm run deploy
```

`npm start` opens the app in Fusion against the project you are logged into. The first run may ask to install a locally trusted certificate.

Local function runs load `functions/Operations_Summarizer_Function_v2/.env` when that file exists (`CDF_PROJECT`, `CDF_URL` or `CDF_CLUSTER`, and either `CDF_TOKEN` or client-credentials variables). The config node must already be deployed.

```bash
python functions/Operations_Summarizer_Function_v2/run_local.py
```

From the repository root, the function tests use the repo dev environment (the function is not a uv workspace member yet):

```bash
uv run pytest modules/tools/apps/operations_summarizer/functions/Operations_Summarizer_Function_v2 -q
```

App checks, from this directory:

```bash
npm run type-check
npm run lint
npm test
npm run build
```

## Module Structure

```
operations_summarizer/
├── module.toml
├── default.config.yaml
├── auth/apps.operations-summarizer.Group.yaml
├── data_modeling/          # spaces, container, view, data model, config node
├── functions/
│   ├── operations_summarizer.Function.yaml
│   └── Operations_Summarizer_Function_v2/
├── agents/operation_summarizer.Agent.yaml
├── src/                    # Fusion app
├── app.json                # @cognite/cli app registration
├── manifest.json           # app network permissions
├── PRD.md
├── SPEC.md
└── App-Brief.md
```

## Support

- [Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/)
- [Cognite Hub](https://hub.cognite.com/)
- [PRD.md](./PRD.md), [SPEC.md](./SPEC.md), [App-Brief.md](./App-Brief.md)
