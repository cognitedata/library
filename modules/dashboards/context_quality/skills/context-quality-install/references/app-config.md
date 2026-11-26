# App configuration

Two files in the app folder decide where the app runs and which backend it reads. Full instructions: the app
`README.md`, sections 1 to 3.

## `app.json`

| Key | Set it to |
|---|---|
| `externalId` | Keep `context-quality-dashboard`. The backend's dashboard group grants App Hosting access to this ID |
| `versionTag` | Raise it for every release (`0.0.1` -> `0.0.2`) |
| `deployments[0].org` | The Fusion organisation (the subdomain of the Fusion URL) |
| `deployments[0].project` | The CDF project; usually `environment.project` in `config.<env>.yaml` |
| `deployments[0].baseUrl` | `https://<cluster>.cognitedata.com` |
| `deployments[0].deployClientId` | Empty for interactive deploys. A client ID only for CI or non-interactive deploys |
| `deployments[0].deploySecretName` | The **name** of the environment variable that holds the deploy client's secret (non-interactive only). Never the secret itself |

The first entry in `deployments` is used by `npm run dev` and by `apps deploy` unless `-d` picks another.

## `.env.local`

Copy `.env.example`. Keep the defaults unless the backend's names changed.

| Variable | Default | Backend variable |
|---|---|---|
| `VITE_QUALITY_BACKEND` | `cdf` | `mock` shows sample data with a "Demo mode" banner |
| `VITE_CQ_STREAM_ID` | `context_quality_metrics` | `metricsStreamExternalId` |
| `VITE_CQ_SCHEMA_SPACE` | `sp_context_quality_metrics` | `metricsSchemaSpace` |
| `VITE_CQ_WORKFLOW_ID` | `wf_context_quality_metrics` | `metricsWorkflowExternalId`. Leave out with the version for a read-only app (no run buttons) |
| `VITE_CQ_WORKFLOW_VERSION` | `v1` | `metricsWorkflowVersion` |
| `VITE_CQ_WINDOW_DAYS` | unset (stream limit) | Most days of history to read, 1 to 365 |
| `VITE_CQ_AUTO_START_FIRST_RUN` | unset (off) | `true` starts the built-in checks on the first visit |

Vite inlines these at build time: they must be set when running `npm run dev`, `npm run build` and `apps deploy`. In
CI, give the build step the same `VITE_*` values. For several environments, use `.env.<name>` and `vite --mode <name>`.

## Commands

| Purpose | Command | Who runs it |
|---|---|---|
| Install dependencies | `npm install` | agent |
| Trusted local HTTPS (once per machine) | `npx @cognite/cli apps setup-https` | user (`!`) |
| Sign in | `npx @cognite/cli auth login` | user (`!`) |
| Run in Fusion | `npm run dev` (port 3004, fixed) | user, keeps running |
| Sample data | `npm run dev:mock` | user |
| Checks | `npm test && npm run build` | agent |
| Upload a DRAFT | `npx @cognite/cli apps deploy --interactive` | user (`!`) |
| DRAFT -> PUBLISHED | `npx @cognite/cli apps publish . --interactive` | user (`!`), after "yes" |
| Make it the active version | `npx @cognite/cli apps activate . --interactive` | user (`!`), after "yes" |
| State | `npx @cognite/cli apps status .` | agent |
