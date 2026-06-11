# File Annotation Dashboard

## Overview
This app is a React (Vite) dashboard for monitoring annotation quality, managing patterns and tracking the pipeline health.

Main pages:
- Annotation Quality
- Pattern Management
- Pipeline Health

## Main entry flow
1. Select an extraction pipeline.
- The list is filtered by a configured substring.
- Default filter is `file_annotation` (see `fetchExtractionPipelines` in `usePipelineConfig.ts`).
- In Refining, a common substring is `files-annotation`.
2. Choose a page card.
3. Click the action button to open the selected page.

## Page summary

### 1) Annotation Quality
Tabs:
- Overall
- Per-File

Highlights:
- Coverage KPIs and breakdowns.
- Per-file aggregation table and filters.
- Actual vs potential annotations by tag.
- Coverage distribution chart.
- File preview (bounding boxes), when available.

### 2) Pattern Management
Sections:
- Manual Patterns
- Import CSV
- Propose
- Refresh Cache

Highlights:
- Edit and persist manual patterns.
- Stage CSV proposals into manual patterns.
- Generate candidate patterns from automatic patterns.
- Rebuild cache used by the annotation pipeline.

### 3) Pipeline Health
Tabs:
- Overview
- File Explorer
- Run History

Highlights:
- Processing KPIs and throughput.
- File status table and log viewer.
- Run-level summaries and details.

## In-memory cache behavior
- Annotation Quality (Per-File) loads large datasets in memory.
- Switching pages/pipelines clears in-memory queries, but browser memory may not drop immediately.
- Use hard reload (`Ctrl+F5`) after leaving Per-File if you need memory to drop quickly.

## Running the App

### Prerequisites
Tools required in PATH:
- Node.js 20+
- npm 11+

### Platform setup

#### Windows
1. Install Node.js LTS from https://nodejs.org.

#### macOS
1. Install Node:
```bash
brew install node@20
```

#### Linux
1. Install Node (example with nvm):
```bash
curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
nvm install 20
nvm use 20
```

#### PATH configuration (no admin access)
If you cannot install system-wide, install for current user and add to PATH.

Windows (Registry):
1. Open Registry Editor.
2. Go to `HKEY_CURRENT_USER\Environment`.
3. Edit `Path` and add Node/npm install paths.

Common user install paths:
- Node: `%LOCALAPPDATA%\Programs\nodejs\`
- npm/npx global bin: `%APPDATA%\npm`

Verify:
```bash
node --version
npm --version
npx --version
```

### Install dependencies (required)
Before using Run and Debug or running any app command, install project dependencies:

```bash
npm install
```

Repeat this command whenever `package.json` or `package-lock.json` changes.

### Runtime Selection
- `cdf_local`: local app execution using token-proxy + CDF project.
- `cdf_host`: host-authenticated flow through Flows host app.
- `mock`: local app execution with mock data.

Set mode with `VITE_RUNTIME_MODE`.

### Option A: VS Code Run and Debug
Available configurations:
- `Flows | File Annotation Dashboard: Local Mode`
- `Flows | File Annotation Dashboard: Host Mode`
- `Flows | File Annotation Dashboard: Mock Mode`

#### A1) Local Mode
Run `Flows | File Annotation Dashboard: Local Mode`.

What it does:
- Runs task `file-annotation: start local stack (flows)`.
- Starts local credential proxy script (`node server/proxy.mjs`, via `npm run proxy`) and Vite (`npm start`).
- Opens `https://localhost:3001`.
- Injects `VITE_RUNTIME_MODE=cdf_local` from `launch.json`.

#### A2) Host Mode
Run `Flows | File Annotation Dashboard: Host Mode`.

What it does:
- Starts `npm start`.
- Injects `VITE_RUNTIME_MODE=cdf_host` from `launch.json`.

#### A3) Mock Mode
Run `Flows | File Annotation Dashboard: Mock Mode`.

What it does:
- Starts `npm start`.
- Injects `VITE_RUNTIME_MODE=mock` from `launch.json`.
- Does not start token-proxy.

VS Code env injection behavior:
- `launch.json` values override `.env` only for that process.
- `.env` on disk is not modified.

### Option B: Terminal

#### B1) Local CDF Mode (works without Flows host access)
1. Set `.env`:
- `VITE_RUNTIME_MODE=cdf_local`
2. Start local stack:
```bash
npm install
node scripts/start-local-stack.mjs
```

Manual split (optional):
```bash
npm install
npm run proxy
```
Then in a second terminal:
```bash
npm start
```

#### B2) Mock Mode (no external connections)
1. Set `.env`:
- `VITE_RUNTIME_MODE=mock`
2. Start dev server:
```bash
npm install
npm start
```

#### B3) Host Mode
1. Set `.env`:
- `VITE_RUNTIME_MODE=cdf_host`
2. Start dev server:
```bash
npm install
npm start
```
3. Open through CDF development URL (not plain localhost):
```text
https://{org}.fusion.cognite.com/{project}/flows-apps/development/{appExternalId}/3001
```

## Deploy to CDF (Flows)

### Important: CI/CD strategy for Flows apps
Flows App Hosting deployment is not handled by the same CI/CD path used for regular toolkit modules.

For this app, CI/CD must run a dedicated flow that:
- installs Node dependencies (`npm install`)
- builds the app (`npm run build`)
- runs App Hosting deploy commands (`npx @cognite/cli@latest apps ...`)
- exports deployment credentials required by `deploySecretName`

Recommended repository layout:
- keep Flows app folders outside the main `modules` deployment path
- configure a dedicated CI/CD pipeline/job specifically for Flows app build + deploy

Reason: treating a Flows app as a standard module usually causes several warning, since `flows` is not a dedicated 
folder to be deployed through toolkit.

### 1) Fill `app.json` (required)
Before running `npm start` in Host Mode or any deploy command, update `app.json`.

Required for all modes:
- `externalId`
- `deployments[0].org`
- `deployments[0].project`
- `deployments[0].baseUrl`

Interactive deploy defaults:
- `deployments[0].deployClientId` may be empty (`""`).
- `deployments[0].deploySecretName` may be empty (`""`).

Required for CI/CD deploy:
- `deployments[0].deployClientId`
- `deployments[0].deploySecretName`

If using Microsoft Entra ID:
- `deployments[0].idpType=entra_id`
- `deployments[0].tenantId=<tenant-id>`

CI/CD secret export example:
```bash
export <DEPLOY_SECRET_ENV_VAR_NAME>="<your-client-secret>"
```

### 2) Confirm deployment prerequisites
- Dataset `published-custom-apps` exists.
- Deploy identity has `apphosting:read` and `apphosting:write`.
- Add `apphosting:run` if same identity also runs apps.
- `package-lock.json` exists (`apps deploy` requirement).

### 3) Deploy commands
```bash
npx @cognite/cli@latest apps deploy --interactive
```

Versioning rule for redeploy:
- App Hosting does not allow overwriting an existing published `versionTag`.
- Before each redeploy, bump `versionTag` in `app.json` (for example `0.0.1` -> `0.0.2`).

Useful options:
```bash
npx @cognite/cli@latest apps deploy --interactive -d 0
npx @cognite/cli@latest apps deploy --interactive --skip-build
```

Lifecycle commands:
```bash
npx @cognite/cli@latest apps status . --interactive
npx @cognite/cli@latest apps publish . --interactive
npx @cognite/cli@latest apps activate . --interactive
```

### 4) Deploy troubleshooting
- Interactive sign-in fails:
```bash
npx @cognite/cli@latest apps deploy --interactive --org {org}
```
- `409 Version already exists`:
  - cause: current `versionTag` already exists (often in `PUBLISHED` lifecycle state)
  - fix: bump `versionTag` in `app.json` and run deploy again
- `Deployment secret not found`: confirm env var name matches `deploySecretName`.
- Build/deploy errors: run local build first:
```bash
npm run build
```

## Environment Configuration

### Create `.env`
```bash
cp env.app.properties .env
```
Windows PowerShell:
```powershell
Copy-Item env.app.properties .env
```

### Key environment variables
- `VITE_RUNTIME_MODE`
	- `cdf_local`: local CDF via token proxy
	- `cdf_host`: host-managed auth and CDF context
	- `mock`: local mock data
- `VITE_PROJECT_LABEL`
	- display label only

### Mode behavior
When `VITE_RUNTIME_MODE=cdf_host`:
- App uses Flows host SDK/auth.
- Local-only settings are ignored: `VITE_PROJECT_LABEL`.
- Not used in host auth: `VITE_TOKEN_PROXY_URL`.

When `VITE_RUNTIME_MODE=mock`:
- App uses local mock data (no token proxy required).
- App does not call CDF APIs or host authentication endpoints.

When `VITE_RUNTIME_MODE=cdf_local`:
- App uses local CDF via token proxy.

The Flows CLI reads `app.json` for org/project/baseUrl in hosted development and deployment.

### Local Mode with CDF data
When `VITE_RUNTIME_MODE=cdf_local`, configure:
- `VITE_TOKEN_PROXY_URL`
- `VITE_CDF_PROJECT`
- `VITE_CDF_URL`

Client credentials must be configured only in `token-proxy/.env` (see `token-proxy/env.local.proxy.properties`).
Token endpoint/scopes can be resolved from `token-proxy/.env` or inherited from app `VITE_*` values if provided.

### Local credential proxy script
This app uses local script `server/proxy.mjs` so credentials are not exposed to browser runtime.

Start with either command:
```bash
npm run proxy
# or
node server/proxy.mjs
```

Proxy target variable:
- `VITE_TOKEN_PROXY_URL`

Optional proxy settings:
- `TOKEN_PROXY_PORT`
- `HTTPS_PROXY`, `HTTP_PROXY`
- `DISABLE_TLS_VALIDATION`
- `NODE_EXTRA_CA_CERTS`

Local stack helper:
```bash
node scripts/start-local-stack.mjs
```

## Flows App Hosting Notes

### Infrastructure
This app uses `"infra": "appsApi"` in `app.json`.

### Capabilities
- Draft-first deployment lifecycle
- Versioned releases via `versionTag`
- App hosting capabilities (`apphosting:*`)
- Controlled deployment via `published-custom-apps` dataset

### Access control
- Users: `apphosting:read`, `apphosting:run`
- Deploy identity: `apphosting:write`

## Project Structure
- `src/pages`: page-level screens
- `src/components`: reusable UI and visual components
- `src/hooks`: data fetching and state helpers
- `src/shared`: shared utils/types/constants/domain logic
- `src/providers`: app-level SDK/auth/global providers
- `src/runtime`: auth and runtime mode helpers
- `src/mocks`: local mock data and mock helpers

## Notes
- `env.app.properties` lists expected app env keys/defaults.
- `token-proxy/env.local.proxy.properties` lists server-side credential keys/defaults.
- Keep credentials out of version control.
