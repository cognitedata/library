# Work Package Generator

An AI-powered React application for generating work packages and industrial canvases from Cognite Data Fusion (CDF). Search for work orders, select from results, and let AI recommend templates and resources—then create or update a canvas in one flow.

## Quick Start

> **Deploy the toolkit folder first.** The app has no in-code defaults. It reads its configuration from a CDF config node (`inst_wpg_app_config` / `wpg_config`) that is created by the `toolkit/` folder. Without it, the app shows a setup screen instead of the workflow. From `toolkit/`, run `cdf build` and `cdf deploy` for your project (see `toolkit/modules/work_package_generator/README.md`), then run the app.

```bash
# 1. Deploy the config (once per project, and after every config change)
cd toolkit && cdf build && cdf deploy && cd ..

# 2. Run the app
npm install
npm start
```

Before certifying or sharing the app, fill in `App-Brief.md` with your own details (see [App Brief](#app-brief)).

The app opens in the browser (or in Cognite Fusion when running in the Flows host). On first run you may be prompted to install a locally-trusted SSL certificate for HTTPS dev server.

## Features

- **Workflow steps** – Five-step flow: Work Order → Template → Resources → Selections → Create, with a horizontal stepper and Back/Next navigation
- **Work order search** – Search by ID, name, or description; results shown in a table
- **Select before starting** – Pick a work order from the search results, then click “Use selected work order” to start the process
- **Reset and search again** – From the Work Order step, use Find to reset and run a new search to choose a different work order
- **AI template recommendation** – After a work order and asset are loaded, AI suggests a template canvas
- **AI resource selection** – AI selects resources (e.g. files, time series) per template field
- **Canvas creation** – Create a new canvas or update an existing one from the selected template and resources
- **Process loading bar** – Animated loading bar and message while searching, loading, or running AI
- **CDF integration** – Uses the Cognite Data Modeling (DMS) and CDF SDK via `@cognite/app-sdk` and `@cognite/sdk`
- **Modern UI** – React, TypeScript, Tailwind CSS, and `@cognite/aura` (select and slot primitives come from Radix)

## How to Use

1. **Search** – Enter a work order ID, name, or description and click **Find**.
2. **Select** – In the results table, click a row to select a work order. Click **Use selected work order** to start.
3. **Template** – After the asset and templates load, pick a template (AI recommendation is highlighted) or choose another.
4. **Resources** – Expand resource types to see available files and time series linked to the asset.
5. **Selections** – Review AI-selected resources per template field (or wait for AI to finish).
6. **Create** – Enter a canvas name, then **Create canvas** or **Update existing** if a canvas with that name already exists.

To switch work order mid-flow: go to the Work Order step, type a new query, and click **Find**. The process resets and the search table appears so you can select a different work order.

## Project Structure

```
├── src/
│   ├── api/                    # CDF and canvas API
│   │   ├── retrieve-data.ts    # Work orders, assets, templates, instances
│   │   ├── ai-agent.ts         # AI selection and recommendation
│   │   └── canvas-builder.ts   # Canvas create/update
│   ├── components/
│   │   ├── layout/             # Navigation
│   │   ├── ui/                 # Buttons, tables, accordion, etc.
│   │   └── work-package/       # Stepper, search bar, creation section
│   ├── app-config/             # Loads and validates the CDF config node
│   ├── contexts/               # Navigation context
│   ├── hooks/
│   │   └── useWorkPackage.ts   # Search, select, templates, AI, create
│   ├── pages/
│   │   ├── Home.tsx
│   │   └── WorkPackageGenerator.tsx
│   ├── store.ts                # Zustand store (work order, asset, steps)
│   ├── types.ts                # WorkOrder, Asset, TemplateInstance, etc.
│   └── utils/                  # CDF helpers, formatting, API
├── app.json                    # App identity and CLI deployment target
├── toolkit/                    # CDF spaces, WPGConfig schema, and config node
├── SPEC.md                     # User-visible behavior (source of truth)
├── PRD.md                      # Product goals, non-goals, metrics
└── package.json
```

## Key Technologies

- **React 19** – UI
- **TypeScript** – Typing
- **Vite** – Build and dev server
- **Tailwind CSS v4** – Styling
- **Zustand** – Global state (work order, asset, templates, selections)
- **TanStack React Query** – Data fetching and mutations
- **@cognite/sdk** – CDF client (instances, views)
- **@cognite/app-sdk** – Flows app hosting and Fusion auth
- **@cognite/aura** – Cognite design system (accordion, dialog, select, buttons). Aura’s stylesheet loads Inter, Space Grotesk, and Source Code Pro from Google Fonts; the app does not add a second font link. Select and slot primitives are Radix. Tables are local markup because Aura 1.47 has no Table export. Theme tokens are compiled into CSS at build time; a customer theme change needs a rebuild.
- **Canvas preview** – Industrial Canvas rendering, bundled under `src/canvas-preview/`. Those files are a vendored, read-only preview surface; see `src/canvas-preview/README.md` for why the large components are not split.
- **Vitest** – Tests
- **Biome** – Lint, format, and import order
- **ESLint** – Aura design-system rules only (`aura/no-overriding-styles`); Biome handles all other linting. ESLint runs through `scripts/eslint-ts6-preload.cjs` because `typescript-eslint` does not support TypeScript 7 yet, and `eslint` is pinned to the version `@cognite/aura` requires

## Scripts

| Command | Description |
|--------|-------------|
| `npm start` | Start dev server (HTTPS) |
| `npm run build` | TypeScript check and production build |
| `npm run preview` | Serve production build locally |
| `npm test` | Run tests |
| `npm run test:watch` | Tests in watch mode |
| `npm run lint` | Lint with Biome |
| `npm run lint:aura` | Check Aura design-system rules with ESLint |
| `npm run lint:fix` | Fix lint issues |
| `npm run format` / `npm run format:check` | Format with Biome / check formatting |
| `npm run type-check` | TypeScript check only |
| `npm run deploy` | Build and deploy (see below) |

## Configuration

- **Toolkit module** – Per-project settings live on one config node, `inst_wpg_app_config` / `wpg_config`. Deploy `toolkit/modules/work_package_generator` before the app. See that module's README for variables, access, and the overwrite rule. The same app bundle runs in every project.
- **`app.json`** – Identity and the Cognite CLI deployment target (`name`, `externalId`, `versionTag`, `infra`, `deployments`). The running app does not read this file. The shipped `deployments[0]` values are placeholders (`ORG`, `PROJECT`, `BASE_URL`, `CLIENT_ID`, `SECRET_NAME`). Replace each one by hand for the target project before deploying. The CLI does not fill them in from `.env`.
- **Environment** – Port and dev settings can be overridden via `.env` (e.g. `PORT`, `DEV_PORT`).

## Setup for a new CDF project

This section walks through taking the app from source control, pointing it at your Cognite organization and CDF project, satisfying data model and AI requirements, defining canvas templates, finding generated work packages (canvases) in CDF, and deploying the Flows app.

### 0. Deploy the toolkit folder

The app cannot start without its config node. Before running the app locally or deploying it, deploy the `toolkit/` folder to the target CDF project (`cdf build` and `cdf deploy`). Re-deploy it whenever you change config values. See [Configuration](#configuration).

### 1. Get the code from Git

1. Clone the repository that contains this app (for example the repo that includes the `work-package-generator` folder).
2. Open a terminal in the app directory:

   ```bash
   cd work-package-generator
   ```

3. Install dependencies (Node 22.12+ is required; CI uses Node 22):

   ```bash
   npm install
   ```

4. Optional: copy `.env.template` to `.env` if you use local overrides (for example `PORT` / `DEV_PORT`). The Vite dev server defaults are in `vite.config.ts`.

5. Make sure the toolkit folder is deployed (step 0), then start the app locally:

   ```bash
   npm start
   ```

   For a full production build and typecheck: `npm run build`.

### 2. Point the app at your organization and CDF project

Deploy the toolkit module first (`cdf build` and `cdf deploy` from `toolkit/`). That creates the config node. Then deploy the app. Project and cluster come from the signed-in CDF session. The Fusion org comes from the host page. Set `wpg_fusion_fallback_organization` only when the app is opened outside Fusion.

#### 2.1 CLI deployment (`deployments[0]`)

`app.json` `deployments` is for the Cognite CLI only. The running app does not read it.

| Field | Purpose |
|--------|--------|
| `org` | Fusion organization slug stored for the CLI deployment record. Example: `cog-demos`. |
| `project` | CDF project name you deploy the app to. |
| `baseUrl` | CDF API base URL for that cluster, e.g. `https://az-eastus-1.cognitedata.com`. |
| `deployClientId` | Service principal client ID for non-interactive deploy (replace the `CLIENT_ID` placeholder). |
| `deploySecretName` | Name of the secret holding the client secret in your CI environment. |

#### 2.2 App identity

- **`externalId`** — Unique application id in CDF/Fusion. Use a distinct value if you deploy multiple instances.
- **`name`**, **`description`**, **`versionTag`** — Shown in Fusion and tooling.

#### 2.3 AI agent

The Work Package Generator creates and uses an AI agent at runtime. The external id is `{project}_{agent.externalIdSuffix}` (shipped suffix: `work_package_generator_helper_agent`), unless `agent.existingAgentExternalId` is set on the config node. In that case the app uses that id and does not create an agent. Tools come from `agent.tools` (shipped `[]`). Agent instructions come from `agent.instructions` and may be an empty string. The toolkit agent YAML and the config node share the agent variables in `toolkit/config.<env>.yaml` (`modules.work_package_generator`).

### 3. Data model, spaces, and permissions

The app expects **Cognite Data Modeling** instances your users can **read** (and **write** for canvas creation). The shipped config node targets standard Cognite Core Data Model and Industrial Canvas views. Change those keys on the node when the customer model differs. See [Config keys](#config-keys).

#### 3.1 Work orders (search and selection)

- **View:** `dataModel.views.activity` (shipped: `cdf_cdm` / `CogniteActivity` / `v1`).
- Work orders are **node** instances of that view. The UI searches by external id, name, or description and reads linked **assets** from the activity’s `assets` relation.

**Your project must have** maintenance or work-order style activities on that view (or you must point `dataModel.views.activity` at the customer's view).

#### 3.2 Assets and equipment

- **Views:** `CogniteAsset` and `CogniteEquipment` in `cdf_cdm` (`v1`), used when resolving context and resources from the work order’s linked assets.

#### 3.3 Files and time series (resource step)

- **Views:** `CogniteFile`, `CogniteTimeSeries` in `cdf_cdm` (`v1`).
- Resources are discovered via relations from the asset (and, for time series, from the activity where applicable), as implemented in **`src/api/retrieve-data.ts`**.

#### 3.4 Industrial Canvas (templates and output)

- **Canvas definition** lives under `canvas.schemaSpace` (shipped: `cdf_industrial_canvas`). The root, item, and annotation views are `canvas.canvasView`, `instanceView`, and `annotationView`.
- **New canvases** (your work packages) are created as nodes in `canvas.instanceSpace` (shipped: `IndustrialCanvasInstanceSpace`).

If your tenant uses different spaces or view versions, update those `canvas` and `dataModel` properties on the config node and redeploy the toolkit module. A JavaScript rebuild is not required.

#### 3.5 Capabilities

`src/api` calls DMS `instances.search`, `instances.query`, `instances.retrieve`, `instances.upsert`, and `instances.delete`, plus `dataModels.list` for the config view version and `profiles.me()`. `src/api/ai-agent.ts` calls `POST /ai/agents`, `POST /ai/agents/byids`, and `POST /ai/agents/chat`. Names below match [DMS access control](https://docs.cognite.com/cdf/dm/dm_concepts/dm_access_control) and [Agents capabilities](https://docs.cognite.com/cdf/access/guides/capabilities). Shipped runtime is `1.2.0`, so `sessions:create` (required for agent runtime 2.0+) is not listed.

| Capability | Actions | Scope for the shipped config | Used for |
| --- | --- | --- | --- |
| `dataModels` | `READ` | spaces `cdf_cdm`, `cdf_industrial_canvas`, `cdf_apps_shared`, `dm_sol_wpg_app` | Schema of the views referenced by instance calls, and the WPG config data model |
| `dataModelInstances` | `READ` | spaces that hold activities, assets, files, time series, and template canvases, plus `inst_wpg_app_config` | Search, query, retrieve, and the config node |
| `dataModelInstances` | `WRITE` | `inst_wpg_app_config` | Config editors only. Keep this space out of other scoped write grants. |
| `dataModelInstances` | `READ`, `WRITE` | `IndustrialCanvasInstanceSpace`, `SolutionTagsInstanceSpace` | Canvas create/update/delete and solution-tag upsert |
| `agents` | `READ`, `WRITE`, `RUN` | `all` if the app must create `{project}_{suffix}`; otherwise that agent’s external id plus `WRITE` on `all` only when creation is required (scoped write cannot create) | List/retrieve, create, chat |
| `userProfiles` | `READ` | (confirm) usually granted to any group member | `profiles.me()` for the canvas owner |

The deploy service principal needs whatever the Cognite CLI deploy path requires. Confirm that set separately; this table is the signed-in app user.

### 4. AI agents

Template recommendation and per-field resource selection call the CDF AI Agents HTTP API (see **`src/api/ai-agent.ts`**):

1. **`getOrCreateAgent`** — Looks up the agent by external id. If `agent.existingAgentExternalId` is set, that id is used and a missing agent is an error. Otherwise, if `{project}_{agent.externalIdSuffix}` is missing, it creates an agent with the model, runtime, name, description, instructions, and tools from `agent`.
2. **`queryAiForSelection`** / canvas recommendation — POST to the agent chat endpoint.

**Requirements:**

- The signed-in user (or app identity) must be allowed to use **AI agents** in the project.
- If agent creation is blocked by policy, create an agent with external id **`{your_project}_{agent.externalIdSuffix}`** manually and grant access. Change `agent.model` when the customer should use a different model.

If AI is unavailable, the UI may still allow **manual** template and resource choices depending on your workflow; fix capability errors in Fusion/CDF IAM if calls fail with 403/401.

### 5. Creating canvas templates the app will list

The app discovers templates by listing **canvas** instances (Industrial Canvas roots) whose **`name` property starts with `templates.namePrefix`** (comparison is case-insensitive; shipped prefix: `template`).

**Practical steps:**

1. In **Fusion → Industrial Canvas** (or your standard canvas authoring flow), create a canvas document.
2. Set the canvas **name** to something like `template — Maintenance overview` or `template_work_package_v1` (must start with `template`).
3. Add **FDM instance** placeholders on the canvas for:
   - The **main** work order and asset representations (the app detects “main” slots by label heuristics in **`useTemplateFields`** / **`getTemplateCanvasInstances`**).
   - Additional slots whose **labels** correspond to resource types in `resourceTypes`. A slot is selectable when its view external id matches the view that resource type points at (shipped file and time series types use `CogniteFile` and `CogniteTimeSeries`).
4. Save/publish so the canvas exists as DMS instances your users can read.

**Changing the naming rule:** Set `templates.namePrefix` on the config node and redeploy the toolkit module. A JavaScript rebuild is not required.

### 6. Finding work packages (generated canvases) in CDF

In this app, a **work package** is a **created Industrial Canvas** instance, not a separate “WorkPackage” type.

**After creation:** The app builds a Fusion URL with **`generateCanvasLink`** (`src/api/canvas-builder.ts`):

`https://<org>.fusion.cognite.com/<project>/industrial-canvas/canvas?canvasId=<uuid>&cluster=<cluster>.cognitedata.com&workspace=industrial-tools`

Use the link from the success flow to open it directly.

**In Data modeling / APIs:**

- **Space:** `canvas.instanceSpace` (shipped: `IndustrialCanvasInstanceSpace`).
- **Root node:** `externalId` is a **UUID** returned as `canvasId` from create; the canvas **name** is the user-entered canvas name from the final step.
- Query **instances** on `canvas.canvasView` and filter by `name` or `externalId` as needed.

**In Fusion:** Industrial Canvas library / open by id using the same `canvasId` query parameter as in the generated link.

### 7. Deploy the app

#### 7.1 Interactive (local)

From the app root:

```bash
npm run deploy
```

This runs the Cognite CLI in interactive mode (see package.json: deploy). You can pass CLI flags supported by `@cognite/app-sdk` (for example target project or skip build).

Options:

- `-d 0` – Use first deployment target
- `-d <project-name>` – Use specific project
- `--skip-build` – Deploy without rebuilding

#### 7.2 CI / non-interactive

- Set **`deployClientId`** and **`deploySecretName`** in `app.json` to match your pipeline secrets.
- Provide the client secret in the environment under the name referenced by `deploySecretName`.
- Run the Cognite CLI without `--interactive` (there is no `deploy:ci` npm script):

  ```bash
  npx @cognite/cli@latest apps deploy
  ```

The included **`.github/workflows/ci.yml`** runs lint, tests, and build on `main` but does **not** deploy automatically; add a deploy job and CDF secrets if you want continuous deployment.

#### 7.3 Private npm packages

CI installs with `NODE_AUTH_TOKEN` (see workflow). Mirror that for any automated environment that pulls from a private registry.

### 8. Quick verification checklist

- [ ] Toolkit module is deployed, and app users can read `dm_sol_wpg_app` and `inst_wpg_app_config`.
- [ ] `app.json` `project` and `baseUrl` match the CLI deploy target. Fusion org comes from the host page.
- [ ] CDM activities exist and are searchable as work orders.
- [ ] Linked assets have related files/time series if you expect the Resources step to be useful.
- [ ] Industrial Canvas model is present; at least one canvas named with the `template` prefix appears in the Template step.
- [ ] User can create instances in `IndustrialCanvasInstanceSpace`.
- [ ] Config node is complete for this customer (views, canvas spaces, agent model and suffix).
- [ ] AI agent API works or you created `{project}_{agent.externalIdSuffix}` manually.
- [ ] After create, Fusion opens the canvas using the host org (or `fusion.fallbackOrganization`) and the returned `canvasId`.

For day-to-day usage of the wizard itself, see [How to Use](#how-to-use).

## Config keys

Edit the toolkit config node (`toolkit/modules/work_package_generator/data_modeling/wpg_config.Node.yaml`, with values from `toolkit/config.<env>.yaml`) and redeploy. A JavaScript rebuild is not required. Keys below are the node properties. You can add extra entries under `dataModel.views` and `resourceTypes`. A resource type `view` value must be a key in `dataModel.views`. `limits.instancePageSize`, `upsertBatchSize`, and `deleteBatchSize` must be integers from 1 to 1000. `schemaVersion` must be `1`.

| Key | Change per customer? | Shipped value / role |
| --- | --- | --- |
| `deployments[0].org`, `.project`, `.baseUrl` | Yes, for the CLI | Fusion org, CDF project, and API base URL recorded for deploy. The running app does not read them. |
| `deployments[0].deployClientId`, `.deploySecretName` | Yes | Non-interactive deploy credentials. |
| `dataModel.views.asset` | If the model differs | `cdf_cdm` / `CogniteAsset` / `v1` |
| `dataModel.views.equipment` | If the model differs | `cdf_cdm` / `CogniteEquipment` / `v1` |
| `dataModel.views.activity` | If the model differs | `cdf_cdm` / `CogniteActivity` / `v1` (work orders) |
| `dataModel.views.timeSeries` | If the model differs | `cdf_cdm` / `CogniteTimeSeries` / `v1` |
| `dataModel.views.file` | If the model differs | `cdf_cdm` / `CogniteFile` / `v1` |
| `dataModel.views.solutionTag` | If the model differs | `cdf_apps_shared` / `CogniteSolutionTag` / `v1` |
| `dataModel.activityContainer` | If the model differs | Container used to traverse from an activity (`cdf_cdm` / `CogniteActivity`) |
| `canvas.schemaSpace` | If the model differs | `cdf_industrial_canvas` |
| `canvas.instanceSpace` | If the space differs | `IndustrialCanvasInstanceSpace` |
| `canvas.canvasView` | If the model differs | `Canvas` / `v1` |
| `canvas.instanceView` | If the model differs | `FdmInstanceContainerReference` / `v1` |
| `canvas.annotationView` | If the model differs | `CanvasAnnotation` / `v1` |
| `canvas.dimensions` | Rarely | Default item box: x 0, y 0, width 600, height 400, max 1200 |
| `canvas.userId` | Rarely | Canvas owner fallback: `work-package-generator` |
| `canvas.visibility` | Rarely | `"public"` or `"private"` on the canvas root |
| `canvas.style` | Rarely | Sticky header colors and border on the base template |
| `resourceTypes.*` | If relations differ | `activity`, `asset`, `equipment`, `file`, `timeseries`. Each has `displayName`, `view` (a view key), `properties`, `assetRelationProperty`, `assetRelationIsArray`. Time series also has `activityContainerIdentifier`. |
| `context.workOrder.properties` | If prompts should use other fields | Property names sent to the agent for the work order |
| `context.asset.properties` | If prompts should use other fields | Property names sent to the agent for the asset |
| `agent.model` | Yes | `cognite/fast` |
| `agent.runtimeVersion` | If the agent runtime differs | `1.2.0` |
| `agent.externalIdSuffix` | Yes, if the agent id should differ | Runtime id is `{project}_{suffix}` |
| `agent.name`, `.description`, `.instructions` | Yes | Create-agent payload. Instructions may be `""`. |
| `agent.existingAgentExternalId` | Optional | When set, use this agent and do not create one. |
| `agent.tools` | Rarely | Tools on the create payload. Shipped `[]`. |
| `fusion.hostSuffix` | If the Fusion host differs | Shipped `.fusion.cognite.com`. Includes the leading dot. |
| `fusion.canvasWorkspace` | If the canvas workspace differs | Shipped `industrial-tools`. |
| `fusion.fallbackOrganization` | When the app is not opened from Fusion | Optional org slug. Leave empty inside Fusion. |
| `workOrderProperties` | If activity property names differ | Maps CDF property names onto the work order fields the app reads, filters, and sorts. |
| `prompts.resourceSelection`, `.templateSelection` | If the agent wording should change | Prompt text with `{{placeholder}}` tokens. Missing tokens fail at startup. |
| `ui` | If labels or filter options differ | Status and priority options, instruction examples, terms (`workOrder`, `workPackage`, `functionalLocation`), and the five stepper labels. |
| `templates.namePrefix` | If template names use another prefix | `template` |
| `templates.baseTemplate` | Rarely | Built-in “All Resources” template. `id` `__base_all_resources__` is synthetic and is not stored in CDF. |
| `solutionTag` | If the tag should differ | Instance `SolutionTagsInstanceSpace` / `wpg-work-package` / `Work Package` |
| `limits.*` | Rarely | Page, batch, search, and scan caps. The three DMS batch sizes must stay within 1000. |
| `cluster.apiHostSuffix` | If the API host is not Cognite’s | `cognitedata.com` |
| `cluster.hostAliases` | Rarely | Host that is not `{clusterId}.cognitedata.com`. Shipped map sends `api.cognitedata.com` to cluster id `api`. Leave it unless the API host does not follow that pattern. |

## Known limitations

- **Private-link orgs.** `fusionOrganizationFromHostname` only matches `fusion.hostSuffix` (shipped `.fusion.cognite.com`). A private-link or custom Fusion host does not yield an org unless that suffix is changed; the app then uses `fusion.fallbackOrganization` when it is set.
- **Theming.** Aura tokens are compiled into CSS at build time. Per-customer colors need a rebuild.
- **Fonts.** `@cognite/aura` `styles.source.css` imports Inter, Space Grotesk, and Source Code Pro from `fonts.googleapis.com`. `manifest.json` keeps those network permissions so the host CSP allows that request. The app does not ship the font files.

## Requirements and Design

- **PRD** – See `PRD.md` in the project root for purpose, scope, and requirements.
- **Spec** – See `SPEC.md` for user-visible behavior. Deploy the `toolkit/` folder first. Deploy and CDF prerequisites are README §0 and §3–4.

## App Brief

`App-Brief.md` ships with template values (for example the `<ootb-apps-maintainer alias>` placeholder, empty expected users, business value, and milestones, and "unset" support path). **Update it with your own information** (customer, tier, owner, maintainer, support path, users, value, milestones, user role, problem, and success criteria) before you certify or hand over the app. Changing the maintainer alias after certification triggers re-certification.

## Troubleshooting

- **SSL on first run** – Accept the certificate prompt for local HTTPS.
- **Port in use** – Set `PORT=3002` (or another port) in the environment or `.env`.
- **Build fails** – Run `npm run type-check` and fix TypeScript errors; if needed, `rm -rf node_modules package-lock.json` and `npm install` again.
- **Setup screen on start** – The config node is missing or invalid. Deploy the `toolkit/` folder to this project and check the listed problems.
- **Auth** – Ensure you are logged into the correct CDF project and that the app’s deployment target in `app.json` matches.

## Resources

- [Cognite Data Fusion](https://docs.cognite.com)
- [Cognite SDK (TypeScript)](https://docs.cognite.com/sdks/typescript/)
- [Cognite App SDK](https://www.npmjs.com/package/@cognite/app-sdk)
- [React](https://react.dev) · [Vite](https://vitejs.dev) · [Tailwind CSS](https://tailwindcss.com)

---

Built with React, TypeScript, and Cognite Data Fusion.
