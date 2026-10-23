# Feature Specification: Work Package Generator

**App**: Work Package Generator (`work-package-generator`)  
**Status**: Current product  
**Related**: [PRD.md](./PRD.md) (goals, non-goals, success metrics), [README.md](./README.md) §3–4 (data model, permissions, and AI agents)

This file is the source of truth for **user-visible behavior**. Update it when workflow, CDF reads/writes, or AI agent behavior changes. Implementation details live in code; product intent and constraints live here.

---

## Overview

The Work Package Generator is an AI-assisted Cognite Fusion (Flows) app that builds an Industrial Canvas work package from a work order. The user searches for a work order, confirms a canvas template, reviews related CDF resources, reviews AI slot assignments, then creates or updates a canvas and opens it in Fusion.

Without the app, planners must find the work order, hunt for files and time series, open Industrial Canvas, and place resources by hand. The app replaces that with a gated five-step flow.

---

## Personas

- **Maintenance planner** — prepares a work package for field technicians.
- **Operations engineer** — assembles context for planned or reactive work.
- **Solution architect** — deploys the app and template canvases into a customer CDF project.

Users know work-order language (IDs, descriptions, assets). They do not need to author Industrial Canvas or call CDF APIs.

---

## Out of scope

- Creating or editing canvas templates (templates are authored in Industrial Canvas; names must start with `template`, case-insensitive).
- Creating, editing, or bulk-processing work orders (read-only `CogniteActivity`).
- Canvas approval, versioning, or audit workflows.
- Provisioning CDM or Industrial Canvas data models in the tenant.

---

## User scenarios and testing

### User Story 1 — Search and commit a work order (Priority: P1)

As a planner, I want to search work orders by ID, name, or description and commit one, so the app can load its linked asset and start the package.

**Independent test**: Search, select a row, confirm. The Work Order step shows the selected activity and the first linked asset.

**Acceptance scenarios**:

1. **Given** the Work Order step, **When** I search, **Then** results are a paginated table (20 per page) with external ID, name, description, and status, using CDF `instances.search` (exact ID and full-text).
2. **Given** results, **When** I select a row and confirm, **Then** the app loads the first linked asset from `CogniteActivity.assets[]` and the workflow can advance.
3. **Given** a committed work order, **When** I search again from the Work Order step, **Then** the flow resets so I can pick a different work order.
4. **Given** no work order yet, **When** I try later steps, **Then** those steps stay gated until I commit a work order.

### User Story 2 — Choose a template (Priority: P1)

As a planner, I want a recommended Industrial Canvas template with the option to pick another, so the package structure matches the work type.

**Independent test**: After a work order and asset load, templates appear; one is marked as the AI recommendation; continuing commits the choice.

**Acceptance scenarios**:

1. **Given** templates in `cdf_industrial_canvas` / `Canvas` / `v1`, **When** the Template step loads, **Then** only canvases whose name starts with `template` (case-insensitive) are listed.
2. **Given** work order and asset context, **When** the helper agent returns a recommendation, **Then** that template is visually distinguished and I can still select a different one.
3. **Given** a selected template, **When** I have not confirmed the Template step, **Then** later steps stay gated and template field structure is not treated as committed.
4. **Given** AI recommendation fails, **When** templates still load, **Then** I can choose a template manually and continue.

### User Story 3 — Review and adjust resources (Priority: P1)

As a planner, I want to see files, time series, and other configured resource types linked to the asset (and optional activity links), so I know what the AI can assign.

**Independent test**: Confirm Template, open Resources, expand types, add or remove candidates, then continue.

**Acceptance scenarios**:

1. **Given** a confirmed template, **When** Resources loads, **Then** instances are discovered via each type’s asset relation (and optional activity container identifier, e.g. `timeSeries` on the activity).
2. **Given** large result sets, **When** discovery runs, **Then** at most 2000 instances are loaded per type; the Resources UI displays a capped subset (50).
3. **Given** the resource list, **When** I add or remove a candidate, **Then** later AI selection uses the updated candidate set.
4. **Given** Resources not confirmed, **When** I try Selections or Create, **Then** those steps stay gated.

### User Story 4 — Review AI slot assignments (Priority: P1)

As a planner, I want the AI to fill template slots and to override or re-run selections, so the canvas gets the right files and time series.

**Independent test**: Confirm Resources, wait for or run AI selection, pick a different resource for a slot, then continue to Create.

**Acceptance scenarios**:

1. **Given** template child FDM instance placeholders, **When** fields load, **Then** each `file` / `timeseries` (and other configured) slot is listed; “main” work order and “main” asset/equipment slots are filled by label heuristics, not the agent.
2. **Given** candidates and slot type, **When** AI runs, **Then** the agent is called per applicable slot with work order, asset, and candidate properties, and the parsed response is a single resource external ID mapped to that slot label.
3. **Given** one slot fails, **When** others succeed, **Then** the failed slot shows an error and does not block the rest.
4. **Given** Selections, **When** I re-run AI or pick a resource in the picker, **Then** the store updates for that slot.
5. **Given** automation review is shown, **When** I have not acknowledged it, **Then** Create stays gated.

### User Story 5 — Preview, save, and open the canvas (Priority: P1)

As a planner, I want to name the canvas, preview the draft, create or update it in CDF, and open it in Fusion.

**Independent test**: Enter a name, review preview, Create (or Update existing), then use the Fusion deep link.

**Acceptance scenarios**:

1. **Given** Selections acknowledged, **When** I am on Create, **Then** I can enter a free-text canvas name and see a draft preview of the composed canvas. Preview reads (files, time series, diagram annotations, views) share the app CDF call queue so bursts retry on HTTP 429.
2. **Given** Create mode, **When** save succeeds, **Then** a new canvas root is written to `IndustrialCanvasInstanceSpace` with a generated UUID `externalId`, `visibility` public, `createdBy`/`updatedBy` set to the current CDF `userIdentifier`, a **Work Package** solution tag, child FDM references and edges in batches of ≤1000, and template annotations copied unchanged.
3. **Given** Update mode, **When** a canvas with that exact name exists, **Then** its children are deleted (up to 100K items; exceeding the cap fails rather than partial delete) and new items are written.
4. **Given** success, **When** I view Create, **Then** I get a Fusion URL (and copy-link) to open the canvas.
5. **Given** save fails (including missing write permission on `IndustrialCanvasInstanceSpace`), **When** the error returns, **Then** I see a message and can retry.

### User Story 6 — Run inside Fusion with project context (Priority: P1)

As a user, I want the app to authenticate via Fusion and use the current CDF project, so search and writes hit the right tenant.

**Independent test**: Open the app in Fusion; after auth, the landing workflow renders. Open outside Fusion; connection/auth error is clear.

---

## Edge cases

- Work order has no `assets[]` — cannot proceed past work-order resolution; show a clear error.
- Work order has multiple assets — only the **first** asset is used.
- No canvases named `template*` — Template step is empty; user cannot continue until templates exist.
- Agent missing — app creates `{project}_work_package_generator_helper_agent` via CDF AI Agents API if permitted.
- Reload mid-flow — session is in-memory (Zustand); user starts again from search.
- Slot type has no candidates — slot stays empty / error; user can add a resource or re-run after adding candidates.
- Canvas name collision in Create vs Update — Update finds by exact name (scan cap 500); Create always allocates a new `externalId`.

---

## Functional requirements

- **FR-001**: Authenticate through the Fusion / `@cognite/app-sdk` host; do not require the user to paste CDF tokens in the UI.
- **FR-002**: Search `CogniteActivity` nodes and display paginated results (limit 20).
- **FR-003**: Resolve context from the first linked asset (`CogniteAsset`); support `CogniteEquipment` where template slots or config require it.
- **FR-004**: List Industrial Canvas templates from `cdf_industrial_canvas` / `Canvas` / `v1` with name prefix `template`.
- **FR-005**: Recommend a template via the project helper agent; user selection is authoritative.
- **FR-006**: Discover resource candidates from `resourceTypes` (files, time series, assets, equipment, activities as configured).
- **FR-007**: Load template slots from canvas child FDM instance nodes; assign main work-order and main asset slots by heuristics.
- **FR-008**: Run AI resource selection per applicable slot; allow re-run and manual pick.
- **FR-009**: Gate steps: work order committed → template confirmed → resources confirmed → automation review acknowledged → create.
- **FR-010**: Create or update a canvas in `IndustrialCanvasInstanceSpace`; set new canvases public; stamp owner from `profiles.me()` `userIdentifier`; attach the Work Package canvas label; copy annotations; batch upserts/deletes at CDF limits.
- **FR-011**: After success, show a Fusion deep link. The org comes from the Fusion page hosting the app (the parent origin when the app is embedded); project and cluster come from the running CDF session. `fusion.fallbackOrganization` on the config node is the only fallback when that host is not Fusion.
- **FR-012**: Surface loading and error states for search, template load, resource load, AI, field load, and canvas save.
- **FR-013**: Support desktop (horizontal stepper) and compact/mobile (vertical stepper) layouts without changing the workflow contract.

---

## CDF data

### Views read

| View | Space | Version | Purpose |
|------|-------|---------|---------|
| `CogniteActivity` | `cdf_cdm` | `v1` | Work order search and properties |
| `CogniteAsset` | `cdf_cdm` | `v1` | Linked asset context |
| `CogniteEquipment` | `cdf_cdm` | `v1` | Equipment context / slots |
| `CogniteFile` | `cdf_cdm` | `v1` | File candidates (`assets[]`) |
| `CogniteTimeSeries` | `cdf_cdm` | `v1` | Time series candidates (`assets[]`; optional activity `timeSeries`) |
| `Canvas` | `cdf_industrial_canvas` | `v1` | Template list and canvas root |
| `FdmInstanceContainerReference` | `cdf_industrial_canvas` | `v1` | Template slots / canvas items |
| `CanvasAnnotation` | `cdf_industrial_canvas` | `v1` | Annotations copied onto the new canvas |

Identifiers live on the `wpg_config` node in space `inst_wpg_app_config` (view `WPGConfig` in `dm_sol_wpg_app`). The `toolkit/` folder creates that node and must be deployed before the app is run (`cdf build` and `cdf deploy`). Change that node, through the toolkit module, when a tenant uses different spaces or view versions. The app loads the node at startup, so the JavaScript bundle does not need a rebuild. There are no in-code defaults: a missing node, a 403, or an invalid section shows a setup screen and lists every problem with its path.

### Spaces written

| Space | Purpose |
|-------|---------|
| `IndustrialCanvasInstanceSpace` | New/updated canvas root, child nodes, edges. Space is `canvas.instanceSpace`. |
| `SolutionTagsInstanceSpace` | Ensure the **Work Package** solution tag (`solutionTag`) and attach it on create/update. View is `dataModel.views.solutionTag`. |

### AI

| Item | Convention |
|------|------------|
| Agent external ID | `{cdfProject}_{agent.externalIdSuffix}` (shipped suffix `work_package_generator_helper_agent`), or `agent.existingAgentExternalId` when that is set |
| Ensure exists | Look up by id. Create with `POST /ai/agents` only when `existingAgentExternalId` is unset and the agent is missing. A configured id that is missing is an error. |
| Create payload | Model, runtime, name, description, instructions, and tools from `agent`. |
| Template prompt | `prompts.templateSelection` filled with the work order, asset, and candidate templates; parse recommended canvas external ID |
| Slot prompt | `prompts.resourceSelection` filled with the work order, asset, and typed candidates; parse one resource external ID |

---

## Constraints

| Constraint | Detail |
|------------|--------|
| Single asset | First `CogniteActivity.assets[]` only |
| Template prefix | `templates.namePrefix` (shipped `template`) |
| One helper agent per project | External ID is `{project}_{agent.externalIdSuffix}`, unless `agent.existingAgentExternalId` names an agent that already exists |
| Resource fetch cap | `limits.maxResourceInstancesTotal` (shipped 2000) |
| Resource UI cap | `limits.resourcesDisplayLimit` (shipped 50) |
| Template child load cap | `limits.maxTemplateCanvasChildItems` (shipped 5000) |
| Canvas list scan | `limits.maxCanvasListScanItems` (shipped 10000) |
| Cleanup cap | `limits.maxCanvasCleanupItems` (shipped 100000) |
| No post-write canvas validation | Structure is not re-read to verify |
| Permissions | Write errors appear at create time, not as a pre-flight |

### Aura exceptions

App controls use Aura (`Button`, `Input`, `CheckboxItemControl`, dialogs, accordion, select). These stay native because Aura has no equivalent that keeps the layout:

| Control | Why it stays native |
|---------|---------------------|
| `WorkflowStepPill` stepper button | Pill states (completed, current, pending) in horizontal and vertical orientations. Aura `Button` variants do not encode step progress. |
| `TemplateCanvasCard` | The whole card is the hit target, with selection chrome, badges, and a slot list inside. |
| `components/ui/table.tsx` | Aura 1.47 does not export a Table. |

Vendored create-step preview components under `src/canvas-preview/` are documented in `src/canvas-preview/README.md` and are not restyled onto Aura.

---

## Workflow map

| UI step | Internal `WorkflowStep` |
|---------|-------------------------|
| Work Order | `work-order-selection` |
| Template | `canvas-selection` |
| Resources | `canvas-generation` |
| Selections | `ai-selection` |
| Create | `canvas-creation` |

Global workflow state is the Zustand store (`src/store.ts`): work order, asset, candidates, template, AI selections, current step, loading, error.

---

## Success criteria

- End-to-end canvas from a valid work order in under 5 minutes for a typical package.
- Planner can complete the flow without authoring Industrial Canvas or using the CDF API console.
- AI slot picks are reviewable and overridable before save.
- Created canvas is openable in Fusion Industrial Canvas via the generated link.
