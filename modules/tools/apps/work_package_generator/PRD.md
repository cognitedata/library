# Work Package Generator — Product Requirements Document

## Overview

The Work Package Generator is an AI-powered web application that automates the creation of Industrial Canvas work packages in Cognite Data Fusion (CDF). It guides maintenance or operations teams through a structured five-step workflow: locate a work order, select an appropriate canvas template, discover related resources, let AI assign resources to template slots, and create the final canvas.

Without this tool, engineers must manually locate work orders, browse for related files and time series, open Industrial Canvas, drop in resources one-by-one, and wire everything together. This process is error-prone, time-consuming, and requires deep knowledge of which data exists in CDF. The Work Package Generator replaces that manual process with a guided, AI-assisted flow that produces a complete, consistent canvas in minutes.

---

## Target Users

- **Maintenance planners** preparing work packages for field technicians
- **Operations engineers** assembling context for planned or reactive work orders
- **CDF solution architects** deploying pre-built canvas workflows for customers

Users are expected to understand the work order domain (e.g., asset IDs, work descriptions) but need no knowledge of CDF internals or Industrial Canvas authoring.

---

## Problem Statement

Creating a meaningful Industrial Canvas for a specific work order requires:

1. Knowing which canvas template applies to the work type
2. Knowing which files and time series are relevant to the affected asset
3. Manually placing each resource into the correct template slot
4. Configuring work order and asset context in the canvas

This is a repetitive, expert-knowledge-heavy task that does not scale as the number of work orders and data sources grows. The Work Package Generator automates all four steps above using CDF data and AI agents.

---

## Goals

- Reduce time to produce a complete Industrial Canvas work package from ~30 minutes (manual) to under 5 minutes
- Eliminate the need for canvas authoring expertise from the end user
- Ensure consistent, reproducible canvas structure by using versioned templates
- Leverage AI to surface the most contextually relevant resources per template slot

---

## Non-Goals

- Creating or editing canvas templates (templates must be pre-authored in Industrial Canvas)
- Managing or editing work orders (read-only access to CDF activities)
- Bulk canvas generation for multiple work orders simultaneously
- Canvas versioning, approval workflows, or audit trails
- Any data model setup — the app assumes CDM work orders, linked assets, and canvas templates already exist

---

## Five-Step Workflow

### Step 1 — Work Order Selection

The user searches for a work order by ID, name, or description. Results are returned in a paginated table (20 results per page). The user selects a row and confirms selection. On confirmation, the app loads the work order's linked asset.

**Acceptance criteria:**
- Search supports exact ID match and full-text search via CDF `instances.search`
- Results display: external ID, name, description, status
- Pagination navigates via cursor (next/previous)
- Selecting a work order loads its first linked asset via the `assets` relation on `CogniteActivity`
- User can reset and search again from this step at any point in the workflow

### Step 2 — Template Selection

Available Industrial Canvas templates are listed (canvases whose name starts with `template`, case-insensitive). An AI agent evaluates all templates against the work order and asset context and highlights a recommendation. The user can accept the recommendation or choose a different template.

**Acceptance criteria:**
- Templates fetched from `cdf_industrial_canvas` space, `Canvas` view
- Only canvases with name prefix `template` are shown
- AI recommendation calls the CDF AI Agents API with work order + asset context
- Recommended template is visually distinguished; user can override
- Selecting a template does not yet load its field structure (gate on explicit continue)

### Step 3 — Resources

After template selection is confirmed, the app discovers files and time series linked to the selected asset. Resources are grouped by type and shown in an expandable accordion. This step is informational — it helps the user verify which resources will be available for AI selection.

**Acceptance criteria:**
- Files discovered via `CogniteFile.assets[]` relation
- Time series discovered via `CogniteTimeSeries.assets[]` relation, with optional supplemental discovery via `activityContainerIdentifier` property on the activity type (e.g., `timeSeries` direct relation on the work order)
- Maximum 2000 resource instances loaded per type
- Resource table shows: external ID, name, description, and type-specific properties

### Step 4 — AI Resource Selection

The template's slot structure (FDM instance placeholder nodes) is loaded. For each slot of type `file` or `timeseries`, the AI agent selects the most appropriate resource from the candidates discovered in Step 3. The user can review selections and re-run AI selection if needed.

**Acceptance criteria:**
- Template slots extracted from canvas child FDM instance nodes
- "Main" work order slot and "main" asset slot detected by label heuristics (not AI — hard-coded)
- AI called once per slot (file/timeseries type); prompt includes work order context, asset context, available resources with properties
- AI response parsed to extract a single external ID per slot
- Partial success handled: a failing slot displays an error without blocking the rest
- User can re-run AI selection with the "Run AI Selection" button

### Step 5 — Canvas Creation

The user enters a canvas name and chooses to create a new canvas or update an existing one by the same name. The app builds the canvas by copying the template structure and substituting placeholder slots with real CDF node references. A Fusion deep link is generated on success.

**Acceptance criteria:**
- Canvas name is user-entered free text
- "Create" mode: generates a UUID for the new canvas `externalId`
- "Update" mode: finds existing canvas by exact name match, deletes its child items (up to 100K), then writes new items
- Canvas root node written to `IndustrialCanvasInstanceSpace`
- Child FDM instance reference nodes and edges written in batches of ≤1000 items
- Annotations copied from template unchanged
- On success: Fusion URL displayed for one-click navigation to the created canvas
- On failure: error message displayed with retry option

---

## AI Integration

### Agent Setup

A single CDF AI Agent is used per project. The agent external ID is `{project}_work_package_generator_helper_agent`. The app calls `GET /ai/agents` to check if the agent exists and `POST /ai/agents` to create it with a minimal configuration if it does not.

### Template Recommendation

All available templates are sent to the agent in a single prompt. The agent returns the external ID of the recommended template. The recommendation is used only as a visual highlight — the user always makes the final selection.

### Resource Selection

For each template slot (excluding main work order and main asset slots), the agent receives:
- Work order name, description, status, and other properties
- Asset name, type, category, specs, and other properties
- Available resources of the slot's type, with their names, descriptions, and type-specific properties

The agent returns the external ID of the single best-matching resource. The app parses the response and maps it to the `AiSelections` store (`label → resource`).

---

## Data Model Requirements

The app requires the following to be present in the target CDF project:

| Requirement | Details |
|---|---|
| Work orders as `CogniteActivity` | View: `cdf_cdm/CogniteActivity/v1` |
| Work orders linked to assets | `CogniteActivity.assets[]` relation populated |
| Assets as `CogniteAsset` | View: `cdf_cdm/CogniteAsset/v1` |
| Files linked to assets | `CogniteFile.assets[]` relation populated |
| Time series linked to assets | `CogniteTimeSeries.assets[]` relation populated |
| Canvas templates in Industrial Canvas | Canvases with name starting with `template` |
| `IndustrialCanvasInstanceSpace` write permission | Required for canvas creation |
| CDF AI Agents enabled | Required for template recommendation and resource selection |

---

## Configuration

### Config node and `app.json`

Per-project settings (views, canvas spaces, resource types, agent, templates, solution tag, limits) live on one CDF config node, `inst_wpg_app_config` / `wpg_config`, deployed by this module's Toolkit resources (`agents/`, `data_modeling/`). They must be deployed before the app is run or deployed; the app shows a setup screen without the node. There are no in-code defaults. `app.json` holds only app identity and the CLI deployment target; the running app does not read it. The runtime agent id is `{project}_{agent.externalIdSuffix}`, unless `agent.existingAgentExternalId` is set.

Shipped identifiers:

- Activity view — `cdf_cdm` / `CogniteActivity` / `v1`
- Asset view — `cdf_cdm` / `CogniteAsset` / `v1`
- Canvas view — `cdf_industrial_canvas` / `Canvas` / `v7`
- Canvas instance space — `IndustrialCanvasInstanceSpace`

---

## Key Constraints and Limitations

| Constraint | Detail |
|---|---|
| Single asset per work order | App uses only the first asset in `CogniteActivity.assets[]` |
| Template name convention | Name must start with `templates.namePrefix` (shipped `template`, case-insensitive) |
| AI agent name is project-scoped | One agent per CDF project; external ID is `{project}_{agent.externalIdSuffix}` |
| Resource limits | `limits.maxResourceInstancesTotal` per type (shipped 2000) |
| Update mode cleanup limit | `limits.maxCanvasCleanupItems` (shipped 100000) |
| No canvas validation | App does not verify canvas structure post-creation |
| No permission pre-check | If the user lacks write access to `IndustrialCanvasInstanceSpace`, the error surfaces only at creation time |
| Activity-based resource discovery | Optional: only works if `activityContainerIdentifier` property is set on the activity type |
| Template authoring is manual | Templates must be authored separately in Industrial Canvas before use |

---

## Technology Stack

| Layer | Technology |
|---|---|
| Framework | React 19, TypeScript, Vite |
| State | Zustand (global workflow state), TanStack React Query (data fetching) |
| CDF integration | `@cognite/sdk` v10, `@cognite/app-sdk` (auth + Fusion) |
| Canvas | Canvas preview bundled under `src/canvas-preview/` |
| UI | Tailwind CSS v4, Lucide icons |
| Testing | Vitest, React Testing Library |
| Linting | Biome, plus ESLint for Aura design-system rules only |

---

## Success Metrics

- Canvas successfully created for a given work order in < 5 minutes end-to-end
- AI resource selection accuracy ≥ 80% (as judged by planners reviewing selections before confirming)
- App usable without CDF or Industrial Canvas expertise
- Zero manual CDF API calls required from the user during the workflow
