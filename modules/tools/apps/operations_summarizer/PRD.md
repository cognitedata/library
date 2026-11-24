# Product Requirements Document — Operations Summarizer

**Version:** 0.0.1  
**Last updated:** April 2026  
**Platform:** Cognite Flows (embedded in Cognite Fusion)  
**CDF project:** `<your-project>` (org: `<your-org>`)

---

## 1. Overview

The **Operations Summarizer** is an AI-powered Flows application embedded inside Cognite Fusion. It transforms raw operational data stored in Cognite Data Fusion (CDF) into concise, structured natural-language summaries, and provides a testing framework to validate that those summaries remain accurate and consistent over time.

Operations teams use it to quickly understand what changed in their data platform over a selected period—across assets, files, work orders, time series, and equipment—without having to write queries or read raw tables.

---

## 2. Goals & Non-Goals

### Goals

- Enable operations teams to generate AI summaries of CDF operational data for any date range in a few clicks.
- Store and manage a history of generated summaries inside CDF.
- Allow teams to validate AI output consistency by re-running summaries against a saved baseline and scoring the comparison.
- Provide an admin interface to configure the underlying Atlas AI agent (instructions and per-data-type prompts) without code changes.

### Non-Goals

- The app does **not** replace existing CDF dashboards or provide real-time monitoring.
- The app does **not** process sensor/time-series *values*; it summarizes metadata (names, descriptions, and relationships) only.
- The app does **not** support multi-tenant or multi-project selection within a single session.
- Bulk export or scheduled/automated summary generation is out of scope for the initial version.

---

## 3. User Personas

| Persona | Role | Primary need |
|---|---|---|
| **Operations Analyst** | Day-to-day user | Generate summaries and review history |
| **Operations Manager** | Oversight | Review aggregated test scores and trends |
| **Platform Admin** | CDF admin with `agents:write` | Configure the Atlas AI agent and prompts |
| **Developer** | Engineers extending the app | Add data types, modify prompts, deploy to new projects |

---

## 4. Application Pages & Features

The app has three pages, accessible from a sticky top navigation bar with a theme toggle (light/dark).

### 4.1 Summaries Page

**Purpose:** Create new AI-generated operational summaries and manage previously saved ones.

#### 4.1.1 Create New Summary

- Expand an inline creation form via the **Create new summary** button.
- **Date range picker** — select start and end dates for the summary period.
- **Data type selector** — toggle which of the five data types to include:
  - Assets, Files, Work Orders, Time Series, Equipment.
- **Generate** calls `Operations_Summarizer_Function_v2`. The function reads matching instances, summarizes each selected data type, and returns one summary. The app shows progress from the function log and a truncation warning when the function caps a data type.
- A **progress bar** with status messages tracks each generation step (fetching → generating → complete).
- After generation, the **Summary Result Panel** renders the formatted output and offers:
  - **Save** — persist the summary to CDF as a `CogniteFile` instance in the `ops_summaries` space.
  - **Download Markdown** — download `.md` file locally.
- If generation fails mid-way, an **Error Recovery Dialog** offers retry (from the failed step) or reset.

#### 4.1.2 Manage Past Summaries

- A paginated table lists all saved summaries (cursor-based pagination from CDF).
- **Filters:**
  - Date range filter (applied server-side).
  - Data type content filter (client-side toggle, multi-select dropdown).
  - "Clear" resets all active filters.
- Each row supports:
  - **View** — opens a **Summary Detail Modal** showing the full summary text rendered as Markdown.
  - **Test** — pre-populates the Tests page with this summary as the baseline.
  - **Download** — download as Markdown or PDF (with error recovery dialog on failure).
  - **Delete** — confirmation dialog before removal from CDF.
- A **Refresh** button re-fetches the list from CDF, with visual success/error feedback.

---

### 4.2 Tests Page

**Purpose:** Validate AI consistency by re-generating a summary for the same period as a previously saved baseline and scoring the comparison.

#### 4.2.1 Create New Test

- Expand an inline creation form via the **Create new test** button.
- **Baseline picker** — a dropdown of saved summaries (up to the most recent `OPERATION_SUMMARY_PICKER_MAX`), filterable by date range.
- **Test configuration card** (`TestConfigCard`) once a baseline is selected:
  - Override data type selection and date range for the re-generation run (defaults to the baseline's original settings).
- **Run Test** triggers:
  1. Re-generate a summary using the same AI pipeline as the Summaries page.
  2. Compare the new summary against the baseline content using the AI agent.
  3. Compute three scores (0–100):
     - **Similarity Score** — textual/semantic overlap with the baseline.
     - **Data Accuracy Score** — alignment of data counts (asset/file/work-order counts, etc.).
     - **LLM Score** — AI-judged quality of the new summary.
  4. Produce a **Comparison Analysis** text.
- **Test Results Panel** renders all scores, analysis, and offers:
  - **Save** — persist the test report to CDF.
  - **Export YAML** — structured YAML export of the test result.
  - **Copy YAML** / **Download YAML**.
  - **Clear** to start a new test.

#### 4.2.2 Test History & Statistics

- A paginated table lists all saved test reports.
- **Score legend** visible at the top of the page:
  - Excellent: 90+ (green)
  - Good: 70–89 (amber)
  - Needs attention: < 70 (red)
- **Stats bar** (shown when not in create mode) displays:
  - Total test count, average score, and counts per tier (Excellent / Good / Poor).
  - Streamed progressively as stats load.
- **Score range filter** (min / max inputs) to narrow visible rows on the current page.
- Each row supports: **View** (opens `TestReportModal`), **Download Markdown**, **Delete**.
- An **Info** button opens a `TestingInfoDialog` explaining the scoring methodology.

---

### 4.3 Settings Page

**Purpose:** Configure the Atlas AI agent used for summary and test generation.

- **Agent Status card** — displays active agent name, model, and external ID.
- **Create / Update Agent** button — creates or updates the agent in CDF using the current instructions.
- **Delete Agent** button — permanently removes the agent (with confirmation dialog listing consequences).
- **Prompts card:**
  - **Base Instructions** textarea — free-text system-level instructions for the agent; includes a **Reset** button to restore defaults.
  - **Data Type Prompts** (collapsible, one per data type) — per-type instruction snippets appended to the base instructions during generation; each has a **Reset** and a **Reset All** button.
- All edit controls are gated on the `agents:write` CDF capability; users without it see a read-only view with an explanatory banner.
- Permission check failure surfaces a **Retry** button.

---

## 5. Data Model & CDF Integration

### 5.1 CDF Views Queried

| Data type | CDF view | Space | Version |
|---|---|---|---|
| Assets | `CogniteAsset` | `cdf_cdm` | `v1` |
| Files | `CogniteFile` | `cdf_cdm` | `v1` |
| Work Orders | `CogniteActivity` | `cdf_cdm` | `v1` |
| Time Series | `CogniteTimeSeries` | `cdf_cdm` | `v1` |
| Equipment | `CogniteEquipment` | `cdf_cdm` | `v1` |

All queries use the CDF Data Modeling (DM) Instances API. Pagination is cursor-based; the app fetches pages of up to `INSTANCE_QUERY_LIMIT` (1 000) instances per request.

### 5.2 Summary Storage

- **Space:** `ops_summaries`
- **View:** `CogniteFile` (`cdf_cdm` / `v1`)
- Each generated summary is uploaded as a file and registered as a DM instance in the `ops_summaries` space.
- Metadata embedded in the file name and properties: period start/end, selected data types, generation timestamp.

### 5.3 Test Report Storage

- Test reports are also stored as `CogniteFile` instances in the `ops_summaries` space.
- Each report references the original baseline summary's external ID and space.
- YAML export is a client-side format only; the persisted form in CDF is the structured file instance.

### 5.4 AI Agent

- Managed through the CDF Agents API (`/api/v1/projects/{project}/ai/agents`).
- External ID convention: `{configuredName}_operation_summarizer_helper_agent`.
- Default LLM: `azure/gpt-5.1`.
- The app uses **direct agent chat calls** (not Atlas Chat streaming); each generation step is a separate API call.

---

## 6. Non-Functional Requirements

| Category | Requirement |
|---|---|
| **Authentication** | Delegated via `CogniteSdkProvider` / `useCogniteSdk()` from `@cognite/app-sdk` (Fusion host token). No credentials stored client-side. |
| **Authorization** | Summary generation requires CDF Data Modeling read access on `cdf_cdm` and write access on `ops_summaries`. Agent management additionally requires `agents:write`. |
| **Performance** | Pages lazy-loaded (code-split). Heavy dependencies (react-pdf, html2canvas) load only when the Summaries page is visited. |
| **Concurrency** | CDF DM write operations are rate-limited via a `Semaphore` / `QueuedTaskRunner` utility to avoid 429 errors. |
| **Data safety** | Maximum of 15 000 instances per data type per summary run (configurable via `MAX_INSTANCES_PER_DATATYPE`). Truncation is surfaced as a user-visible warning. |
| **Error handling** | Error boundaries wrap each page. Network failures expose retry dialogs (generation, download, permission check). All async errors are caught and surfaced in the UI; no unhandled promise rejections. |
| **Accessibility** | ARIA labels on all interactive controls. Live regions for async loading states. Keyboard-navigable dialogs and tables. |
| **Theming** | Full dark/light mode support via Tailwind CSS `dark:` variants and Aura design tokens. |
| **Testing** | Vitest test suite. Minimum coverage: every service, ViewModel hook (loading/success/error states), and utility function. Integration tests cover component + hook + service contract boundaries. |
| **Type safety** | Strict TypeScript; no `any`; `unknown` for external API responses with explicit runtime guards. |
| **Linting** | Biome enforces code style; CI blocks on lint failures. |

---

## 7. Architecture

### 7.1 Stack

| Layer | Technology |
|---|---|
| Framework | React 19 (functional components) |
| Language | TypeScript (strict) |
| Build | Vite |
| Styling | Tailwind CSS v4 + Aura UI (Cognite design system) |
| Components | shadcn/ui primitives + custom components |
| State | React context + `useReducer` (per-page slice model) |
| Data fetching | TanStack React Query |
| CDF integration | `@cognite/sdk` + `@cognite/app-sdk` (`useCogniteSdk()` hook) |
| Testing | Vitest + React Testing Library |
| Linting | Biome |
| Icons | Lucide React |

### 7.2 Key Architectural Patterns

- **ViewModel hooks** (`useSummariesPageModel`, `useTestsPageModel`) — all business logic lives in hooks; page components are pure renderers.
- **Dependency injection** — hooks accept a context (`SummariesPageModelDepsContext`, `TestsPageModelDepsContext`) for testability; concrete implementations are provided by matching `*Provider` components.
- **Lazy-loaded pages** — `SummariesPage`, `TestsPage`, and `SettingsPage` are dynamically imported; a Suspense fallback and per-page `ErrorBoundary` ensure graceful loading and fault isolation.
- **Context slices** — global state is split into typed slices (`agentSlice`, `summaryGenerationSlice`, `testingSessionSlice`) combined by `rootReducer` inside `OperationsSummarizerContext`.
- **Interface-based services** — CDF interactions are isolated in service modules (`cdfHelper`, `prompts`) that are never referenced directly by components.

### 7.3 Project Structure

```
src/
├── App.tsx                         # Root component; routing via NavigationContext
├── main.tsx                        # Entry point
├── types/operations.ts             # Shared type contract (source of truth)
├── config/dataTypes.ts             # Data type configs, prompts, constants
├── contexts/                       # Global + page-level React contexts
├── features/dashboard-agent/       # Agent load + config feature module
├── hooks/                          # ViewModel hooks and CDF data hooks
├── pages/                          # Page components (thin renderers)
├── components/
│   ├── layout/Navigation.tsx       # Top nav bar
│   ├── ui/                         # shadcn/ui primitives
│   ├── summaries/                  # Summary-specific components
│   ├── tests/                      # Test-specific components
│   └── common/                     # Shared UI (FilterBar, ProgressBar, dialogs)
├── services/                       # CDF API service modules
└── utils/                          # Pure utility functions (all unit-tested)
```

---

## 8. Configuration & Deployment

### 8.1 Application Configuration (`app.json`)

```json
{
  "name": "Operations Summarizer",
  "description": "Summarizes and analyzes operations data for quick insights and reporting.",
  "externalId": "operations-summarizer",
  "versionTag": "0.0.1",
  "deployments": [
    {
      "org": "<your-org>",
      "project": "<your-project>",
      "baseUrl": "https://<cluster>.cognitedata.com",
      "published": true,
      "deployClientId": "${CLIENT_ID}",
      "deploySecretName": "OPS_SUMMARIZER_SECRET"
    }
  ]
}
```

### 8.2 Environment Variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | Dev server port | `3001` |
| `DEV_PORT` | Fusion dev server port | `1987` |
| `companyName` (config node) | Optional company name | `""` |

### 8.3 CDF Permissions Required

| Capability | Scope | Purpose |
|---|---|---|
| `dataModelInstances:read` | `cdf_cdm` space, all views | Fetch assets, files, activities, time series, equipment |
| `dataModelInstances:read` / `write` | `ops_summaries` space | Read and persist summaries, test reports, and podcast files |
| `files:read` / `write` | Files API | Upload summary content as files |
| `agents:read` | All | Load available agents |
| `agents:write` | All | Create the default agent from the config node when it is missing |

### 8.4 Pointing at a Different CDF Project

1. Update `deployments` in `app.json` with the new `org`, `project`, and `baseUrl`.
2. Ensure the CDF views (`VIEW_IDS` in `src/types/operations.ts`) match what is deployed in the target project's Data Modeling.
3. Update or create the `ops_summaries` space and grant appropriate ACLs.
4. Set the deploy secret in CI and update the `deploySecretName` field.

---

## 9. Summary Generation Pipeline (Detail)

```
User clicks Generate
        │
        ▼
App calls Operations_Summarizer_Function_v2
with the date range, selected data types, and config-node prompts
        │
        ▼
Function reads instances, summarizes them, and returns markdown
        │
        ▼
App shows progress from the function log, then the result panel
```

Prompts and agent instructions live on the config node. The app creates the default agent from that node only when the agent is missing. The Tests page still calls the agent to compare two summaries.

---

## 10. Test Scoring Methodology

Each test run produces three component scores (0–100):

| Score | Measures |
|---|---|
| **Similarity Score** | Textual/semantic closeness of the new summary to the baseline |
| **Data Accuracy Score** | Comparison of data counts (per data type) between new and original runs |
| **LLM Score** | AI-judged quality and completeness of the new summary |

**Score tiers:**

| Range | Label | UI colour |
|---|---|---|
| 90–100 | Excellent | Green |
| 70–89 | Good | Amber |
| 0–69 | Needs attention | Red |

Test results can be exported as YAML for integration with external validation pipelines or CI/CD workflows.

---

## 11. Glossary

| Term | Definition |
|---|---|
| **CDF** | Cognite Data Fusion — the industrial data platform underpinning this app |
| **DM / Data Modeling** | CDF's graph-based data model system (spaces, views, instances) |
| **Flows** | Cognite's framework for building apps embedded inside Cognite Fusion |
| **Atlas AI** | Cognite's AI agent platform, accessed via the CDF Agents API |
| **Summary** | A natural-language overview of CDF operational data for a specified period |
| **Test Report** | A stored comparison between a new AI-generated summary and a baseline |
| **Baseline** | A previously saved summary used as the reference for a test run |
| **Chunk** | A subset of instances (≤ CHUNK_SIZE) processed in one AI call |
| **ops_summaries** | The CDF DM space where summaries, test reports, and podcast files are persisted. Podcast external IDs start with `ops_podcast_`. |
| **ViewModel** | A custom hook that encapsulates all business logic for a page component |
