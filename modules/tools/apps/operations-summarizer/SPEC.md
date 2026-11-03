# Feature Specification: Operations Summarizer

**App**: Operations Summarizer (`operations-summarizer`)  
**Status**: Current product  
**Related**: [PRD.md](./PRD.md) (goals, non-goals, success metrics), [SETUP-INSTRUCTIONS.MD](./SETUP-INSTRUCTIONS.MD) (deploy and CDF prerequisites)

This file is the source of truth for **user-visible behavior**. Update it when workflow, CDF reads/writes, or AI agent behavior changes. Implementation details live in code; product intent and constraints live here.

---

## Overview

The Operations Summarizer is an AI-assisted Cognite Fusion (Flows) app that turns CDF operational metadata into a plain-language summary for a chosen date range. The user picks data types, generates a summary, saves it, and can later re-run the same period against a saved baseline to score consistency.

Without the app, analysts write queries or read raw tables to see what changed across assets, files, work orders, time series, and equipment. The app replaces that with a three-page flow: Summaries, Tests, and Settings.

The app summarizes **metadata** (names, descriptions, relationships). It does not summarize time-series values or provide live monitoring.

---

## Personas

- **Operations analyst** — generates summaries and reviews history.
- **Operations manager** — reviews test scores and trends.
- **Platform admin** — configures the Atlas AI agent and per-data-type prompts (`agents:write`).

Users know operational data types and date ranges. They do not need to write CDF queries or call the Agents API.

---

## Out of scope

- Real-time dashboards or sensor-value analysis.
- Multi-project or multi-tenant selection inside one session.
- Scheduled or bulk summary generation from the UI.
- Authoring CDM views (`CogniteAsset`, `CogniteFile`, `CogniteActivity`, `CogniteTimeSeries`, `CogniteEquipment`).

---

## User scenarios and testing

### User Story 1 — Generate a summary (Priority: P1)

As an analyst, I want to pick a date range and data types and generate a summary, so I can read what changed without writing queries.

**Independent test**: Open Summaries, expand Create, set dates and types, Generate. A progress bar advances and a result panel shows formatted text with Save and Download Markdown.

**Acceptance scenarios**:

1. **Given** Summaries, **When** I open Create, **Then** I can set a start and end date and toggle Assets, Files, Work Orders, Time Series, and Equipment.
2. **Given** a valid range and at least one type, **When** I generate, **Then** the app calls `Operations_Summarizer_Function_v2`, which reads matching instances and returns one summary.
3. **Given** Work Orders, **When** they are fetched, **Then** they are grouped into Completed, Ongoing, and Scheduled before summarization.
4. **Given** more instances than the per-type cap, **When** generation finishes, **Then** the summary is truncated and I see a truncation warning.
5. **Given** a finished summary, **When** I save, **Then** it is stored as a `CogniteFile` in the `ops_summaries` space.
6. **Given** generation fails mid-run, **When** the error dialog appears, **Then** I can retry from the failed step or reset.

### User Story 2 — Manage saved summaries (Priority: P1)

As an analyst, I want a history of saved summaries I can filter, open, download, delete, or send to testing.

**Independent test**: With saved summaries in CDF, filter the table, open one, download Markdown or PDF, and delete one after confirmation.

**Acceptance scenarios**:

1. **Given** saved summaries, **When** the table loads, **Then** rows are cursor-paginated from CDF.
2. **Given** filters, **When** I set a date range, **Then** it is applied server-side; **When** I toggle data types, **Then** that filter is applied client-side. Clear resets both.
3. **Given** a row, **When** I view it, **Then** a detail modal shows the summary as Markdown, including a podcast tab when a podcast exists or can be generated.
4. **Given** a row, **When** I choose Test, **Then** the Tests page opens with that summary as the baseline.
5. **Given** a row, **When** I delete and confirm, **Then** the instance is removed from CDF.
6. **Given** the list, **When** I refresh, **Then** the table reloads and shows success or error feedback.

### User Story 3 — Score a summary against a baseline (Priority: P1)

As a manager, I want to re-run a saved summary and see similarity, data-accuracy, and quality scores, so I can tell whether the AI stayed consistent.

**Independent test**: On Tests, pick a baseline, run the test, and see three 0–100 scores plus comparison text. Save the report or export YAML.

**Acceptance scenarios**:

1. **Given** saved summaries, **When** I open Create test, **Then** I can pick a recent baseline and override its date range and data types.
2. **Given** a baseline, **When** I run the test, **Then** the app regenerates a summary with the same pipeline, compares it to the baseline, and shows Similarity, Data Accuracy, and LLM scores (0–100) plus analysis text.
3. **Given** scores, **When** they render, **Then** tiers are Excellent (90+), Good (70–89), and Needs attention (below 70).
4. **Given** a finished test, **When** I save, **Then** the report is stored in `ops_summaries` and references the baseline. YAML export and copy stay client-side.
5. **Given** saved reports, **When** I am not creating a test, **Then** a stats bar shows total count, average score, and counts per tier, and I can filter the current page by score range.
6. **Given** a report row, **When** I view, download Markdown, or delete, **Then** those actions match the summaries table pattern (confirm before delete).

### User Story 4 — Configure the agent (Priority: P2)

As an admin, I want to view and edit the helper agent’s instructions and per-type prompts, so summaries change without a code deploy.

**Independent test**: Open Settings with `agents:write`. Edit base instructions, reset a data-type prompt, and create or update the agent. Without `agents:write`, controls are read-only.

**Acceptance scenarios**:

1. **Given** Settings, **When** the agent loads, **Then** I see its name, model, and external ID.
2. **Given** `agents:write`, **When** I edit base instructions or a data-type prompt and create or update, **Then** the CDF agent is written with those instructions.
3. **Given** edited prompts, **When** I reset one or reset all, **Then** the defaults return.
4. **Given** no `agents:write`, **When** I open Settings, **Then** I see a read-only view and an explanatory banner.
5. **Given** a permission check failure, **When** it surfaces, **Then** I can retry.
6. **Given** delete, **When** I confirm, **Then** the agent is removed and the dialog states the consequences.

### User Story 5 — Run inside Fusion (Priority: P1)

As a user, I want the app to authenticate through Fusion and use the current CDF project.

**Independent test**: Open the app in Fusion; after auth, Summaries renders. Open outside Fusion; the connection or auth error is clear.

---

## Edge cases

- No instances in range — generation completes with an empty or minimal summary for that type; the result is still shown.
- Dataset larger than the per-type cap — generation stops at the cap and shows a truncation warning.
- Work order status missing — the instance still appears; bucketing uses the status fields the pipeline already reads.
- Agent missing — the app creates the default agent from the config node when the user has `agents:write`. Generation fails with a recoverable error if no agent is available.
- Podcast function not deployed — the podcast tab shows that no podcast exists and generation fails with a visible error; viewing the summary text still works.
- Reload mid-generation — in-progress generation is not restored; saved summaries remain in CDF.

---

## Functional requirements

- **FR-001**: Authenticate through the Fusion / `@cognite/app-sdk` host; do not require the user to paste CDF tokens in the UI.
- **FR-002**: Provide Summaries, Tests, and Settings, with light and dark theme.
- **FR-003**: Generate a summary for a date range across the selected CDM data types by calling `Operations_Summarizer_Function_v2`.
- **FR-004**: The function caps instances per data type and the app shows a truncation warning when the cap is hit.
- **FR-005**: Save summaries and test reports as `CogniteFile` instances in `ops_summaries`.
- **FR-006**: List, filter, view, download (Markdown and PDF for summaries), and delete saved summaries; offer Test to seed the Tests page.
- **FR-007**: Re-run a baseline and persist Similarity, Data Accuracy, and LLM scores plus comparison analysis.
- **FR-008**: Show score tiers and aggregate test stats.
- **FR-009**: Settings shows the selected agent and the prompts stored on the config node. Prompts change only by editing that node. The app creates the default agent from the config node when it is missing and the user has `agents:write`.
- **FR-010**: Surface loading and error states for generation, save, list, download, delete, test run, and permission checks, with retry where the flow can resume.
- **FR-011**: From a saved summary, generate or play a podcast (audio plus transcript) by calling `Operations_Summarizer_Function_v2`.

---

## CDF data

### Views read

| View | Space | Version | Purpose |
|------|-------|---------|---------|
| `CogniteAsset` | `cdf_cdm` | `v1` | Asset metadata in range |
| `CogniteFile` | `cdf_cdm` | `v1` | File metadata in range; also the shape of saved summaries |
| `CogniteActivity` | `cdf_cdm` | `v1` | Work orders, bucketed by status |
| `CogniteTimeSeries` | `cdf_cdm` | `v1` | Time series metadata (not values) |
| `CogniteEquipment` | `cdf_cdm` | `v1` | Equipment metadata in range |

Identifiers live in `src/types/operations.ts` (`VIEW_IDS`). Change them only when the tenant’s views differ.

### Spaces written

| Space | Purpose |
|-------|---------|
| `ops_summaries` | Saved summaries and test reports as `CogniteFile` nodes, plus uploaded file content |

### AI and functions

| Item | Convention |
|------|------------|
| Agent external ID | `{configuredName}_operation_summarizer_helper_agent` |
| Calls | The app calls `Operations_Summarizer_Function_v2` for summaries and podcasts. The Tests page still calls the agent directly to compare two summaries. |
| Default model | `agent.defaultLanguageModel` on the config node (used only when the app creates a missing default agent) |
| Summarizer function | `Operations_Summarizer_Function_v2` (Toolkit module `summarizer_function`) for summaries and podcasts |

---

## Constraints

| Constraint | Detail |
|------------|--------|
| Metadata only | Time-series values are not read |
| Instance page size | Up to 1 000 instances per Data Modeling request |
| Chunk size | 50 instances per AI call |
| Per-type cap | 15 000 instances; excess is dropped and warned |
| One project per session | Project comes from the Fusion host |
| Agent writes | Settings mutations require `agents:write` |
| Summary writes | Require Data Modeling write on `ops_summaries` and Files write |

---

## Success criteria

- An analyst can generate a summary for a typical date range in a few clicks and read it without the CDF query console.
- A saved summary can be re-run as a test and yields three scores plus a stored report.
- An admin can change prompts and update the agent without a frontend deploy.
- A user without `agents:write` can still generate and review summaries.
