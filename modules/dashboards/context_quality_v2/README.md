# Contextualization Quality Dashboard — Records & Streams rebuild

## Overview

This module is where the Context Quality Dashboard's Records & Streams (R&S) backend
is being built from scratch, per the PRD's §7/§8 decision to move off the current
module's flat-JSON-file metrics storage. It is deliberately a **new, separate module**
from `dashboards/context_quality` rather than an in-place change to it: the backend is
being developed fresh here and will replace `dashboards/context_quality` once it's
ready, rather than evolving the existing module in place. Until then, both modules are
registered and deployable independently.

**Current scope:** the Records & Streams container/view/stream schema only (this
module's `data_modeling/` and `streams/`). The backend extraction function and the
Flow app itself are separate, still-exploratory efforts that will land in this module
as they're built — see "Status" below.

## Status

- **Schema:** committed here, matches the Flow app's DTOs (`src/types/dto.ts`,
  `src/types/filters.ts` in the dashboard repo) and
  `phase-7.5-backend-contract-audit.md` §6.
- **Backend extraction function:** exploratory, not yet in this module. The plan is
  one CDF Function using the Cognite SDK that reads each configured view/dataset and
  writes raw counts to this schema, incrementally (checkpointed, not a full re-read
  every run). Scope and exact view configuration are still being worked out — see the
  "lowest reporting level" principle below, which is the one thing that's settled
  regardless of how many functions it ends up being.
- **Flow app:** not yet pointed at this module's R&S backend; still reads from the
  mock services in the dashboard repo.

## Records & Streams schema

### The one settled architectural principle: lowest reporting level, app aggregates

The backend never writes a precomputed value for a filter-combination scope (e.g. "68%
for facility=X AND owner=Y"). It writes raw
`eligibleCount`/`passingCount`/`issueCount` at the **lowest level it can read a given
metric at** — a unit, a plant, or a facility, whichever grain the source view
actually supports (this varies: equipment has a space per plant, assets use a `plant`
property, time series use a per-plant space, and some metrics report at an even finer
unit level). The Flow app sums those raw counts across whatever rows match the active
`SliceFilters` and derives a percentage itself — the raw data lives in Records &
Streams, and the scope-specific calculation happens in the app at read time. This is
why `valuePercent` appears on these containers only as a per-row convenience value —
it is never valid to sum or average across rows.

Three containers, split by lifecycle need (not changed by the above — see
`docs/records_and_streams_design.md` in the dashboard repo for the full rationale):

| Container | Kind | Covers | Parity with |
|---|---|---|---|
| `ContextQualityCurrentMetric` | mutable node, upserted | current raw count at the lowest reporting level | `AggregateMetricRowDto` |
| `ContextQualityMetricSnapshot` | immutable record (`BasicArchive`) | historical trend points — same raw-count shape, over time | `SnapshotPointDto` |
| `ContextQualityDrilldownRow` | mutable node, upserted | current backlog of specific broken entities (already the finest possible grain — unaffected by the above) | `DrilldownRowDto` / `IssueDto` |

### Why `reportingUnitId`, not a scope-combination key

`ContextQualityCurrentMetric` and `ContextQualityMetricSnapshot` are keyed on
`(metricId, reportingUnitId, tier)` — `reportingUnitId` is whatever the lowest grain
actually was for that read, denormalized alongside (not replaced by)
`facilityId`/`divisionId`/`disciplineId` so the app can filter/rollup without a join.
This is deliberately *not* a hash over the four `SliceFilters` fields: storing a row
per filter combination would mean precomputing a combinatorial set of scopes, most of
which would never be queried, and would still be wrong the moment someone picks a
combination that wasn't precomputed. One row per actual lowest-grain unit, summed at
read time, is correct for every possible combination without needing to know them in
advance.

`ownerId` is carried on every container as best-effort only — it's typically derived
via prefix/path mapping (`OwnerMappingDto`), not a native property of a reporting
unit, so owner-scoped rollups over `ContextQualityCurrentMetric`/
`ContextQualityMetricSnapshot` are not guaranteed correct yet. Open question, not yet
resolved.

### Template: `BasicArchive` (immutable) — `ContextQualityMetricSnapshot` only

| Limit (`BasicArchive`) | Value |
|---|---|
| Max records/stream | 50,000,000 |
| Max volume/stream | 50 GB |
| Ingest rate | 170,000 records / 170 MB per 10 minutes |
| Max `lastUpdatedTime` filter window | 365 days |
| Retention | Unlimited — and, since deletion is only available for mutable streams, permanent once written |

Sizing at this grain (lowest reporting unit, not filter combinations) stays small —
tens to low hundreds of reporting units × ~45 metrics × 3 tiers is still orders of
magnitude under the 50M-record cap even retained for years.

### Query patterns

- **`ContextQualityMetricSnapshot` (immutable):** every `filter`/`aggregate` call
  requires a `lastUpdatedTime` range (max 365 days on `BasicArchive`). "Fetch latest
  run": `aggregate` with `Max(lastUpdatedTime)`, then `filter` on `runId`. Use `sync`
  rather than `filter` once a query would return more than 1,000 rows (no pagination
  on `filter`).
- **`ContextQualityCurrentMetric` / `ContextQualityDrilldownRow` (mutable nodes):**
  standard `list`/`filter` with ordinary cursor pagination — no Records-specific rules.

## Module Components

```
context_quality_v2/
├── data_modeling/
│   ├── context_quality_metrics.Space.yaml      # schema space
│   ├── context_quality_instances.Space.yaml    # instance space for written node instances
│   ├── containers/
│   │   ├── ContextQualityCurrentMetric.Container.yaml   # usedFor: node (mutable)
│   │   ├── ContextQualityMetricSnapshot.Container.yaml  # usedFor: record (immutable)
│   │   └── ContextQualityDrilldownRow.Container.yaml    # usedFor: node (mutable)
│   └── views/
│       ├── ContextQualityCurrentMetric.View.yaml
│       ├── ContextQualityMetricSnapshot.View.yaml       # streamId-bound
│       └── ContextQualityDrilldownRow.View.yaml
├── streams/
│   └── context_quality_metric_snapshots.Streams.yaml    # BasicArchive
├── default.config.yaml
├── module.toml                              # id: dp:dashboards:context_quality_v2
└── README.md                                # This file
```

## Deployment

### Prerequisites

- A Cognite Toolkit project set up locally, with the standard `cdf.toml` file
- Valid authentication to your target CDF environment

### Adding the module

```bash
cdf modules add .
```

Select **Dashboards** → **Contextualization Quality Dashboard (R&S rebuild)**. Then:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

This module has no Functions or Streamlit/Flow app yet — deploying it provisions only
the schema (spaces, containers, views, stream) described above.

## Support

- See `docs/records_and_streams_design.md` and `phase-7.5-backend-contract-audit.md`
  in the `context-quality-dashboard` (dashboard/mockup) repo for the full design
  rationale and DTO parity mapping this schema was built against.
- For troubleshooting Toolkit deployment, see
  [docs.cognite.com](https://docs.cognite.com) or `#topic-deployment-packs` on Slack.
