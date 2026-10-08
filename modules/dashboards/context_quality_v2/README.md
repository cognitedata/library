# Contextualization Quality Dashboard — Records & Streams Backend

## Overview

This module hosts the Context Quality Dashboard's Records & Streams (R&S) backend,
per the PRD's §7/§8 decision to move off the current `dashboards/context_quality`
module's flat-JSON-file metrics storage. It is a **separate module** from
`dashboards/context_quality` so the two can be developed and deployed independently;
this module will replace `dashboards/context_quality` once the backend function and
Flow app are wired up against it.

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
for facility=X AND owner=Y"). It writes raw counts at the **actual grain it could
count a given metric at** — a unit, a plant, or a facility, whichever grain the source
view supports, and further broken out by discipline/owner whenever the function could
resolve those per-entity at count time (see "Row grain," below — this is not just
"facility," since a single unit commonly spans several disciplines). The Flow app sums
those raw counts across whatever rows match the active `SliceFilters` and derives a
percentage itself — the raw data lives in Records & Streams, and the scope-specific
calculation happens in the app at read time. No container stores a precomputed percent
at all: it is fully derivable from the counts, and storing it invited exactly the
mistake of reading it for a rolled-up scope where it would be wrong.

Four containers, split by lifecycle need (not changed by the above — see
`docs/records_and_streams_design.md` in the dashboard repo for the full rationale):

| Container | Kind | Covers | Parity with |
|---|---|---|---|
| `ContextQualityCurrentMetric` | mutable node, upserted | current raw counts at the lowest reporting level | `AggregateMetricRowDto` |
| `ContextQualityMetricSnapshot` | immutable record (`BasicArchive`) | historical trend points — same raw-count shape, over time | `SnapshotPointDto` |
| `ContextQualityCurrentExclusion` | mutable node, upserted | current §6.1/§6.2 exclusion counts, same grain as current metrics | `ExclusionBucket` (current state) |
| `ContextQualityDrilldownRow` | mutable node, upserted | current backlog of specific broken entities (already the finest possible grain) | `DrilldownRowDto` / `IssueDto` |

`ContextQualityMetricSnapshot`'s `exclusionCount` records keep this same count's
history over time — the current/history split applies to exclusions exactly the same
way it applies to metrics, so Dimension-page reads of "today's excluded buckets" never
need a time-ranged query into the immutable stream just to show the current value.

### Row grain: `reportingUnitId` (+ discipline/owner when known), not a scope-combination key

`ContextQualityCurrentMetric` and `ContextQualityMetricSnapshot` are keyed on
`(metricId, reportingUnitId, disciplineId, ownerId, tier)`;
`ContextQualityCurrentExclusion` and the snapshot's `exclusionCount` records on
`(dimensionId, exclusionCategory, reportingUnitId, disciplineId, ownerId)`.
`reportingUnitId` is whatever the lowest grain actually was for that read.
`disciplineId`/`ownerId` are part of the key but **nullable** — null means the
function could not break the count out at that granularity; non-null means the row
covers only that one discipline/owner within `reportingUnitId`. Only combinations
that actually exist in the data get rows (not a cross product): a reporting unit with
one discipline gets one row; a reporting unit spanning three disciplines gets up to
three. This matters because a single row cannot correctly claim one discipline/owner
value unless the counts were genuinely produced at that granularity — a plant or unit
commonly contains more than one discipline, so collapsing them into one row with one
`disciplineId` would make discipline-filtered rollups silently wrong.
`facilityId`/`divisionId` are coarser than `reportingUnitId` and stay denormalized,
descriptive-only fields (not part of the key) — every reporting unit sits inside
exactly one facility/division, so no combinatorial handling is needed for those two.

This grain is deliberately *not* a hash over all four `SliceFilters` fields at once:
storing a row per arbitrary filter combination would mean precomputing a combinatorial
set of scopes, most of which would never be queried, and would still be wrong the
moment someone picks a combination that wasn't precomputed. One row per combination
that actually exists, summed at read time, is correct for every possible rollup
without needing to know the combinations in advance.

**Tiering rule:** for a given `(metricId, reportingUnitId, disciplineId, ownerId)`,
the function writes *either* per-tier rows (`tier`: 1, 2, 3 — whichever apply) *or*
exactly one untiered row (`tier`: null), **never both**. Mixing them would let a naive
sum double-count. The app derives "all tiers" by summing the per-tier rows when they
exist, and reads the untiered row directly when tiering is off for that project/metric.

**Entity-ownership rule (for the function, not a schema field):** an entity's
`reportingUnitId` must come from the entity's own space/property, never from a linked
or target entity's space/property. Cross-space links are common (e.g. a time series in
one plant's space linked to an asset tagged to a different plant) — without this rule,
the same entity could be miscounted into the wrong unit, which breaks the "sum rows to
get the correct total" invariant everything above depends on.

**`ownerId`** is carried as best-effort only — it's typically derived via prefix/path
mapping (`OwnerMappingDto`), not a native property of a reporting unit, so owner-scoped
rollups are not guaranteed correct until this is resolved. Open question, not yet
resolved.

**Ratio vs. average metrics:** `eligibleCount` is the denominator for both shapes.
Ratio metrics (e.g. TS-to-asset rate) also populate `passingCount`/`issueCount`.
Average metrics (e.g. Average Depth, Average Children, average annotation confidence —
see `context_quality_formulas.md` in the dashboard repo) populate `valueSum` instead;
the app computes the average as `ΣvalueSum ÷ ΣeligibleCount` across matching rows, the
same "sum raw values, divide at read time" pattern as the ratio case.

**Custom quality rules:** the app identifies customer-defined rules by `ruleId`
(`src/types/qualityRules.ts`), not `metricId`. Proposed encoding, not yet confirmed:
write `metricId` as `custom:<ruleId>`, with `dimensionId` set to whichever existing
dimension the rule's underlying relationship is associated with in the Configuration
UI.

**Display fields are deliberately not stored here.** `labelKey`, `direction`,
`actionKey`, and similar per-metric display properties are static and already live in
the Flow app's own metric catalog (`src/services/mock/metricCatalog.ts`); storing them
per row would duplicate that catalog and risk drifting from it. `status` is also not
stored: it's computed today from tier-1 targets after rollup
(`computeDimensionStatus`), so a value computed per reporting unit would be meaningless
once the app rolls up to a wider scope.

**Open, unresolved question — not a schema change:** the mock computes a *dimension's*
overall percent as a plain average of its scored tier percentages
(`computePortfolioAggregatePercent` / `aggregation.ts`), not as
`sum(passing)/sum(eligible)` across tiers. Both algorithms are derivable from the raw
counts stored here (sum-of-counts directly; average-of-percents by computing percent
per row first, then averaging those), so this schema doesn't block either — but which
algorithm applies at which rollup level (tier→dimension, and any facility→org
rollup) is a product decision this schema doesn't make for you. Needs resolving before
the app's reader is built, not after.

### Template: `BasicArchive` (immutable) — `ContextQualityMetricSnapshot` only

| Limit (`BasicArchive`) | Value |
|---|---|
| Max records/stream | 50,000,000 |
| Max volume/stream | 50 GB |
| Ingest rate | 170,000 records / 170 MB per 10 minutes |
| Max `lastUpdatedTime` filter window | 365 days |
| Retention | Unlimited — and, since deletion is only available for mutable streams, permanent once written |

**Every run must write a complete snapshot, not just what changed.** The backend
function's own *reads* are incremental and checkpointed, but a snapshot run's rows are
summed together to build one trend point — if a run only wrote the reporting
units/discipline combinations that changed since the last run, that run's sum would be
partial and the trend line would show a drop that didn't happen. Each run must
regenerate the full current-state snapshot (from `ContextQualityCurrentMetric`) and
append that in full, even though reading from CDF to get there is incremental.

**Sizing, at an estimated ~100 reporting units × ~45 metrics × 3 tiers ≈ 13,500
records/run** (discipline/owner fan-out will push this higher in practice, since only
real combinations get rows, not a multiplier applied uniformly): **at one run/day,
roughly 5M records/year — comfortably under the 50M cap for years. At hourly runs,
that's ~120M/year — over the cap within the first year.** Run frequency is a real
constraint on this design, not a detail to decide later; pin it down before committing
to a schedule, and re-check this sizing once real per-unit/discipline counts are known.

### Query patterns

- **`ContextQualityMetricSnapshot` (immutable):** every `filter`/`aggregate` call
  requires a `lastUpdatedTime` range (max 365 days on `BasicArchive`) — so a trend
  longer than a year needs several chained queries, not one. "Fetch latest run":
  `aggregate` with `Max(lastUpdatedTime)`, then `filter` on `runId`. Use `sync` rather
  than `filter` once a query would return more than 1,000 rows (no pagination on
  `filter`).
- **`ContextQualityCurrentMetric` / `ContextQualityCurrentExclusion` /
  `ContextQualityDrilldownRow` (mutable nodes):** standard `list`/`filter` with
  ordinary cursor pagination — no Records-specific rules. At current sizing (tens of
  thousands of rows at most), indexes matter less than query shape; both node
  containers are indexed on `(metricId or dimensionId, facilityId)` to match how the
  Flow app's Dimension and metric-card pages actually read, not on `tier`.

## Module Components

```
context_quality_v2/
├── data_modeling/
│   ├── context_quality_metrics.Space.yaml       # schema space
│   ├── context_quality_instances.Space.yaml     # instance space for written node instances
│   ├── containers/
│   │   ├── ContextQualityCurrentMetric.Container.yaml     # usedFor: node (mutable)
│   │   ├── ContextQualityMetricSnapshot.Container.yaml    # usedFor: record (immutable)
│   │   ├── ContextQualityCurrentExclusion.Container.yaml  # usedFor: node (mutable)
│   │   └── ContextQualityDrilldownRow.Container.yaml      # usedFor: node (mutable)
│   └── views/
│       ├── ContextQualityCurrentMetric.View.yaml
│       ├── ContextQualityMetricSnapshot.View.yaml         # streamId-bound
│       ├── ContextQualityCurrentExclusion.View.yaml
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

Select **Dashboards** → **Contextualization Quality Dashboard — Records & Streams Backend**. Then:

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
