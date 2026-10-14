# Context Quality metrics contract (version 1)

This is the canonical contract for the Context Quality metrics run log. The library module `dashboards/context_quality_v2` owns it, together with the Records stream, containers and views that carry it. Consumers (the Context Quality Dashboard is the reference consumer) and producers (the metrics function) depend on this file. Neither defines the contract on its own.

**Baseline.** Version 1 is exactly what is already deployed and in use in the first (legacy) deployment of this backend: an immutable Records run log in two containers, six row kinds, and a run-complete marker that is written last. It was extracted from the dashboard's Phase 1 contract and its golden samples without changing their meaning. Nothing here requires renaming, recreating or migrating the legacy deployment's resources or their history.

## Files

| File | What it is |
|---|---|
| `CONTRACT.md` | This document: the rules and semantics |
| `contract.json` | The same identifiers, row kinds and properties in machine-readable form |
| `samples.json` | One golden record per row kind, including minimal records, edge cases and invalid records, with what a consumer must parse from each |
| `../data_modeling/`, `../streams/` | The container, view and stream definitions that carry the contract |

`tests/test_context_quality_contract.py` keeps them in step. It checks that the samples fit the containers, that `contract.json` accounts for every container property, that the tables in this file match `contract.json`, and that the deployment variables reproduce the identifiers of the legacy deployment.

A consumer that needs the contract for its own tests carries a **versioned snapshot** of `samples.json` and the parts of `contract.json` it uses. The snapshot is a copy: change the contract here first.

## 1. Where the data lives

| Item | Library variable | Legacy deployment value (preserved) |
|---|---|---|
| Stream | `metricsStreamExternalId` | `cq_demo_metrics` |
| Stream template | `metricsStreamTemplate` | `ImmutableTestStream` |
| Schema space | `metricsSchemaSpace` | `sp_cq_demo_metrics` |
| Instance space (the `space` of each record) | `metricsInstanceSpace` | `sp_cq_demo_metrics_data` |
| View version | `metricsVersion` | `v1` |
| Run-log container | fixed by this contract | `ContextQualityRelationship` |
| File-annotation container | fixed by this contract | `ContextQualityFileAnnotation` |

The stream, spaces and view version are deployment choices, set through the module's variables. The two container and view external ids are part of the contract and are **not** variables: consumers rely on them. Both containers are `usedFor: record`. Each view has the same external id as its container and lists the stream in `streamId`.

**Record identity.** A record's `externalId` is `{runId}:{rowKey}` in the instance space, where the row key starts with one of the prefixes listed in `contract.json` (`run:started`, `run:complete`, `agg:`, `excl:`, `drill:`, `cls:`, `anno:`). Consumers never read the key. Only the properties matter to them. `drilldown` and `classification` keys are `drill:{metricId}:{digest}` and `cls:{classificationKind}:{digest}`, where the digest is the first 32 hex characters of the SHA-256 of the compact JSON of the record's complete logical key (for a drilldown record: row kind, metric, dimension, instance space and entity id; for a classification record: the full grain including the classification label). Entities with the same id in different spaces therefore never share a record id, and rewriting the same run keeps the same ids. The other kinds keep their readable keys. Runs written before this scheme used readable `drill:{metricId}|{entityId}` and `cls:` keys; consumers never read keys, so both are valid.

**Counts, not percentages.** The backend writes raw counts. Consumers sum the rows for the scope the user picked and divide.

## 2. Row kinds

`rowKind` is a required text property on both containers. Its values are fixed:

| `rowKind` | Container | Written by the backend | Read by the dashboard (reference consumer) |
|---|---|---|---|
| `run` | `ContextQualityRelationship` | yes (`started`, then `complete`) | yes, `parseRunRow`, filtered to `status = complete` |
| `aggregate` | `ContextQualityRelationship` | yes | yes, `parseAggregateRow` |
| `exclusion` | `ContextQualityRelationship` | yes | yes, `parseExclusionRow` |
| `drilldown` | `ContextQualityRelationship` | yes, capped per metric and unit | yes, `parseDrilldownRow` |
| `classification` | `ContextQualityRelationship` | yes | yes, `parseClassificationRow` |
| `annotationBucket` | `ContextQualityFileAnnotation` | yes | **no**, there is no reader yet |

A parser returns `null` for a record that lacks a required field or has it with the wrong type. The dashboard then skips that record silently.

### 2.1 Fields shared by every record

These come from the container and are not nullable. The backend adds them to every record.

| Field | Type | Meaning |
|---|---|---|
| `rowKind` | text | One of the values above |
| `runId` | text | `run-YYYYMMDDTHHMMSS-xxxxxx`. Every row of one run has the same id |
| `runTimestamp` | timestamp | When the run started (ISO 8601 UTC, `.000Z`) |

### 2.2 `run`

| Field | Type | Required by dashboard | Meaning |
|---|---|---|---|
| `runId`, `runTimestamp` | text, timestamp | **yes** | Same as above |
| `status` | text | filter only | `started` or `complete`. `failed` is allowed by the design but the function does not write it today |
| `scope` | text | no | `all` today |
| `warnings` | text | no | Written on the `complete` marker when the run has anything to warn about: compact JSON, a list of warnings (section 3.3). Consumers should treat it as text |
| `rowsWritten` | int64 | no | Rows written by the run, on the `complete` row only |

### 2.3 `aggregate`

One row per metric, reporting unit, division, discipline, owner, tier and sub-key. The grain fields below are all optional.

| Field | Type | Required by dashboard | Meaning |
|---|---|---|---|
| `runId`, `runTimestamp` | | **yes** | |
| `metricId` | text | **yes** | See section 5.2 |
| `dimensionId` | text | **yes** | See section 5.1 |
| `reportingUnitId` | text | **yes** | Smallest area counted. Today one unit per facility space |
| `facilityId`, `divisionId`, `disciplineId`, `ownerId` | text | no | Null means "not known". A null never matches an active filter for that field |
| `tier` | int32 | no | 1, 2 or 3. Anything else, or null, reads as untiered (section 6.1) |
| `subKey` | text | no | For metrics written once per value, for example `approvalMix` (`Approved`, `Suggested`, `Rejected`) |
| `eligibleCount` | int64 | no (reads as 0) | Denominator, or population size for averages |
| `passingCount` | int64 | no (reads as 0) | Numerator for ratio metrics |
| `issueCount` | int64 | no (reads as 0) | Items that fail |
| `valueSum` | float64 | no (reads as 0) | For averages. `confidenceAverage` stores the sum of confidences (0 to 1). The dashboard divides by `eligibleCount` |
| `notApplicable` | boolean | not read | `true` on a row whose `eligibleCount` is 0: the metric has a source population in the reporting unit but nothing eligible to measure (section 6.2). Absent on every measured row |
| `matchType`, `ruleSetVersion` | text | not read | Reserved |

### 2.4 `exclusion`

| Field | Type | Required by dashboard | Meaning |
|---|---|---|---|
| `dimensionId` | text | **yes** | |
| `exclusionCategory` | text | **yes** | See section 5.3 |
| `facilityId`, `divisionId`, `disciplineId` | text | no | |
| `excludedCount` | int64 | no (reads as 0) | Entities left out of the metric's denominators |

### 2.5 `drilldown`

One row per failing entity. The function writes at most `drilldownCap` (500) rows per metric and reporting unit and does not record that it truncated.

| Field | Type | Required by dashboard | Meaning |
|---|---|---|---|
| `metricId`, `dimensionId` | text | **yes** | Only the metrics marked `drilldown` in section 5.2 |
| `entityId` | text | **yes** | The entity's external id |
| `entityLabel` | text | no (falls back to `entityId`) | Display name |
| `detail` | text | no (reads as empty) | Why the entity fails |
| `facilityId`, `divisionId`, `disciplineId`, `ownerId` | text | no | |
| `tier` | int32 | no (**reads as 3**, not untiered) | |
| `cellsJson` | text | no | A JSON object `{ "<classificationKind>": "<label>" }`. Entries whose value is not a string are dropped. Invalid JSON reads as no classes |
| `entityRef`, `matched` | text, boolean | not read | `entityRef` is `space:externalId`. `matched` is always false |

### 2.6 `classification`

One row per classification value, reporting unit, division, discipline, owner and tier. Classification kinds are discovered from the project (section 5.4).

| Field | Type | Required by dashboard | Meaning |
|---|---|---|---|
| `classificationKind` | text | **yes** | `assetType`, `assetClass`, `equipmentType`, `fileCategory` today |
| `classificationId` | text | **yes** | The reference node's external id, or `__none__` for entities with no class |
| `classificationName` | text | no (falls back to the id) | Display label. "Unclassified" for `__none__` |
| `dimensionId`, `reportingUnitId` | text | not read | Written for completeness |
| `facilityId`, `divisionId`, `disciplineId`, `ownerId` | text | no | |
| `tier` | int32 | not read | Written, but the dashboard does not slice by it |
| `eligibleCount`, `passingCount` | int64 | no (read as 0) | Entities of that class, and how many pass the participation test for their kind |
| `issueCount` | int64 | not read | `eligibleCount - passingCount` |

### 2.7 `annotationBucket` (container `ContextQualityFileAnnotation`)

Summaries of file annotations by status and confidence. The container also allows `fileSummary` style fields (`fileExternalId`, `fileSourceId`, `matchedCount`, `unmatchedCount`, `detectedTagCount`, `unmatchedTagsJson`). The function does not write them today. The dashboard has no parser for this container, so nothing here is read yet.

| Field | Type | Meaning |
|---|---|---|
| `reportingUnitId`, `facilityId`, `divisionId`, `disciplineId`, `ownerId`, `tier` | | Same grain as other rows |
| `annotationType` | text | `asset-tag` today (fixed by the function) |
| `status` | text | Annotation status, for example `Approved`, `Suggested`, `Rejected` |
| `confidenceBucket` | text | For example `high`, `medium`, `low` |
| `count` | int64 | Annotations in the bucket |
| `confidenceSum` | float64 | Sum of confidence values in the bucket |

## 3. The run-complete marker

1. The backend writes `run:started` (`status: started`).
2. It writes all `aggregate`, `exclusion`, `drilldown` and `classification` rows, then the `annotationBucket` rows.
3. It writes `run:complete` (`status: complete`, `rowsWritten`) **last**.

The records are immutable. A run is readable only once its `complete` row exists. **"Current" means the run with the latest `runTimestamp` among the rows where `rowKind = run` and `status = complete`.** The dashboard reads only that run's rows for the overview, dimension pages, drilldowns, issues and classifications. History (trends) reads one metric across every completed run, and ignores rows from runs that have no `complete` row.

A run that failed part-way leaves rows behind and no `complete` marker. It is never shown, and it expires with the stream's retention. If the stream has no completed run in its retention window, a consumer has no data and should say so, telling an empty stream from a stream that only holds unfinished runs.

The marker is the only thing that makes a run visible. A producer must write it last, once, and only after every other row was accepted.

### 3.1 Contract version

The `run` marker may carry a `contractVersion` text property (`1`, `1.2`, ...). It is optional:

- **Absent means version 1.** Every run written so far is version 1.
- A consumer states which major version it understands. The reference dashboard understands major version **1**: if the latest completed run declares any other major version, or a value that is not a version, it refuses to read it instead of showing numbers it may misread.
- Only the latest run's marker is checked. An older run with a different version does not matter.
- The property does not exist in the `ContextQualityRelationship` container today. Declaring a version requires adding that property to the container first. That is an additive change (section 8), but it has not been done: see the open decisions in the module README. Until then nothing declares a version and every run reads as version 1.

### 3.3 Run warnings

The `complete` marker's `warnings` property is `null` when there is nothing to warn about. Otherwise it is compact JSON: a list of warning objects sorted by `code`, then `subject`, then `reason`, with sorted keys, so the same run always gives the same text. Each warning has a `code` and a human-readable `message`, usually a `subject`, and code-specific counts. Warnings never contain entity ids or names. The text is limited to 4,000 characters: if the warnings do not fit, the last ones are dropped and a final `warnings-truncated` entry says how many.

| Code | Subject | Meaning | Other fields |
|---|---|---|---|
| `mapping-empty` | the mapping key (`tierByTypeCode`, `disciplineByTypeCode`, `assetTypeByEquipmentTypeCode`) | A required mapping is empty although the entities it applies to exist | |
| `unmapped-asset-types` | | Assets that are not roots and have no type code, or one `tierByTypeCode` does not map (they have no tier) | `untyped`, `unmapped`, `codes` (up to 10, sorted) |
| `parents-not-loaded` | | Assets whose parent is not among the loaded assets. They count as having a parent and their depth is a lower bound (section 6.6) | `assets` |
| `unmapped-equipment-types` | | Equipment whose type `assetTypeByEquipmentTypeCode` does not map | `unmapped`, `codes` (up to 10, sorted) |
| `optional-source-unavailable` | the view as `space.view` | An optional source view does not exist in the project, so what depends on it is not measured | |
| `no-source-entities` | the population (`assets`, `equipment`, `timeseries`, `work_orders`, `notifications`, `files`, `annotations`, `objects3d`) | No entities of that kind exist in any facility space | |
| `metric-not-applicable` | the metric, `/subKey` where it has one | Not-applicable rows were written for it (section 6.2) | `reason` (`mapping-empty:tierByTypeCode`, `mapping-empty:assetTypeByEquipmentTypeCode`, `all-excluded`, `no-eligible-entities`), `units` |
| `drilldown-truncated` | the metric | The drill-down list was cut at the configured limit; aggregate counts are complete | `limit`, `dropped`, `units` |
| `warnings-truncated` | | The warnings did not fit | `dropped` |

A run with no facility space, or whose required source views cannot be read, is not written at all: nothing, not even the `started` marker, is written, so the previous run stays current. A missing optional source is a warning, not a failure.

## 4. Stream constraints the contract depends on

- The stream is immutable. Records cannot be changed or deleted, so a bad row stays until it expires. That is why a run only becomes visible when its `complete` marker is written (section 3).
- A stream's template cannot be changed after the stream is created. The template, and the limits that come with it, are therefore a property of the deployed stream, not of the contract. In the legacy deployment the stream uses `ImmutableTestStream`: records are removed after 7 days and a query may not span more than 7 days. `BasicArchive` keeps records without expiry and allows spans up to 365 days.
- A records query returns at most 1,000 records and has no paging, so a consumer splits a query that fills a page (by dimension, tier, metric or classification kind). A query that stays full after splitting cannot be answered.
- A query must stay inside the stream's `maxFilteringInterval`. Consumers read the limit from the stream's settings and never ask for a longer span.
- The history a consumer can show is bounded by the stream, not by the contract.

## 5. Identifiers

### 5.1 Dimension IDs

| `dimensionId` | Optional dimension | Headline metric | Headline sub-key |
|---|---|---|---|
| `asset-hierarchy` | no | `hierarchyCompletionRate` | |
| `equipment-asset` | no | `associationRate` | |
| `time-series` | no | `tsToAssetRate` | |
| `files` | no | `fileToAssetRate` | |
| `maintenance` | yes | `woToAssetRate` | |
| `file-annotation` | yes | `approvalMix` | `Approved` |
| `model-3d` | yes | `threeDToAssetRate` | |

The headline metric is the one whose pass rate stands for the dimension on the Overview. A dimension with no rows is shown as disabled. The `optional` flag only marks the dimension as one that a project may not have.

### 5.2 Metric IDs

Every metric the reference dashboard knows. "Kind" says how the dashboard reads it: `aggregate` metrics are read from `aggregate` rows and `drilldown` metrics from `drilldown` rows. The issue kind is the Issues page category. "Function writes" is whether the baseline function writes the metric today (`emitted` in `contract.json`). A metric nobody writes simply has no rows.

| `metricId` | Dimension | Kind | Direction | Issue kind | Lifecycle | Function writes |
|---|---|---|---|---|---|---|
| `hierarchyCompletionRate` | asset-hierarchy | aggregate | critical | – | – | yes |
| `orphanRate` | asset-hierarchy | aggregate | critical | – | – | yes |
| `orphansByTier` | asset-hierarchy | drilldown | informational | orphan-asset | – | yes |
| `depthOutliers` | asset-hierarchy | drilldown | informational | depth-outlier | – | yes |
| `typeCoverageGaps` | asset-hierarchy | drilldown | critical | type-gap | – | yes |
| `associationRate` | equipment-asset | aggregate | critical | – | – | yes |
| `assetEquipmentCoverage` | equipment-asset | aggregate | critical | – | – | yes |
| `serialMfrCompleteness` | equipment-asset | aggregate | critical | – | – | yes |
| `unmatchedWithCandidates` | equipment-asset | drilldown | informational | unmatched-equipment | – | yes |
| `completenessByManufacturer` | equipment-asset | drilldown | informational | – | – | – |
| `typeInconsistencies` | equipment-asset | drilldown | informational | type-inconsistency | – | yes |
| `tsToAssetRate` | time-series | aggregate | critical | – | – | yes |
| `monitoringCoverage` | time-series | aggregate | critical | – | – | yes |
| `freshness` | time-series | aggregate | informational | – | – | yes |
| `orphansByPrefix` | time-series | drilldown | informational | orphan-timeseries | – | yes |
| `staleTimeSeries` | time-series | drilldown | informational | stale-timeseries | – | yes |
| `tier1CoverageGaps` | time-series | drilldown | critical | tier1-ts-gap | – | yes |
| `inferredTagsWithoutMatch` | time-series | drilldown | informational | – | – | – |
| `woToAssetRate` | maintenance | aggregate | critical | – | – | yes |
| `woToNotificationRate` | maintenance | aggregate | critical | – | – | yes |
| `notificationToAssetRate` | maintenance | aggregate | critical | – | – | yes |
| `woCompletionRate` | maintenance | aggregate | critical | – | – | yes |
| `notificationToWoRate` | maintenance | aggregate | critical | – | – | yes |
| `assetMaintenanceCoverage` | maintenance | aggregate | critical | – | – | – |
| `notificationToEquipmentRate` | maintenance | aggregate | critical | – | – | – |
| `woToEquipmentRate` | maintenance | aggregate | critical | – | – | – |
| `failureDocumentationRate` | maintenance | aggregate | critical | – | – | – |
| `equipmentMaintenanceCoverage` | maintenance | aggregate | critical | – | – | – |
| `woWithoutNotification` | maintenance | drilldown | informational | wo-without-notification | – | yes |
| `unlinkedMaintenance` | maintenance | drilldown | informational | unlinked-maintenance | – | yes |
| `tier1MaintenanceGaps` | maintenance | drilldown | informational | – | – | – |
| `annotationCount` | file-annotation | aggregate | informational | – | – | yes |
| `confidenceAverage` | file-annotation | aggregate | informational | – | – | yes |
| `approvalMix` | file-annotation | aggregate | informational | – | – | yes |
| `lowConfidenceQueue` | file-annotation | drilldown | informational | low-confidence-annotation | – | yes |
| `unprocessedBacklog` | file-annotation | drilldown | informational | – | – | – |
| `rejectionByType` | file-annotation | drilldown | informational | – | – | – |
| `pendingReview` | file-annotation | drilldown | informational | pending-review | – | yes |
| `patternVsAnnotation` | file-annotation | drilldown | informational | – | – | – |
| `threeDToAssetRate` | model-3d | aggregate | critical | – | – | yes |
| `asset3dCoverage` | model-3d | aggregate | critical | – | – | yes |
| `bboxCompleteness` | model-3d | aggregate | critical | – | – | yes |
| `unlinkedTier1` | model-3d | drilldown | informational | unlinked-3d | – | yes |
| `bboxGaps` | model-3d | drilldown | informational | bbox-gap | – | yes |
| `modelTypeCoverageByTier` | model-3d | aggregate | critical | – | – | – |
| `fileToAssetRate` | files | aggregate | critical | – | – | yes |
| `metadataCompleteness` | files | aggregate | critical | – | – | yes |
| `unlinkedByCategory` | files | drilldown | informational | unlinked-file | – | yes |
| `missingMetadata` | files | drilldown | informational | – | – | – |
| `uploadStatusGaps` | files | drilldown | informational | – | – | – |
| `staleAnnotationLinkages` | file-annotation | drilldown | informational | – | yes | – |
| `uncontextualizedRevisions` | files | drilldown | informational | – | yes | – |
| `activeChildrenUnderRetiredParents` | asset-hierarchy | drilldown | informational | active-child-retired-parent | yes | yes |
| `brokenCrossViewLinks` | equipment-asset | drilldown | informational | – | yes | – |

### 5.3 Exclusion categories

| `exclusionCategory` | Source | Written by the function today |
|---|---|---|
| `missing-upstream-target` | system | no |
| `invalid-ocr` | system | no |
| `synthetic-timeseries` | system | yes |
| `historical-retired` | system | no |
| `location-centric-files` | system | yes |
| `user:file-extension` | user | yes |
| `user:tag-pattern` | user | no |
| `user:decommissioned` | user | yes |

### 5.4 Classification kinds

`assetType`, `assetClass`, `equipmentType`, `fileCategory`. The function emits a kind only if some entity references an existing reference node of that kind. The dashboard orders these four first and shows any other kind last, with a label made from the camel-case name. The dashboard translates only these four.

## 6. Semantics

### 6.1 Tier

| Value | Meaning | Target | Scored |
|---|---|---|---|
| 1 | Major equipment | 95% | yes |
| 2 | Instrumentation | 60% | yes |
| 3 | A tier the project's `tierByTypeCode` maps asset types to. Never assigned by default | none | no |
| null or other | Untiered: the tier is unknown | | |

The function assigns tiers like this, and never invents one:

1. An asset gets the tier its own type code has in `tierByTypeCode` (`{type code: 1, 2 or 3}`). The mapping is empty by default, because tiers are project-specific. An asset with no type, or a type the mapping does not list, has **no tier**; that is not tier 3.
2. An entity that is not an asset (equipment, time series, file, work order, notification, 3D object, file annotation) takes the tier of the asset it links to, if that asset was loaded and has one.
3. An entity with no linked asset, or whose asset has no tier or was not loaded, has no tier. No other rule applies.

A row with no tier carries no `tier` property. It is unknown, not tier 3: a tier 3 row exists only for assets the project maps to 3 and the entities linked to them. The targets are fixed in the app (`TIER_TARGETS`).

With tiering on, the overview scores each tier separately, so a row with no tier is counted in no tier score. With tiering off, the dashboard pools all rows, tiered or not. Drilldown rows with no tier are shown as tier 3.

### 6.2 Availability

- **A dimension is available when it has at least one aggregate row** in the current run. With no row it is shown as disabled. Optional dimensions are also flagged as optional.
- **A metric is measured for a reporting unit when it has aggregate rows there with `eligibleCount > 0`.** A measured zero is `eligibleCount > 0` and `passingCount = 0`: 0 of N, shown as 0%.
- **A metric is not applicable for a reporting unit** when entities of the kind it is computed from exist in that unit but none is eligible (everything excluded, or a required mapping or tier is missing). The function writes one row for it with `eligibleCount` 0, `passingCount` 0, `issueCount` 0 and `notApplicable` true, at unit grain (no division, discipline or tier, the dimension's default owner). A consumer must show no value for it, never 0% and never as healthy. The run's `warnings` say why.
- **A metric with no row for a unit was not measured there.** When no entity of the kind it is computed from exists in the unit, nothing is written, so the dimension is simply absent for that unit. `activeChildrenUnderRetiredParents` only counts violations and has no population of its own: no rows means no violations were found.
- **Registered rows are bounded.** Not-applicable rows are written per metric and reporting unit only when the unit has no row for that metric, never per slice, so the extra rows are at most metrics x units.
- A dimension is available when it has at least one aggregate row, and not-applicable rows count: a dimension whose every metric is not applicable is available with no value (consumers should not show it as healthy).
- A tier is available when it has aggregate rows for the headline metric. Otherwise its score is empty.

### 6.3 Sums and percentages

Percent = `passingCount / eligibleCount`, summed over the rows in scope. For `confidenceAverage` it is `valueSum / eligibleCount`. For `approvalMix` only the `Approved` sub-key is used for the headline rate.

### 6.4 Issue kinds

Drilldown metrics map to Issues page categories in `ISSUE_KIND_BY_METRIC`, see the table in 5.2. A drilldown metric with no issue kind is available in the dimension drilldown but never appears on the Issues page.

### 6.5 Metric direction

The dashboard derives a metric's direction from its id: an id containing `Rate`, `Coverage` or `Completeness` is `critical`, everything else is `informational`. A critical metric below 60% is shown as critical, and a metric with no issues is shown as good. The direction is not stored in the records. Two drilldown metrics (`typeCoverageGaps`, `tier1CoverageGaps`) read as critical only because their names contain "Coverage".


### 6.6 Hierarchy: roots, orphans and parents

The hierarchy is read from the assets' `parent` relationships among the loaded assets (those in the facility spaces). Whether a parent or child lies in the same facility space does not matter.

| Asset | Meaning | `hierarchyCompletionRate` | `orphanRate` |
|---|---|---|---|
| **Root** | No parent, and at least one loaded asset names it as parent | not eligible (a root is not expected to have a parent) | eligible, passes |
| **Child** | Has a parent | eligible, passes | eligible, passes |
| **Orphan** (standalone) | No parent and no children | eligible, fails | eligible, fails; listed in `orphansByTier` |

- An asset whose parent is not among the loaded assets (a parent in a space that is not a facility, or deleted) has a parent: it is a child, it passes, and it is never counted as a root or an orphan. The run warns with `parents-not-loaded`, and its depth counts the unloaded parent as one level.
- Roots are also left out of `typeCoverageGaps` (a root is structural, not a typed asset) and out of the asset classifications' eligible population, and out of the tier 1 and 2 coverage metrics.
- Entities that are not assets are not part of the hierarchy.

## 7. Reading rules for consumers

The Context Quality Dashboard (the reference consumer) reads the log this way. Other consumers should do the same, so that the same data gives the same numbers everywhere.

| Case | Rule |
|---|---|
| Unknown `rowKind` | Not queried, so never seen |
| Unknown extra property | Ignored |
| Missing optional property | Null for text, 0 for counts |
| Missing or wrongly typed required property | The record is skipped |
| Unknown `metricId`, `dimensionId` or `exclusionCategory` | Not shown. The data is hidden rather than misread (see section 10) |
| `tier` other than 1, 2 or 3 | Aggregate rows read as untiered. Drilldown rows read as tier 3 |
| Unknown `classificationKind` | Kept, shown after the known kinds |
| `status` other than `complete` on a `run` row | The run is never read |
| Unsupported `contractVersion` on the latest run | Refuse to read the backend and say so |

## 8. Compatibility requirements for historical records

The stream is immutable and old runs stay readable until retention removes them. The function and the dashboard are deployed separately, so both must cope with runs written by an earlier version.

1. **Only additive changes.** New optional properties, new metric ids, new classification kinds and new exclusion categories may be added. A new row kind needs a dashboard release before the function writes it.
2. **Never rename or repurpose a property, a metric id, a dimension id or a category.** An old run still carries the old value for up to the retention window.
3. **Never change the meaning of a count.** A change in what `eligibleCount` or `passingCount` mean for a metric needs a new metric id, otherwise trends mix two definitions.
4. **Required fields stay required, optional fields stay optional.** Making a field required breaks old runs. Parsers must keep tolerating the absence of any optional field.
5. **`complete` marker semantics do not change.** A producer that does not write the marker last produces runs the dashboard shows half-written, or never shows.
6. **Container properties are never removed and their types never change.** Add a new property instead.
7. **The dashboard must keep reading runs from before the latest function version.** In the legacy deployment today:
   - The first runs (`run-20261012T174025-9ca009`, `run-20261012T175322-4c4fbf`) have no `classification` rows and no `cellsJson` on drilldown rows. The dashboard shows empty classification tables for them and no classes in drilldowns.
   - The 8 weekly history runs were fabricated by the demo `seed_history` action. They contain `aggregate` rows only, with no exclusion, drilldown or classification rows.
8. **Contract version.** This document is version 1. A change that breaks a rule above must raise the major version, describe the migration, and be declared on the `run` marker (section 3.1) so older dashboards refuse it instead of misreading it.

## 9. Not part of the contract

- Facility, division, discipline and owner ids are open sets. What they mean (for example a facility is an instance space, a division is a tag) belongs to the producer's configuration and is not fixed here.
- Display names for those ids, tier targets and the 60% critical threshold. They belong to the consumer.
- Demo and seed data, and the synthetic history that a demo module may write. Producers of real runs never write them.

## 10. Known gaps

These are current behaviours that the contract records without endorsing them. They come from the portability audit.

- The dashboard (section 7) does not read `notApplicable`. It shows `eligibleCount` 0 as no value, but a dimension with only not-applicable rows is shown as available, and its status logic can read as healthy.
- The dashboard reads a drilldown row with no tier as tier 3 (section 6.1), although the function now leaves unknown tiers absent.
- A parentless asset whose children all live outside the loaded (facility) spaces looks standalone, so it is counted as an orphan rather than a root (section 6.6).
- The dashboard hides unknown metric ids, dimension ids and exclusion categories.
- Metric direction is guessed from the id. A rename flips the severity.
- `annotationBucket` rows are written but not read.
- The contract version is optional and the container has no property for it yet, so no run declares one (section 3.1).
- Dimension, metric and exclusion lists are copied into several files on the consumer side. `contract.json` is now the single source to copy from.

## 11. Changing the contract

1. Change `contract.json`, `samples.json` and this file together.
2. Change the container and view definitions only additively (section 8). Run `uv run pytest tests/test_context_quality_contract.py`.
3. Raise the major version, and declare it on the `run` marker, for any change that breaks a rule in section 8.
4. Tell the consumers: the dashboard refreshes its snapshot of `samples.json` and runs its parser tests.
