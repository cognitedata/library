# File Annotation Dashboard User Guide

For setup, runtime, and deployment, see [CONFIG.md](CONFIG.md).

![Dashboard Home](./docs/screenshots/01-app-home.png)

## Purpose
The File Annotation Dashboard is designed to help users operate the file annotation process, investigate file-level results, improve pattern quality and troubleshoot pipeline execution.

This guide explains how to use the app in day-to-day scenarios.

## App navigation
After selecting a pipeline, you can open three areas:

| Area | What it is for |
| --- | --- |
| Annotation Quality | Coverage analysis and file-level annotation inspection |
| Pattern Management | Pattern curation and candidate onboarding |
| Pipeline Health | Operational monitoring and troubleshooting |

## Page guide

### Annotation Quality

#### Overall tab
Use this tab to answer: How healthy is annotation quality right now?

![Annotation Quality - Overall](./docs/screenshots/02-annotation-quality-overall.png)

What you can do:
- Review annotation coverage KPIs.
- Analyze coverage split by dimensions such as file resource type, tag resource type and scope (e.g. site or unit).
- Identify where to drill down next.

#### Per-File tab
Use this tab to answer: What was detected for each file versus what was actually linked?

![Annotation Quality - Per-File](./docs/screenshots/03-annotation-quality-per-file.png)

What you can do:
- Filter and sort files by coverage and metadata.
- Open a file in preview mode.
- Compare Actual Annotations and Potential Annotations.
- Navigate and search annotations with the Annotation Navigator.

### Pattern Management
Use this page to maintain matching behavior.

![Pattern Management](./docs/screenshots/04-pattern-management.png)

What you can do:
- Add, edit and save manual patterns.
- Import patterns from CSV.
- Generate proposals by primary scope (based on automatic patterns).
- Refresh cache after alias updates.

### Pipeline Health
Use this page to monitor and debug execution behavior.

#### Overview tab
Use this tab to answer: Is the pipeline currently healthy?

![Pipeline Health - Overview](./docs/screenshots/05-pipeline-health-overview.png)

What you can do:
- Review live KPIs (for example, files awaiting processing, processed volume, failure indicators).
- Check throughput behavior over time.
- Identify whether there is an immediate operational issue before drilling down.

#### Files tab
Use this tab to answer: What happened to this specific file?

![Pipeline Health - Files](./docs/screenshots/06-pipeline-health-files.png)

What you can do:
- Search files by name, source ID, external ID or status.
- Inspect status and file-level metadata.
- Open stage logs (Prepare, Launch, Finalize, Promote) for troubleshooting.

#### History tab
Use this tab to answer: How has the pipeline performed over recent runs?

![Pipeline Health - History](./docs/screenshots/07-pipeline-health-history.png)

What you can do:
- Filter runs by time window, status and caller type.
- Compare success and failure outcomes.
- Open run details to support trend analysis and escalation.

## Task-based tutorials

### Task 1: Check annotations for a specific file
Use this when a user asks: Did this file get annotated correctly?

1. Open the app and select the target extraction pipeline.
2. Open Annotation Quality.
3. Confirm you are on Overall first, then switch to Per-File.
4. Scroll to the File Aggregation section.
5. Find the file using search, filters or sorting.
6. Select the file row and click Preview.
7. Scroll down to the File Preview section.
8. Review Actual Annotations and Potential Annotations.
9. Hover or click annotations to inspect details.
10. Use Annotation Navigator to search by tag and jump to results.

Expected outcome:
- You can confirm whether the file was correctly annotated and identify missing or only-potential matches.

[Screenshot sequence here: Task 1 - pipeline selection to preview]

### Task 2: Find low-coverage files to prioritize remediation
Use this when a team wants to improve quality where impact is highest.

1. Open Annotation Quality > Per-File.
2. Apply coverage and metadata filters.
3. Sort files by lowest annotation coverage first.
4. Identify repeated patterns in low-performing files.
5. Open one or two representative files in Preview to confirm behavior.
6. Mark tags that do not appear even as potential annotations, because this usually indicates a missing pattern.
7. Use those missing-tag findings as direct input for Task 3 (add or edit manual pattern).

Expected outcome:
- You have a shortlist of files and categories to target with pattern or data improvements.
- You also identify tags that are not detected even as potential annotations, creating a clear remediation list for Task 3.

[Screenshot sequence here: Task 2 - Per-File low coverage investigation]

### Task 3: Add a manual pattern and make it usable
Use this when matching fails for known tag formats.

1. Open Pattern Management.
2. Go to Manual Patterns.
3. Add a new pattern row or edit an existing one.
4. Fill required fields and save.
5. Trigger Refresh Cache.
6. Return to quality views after the next pipeline cycle to validate impact.

Expected outcome:
- The new pattern is stored and available for runtime matching after cache refresh.

[Screenshot sequence here: Task 3 - add manual pattern and refresh cache]

### Task 4: Import multiple patterns from CSV
Use this when many patterns need to be onboarded at once.

Clean-start recommendation (for guaranteed updated results):
1. Go to Manual Patterns table first.
2. Click the Select All checkbox (square next to the first header).
3. Click Delete Selected.
4. This clears old manual rows before import, so you start from a clean list.

1. Open Pattern Management > Import CSV.
2. Upload the CSV file.
3. Validate parsed rows.
4. Remove invalid rows.
5. Commit selected rows to the manual catalog.
6. Return to Manual Patterns and click Save.
7. Refresh cache.

Expected outcome:
- Multiple patterns are onboarded quickly and are ready for use.
- With clean-start + Save, both Manual Patterns and the ManualPatternScopes column in Annotation Entities Cache are refreshed without legacy values.

[Screenshot sequence here: Task 4 - CSV import and save]

### Task 5: Refresh Annotation Entities Cache after alias changes
Use this when aliases changed and you want a more complete and current pattern view before analysis.

1. Open Pattern Management.
2. Click Discover Scopes.
3. Review discovered scopes and confirm the set you want to process.
4. Click Generate Preview.
5. Review preview output to validate what will be written.
6. Click Write Cache Rows.
7. Wait for completion message.
8. Re-check pattern tables for the scopes you are investigating.

What each step does:
- Discover Scopes: loads the current scope universe from entities/aliases and prepares the scope list for refresh.
- Generate Preview: computes candidate cache rows without persisting, so you can inspect scope and pattern impact first.
- Write Cache Rows: persists generated rows to cache storage so Launch and downstream operations can use updated context.

Why this helps:
- Without manual refresh, cache is eventually updated by incremental pipeline runs.
- After cache expiration, Launch will refresh that scope naturally.
- However, manual refresh is useful when you need an up-to-date view now, for example to investigate pattern coverage by scope.

Advanced extension options:
- This refresh flow is intended to simulate what Launch does for scope/entity/pattern cache generation.
- If your project extends Launch behavior (for example custom DataModelService or CacheService), reflect the same logic in the app refresh path.
- DataModelService customizations can change entity/alias query strategy, grouping rules and enrichment before generation.
- CacheService customizations can change key strategy, row shape, write policy and invalidation logic.
- If app refresh and Launch are not aligned, users may see cache previews/results in the app that differ from runtime Launch behavior.

Connection to annotation gaps:
- If a tag does not appear even as Potential Annotation, one cause is scope context: the file scope may not contain any entity alias matching that pattern.
- In that case, use the next task (Propose) to propagate patterns found in other scopes.

Scope behavior notes:
- Best results come when primary and secondary scopes are configured.
- If only primary scope exists, proposal fallback is GLOBAL.
- If no scope exists, scope-based propagation has no practical effect.

Expected outcome:
- Cache-backed entity and pattern context is refreshed and ready for proposal and validation.

[Screenshot sequence here: Task 5 - discover scopes, preview, write cache rows]

### Task 6: Propose patterns by primary scope
Use this to propagate patterns that are present in some units/scopes but missing in others.

Clean-start recommendation (for guaranteed updated results):
1. Go to Manual Patterns table first.
2. Click the Select All checkbox (square next to the first header).
3. Click Delete Selected.
4. This removes old rows so proposal results are written into a clean baseline.

1. Open Pattern Management > Propose.
2. Select the primary scope target for propagation.
3. Generate proposals.
4. Review suggested patterns and select relevant ones.
5. Create manual patterns from selected proposals.
6. Go to Manual Patterns and click Save.
7. Validate in Annotation Quality > Per-File on affected files.

Why this helps:
- Propose can turn scope-specific recurring patterns into manual patterns at primary-scope level.
- This enables matching for files that previously had no potential detection due to scope alias coverage differences.
- Save after proposal updates both Manual Patterns and ManualPatternScopes cache values using the new clean set.

Expected outcome:
- Patterns are propagated across scope context, improving detection consistency and reducing missing potential annotations.

[Screenshot sequence here: Task 6 - propose and create manual patterns]

### Task 7: Debug a failed file
Use this when a file is stuck, failed or did not reach expected state.

1. Open Pipeline Health > Files.
2. Search by file name, external ID or source ID.
3. Select the file.
4. Open stage logs one by one: Prepare, Launch, Finalize, Promote.
5. Capture the first failing stage and key error details.
6. If needed, download logs for sharing and escalation.

Expected outcome:
- You identify where processing failed and what evidence supports the next action.

[Screenshot sequence here: Task 7 - files and stage-by-stage log review]

### Task 8: Review operational trend in recent runs
Use this for daily or weekly pipeline health checks.

1. Open Pipeline Health > History.
2. Select a time window.
3. Filter by status and caller type.
4. Compare success and failure patterns.
5. Open specific runs to inspect details.

Expected outcome:
- You can report run quality trends and identify operational hotspots.

[Screenshot sequence here: Task 8 - history filters and trend review]

## Common user questions

### Why is preview not showing for a file?
- The file may not have supported preview format.
- Preview data may be unavailable for that file.

### Why does Per-File feel slow sometimes?
- Large datasets are loaded in memory.
- Reduce filter scope to improve responsiveness.

### Why do some pipelines not appear in selector?
- Pipeline list depends on configured selector filters.

## Quick start note
This page is intentionally focused on usage.

For setup, runtime, deployment details and local execution, use the dedicated setup documentation.

## Related guides
- Setup and runtime: [CONFIG.md](CONFIG.md)
- Product requirements: [PRD.md](PRD.md)
- Pipeline Overview
- Configuration Guide
- Operations Guide
- Developer Extension Guide
