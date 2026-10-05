# CDF Best Practices Audit

**Project:** `modules/contextualization/cdf_entity_matching` (deployment pack module `dp:contextualization:cdf_entity_matching`)
**Date:** 2026-10-05
**Audited by:** cog-vd-audit (cog-vd-best-practices plugin), with an extra lens on efficiency and simplicity

---

## Summary

| Domain | [PASS] Pass | [WARN] Warning | [FAIL] Fail | Status |
|---|---|---|---|---|
| A. Naming Conventions | 3 | 5 | 2 | [RED] |
| B. Data Modeling      | 1 | 1 | 0 | [YELLOW] |
| C. Transformations    | – | – | – | > No files found — skipping. |
| D. Functions          | 6 | 15 | 3 | [RED] |
| E. Workflows          | 4 | 3 | 0 | [YELLOW] |
| F. DMS Queries        | 4 | 1 | 2 | [RED] |
| **Total**             | **18** | **25** | **7** | |

> [GREEN] No failures - [YELLOW] Warnings only - [RED] One or more failures

The biggest simplicity win is retiring `fn_dm_context_timeseries_entity_matching`. It is deployed but no workflow calls it, and it is a 78–100% copy of the newer submit/collect matcher. Removing it cuts about 1,950 lines of code and 1,450 lines of tests.

## Remediation status

Critical issues and next steps 1–9 are fixed. Remaining naming debt is only the identifiers in the [Naming migration plan](#naming-migration-plan) that still use legacy prefixes (`db_`, `sp_`, persona-less `gp_`).

| Fix | Status |
|---|---|
| `rawAcl` on the processing group | Done |
| Aliases function raises on failure; reads paged (1,000 per page, `name` and `aliases` only), retried on 408/429/5xx/transport errors, written per page | Done |
| Manifest `dbName` (critical issue 4) | Reclassified: not a finding. Every `upload_data` manifest in the repo hard-codes its database, since the data plugin does not substitute Toolkit variables. Keep it in step with `dbName` by hand |
| Drop `location_name` / `source_name`; fixed resource IDs; sentence-case display names | Done. Pipeline `ep_ctx_entity_matching`, data set `ds_entity_matching`, group `gp_entity_matching_processing`. Extraction pipeline `source` and data set `consoleSource` use the fixed value `entity_matching`. Wizard no longer injects those variables |
| Entity read in submit: paged query, projected to name, search, link and scope properties, retried per page | Done |
| Manual/rule mapping and staged-match read errors raise; staged reads return `[]` only on 400/404 | Done |
| Workflow: submit `retries: 0`, task timeouts 1200 s; `runAll: False` on both pipelines | Done |
| Retire `fn_dm_context_timeseries_entity_matching` with its registrations; CI now runs the submit/collect and full aliases suites | Done |
| Remove `PerformanceBenchmark`, `os.nice`/GC tuning, psutil memory logging, `gc.collect`; drop `psutil` as a direct dependency | Done. `time_operation` (DEBUG step timings, stdlib only) is kept |
| De-duplicate rule-key and apply loops | Done: one `rule_keys()` for entities and targets, one `_flush_when_full()` for the three batched applies |
| Group: drop `groupsAcl`, `dataModelsAcl` WRITE and the commented `annotationsAcl`; add `datasetsAcl` READ | Done. `filesAcl` and `dataModelInstancesAcl` stay `all: {}`: scoping files to a data set would lock out existing files that have none, and instance spaces can be lists |
| Target cache and staged-match files written to an optional `dataSetExternalId` data set | Done. Unset keeps today's behaviour; a configured but missing data set fails the run |
| Function `owner` from `{{ functionOwner }}`; trigger cron from `{{ workflowSchedule }}` (default still 29 February, i.e. off) | Done |
| Upper-bound pins in `requirements.txt`; aliases drops `cognite-extractor-utils` and lists `pydantic` directly (it came in through extractor-utils) | Done |
| Delete unused `entity.match` node; `contextualization_rule_input` in `rawTables` | Done |
| Pydantic `FunctionInput` on both handlers: invalid `stage`, empty `ExtractionPipelineExtId` or unknown `logLevel` fails before any CDF call | Done |
| Remaining prefix / persona renames (`db_`, `sp_`, `gp_`, aliases EP, workflow) | Plan only, see below |

---

## [CRITICAL] Critical Issues

Findings as of the audit. Remediation status above is the current state.

| # | Domain | File | Finding | Fix |
|---|---|---|---|---|
| 1 | D | `auth/entity.matching.processing.groups.Group.yaml` | No `rawAcl`, yet every function reads and writes RAW (state store, good/bad, manual, rule tables) | Add `rawAcl: [READ, WRITE]` scoped to `tableScope.dbsToTables: {{ dbName }}` |
| 2 | D | `functions/fn_dm_context_aliases_update/handler.py:118-128` | Catches every exception and returns `{"status": "failure"}`. CDF marks the call succeeded, so the workflow goes on to match against stale aliases | Log, then `raise` (same as `stages/stage_runtime.py`) |
| 3 | D | `functions/fn_dm_context_aliases_update/pipeline.py:511-513` | `get_new_items` logs a failed read and returns `None`; the run then "succeeds" having updated nothing | Remove the outer `except` and let the error propagate |
| 4 | D | `upload_data/contextualization_manual_input.Manifest.yaml` | ~~Hard-coded `dbName: db_asset_entity_matching`~~ Withdrawn: repo-wide manifest convention, see Remediation status | – |
| 5 | A | `default.config.yaml` | Resource IDs were templated with `location_name` / `source_name`, so uppercase or per-site values leaked into external IDs | Fixed IDs with no location/source tokens: `ep_ctx_entity_matching`, `ds_entity_matching`, `gp_entity_matching_processing` |
| 6 | A | `functions.Function.yaml`, `extraction_pipelines/*.ExtractionPipeline.yaml`, `data_sets/timeseries.DataSet.yaml` | Display names used colons (`dm:context:*`, `ctx:*`) | Sentence case, e.g. `Entity matching`, `Aliases update` |
| 7 | F | `fn_dm_context_entity_matching/em_pipeline.py:681` (also `:519`) | `instances.list(limit=-1)` over every entity with the full view projection and no 408/429/5xx retry. This is the main timeout risk in submit | Read entities the way targets are read (sync + `Select` of `name`, search property, link and scope properties + retry), or at least wrap in the existing retry helper |
| 8 | F | `fn_dm_context_aliases_update/pipeline.py:483-489` (`BATCH_SIZE = -1` in `constants.py:4`) | Loads all time series, assets and files into memory per run, despite the extraction pipeline docs saying it uses sync | Page through with `instances.sync` / a cursor and process each page; fix the docs |

---

## A. Naming Conventions

**Files scanned:** 22   **Identifiers checked:** 17

### [FAIL] Failures (at audit time — remediated)

| File | Resource | Identifier | Issue | Suggested Fix |
|---|---|---|---|---|
| `default.config.yaml` | Variable feeding EP, data set, group | `location_name` / `source_name` in IDs | Charset and collision risk when values vary | Fixed IDs (see Remediation status) |
| `functions.Function.yaml`, EP and data set YAML | Display names | `dm:context:*`, `ctx:*`, colon-separated | Not sentence case | Sentence case with spaces |

### [WARN] Warnings (remaining)

| File | Resource | Identifier | Note |
|---|---|---|---|
| `auth/entity.matching.processing.groups.Group.yaml` | Access group | `gp_entity_matching_processing` | Not persona-led (`producer_…`). Renaming needs a migration |
| `extraction_pipelines/ctx_aliases_update.*` | Extraction pipeline | `ep_ctx_aliases_update` | Project-scoped; two installs in one project collide. Acceptable for a single-pipeline module |
| `workflows/*.yaml`, `default.config.yaml` | Workflow | `wf_ctx_entity_matching` | Action form often uses `wf_{location}_{intent}`; kept project-scoped like the other building blocks |
| `raw/entityMatchingDb.Database.yaml` | RAW database | `db_asset_entity_matching` | Prefix should be `raw_` |
| `data_modeling/fn.Space.yaml` | Instance space | `sp_entity_matching_fn` | Instance spaces use `inst_` (e.g. `inst_entity_matching_fn`) |

### [PASS] Passed

- Building blocks carry the right prefixes: `fn_`, `ep_`, `wf_`, `ds_`.
- No environment tokens in building-block IDs.
- No GUIDs, spaces or unsafe characters in external IDs.
- No `location_name` / `source_name` Toolkit variables in this module.

---

## B. Data Modeling

Only a space and two nodes; no containers, views or data models.

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `data_modeling/match_type.Node.yaml` | Instance ID / dead resource | `entity.match` contains a dot (instances are `snake_case`) and nothing in the code references it. Delete it, or rename and use it |

### [PASS] Passed

- Space and node spaces use Toolkit variables (`{{ functionSpace }}`).

---

## C. Transformations

> No files found — skipping.

---

## D. Functions

### [FAIL] Failures

| File | Check | Detail | Fix |
|---|---|---|---|
| `auth/entity.matching.processing.groups.Group.yaml` | Required capabilities | `rawAcl` missing | Add `rawAcl` READ/WRITE scoped to `{{ dbName }}` |
| `fn_dm_context_aliases_update/handler.py:118-128` | Failures must raise | Returns a failure dict instead of raising | `raise` after logging |
| `fn_dm_context_aliases_update/pipeline.py:511-513` | Failures must raise | Read error becomes `None` | Let it propagate |

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `functions/fn_dm_context_timeseries_entity_matching/` | Simplicity / dead deployment | Deployed but not called by the workflow; duplicates `em_pipeline.py` (25 shared functions, 78–100% similar). Remove the folder, its `functions.Function.yaml` entry, and its registrations in `scripts/generate_uv_member_projects.py`, `pyproject.toml`, `uv.lock`, CI pytest path, `tests/test_module_test_support.py`, READMEs and the Qualitizer pack list. Keep the shared entity-matching extraction pipeline |
| `functions.Function.yaml` | `owner` / description | `owner: 'Anonymous'` on all three; legacy function said "Contextualization of P&ID files…" (copy-paste) |
| `fn_dm_context_entity_matching/em_pipeline_optimizations.py`, `fn_dm_context_aliases_update/alias_optimizations.py` | Over-engineering | `PerformanceBenchmark`, `patch_existing_pipeline` / `optimize_metadata_processing` (`os.nice`), `monitor_memory_usage` (psutil), `cleanup_memory` (`gc.collect`). Keep `is_retryable` and the retry wrapper; delete the rest (~120–180 + ~150–250 lines) and drop `psutil` |
| `fn_dm_context_entity_matching/em_pipeline.py:272-275`, `:584-587` | Silent degrade | Manual/rule mapping read errors are logged and the run continues with no mappings | Re-raise |
| `fn_dm_context_entity_matching/em_staging.py:82-86` | Silent degrade | A transient 5xx or bad JSON reading staged matches returns `[]`; collect then deletes the staging file, so those manual/rule rows never reach the good table | Return `[]` only for 404; raise otherwise |
| `extraction_pipelines/*.config.yaml` | Efficiency | `runAll: True` by default: every run reprocesses all entities, clears good/bad tables, re-reads all targets (bypassing the sync cache) and re-fits the model | Default `runAll: False` |
| `auth/entity.matching.processing.groups.Group.yaml` | Least privilege | All scopes `all: {}`; `dataModelsAcl WRITE` and `groupsAcl` are not used by the functions; commented-out `annotationsAcl` | Scope instances to the configured spaces, drop unused ACLs, delete the comment block |
| `fn_dm_context_entity_matching/em_logger.py` | Logging | Uses `import logging` (skill recommends `print()` on Azure). The aliases function already prints | Switch to a `print`-based logger, or document why `logging` to stdout is safe here |
| All handlers | Input validation | `data` read by key (`FunctionInputData` TypedDict), not a Pydantic model | Small `FunctionInput(BaseModel)` with `stage`, `extraction_pipeline_ext_id`, `log_level` |
| `requirements.txt` | Pinning | `>=` ranges while CDF installs from this file, not `uv.lock` | Pin upper bounds or exact versions from the lock |
| `fn_dm_context_aliases_update/requirements.txt` | Unused dependency | `cognite-extractor-utils` not imported | Remove (via `generate_uv_member_projects.py`) |
| `fn_dm_context_aliases_update/handler.py:54` | Correctness | Usage-reporting thread `daemon=False` can hold the call open | `daemon=True` (as in `usage.py`) |
| `em_pipeline.py` / `em_targets.py`; three DM flush blocks | Duplication | Same rule-key regex loop and same batched-apply loop repeated | One `rule_keys()` and one `_flush_when_full()` |
| Usage reporting ×3 | Duplication | Mixpanel tracking copied into each handler | Shared `usage.py` per function is fine; make the aliases copy match it |
| Extraction pipeline and data set `rawTables` | Completeness | Lists omit `contextualization_rule_input` | Add it |

### [PASS] Passed

- Each `externalId` matches its code folder name.
- `handle()` returns `{"status": "succeeded", ...}`; the submit/collect stages re-raise on failure.
- Extraction pipeline config parsed into Pydantic models.
- `run_locally()` scaffolded with environment-variable credentials.
- No secrets in `data` or source (the Mixpanel key is a public project token).
- Long ML predict is split into submit and collect, with an 8-minute poll budget in collect.

---

## E. Workflows

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `workflows/entity_matching.WorkflowVersion.yaml` | Idempotency | Submit has `retries: 3` but is not idempotent: a failure after `append_predict_job` (e.g. in `update_pipeline_run`) re-runs everything and queues a second predict job for the same entities. Set `retries: 0` on submit, or skip when an open job already exists |
| `workflows/entity_matching.WorkflowVersion.yaml` | Timeout | `timeout: 9000` (2.5 h) on tasks whose function limit is ~10 min. Use ~900–1200 so a hung call fails fast |
| `workflows/trigger.WorkflowTrigger.yaml` | Trigger intent | `cronExpression: "0 0 29 2 *"` only fires on 29 February, so it is effectively disabled. Document that, or use a real schedule and run collect more often |

### [PASS] Passed

- `onFailure` set on every task.
- `dependsOn` follows real data flow: aliases → submit → collect.
- Version `v1`.
- Task inputs pass only IDs; no large outputs.

---

## F. DMS Queries

### [FAIL] Failures

| File | Check | Detail | Fix |
|---|---|---|---|
| `fn_dm_context_entity_matching/em_pipeline.py:681` | `limit=-1` on unbounded data | All entities, full projection, no retry | Sync + property `Select` + retry, as `em_targets.py` already does |
| `fn_dm_context_aliases_update/pipeline.py:483` | `limit=-1` on unbounded data | All time series/assets/files held in memory | Page with a cursor and process per page |

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `fn_dm_context_aliases_update/alias_optimizations.py:100-102` | Retry coverage | `is_retryable` covers 429 and 5xx but not 408 |

### [PASS] Passed

- Targets: sync endpoint, cached content, property `Select`, adaptive page size, 408/429/5xx retries.
- Manual-mapping lookups batched (5,000 per call), no N+1.
- Client-side "already linked" filter is justified (`exists` treats `[]` as a value).
- Every `instances.list` has a `space` filter.

---

## Next Steps

Prioritised remediation — [FAIL] Fails first, then [WARN] Warnings. Steps 1–9 are done; only the optional renames below remain open.

1. **[CRITICAL]** Add `rawAcl` to the processing group.
2. **[CRITICAL]** Make the aliases function raise on failure (handler and `get_new_items`), so the workflow aborts instead of matching on stale aliases.
3. **[CRITICAL]** Fixed resource IDs (no location/source tokens); sentence-case display names.
4. **[CRITICAL]** Bound the entity read in submit and the alias read (sync/paged, projected, retried).
5. **[WARNING]** Retire `fn_dm_context_timeseries_entity_matching` (~3,400 lines incl. tests).
6. **[WARNING]** Set submit `retries: 0` (or add a duplicate-job guard) and task timeouts ~900–1200 s.
7. **[WARNING]** Default `runAll: False`; stop swallowing mapping and staging read errors.
8. **[WARNING]** Strip benchmark/memory/`os.nice` scaffolding and `psutil`; de-duplicate rule-key and apply loops.
9. **[WARNING]** Tighten group scopes, pin requirements, Pydantic `FunctionInput`, remaining naming migration plan.

---

## Naming migration plan

Resource IDs for the entity-matching pipeline, data set and processing group are already fixed and project-scoped (`ep_ctx_entity_matching`, `ds_entity_matching`, `gp_entity_matching_processing`). The module no longer defines `location_name` or `source_name`.

The warnings left in section A are prefix / persona debt. Changing one of those IDs in the module makes the Toolkit create a new resource next to the old one and leave the old resource and its data behind. Each rename follows the same two releases, and each is its own PR:

1. **Non-breaking release.** Turn the hard-coded identifier into a Toolkit variable whose default is the **current** value. Nothing changes for anyone, but a project can opt in by setting the new value in its `config.<env>.yaml`.
2. **Breaking release** (`feat!:` with a `BREAKING CHANGE:` footer). Change the default to the new value and add upgrade steps to the module README. Projects that want the old name pin it in their config.

| Identifier today | Proposed | Variable | What moves on cut-over |
|---|---|---|---|
| `ep_ctx_aliases_update` | keep, or `ep_ctx_entity_matching_aliases` if a clearer sibling name is wanted | `aliasesPipeline` (new) | Run history stays on the old pipeline. Update the workflow task input and the `run_locally` default in the aliases `handler.py` |
| `wf_ctx_entity_matching` | keep (already project-scoped) | `workflow` (exists) | Only rename if a project convention requires it. Delete the old trigger **before** deploying, or both workflows run on schedule |
| `db_asset_entity_matching` | `raw_entity_matching` | `dbName` (exists) | Copy all five tables, above all `contextualization_state_store` (sync cursors, queued predict jobs) and `contextualization_manual_input`, before the first run. Do it between a collect and the next submit, so no predict job is in flight. Update the two `upload_data` manifests by hand |
| `sp_entity_matching_fn` | `inst_entity_matching_fn` | `functionSpace` (exists) | Recreate the source-system node in the new space; a space can only be deleted once it is empty |
| `gp_entity_matching_processing` | `producer_entity_matching` (persona-led) | `processingGroupName` (new) | Groups are matched by name, so this creates a second group with the same `sourceId`. Check runs, then delete the old group |

`cdf clean` or a manual delete removes the old resources once a project has moved.

---
*Share this file for alignment reviews. For deeper guidance trigger the matching specialist
skill: `cdf-naming-check`, `cognite-data-modeling`, `cognite-transformation`,
`cognite-function`, `cognite-workflow`, or `cognite-dms-queries`.*
