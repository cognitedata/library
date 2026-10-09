# CDF Best Practices Audit

**Project:** `modules/contextualization/cdf_file_annotation` (branch `file_anno_v2`)
**Date:** 2026-10-07
**Audited by:** cog-vd-audit (cog-vd-best-practices plugin) + cog-code-review + security review

Scope: Toolkit YAML/SQL, the `fn_file_annotation` CDF Function, the two Streamlit dashboards,
`local_setup/` notebooks and `upload_data/`. High-impact findings were verified against the code.

---

## Summary

| Domain | [PASS] Pass | [WARN] Warning | [FAIL] Fail | Status |
|---|---|---|---|---|
| A. Naming Conventions | 9 | 10 | 4 | [RED] |
| B. Data Modeling      | 8 | 2 | 1 | [RED] |
| C. Transformations    | 4 | 5 | 2 | [RED] |
| D. Functions          | 6 | 4 | 1 | [RED] |
| E. Workflows          | 5 | 3 | 1 | [RED] |
| F. DMS Queries        | 3 | 3 | 4 | [RED] |
| **Total**             | **35** | **27** | **13** | |

Additional sections below the audit domains: **G. Correctness bugs**, **H. Security**,
**I. Coding standards (AGENTS.md / styleguide)**, **J. Tooling results**.

> [GREEN] No failures - [YELLOW] Warnings only - [RED] One or more failures

---

## [CRITICAL] Critical Issues

| # | Domain | File | Finding | Fix |
|---|---|---|---|---|
| 1 | G | `functions/fn_file_annotation/services/EntitySearchService.py:239-241` + `PromoteService.py:315-323` | A transient search error (429/5xx/timeout) is swallowed and returns `[]`; Promote treats it as "no match" and **deletes** the pattern edge (`DELETE_REJECTED_EDGES`). | Let the error propagate (or return `None` and skip the edge for this run). |
| 2 | G | `functions/fn_file_annotation/stages/*.py` (e.g. `prepare.py:43-74`), `stage_runtime.py` | `run_status` defaults to `"success"` and only flips for 4 exception types; `KeyError`, timeouts, plain `Exception` fail the call but the extraction pipeline run is reported **success**. | Default `run_status = "failure"`; set `"success"` only on the return paths. |
| 3 | G | `functions/.../services/LaunchService.py:485-487` | Files in a scope with no entities/patterns are never moved out of `New`; Launch refetches them every iteration for the whole 7-min budget and, once ≥ `LAUNCH_STATE_LIMIT`, starves all other files. | Mark such states `Failed` (with message) or bump `attemptCount`. |
| 4 | G | `functions/.../services/PromoteService.py:190-194` | Any `CogniteAPIError` (incl. 403) → sleep 15 s, return `None`; Promote loops for 7 min and reports success. | Retry only 408/429/5xx; re-raise others. |
| 5 | G | `streamlit/file_annotation_dashboard_annotation_quality/components.py:739-778, 881, 919` | Saving manual patterns while a filter is active rebuilds each scope from **visible rows only** → hidden patterns are deleted from RAW. | Apply edits to the unfiltered frame, or disable editing while filtered. Add a test. |
| 6 | H | `streamlit/*/client_factory.py:37` | `global_config.apply_settings({"disable_ssl": True})` disables TLS verification globally. | Remove `disable_ssl`. |
| 7 | H | `local_setup/list_file_annotation_edges.ipynb` (line ~3090) | `CONFIRM_DELETE = True` is committed (Run All deletes edges + RAW rows); outputs contain the dev project `jib03` and real listing output. | Commit as `False`; strip outputs (`nbstripout`). |
| 8 | E | `default.config.yaml:103-105` | `workflowSchedule: "0 0 29 2 *"` is documented as "does not run the workflow" but **fires every 29 February** (next 2028-02-29). | Document it honestly, or don't deploy the trigger by default. |
| 9 | C | `transformations/tag_*_detect_in_diagrams*.sql`, `tag_files_to_annotate*.sql` | `cdf_nodes(view)` returns nodes from **all** instance spaces, but the destination writes only `externalId` into one `instanceSpace` → nodes from other spaces are created as stub nodes in the configured space. Full scan every run. With `""` instance space (multi-space mode) the transformations fail. | `WHERE space = '{{ ... }}'` (or select `space` and drop `instanceSpace`); add an `is_new` / tag filter. |
| 10 | C | `transformations/file_to_asset.Transformation.sql/.yaml` | Writes `assets` = annotation-derived list → **overwrites** asset links set by other sources; no `startNodeSpace` filter; `node_reference('{{ targetEntityInstanceSpace }}', …)` and `instanceSpace` break when those are `""` (documented multi-space mode). | Use `startNodeSpace`/`endNodeSpace` columns from RAW, merge with existing `assets`, filter by space. |
| 11 | D | `handler.py:50-58`, `stages/*.py` | Function input `data` is not validated (no Pydantic); missing `ExtractionPipelineExtId` raises `KeyError` before the try block → no pipeline run logged. | Add a `FunctionInput` Pydantic model. |
| 12 | F | Streamlit `data_fetcher.py` (AQ :65, :233-241; PH :58-64, :189-195) | Unbounded `limit=-1` reads of RAW tag tables and all file/state nodes with full property sets; PH `instances.list` with no `space`. Pyodide runtime will run out of memory on real projects. | Read the aggregated `annotation_file_status_report`; `instances.query` with `select`; add `space=`. |
| 13 | A | `auth/file_annotation.Group.yaml`, `raw/rawDb.Database.yaml`, `data_modeling/hdm.datamodel.yaml`, EP display name | `gp_file_annotation` not persona-led; RAW db lacks `raw_` prefix; data model `helper_datamodel` not PascalCase + `_SOL`; EP name `'ctx:file:annotation'` uses colons. | See section A. |

---

## A. Naming Conventions

**Files scanned:** 33 YAML  **Identifiers checked:** 23

### [FAIL] Failures
| File | Resource | Identifier | Issue | Suggested Fix |
|---|---|---|---|---|
| `auth/file_annotation.Group.yaml` | Access group | `gp_file_annotation` | Not persona-led (`<persona>_[scope]_<type>_<env>`) | e.g. `producer_pp_file_annotation_{{env}}` via variable |
| `raw/rawDb.Database.yaml` | RAW db | `db_file_annotation` | Missing `raw_` prefix | `raw_file_annotation_all` (legacy: constant in `fa_constants.py`, SQL, EP — needs migration) |
| `data_modeling/hdm.datamodel.yaml` | Data model | `helper_datamodel` | Must be PascalCase + layer suffix | `FileAnnotationHelper_SOL` |
| `extraction_pipelines/ep_file_annotation.ExtractionPipeline.yaml` | EP display name | `ctx:file:annotation` | Colons as separators; not sentence case | `File annotation` |

### [WARN] Warnings
| File | Resource | Identifier | Note |
|---|---|---|---|
| `default.config.yaml` | Extraction pipeline | `ep_ctx_file_annotation` | Grammar `ep_{data_type}_{location}_{source}`; no location |
| `default.config.yaml` | Dataset | `ds_file_annotation` | No location token (`ds_file_annotation_all`) |
| `default.config.yaml` | Function | `fn_file_annotation` | 2 tokens; recommended 4+ |
| `default.config.yaml` | Workflow | `wf_ctx_file_annotation` | Action form `wf_{location}_{intent}`; no location |
| `default.config.yaml` | Transformations | `tr_file_annotation_status_report`, `tr_tag_assets_detect_in_diagrams`, `tr_tag_files_detect_in_diagrams`, `tr_tag_files_to_annotate` | No `_to_` connector |
| `default.config.yaml` | DM space | `sp_hdm` | Recommended `dm_sol_file_annotation` (legacy, migration needed) |
| `default.config.yaml` | Instance spaces | `sp_dat_pattern_mode_results`, `sp_file_annotation_fn` | Recommended `inst_…` prefix |
| `data_modeling/hdm.space.yaml` | Space | `SolutionTagsInstanceSpace` | Not snake_case — imposed by Canvas labels; informational only |
| `transformations/*.yaml` | Display names | `File Annotation Status Report`, `Tag Assets DetectInDiagrams`, … | Title case; use sentence case |
| `streamlit/*.Streamlit.yaml` | Display names | `File Annotation Dashboard - Annotation Quality` | Hyphen separator, title case |

### [PASS] Passed
View/container `FileAnnotationState` (PascalCase), all 15 properties camelCase, node `pattern_detection_sink_node`,
`file_to_asset` transformation (`_to_`), Streamlit externalIds (no prefix required), workflow task IDs, no env tokens
in building-block IDs, no GUIDs/unsafe characters, no case-only collisions.

---

## B. Data Modeling

### [FAIL] Failures
| File | Check | Detail | Fix |
|---|---|---|---|
| `data_modeling/containers/hdm.container.yaml` | Direct relation needs btree index | `linkedFile` (`type: direct`) has no index; it is used for file → state lookups | Add `linkedFile: {indexType: btree, properties: [linkedFile], cursorable: true}` (6 indexes ≤ 10) |

### [WARN] Warnings
| File | Check | Detail |
|---|---|---|
| `data_modeling/nodes/pattern.node.yaml` | Sink node typed as `CogniteFile` | The static sink node appears in every CogniteFile search/list (incl. this module's own file queries in other spaces). Consider a minimal dedicated view or `CogniteDescribable`. |
| `data_modeling/hdm.space.yaml` | Hard-coded space | `SolutionTagsInstanceSpace` is hard-coded (required by Canvas) — fine, but document it. |

### [PASS] Passed
Every container property exposed in the view; every view property has a description; direct relation `linkedFile`
has `source`; 15 properties / 5 indexes (within limits); no `record` containers; view is in the data model; Toolkit
variables used for space/version; `name`, `sourceCreatedTime`, `sourceUpdatedTime` map to CDM containers.

---

## C. Transformations

### [FAIL] Failures
| File | Check | Detail | Fix |
|---|---|---|---|
| `tag_assets_detect_in_diagrams`, `tag_files_detect_in_diagrams`, `tag_files_to_annotate` (`.sql` + `.yaml`) | Instance space handling | `cdf_nodes()` spans all instance spaces; output omits `space`, destination forces `instanceSpace` → cross-space stub nodes; fails with `""`. | Filter `WHERE space = '{{ fileInstanceSpace }}'` (resp. target space) or emit `space` and remove `instanceSpace`. |
| `file_to_asset.Transformation.sql/.yaml` | Destructive overwrite / multi-space | Replaces whole `assets` list; ignores `startNodeSpace`/`endNodeSpace`; breaks with `""` spaces. | See Critical #10. |

### [WARN] Warnings
| File | Check | Detail |
|---|---|---|
| All 5 transformations | Incremental load | No `is_new()`; full re-read each run. `file_to_asset` runs on every workflow run. |
| All 5 transformation YAMLs | `ignoreNullFields: true` | Set explicitly but intent not documented. |
| All 5 transformation YAMLs | Authentication | No `authentication` block — runs with deploy credentials. Use `{{ functionClientId }}`/secret variables. |
| `file_annotation_status_report.Transformation.yaml` | Orchestration | Not in the workflow and no schedule → dashboard data goes stale unless run manually. |
| `file_annotation_status_report.Transformation.sql` | Scope | Filtered to `{{ fileInstanceSpace }}`; returns nothing in multi-space mode (`""`). |

### [PASS] Passed
No `SELECT *` in final select lists; `conflictMode` explicit; schema/space variables used; uppercase SQL keywords /
lowercase functions; no hard-coded credentials.

---

## D. Functions

### [FAIL] Failures
| File | Check | Detail | Fix |
|---|---|---|---|
| `handler.py`, `stages/*.py` | Pydantic for `data` | Raw `data["ExtractionPipelineExtId"]` / `data.get("logLevel")`. | `FunctionInput` model: `stage: Literal[...]`, `extraction_pipeline_ext_id` (alias), `log_level: Literal[...]`. |

### [WARN] Warnings
| File | Check | Detail |
|---|---|---|
| `functions.Function.yaml:3` | Owner | `owner: "Anonymous"` |
| `stages/*.py`, services | Timeout | 7-min budget checked only between iterations vs 600 s task timeout; Promote (500 edges, per-edge RAW reads), Finalize (50 files + 30 s sleep), `QueryTimeoutRetry` (60 s sleep) can overrun. Pass a deadline into inner loops. |
| `usage.py:24` | Hard-coded identifiers | Mixpanel token; telemetry on by default (opt-out `CDF_USAGE_REPORTING=false`) sends project/cluster names. |
| `handler.py` return | Return shape | Returns `{"status": "success", "data": data}` — echoes input; fine size-wise. |

### [PASS] Passed
externalId matches folder (`fn_file_annotation`); no `import logging`; no credentials in source/payload;
`requirements.txt` fully pinned and in sync with `generate_uv_member_projects.py`; `run_locally` per stage +
`__main__`; description present.

---

## E. Workflows

### [FAIL] Failures
| File | Check | Detail | Fix |
|---|---|---|---|
| `default.config.yaml:103-105`, `WorkflowTrigger.yaml` | Schedule | `"0 0 29 2 *"` fires on 29 Feb (leap years); comment says it never runs. | Fix the comment or gate trigger deployment. |

### [WARN] Warnings
| File | Check | Detail |
|---|---|---|
| `wf_file_annotation.WorkflowVersion.yaml` | Retries | `retries: 0` on all four function tasks — one transient failure aborts the whole run. Consider `retries: 1`. |
| `wf_file_annotation.WorkflowVersion.yaml` | Concurrency | No guard against overlapping runs (schedule + manual). Launch has no claim/lock, and entity-sync cache file + RAW cursor are written in two steps → double launches / missed changes. |
| `wf_file_annotation.WorkflowVersion.yaml` | Missing tasks | Status-report transformation is not orchestrated (see C). |

### [PASS] Passed
`dependsOn` matches real data flow (prepare → launch → finalize → promote → file_to_asset); `onFailure` on every
task; explicit timeouts; outputs are small; `concurrencyPolicy: fail` on the transformation; version `v1`.

---

## F. DMS Queries

### [FAIL] Failures
| File | Check | Detail | Fix |
|---|---|---|---|
| `PromoteService.py:697, 831`; `PromoteCacheService.py:334`; `EntitySearchService.py:229`; `ApplyService.py:155-176, 215-256` | N+1 | Per-edge RAW retrieve, per-text searches, per-file edge queries / applies / RAW inserts. | Batch RAW reads per batch; collect applies; `In(startNode)` edge queries. |
| `DataModelService.py:439-467`; `EntitySyncService.py:199` | Client-side filtering | All tagged instances read, then scope/tags filtered in memory (documented: server filter timed out). Unbounded memory. | Keep, but cap + warn; revisit with indexed scope property. |
| Streamlit `data_fetcher.py` (AQ :65, :233-241; PH :58-64) | `limit=-1` + full properties | Whole RAW tables and all file/state nodes. | See Critical #12. |
| PH `data_fetcher.py:189-195` | `instances.list` without `space` | Lists across all spaces. | Pass `space=`. |

### [WARN] Warnings
| File | Check | Detail |
|---|---|---|
| `RetrieveService.py:165-172, 197-204` | Space filter can be `None` | When no `instanceSpace` is configured. |
| Function services / Streamlit | Retry handling | Only entity sync handles 408/429/5xx; Streamlit wrappers retry 100× (PH uncapped exponential backoff ≈ hang). |
| `RetrieveService.py:111`, `LaunchService.py:443` | Full dumps | Whole job response / entity JSON logged at DEBUG. |

### [PASS] Passed
Cursor pagination in `_query_nodes`; no `properties=["*"]`; `pipelineUpdatedTime` sort has a cursorable btree index.

---

## G. Correctness bugs (beyond Critical)

| Sev | Location | Finding |
|---|---|---|
| Med | `FinalizeService.py:221-227, 601-606` | Oldest still-running job is re-picked every iteration (`limit=1`, clock preserved) → completed jobs behind it are not finalized that run. |
| Med | `FinalizeService.py:287` | Pattern-only jobs fall back to `pageCount=1` → files > 50 pages marked `Annotated` after the first range. |
| Med | `LaunchService.py:445-494, 230-232` | If pattern-mode detect fails after the regular job starts, the regular job id is never stored → orphaned job, file relaunched. |
| Med | `ApplyService.py:344-353` | Regular annotations inside pattern boxes are removed before the `if not entities` check → removed with no replacement. |
| Med | `PromoteService.py:207` | Edges without `startNodeText` are never tagged → re-selected forever; a batch of only these loops to budget. |
| Med | AQ `data_fetcher.py:68, 121-134` | Empty RAW table returns `None` → `AttributeError` on fresh deployments. |
| Med | PH `data_fetcher.py:106, 131, 141` | Early returns skip `annotationStatus → status` rename → `KeyError` in KPIs. |
| Med | PH `components.py:399-402` | `int(None)` on "Load Log" when the run message doesn't match the regex. |
| Med | PH `factories.py:22-30` | Row selection resolves from cumulative `edited_rows` → wrong file after re-selecting. Use `st.dataframe(on_select=...)`. |
| Low | `AnnotationService.py:43-50, 104` | Legacy config path + pattern mode → `AttributeError`. |
| Low | `FinalizeService.py:602-606` | First node's `sourceUpdatedTime` applied to whole batch. |
| Low | PH `data_processor.py:87-90` | Only Launch/Finalize runs counted → Prepare/Promote metrics always 0. |

---

## H. Security

| Sev | Location | Finding | Fix |
|---|---|---|---|
| High | `streamlit/*/client_factory.py:37` | TLS verification disabled globally. | Remove `disable_ssl`. |
| High | `local_setup/list_file_annotation_edges.ipynb` | `CONFIRM_DELETE = True` committed; outputs include project `jib03` and data listings. | `False`; strip outputs; add `nbstripout` pre-commit. |
| Med | `auth/file_annotation.Group.yaml` | `dataModelsAcl` READ+**WRITE** `all` (function never writes schemas); `dataModelInstancesAcl` WRITE `all`; `functionsAcl` WRITE `all`; `entitymatchingAcl` `all`. Commented-out ACL blocks are dead config. | Drop `dataModelsAcl` WRITE; scope instances to spaces when not in multi-space mode; delete commented blocks. |
| Med | Streamlit entrypoints `_report_usage` | Mixpanel tracking with `"ip": 1`, no `CDF_USAGE_REPORTING` opt-out (the function honours it). | Drop `ip`, add opt-out, share one helper. |
| Med | AQ `components.py:732-813`, `data_updater.py:39` | UI writes RAW without validation/confirmation; `ensure_parent=True`; overwrites `created_by` provenance; broad `except`. | Validate, confirm diff, `ensure_parent=False`, keep provenance. |
| Low | `dependencies.py:53,61`, `client_factory.py:20,28` | Entra-only token URL, client name `"DEV_Working"`. | Read from env; descriptive client name. |
| Low | PH `components.py:443-446` | `unsafe_allow_html=True` (integers only — not exploitable). | Use `st.caption`. |

Positive controls: `yaml.safe_load` + Pydantic config validation; whitelisted `stage`; `cleanOldAnnotations` deletes
only edges created by this function; hashed entity-cache file names; dataset-scoped `filesAcl`; only `${...}`
credential placeholders; RAW/telemetry tokens are public analytics keys.

---

## I. Coding standards (AGENTS.md / `.gemini/styleguide.md`)

- **Module names:** 16 PascalCase files (`services/*.py`, `utils/DataStructures.py`, `utils/QueryTimeout.py`); class `entity` lowercase (`DataStructures.py:142`).
- **Broad exceptions:** function — 4 `except Exception` (2 silent in `usage.py`), 2 `raise Exception` (`AnnotationService.py:77,110`); Streamlit — 18 `except Exception`, several returning `[]`/`""`.
- **Untyped dicts / missing hints:** ~52 bare `dict`/`list[dict]` in the function (32 in `EntityCacheService.py`); ~68 missing annotations in Streamlit; `finalize.py:121`, `dependencies.py:51` untyped.
- **Hard-coded literals:** `"fn_file_annotation"` (`ApplyService.py:59`, `PromoteCacheService.py:139`, `DataStructures.py:102-103`) despite Toolkit variable; tag strings duplicated outside `fa_constants.py`; Streamlit duplicates RAW names.
- **Dead code:** commented code `PromoteService.py:246-247`; no-op `except CogniteAPIError as e: raise e` (`LaunchService.py:410`, `RetrieveService.py:205`); unreachable reset query (`ConfigService.py:559-565`); several unused Streamlit params/branches.
- **Size / duplication:** `PromoteService.run` ~225 lines, `FinalizeService._finalize_job` ~215, `Config.build_internal_stage_config` ~230; four near-identical stage `handle`/`run_locally` pairs; identical `client_factory.py` and usage tracking in both apps.
- **Function-local imports:** `promote.py:120-121`; Streamlit `_report_usage`.
- **Dependencies:** Streamlit `requirements.txt` leaves `pandas`, `altair`, `PyYaml` unpinned; `cognite-sdk==7.73.4` vs function's `7.94.0`; unused `pyodide-http`, `python-dotenv`; `requests` undeclared; `requires-python >=3.11` vs repo 3.13+.
- **Packaging:** Streamlit test files are bundled into the deployed app (`build/streamlit/*.json`).

---

## J. Tooling results

| Check | Function | Streamlit |
|---|---|---|
| `ruff check` | pass | pass |
| `ruff format --check` | 2 files fail (`PromoteService.py`, `test_promote_relocate.py`) | 20 of 25 files fail |
| `pyright` | 0 errors | not checked — `pyrightconfig.json` excludes `modules/**/streamlit/**` |
| `pytest` | 158 passed (isolated env) | 7 + 3 pass only per app; together → import-file mismatch (duplicate `test_data_structures.py` basenames) |

Repo `.venv` is broken (`urllib3.exceptions` missing — likely OneDrive sync); `uv sync --reinstall` fixes it. Tests
resolved pydantic 2.13.4 while deploy pins 2.12.4.

---

## Next Steps

1. **[CRITICAL]** Stop data loss: propagate search errors in Promote (#1); fix Streamlit save-with-filter (#5); set notebook `CONFIRM_DELETE = False` and strip outputs (#7).
2. **[CRITICAL]** Make pipeline health truthful: default `run_status` to failure (#2); stop blanket retry in Promote (#4); fix the Launch spin on unlaunchable files (#3).
3. **[CRITICAL]** Remove `disable_ssl` (#6).
4. **[CRITICAL]** Fix the transformations' instance-space handling and the `assets` overwrite (#9, #10); correct the cron comment (#8).
5. **[CRITICAL]** Pydantic `FunctionInput` (#11); bound Streamlit reads (#12).
6. **[WARNING]** Tighten `gp_file_annotation` ACLs; add `linkedFile` btree index; workflow `retries: 1`; schedule or orchestrate the status report.
7. **[WARNING]** Batch the N+1 RAW/DMS calls; pass a deadline into inner loops.
8. **[WARNING]** Naming migration plan (`raw_` db, data model `_SOL`, persona-led group, sentence-case display names) — legacy, plan separately.
9. **[WARNING]** Coding-standard clean-up as separate refactor PRs: `ruff format`, snake_case module names, narrow exceptions, typed models instead of dicts, include Streamlit in pyright.

---
*Share this file for alignment reviews. For deeper guidance trigger the matching specialist
skill: `cdf-naming-check`, `cognite-data-modeling`, `cognite-transformation`,
`cognite-function`, `cognite-workflow`, or `cognite-dms-queries`.*
