# CDF Best Practices Audit

**Project:** library — module `modules/common/cdf_dq_runtime` only
**Date:** 2026-11-06 (re-audit after partial fixes)
**Audited by:** cog-vd-audit (cog-vd-best-practices plugin)

**Skills used:** local marketplace `C:\github\ai-tooling-marketplace\plugins\cog-vd-best-practices\skills` for data modeling (`cdf-data-modeling`) and audit severity alignment. Naming / functions / DMS / workflows use the same plugin rules. Logging / Pydantic / `status` / `run_local` are **[WARN]** only (not CRITICAL), per review feedback.

Scope is this module only. Identifier renames for sample defaults remain deferred unless a migration is planned.

---

## Summary

| Domain | [PASS] Pass | [WARN] Warning | [FAIL] Fail | Status |
|---|---|---|---|---|
| A. Naming Conventions | 16 | 7 | 10 | [RED] |
| B. Data Modeling      | 5 | 1 | 0 | [YELLOW] |
| C. Transformations    | 0 | 0 | 0 | skipped |
| D. Functions          | 7 | 5 | 6 | [RED] |
| E. Workflows          | 4 | 3 | 0 | [YELLOW] |
| F. DMS Queries        | 6 | 2 | 0 | [YELLOW] |
| **Total**             | **38** | **18** | **16** | |

> [GREEN] No failures - [YELLOW] Warnings only - [RED] One or more failures

**Delivery verdict:** still **not merge-ready** for the bootstrap path. Naming defaults and several function deploy blockers remain. Partial fixes since the first audit cleared the timeseries timeout FAIL, notebook N+1 FAIL, empty series-prefix risk, README record-view inconsistency, and hardcoded-bundle config risk (via Schedule.`data` overrides).

### Fixed since first audit

| Area | Change |
|---|---|
| B / README | Package owns record containers + record views; no Toolkit `DataQualityValidationRecord.View.yaml`; single `cdf deploy` + `deploy_infrastructure.py` |
| E | `timeseries_workflow.task.timeout` **900** (was 1800) in `settings.yaml` and function bundle |
| E / config | `timeSeriesExternalIdPrefix` default **`pi:`** (was empty) |
| F | `manage_rulesets.ipynb` batches historic-queue `retrieve`, pages with `space=`, retries transient errors |
| D | Schedule.`data` passes Toolkit variables into the handler override merge (`_write_settings_with_overrides`) |

---

## [CRITICAL] Critical Issues

*(Open [FAIL] findings that block a working / production-safe Toolkit deploy of the bootstrap function. Naming FAILs on sample defaults are listed in section A; they are deferred renames, not bootstrap call blockers.)*

| # | Domain | File | Finding | Fix |
|---|---|---|---|---|
| 1 | D. Functions | `functions/dq_infrastructure_bootstrap/requirements.txt` | Pin is still `cognite-data-quality==0.0.0` (documented WIP). Function build cannot succeed. | Pin a released version that exports `deploy_validation_infrastructure`. |
| 2 | D. Functions | `functions/dq_infrastructure_bootstrap/handler.py` | Entry point is still `handler`. CDF loads `handle`. | Rename to `handle`. |
| 3 | D. Functions | `functions/dq_infrastructure_bootstrap.Function.yaml` | `externalId: dq-infrastructure-bootstrap` ≠ code directory `dq_infrastructure_bootstrap`. | Align external id, directory, and `functionPath: ./handler.py` (e.g. snake_case `dq_infrastructure_bootstrap`). |
| 4 | D. Functions | `functions/dq_infrastructure_bootstrap.Function.yaml` | `functionPath: dq_infrastructure_bootstrap/handler.py` is wrong relative to the code directory. | `functionPath: ./handler.py` |
| 5 | D. Functions | `functions/dq_infrastructure_bootstrap.Schedule.yaml` | No `authentication` block. Non-`dev` Toolkit validation rejects the schedule. | Add `authentication.clientId` / `clientSecret` Toolkit variables. |
| 6 | D. Functions | `functions/dq_infrastructure_bootstrap/handler.py` | `_delete_bootstrap_schedule` runs in `finally` even when deploy fails, so the 5-minute retry schedule is removed after a failed bootstrap. | Delete the schedule only after a successful deploy; keep temp-file cleanup in `finally`. |

---

## A. Naming Conventions

**Files scanned:** 20 YAML resources plus `default.config.yaml` / `settings.yaml` defaults  
**Identifiers checked:** 33

Defaults below are still the sample values in `default.config.yaml` / `settings.yaml`. System CDM/IDM refs are not findings.

### [FAIL] Failures

| File | Resource | Identifier | Issue | Suggested Fix |
|---|---|---|---|---|
| `default.config.yaml` | Space | `dataQuality` | CamelCase; not `inst_*` / `dm_*` | `inst_data_quality` |
| `default.config.yaml` | Schema space | `sp_dq_data_product` | Not `dm_{src\|dom\|sol}_*` | `dm_sol_data_quality` |
| `default.config.yaml` | Data product | `cognite-core-data-quality` | Hyphens | `data_quality_core` |
| `functions/…Function.yaml` | Function | `dq-infrastructure-bootstrap` | Hyphens; no `fn_` | Prefer `dq_infrastructure_bootstrap` now; `fn_…` in a later naming PR |
| `functions/…Schedule.yaml` | Schedule name | `dq-infrastructure-bootstrap` | Hyphens | Match function id |
| `default.config.yaml` | Stream | `test_stream_1` | `test_*` / `*_1` | `cdf_all_validation_results` |
| `default.config.yaml` | Data set | `dq_shacl_rules` | No `ds_` | `ds_data_quality_all` |
| `settings.yaml` | WF prefix | `dq-shacl` | Hyphens; no `wf_` | `wf_all_validate_instances` |
| `settings.yaml` | WF prefix | `dq-ts-shacl` | Hyphens; no `wf_` | `wf_all_validate_timeseries` |
| `settings.yaml` | Trigger prefix | `dq-shacl-trigger` | Hyphens | snake_case `wf_…` |

### [WARN] Warnings

| File | Resource | Identifier | Note |
|---|---|---|---|
| RuleSet YAMLs (7) | Display `name` | e.g. `Asset SHACL Rules` | Title case; prefer sentence case. External ids are snake_case [PASS]. |
| `maintenance_order_rule_logic_test_shacl_rules` | Rule set | contains `test` | Still referenced from the published DataProduct version. |

### [PASS] Passed

16 identifiers: `inst_location`; system spaces/views; seven rule-set external ids (charset).

---

## B. Data Modeling

**Skill:** local `cdf-data-modeling` (marketplace), not Cursor plugin cache.  
**Files on disk:** `dataQuality.Space.yaml`, `data_product_schema.Space.yaml` only. No Toolkit containers/views/data models.

### [FAIL] Failures

None.

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| Package (out of module) | Containers not in repo | Record / state containers remain owned by `cognite-data-quality`. Property descriptions, explicit `usedFor: record`, and no constraints/indexes were not verified against package source in this pass. |

### [PASS] Passed

- Spaces use Toolkit variables (`{{ dataQualitySpace }}`, `{{ dataProductSchemaSpace }}`).
- README now states package ownership of record containers + record views; no Toolkit view under `data_modeling/views/`; deploy order is a single Toolkit deploy then `deploy_infrastructure.py`.
- No orphaned Toolkit views; no solution `implements:`; no CogniteAsset implementers in this module.
- Local skill “record has no view” vs Cognite Docs streamId record views: module correctly keeps record views out of Toolkit / out of data models and documents package ownership.

**Status:** [YELLOW] (package schemas unchecked).

---

## C. Transformations

> No files found — skipping.

---

## D. Functions

**Files:** Function YAML, Schedule YAML, `handler.py`, `requirements.txt`, bundled `config/settings.yaml`.

### [FAIL] Failures

| File | Check | Detail | Fix |
|---|---|---|---|
| `requirements.txt` | Pin | `cognite-data-quality==0.0.0` | Released pin |
| `handler.py` | Entry point | `def handler` | `def handle` |
| Function YAML | Directory = externalId | Hyphenated id vs underscore directory | Align |
| Function YAML | `functionPath` | Nested path | `./handler.py` |
| Schedule YAML | Auth | Missing `authentication` | Toolkit client id/secret vars |
| `handler.py` | Schedule lifecycle | Deletes schedule in `finally` after failures | Delete only on success |

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `handler.py` | Logging | Still uses `import logging` / `logger.warning` | Prefer `print()` on Azure |
| `handler.py` | Pydantic | `data.get("force")` and override keys are untyped | `FunctionInput` model |
| `handler.py` | Return `status` | Relies on package return shape | Ensure `status` in returned dict |
| `handler.py` | `run_local()` | Not scaffolded | Add for local iteration |
| `handler.py` | Timeout / cron | Full infra deploy on `*/5 * * * *` may exceed 15 minutes | Monitor; keep schedule until success |

### [PASS] Passed

- `description` and `owner` set; secrets via `secrets` / `function_secrets`, not schedule `data` credentials.
- `runtime: py312` supported.
- Schedule.`data` injects Toolkit variables; `_write_settings_with_overrides` merges them into settings used by deploy (hardcoded-bundle CRITICAL from first audit is **cleared**).
- `pyyaml` pinned in `requirements.txt`.
- Function YAML lives under `functions/`.

---

## E. Workflows

No Toolkit `*.Workflow.yaml`. Contract is `settings.yaml` + package-generated workflows.

### [FAIL] Failures

None (timeseries timeout FAIL cleared: **900** s; series prefix default **`pi:`**).

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| `settings.yaml` | Version string | `version: "1"` — prefer `v1` for initial delivery |
| Package / settings | `dependsOn` | Generated task graphs not visible in this repo |
| `data_quality.DataProductVersion.yaml` + timeseries rules | Sample / test content | `maintenance_order_rule_logic_test_shacl_rules` still on the published DP; timeseries RuleSetVersion still has sample/canary shapes targeting `pi:160554` / `pi:160245` |

### [PASS] Passed

- `on_failure: abortWorkflow` on instance and timeseries tasks.
- Instance timeout 600 and timeseries timeout 900 are within the function call limit.
- Retries set to 3 on both task blocks.
- Empty `timeSeriesExternalIdPrefix` risk removed from defaults.

---

## F. DMS Queries

### [FAIL] Failures

None (batched historic-queue retrieve + paged list with `space=` and retries).

### [WARN] Warnings

| File | Check | Detail |
|---|---|---|
| Notebooks | Broad lists | `workflows.list` / some `schedules.list` / triggers still scan broadly (operator tools) |
| `views/*.yaml` | `partition_field: lastUpdatedTime` | Confirm package uses this only as a partition key, not as a large `/list` filter |

### [PASS] Passed

- Deploy scripts do not page DMS instances themselves.
- Historic queue list uses space filter, chunked iteration, and retry on 408/429/5xx-class codes.
- `delete_historic_queue_jobs` retrieves selected nodes in one call.
- Notebook output prints selected fields, not full dumps.
- Handler `schedules.list(..., limit=-1)` is Functions API scoped to one function id, not DMS.

---

## Next Steps

1. **[CRITICAL]** Pin `cognite-data-quality`, rename entry point to `handle`, align `externalId` / directory / `functionPath`, add schedule `authentication`, delete bootstrap schedule only on success.
2. **[WARNING]** Function polish: `print()`, Pydantic input, `status` wrapper, `run_local()` — not ship blockers alone.
3. **[WARNING]** Quarantine test / canary rule sets from the default published DataProduct; optionally normalize workflow `version` to `v1`.
4. **[DEFERRED]** Rename sample defaults (`inst_data_quality`, `dm_sol_*`, `fn_*` / `wf_*`, stream, dataset) in a dedicated migration PR.
5. Re-check package-owned record schemas when a real package pin lands.

---
*Share this file for alignment reviews. Specialist skills: `cdf-naming-check`, `cdf-data-modeling`, `cdf-function`, `cdf-workflow`, `cdf-dms-queries` (local marketplace preferred over Cursor cache).*
