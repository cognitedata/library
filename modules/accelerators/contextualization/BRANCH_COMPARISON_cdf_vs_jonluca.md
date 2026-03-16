# Branch Comparison: cdf_key_extraction_aliasing vs key_extraction_aliasing_jonluca

This document summarizes differences between the two divergent branches to support merging into a new trunk.

---

## 1. Directory and Naming

| Aspect | cdf_key_extraction_aliasing | key_extraction_aliasing_jonluca |
|--------|------------------------------|---------------------------------|
| **Pipeline config directory** | `extraction_pipelines/` | `pipelines/` |
| **Workflow external IDs** | `cdf_key_extraction_aliasing` (Workflow, WorkflowVersion, WorkflowTrigger) | `key_extraction_aliasing` |
| **Python import path** | `modules.accelerators.contextualization.cdf_key_extraction_aliasing` | `modules.contextualization.key_extraction_aliasing` |
| **Root path in docs/README** | Some docs still say `modules/contextualization/key_extraction_aliasing` | `modules/contextualization/key_extraction_aliasing` |

**Merge consideration:** Decide on a single name (with or without `cdf_` prefix) and a single path (`accelerators.contextualization` vs `contextualization`). Unify pipeline folder name: either `pipelines` or `extraction_pipelines` (and update `main.py` accordingly—see below).

---

## 2. main.py

- **Import path:** Only difference is the package path:
  - cdf: `modules.accelerators.contextualization.cdf_key_extraction_aliasing.*`
  - jonluca: `modules.contextualization.key_extraction_aliasing.*`
- **Config loading:** Both use `SCRIPT_DIR / "pipelines"` in `_load_configs()`.  
  **Issue:** In the cdf branch the directory is actually `extraction_pipelines/`, so `main.py` there would not find any config files unless a `pipelines` symlink exists or the code is changed to use `extraction_pipelines`.
- **Logic:** Otherwise the two `main.py` files are effectively the same (CDF client, view listing, extraction, aliasing, persistence, report generation).

**Merge action:** Use one pipeline directory name and one `pipelines_dir` in `main.py` (either `pipelines` and rename cdf’s folder, or use `extraction_pipelines` in both and fix `main.py` in cdf).

---

## 3. fn_dm_key_extraction – Structural and Config

| Area | cdf_key_extraction_aliasing | key_extraction_aliasing_jonluca |
|------|-----------------------------|----------------------------------|
| **Engine config** | Dict-style `ConfigData`; engine has `_load_rules()` and uses `config.get("extraction_rules")`, `ValidationConfig` | Strongly-typed `Config` with `ExtractionRuleConfig`; engine uses `config.data.extraction_rules` and `config.data.field_selection_strategy` |
| **Engine constructor** | `KeyExtractionEngine(config: ConfigData, logger=...)` | `KeyExtractionEngine(config: Config, logger=...)` |
| **Handler** | Uses CDF config: `convert_cdf_config_to_engine_config(cdf_config)` → `engine_config`; stores `_cdf_config` and `_engine_config` in data; `KeyExtractionEngine(engine_config)` | Uses CDF config object directly: `KeyExtractionEngine(cdf_config, logger=logger)` (no conversion to engine_config) |
| **Handler local run** | `client_name="KeyExtraction_Local"`, `base_url=f"https://{cdf_cluster}.cognitedata.com"`, pipeline id `ctx_key_extraction_regex` | `client_name="poweruser"`, `base_url=f"https://p001.plink.{cdf_cluster}.cognitedata.com"`, pipeline id `ctx_key_extraction_files_pid_aliases_rob` |
| **Pipeline** | Uses `CogniteFunctionLogger`, no ApplyService; writes keys as `keys[field_name][key.value] = {confidence, extraction_type}`; `_get_target_entities_cdf(..., overwrite, raw_db, raw_table_key)`; `run_all`, `debug` params | Uses `Config` and `GeneralApplyService`; RAW upload groups by `rule_id` and stores richer per-rule structure; uses `ApplyService` for apply step |
| **Services** | `LoggerService`, `PipelineService`, `ReportService` | `LoggerService`, `PipelineService`, `ApplyService` (no ReportService) |

**Merge consideration:** Reconcile engine API (dict vs typed Config) and handler’s use of CDF config (converted engine_config vs direct Config). Decide whether to keep ApplyService (jonluca) or ReportService (cdf) or support both.

---

## 4. fn_dm_key_extraction – Files That Differ

- `cdf_adapter.py`
- `config.py` (cdf: retrieval from CDF extraction pipeline config; jonluca: different config loading)
- `engine/__init__.py`
- `engine/handlers/ExtractionMethodHandler.py`
- `engine/handlers/FixedWidthExtractionHandler.py`
- `engine/handlers/HeuristicExtractionHandler.py`
- `engine/handlers/RegexExtractionHandler.py`
- `engine/handlers/TokenReassemblyExtractionHandler.py`
- `engine/key_extraction_engine.py` (config type and rule loading as above)
- `handler.py` (see above)
- `pipeline.py` (see above)
- `utils/DataStructures.py`
- `utils/FixedWidthMethodParameter.py`
- `utils/HeuristicMethodParameter.py`
- `utils/RegexMethodParameter.py`
- `utils/TokenReassemblyMethodParameter.py`

---

## 5. Pipeline Directory and Contents

- **cdf:** `extraction_pipelines/` – contains key extraction configs and `ctx_aliasing_default.*` (config + ExtractionPipeline YAML). No `ctx_key_extraction_default.ExtractionPipeline.yaml` in the list; other extraction pipelines have both `.config.yaml` and `.ExtractionPipeline.yaml`.
- **jonluca:** `pipelines/` – same logical set of configs plus `ctx_key_extraction_default.ExtractionPipeline.yaml` and `ctx_aliasing_default.*`.

So pipeline *content* is largely the same; only directory name and presence of `ctx_key_extraction_default.ExtractionPipeline.yaml` differ.

---

## 6. Tests and Artifacts

- **cdf:** Many timestamped JSON result files under `tests/results/` (e.g. `20260224_*_cdf_extraction.json`, `*_cdf_aliasing.json`). No `tests/results/__init__.py` in the diff.
- **jonluca:** Fewer result files; has `tests/results/__init__.py`, `tests/README.md`, `tests/view_detailed_results.py`, `tests/run_tests_and_save_results.py`, and `tests/fixtures/` (with sample_data for aliasing and key_extraction).

**Merge:** Keep test helpers and fixtures from jonluca; consider adding a `.gitignore` or script policy for generated `tests/results/*.json` so they don’t dominate the tree.

---

## 7. Docs and Workflows

- **cdf:** `workflows/` has `cdf_key_extraction_aliasing.*` YAMLs and `workflow_diagram.md`, `README.md`.
- **jonluca:** Same structure with `key_extraction_aliasing.*` and same supporting files.

README content in both branches is largely identical (including references to `modules/contextualization/key_extraction_aliasing`). cdf README also mentions `cdf-tk deploy extraction_pipelines`.

---

## 8. Only in One Branch

**Only in cdf_key_extraction_aliasing:**

- Directory `extraction_pipelines/` (vs `pipelines/` in jonluca).
- `functions/common/` at module root (if present).
- `fn_dm_key_extraction/services/ReportService.py`.
- Many `tests/results/*.json` files.

**Only in key_extraction_aliasing_jonluca:**

- Directory `pipelines/` (and `ctx_key_extraction_default.ExtractionPipeline.yaml` there).
- `fn_dm_key_extraction/services/ApplyService.py`.
- `fn_dm_key_extraction/dependencies.py`, `fn_dm_key_extraction/config.py` (different from cdf’s).
- `fn_dm_key_extraction/common/` (logger, config_utils, cdf_utils).
- `fn_dm_key_extraction/engine/handlers/ExtractionMethodHandler.py` (content differs from cdf).
- `tests/fixtures/`, `tests/view_detailed_results.py`, `tests/run_tests_and_save_results.py`, `tests/README.md`, `tests/results/__init__.py`.

---

## 9. Recommended Merge Checklist

1. **Naming and paths**
   - Choose one: workflow externalId with or without `cdf_` prefix.
   - Choose one: `modules.accelerators.contextualization.<name>` vs `modules.contextualization.<name>`.
   - Use a single pipeline directory name (`pipelines` or `extraction_pipelines`) and make `main.py` and any docs reference it consistently.

2. **Config and engine**
   - Unify engine config: either dict-based `ConfigData` (cdf) or typed `Config` (jonluca), and update handler and pipeline to match.
   - Align handler: either convert CDF config to engine format (cdf) or pass CDF config directly into the engine (jonluca).

3. **Services**
   - Decide on ApplyService vs ReportService (or keep both for different use cases) and update pipeline and handler to use the chosen service(s).

4. **Pipeline and RAW**
   - Reconcile pipeline logic: key storage shape (by field vs by rule_id) and use of `run_all`/`debug` (cdf) vs ApplyService (jonluca).
   - Ensure one consistent way to run and deploy extraction pipelines (including `ctx_key_extraction_default` if desired).

5. **Tests and docs**
   - Bring over jonluca test fixtures, `view_detailed_results.py`, `run_tests_and_save_results.py`, and tests README.
   - Update all README and doc paths to the chosen module path and pipeline directory.
   - Ignore or prune generated `tests/results/*.json` in version control.

6. **Smoke check**
   - Run `main.py` with the chosen pipeline directory and import path.
   - Run key extraction (and aliasing) via the chosen handler/pipeline path and confirm results and reports.

---

*Generated from diff and file inspection of both branches.*
