# Current Branch Comparison: cdf_key_extraction_aliasing vs key_extraction_aliasing_jonluca

Generated from recursive diff (excluding `__pycache__` and timestamped test JSON).  
Paths: `modules/accelerators/contextualization/{cdf_key_extraction_aliasing | key_extraction_aliasing_jonluca}`.

---

## 1. Structure: Only in one branch

| Only in **cdf_key_extraction_aliasing** | Only in **key_extraction_aliasing_jonluca** |
|----------------------------------------|---------------------------------------------|
| `functions/common/` (folder) | — |
| `workflows/cdf_key_extraction_aliasing.Workflow.yaml` | `workflows/key_extraction_aliasing.Workflow.yaml` |
| `workflows/cdf_key_extraction_aliasing.WorkflowTrigger.yaml` | `workflows/key_extraction_aliasing.WorkflowTrigger.yaml` |
| `workflows/cdf_key_extraction_aliasing.WorkflowVersion.yaml` | `workflows/key_extraction_aliasing.WorkflowVersion.yaml` |

Both have `pipelines/`, `config/`, `data_sets/`, `docs/`, `functions/`, `tests/`, `scripts/`, `workflows/`.

---

## 2. Import / naming

| File / area | cdf | jonluca |
|-------------|-----|---------|
| **main.py** | `modules.accelerators.contextualization.cdf_key_extraction_aliasing.*` | `modules.contextualization.key_extraction_aliasing.*` |
| **Workflow YAMLs** | `externalId: cdf_key_extraction_aliasing` | `externalId: key_extraction_aliasing` |

All other import diffs under `fn_dm_key_extraction` follow the same pattern (cdf uses `modules.accelerators.contextualization.cdf_key_extraction_aliasing`, jonluca uses `modules.contextualization.key_extraction_aliasing`).

---

## 3. main.py

- **Only substantial diff:** import path (see above).  
- Both use the same `_load_configs()` (dict-based extraction/aliasing config and source_views list) and same flow (CDF client, load configs, loop views, run extraction + aliasing, write JSON, optional persistence).

---

## 4. fn_dm_key_extraction – handler.py

| Aspect | cdf | jonluca |
|--------|-----|---------|
| **Engine init** | `KeyExtractionEngine(cdf_config, logger=logger)` — typed `Config` only | Same |
| **Config path** | No `convert_cdf_config_to_engine_config`; when `"config"` in data: `cdf_config = raw if isinstance(raw, Config) else Config(**raw)` | Same logic; commented-out engine_config conversion |
| **Pipeline call** | `cdf_config=cdf_config` always | `cdf_config=cdf_config if CDF_CONFIG_AVAILABLE and client else None` |
| **run_locally()** | `client_name="KeyExtraction_Local"`, `base_url=f"https://{cdf_cluster}.cognitedata.com"`, pipeline id `ctx_key_extraction_regex`, `print(result)` | `client_name="poweruser"`, `base_url=f"https://p001.plink.{cdf_cluster}.cognitedata.com"`, pipeline id `ctx_key_extraction_files_pid_aliases_rob`, result not printed |

So: cdf uses direct base URL and different default pipeline id; jonluca uses plink URL and different pipeline id; otherwise handler behavior is aligned (typed Config, no engine_config conversion).

---

## 5. fn_dm_key_extraction – config.py

- **cdf (310 lines):** Defines `ValidationConfig` **before** `ExtractionRuleConfig`; `SourceViewConfig(TargetViewConfig)` with `target_prop` and `resource_property=""` default; `ExtractionRuleConfig` has `config` with `alias="parameters"`; `ExtractionRuleConfig.min_confidence` property.
- **jonluca (319 lines):** `SourceViewConfig(ViewConfig)` (no `TargetViewConfig`), `resource_property` required; `ExtractionRuleConfig` uses `config` (no alias shown in diff); `ValidationConfig` with `regexp_match`; `ViewConfig` + `TargetViewConfig` present; more verbose descriptions and comments.

So: cdf has optional `resource_property`, `SourceViewConfig` extends `TargetViewConfig`, and validation/min_confidence ordering and property differ slightly.

---

## 6. fn_dm_key_extraction – pipeline.py

| Aspect | cdf | jonluca |
|--------|-----|---------|
| **Imports** | No `time`, `json`, `traceback`, `ThreadPoolExecutor`, `pandas`; has `CogniteFunctionLogger`, `ExtractionResult`; same `GeneralApplyService` | Uses `time`, `json`, `traceback`, `ThreadPoolExecutor`, `pandas`; `Config`; import path `modules.contextualization...ApplyService` |
| **Entity metadata `keys`** | `result.candidate_keys` (list of `ExtractedKey`) | Same |
| **RAW upload** | Group by `rule_id`; tables `raw_table_key_{rule_id}`; `rule_id = getattr(key, "rule_id", getattr(key, "rule_name", ""))`; safe getattr for `resource_property`/`space` | Same grouping; `rule_id = key.rule_id`; direct `cdf_config.data.source_view.resource_property` / `instance_space` |
| **ApplyService** | Called when `use_cdf_format and cdf_config and getattr(cdf_config.parameters, "apply", True)` | Same; `if cdf_config.parameters.apply` |
| **_get_target_entities_cdf** | Signature `(client, config, logger)`; `source_views` from `config.data.source_views` or `[config.data.source_view]`; `raw_db`, `raw_table_key`, `overwrite` from `config.parameters` | Same idea; jonluca uses `config.data.source_view` only (no `source_views` fallback in diff); entity keys use `f"{rule.name}_{field_name}"` in both |

So: pipeline logic is aligned (group by rule_id, one table per rule, ApplyService); cdf adds defensive getattrs and supports both `source_view` and `source_views`.

---

## 7. fn_dm_key_extraction – engine/key_extraction_engine.py

| Aspect | cdf | jonluca |
|--------|-----|---------|
| **Import path** | `modules.accelerators.contextualization.cdf_key_extraction_aliasing...TokenReassemblyMethodParameter`; `Config, ExtractionRuleConfig` after utils | `modules.contextualization.key_extraction_aliasing...`; `Config, ExtractionRuleConfig` before utils |
| **Logger default** | `CogniteFunctionLogger("INFO", True)` | `CogniteFunctionLogger("INFO", False)` |
| **method_handlers type** | `Dict[str, ExtractionMethodHandler]` (string keys) | `Dict[ExtractionMethod, ExtractionMethodHandler]` (same values, type hint differs) |
| **Rules / composite** | No `rule.enabled` check; no composite block; `source_fields` normalized to list (single or list); `_get_field_value(..., rule.name)`; preprocessing branch with getattr | jonluca has `if not rule.enabled`; commented composite block; `rule.source_fields` sorted directly; `source_field.required` / `source_field.preprocessing`; no list normalization |

So: cdf normalizes `source_fields` and uses getattr for optional attributes; jonluca keeps `rule.enabled` and commented composite logic; both use same rule/strategy flow otherwise.

---

## 8. fn_dm_key_extraction – utils/DataStructures.py

| Aspect | cdf | jonluca |
|--------|-----|---------|
| **SourceFieldParameter** | `field_type="string"` default; `table_id` optional; `max_length` in this class | `field_type` required; `join_fields`; `max_length` in different place (or absent in diff) |
| **ExtractionRule** | **Present** (dataclass: name, extraction_type, method, source_fields, config, composite_strategy, etc.) | **Absent** (only Pydantic ExtractionRuleConfig in config.py) |
| **ExtractedKey** | `rule_id` + `rule_name` (backward compat); constructor `rule_name=None` | `rule_id` only; no `rule_name` |

So: cdf keeps the old `ExtractionRule` dataclass and dual `rule_id`/`rule_name` on `ExtractedKey`; jonluca relies on Pydantic config only and `rule_id` only.

---

## 9. fn_dm_key_extraction – services/ApplyService.py

- Both define `IApplyService` and `GeneralApplyService`.
- **cdf:** Imports `from ..common.logger import CogniteFunctionLogger`; ApplyService reads per-rule RAW tables and applies to `target_prop`; uses `target_view_config` from `config.data.source_view`; node retrieval uses getattr for `space`/`properties`.
- **jonluca:** Imports `from .LoggerService import CogniteFunctionLogger` (same class name, different path); same high-level flow.

So: only import path and minor attribute access differ; behavior is the same.

---

## 10. Files that differ (summary)

**Differ (content):**  
README.md, fn_dm_key_extraction/cdf_adapter.py, config.py, engine/__init__.py, engine/handlers/*.py (ExtractionMethodHandler, FixedWidth, Heuristic, Regex, TokenReassembly), key_extraction_engine.py, handler.py, pipeline.py, services/ApplyService.py, utils/DataStructures.py, utils/FixedWidthMethodParameter.py, utils/HeuristicMethodParameter.py, utils/RegexMethodParameter.py, utils/TokenReassemblyMethodParameter.py, main.py, scripts/generate_report.py, tests/generate_detailed_results.py, and all listed integration/unit test files under tests/.

**Only in one branch:**  
See section 1 (cdf: `functions/common/`, workflow names with `cdf_` prefix; jonluca: workflow names without `cdf_`).

---

## 11. Quick merge-oriented summary

- **Unify:** Import path (accelerators + cdf vs contextualization); workflow externalIds (`cdf_` vs not); handler `run_locally()` (base URL, client name, pipeline id).
- **Config:** cdf has ValidationConfig first and SourceViewConfig extending TargetViewConfig with optional `resource_property`; jonluca has slightly different ordering and required `resource_property`. Decide one shape for merged config.
- **Engine:** cdf normalizes `source_fields` to list and uses getattr for optional fields; jonluca has `rule.enabled` and commented composite. Merge: keep normalization and optional getattrs; decide whether to keep `rule.enabled` and/or composite.
- **DataStructures:** cdf keeps `ExtractionRule` dataclass and `rule_name` on `ExtractedKey`; jonluca does not. Merge: either drop `ExtractionRule` and use only ExtractionRuleConfig, or keep both and keep `rule_name` for backward compat.
- **Pipeline:** Already aligned (group by rule_id, one table per rule, ApplyService); only import path and defensive getattrs differ.
- **Tests:** Many test files differ (import paths and possibly fixtures). After fixing import path, re-run tests on both branches and then on merged tree.

---

*Comparison run: recursive diff between the two module directories; key files inspected for structural and behavioral differences.*
