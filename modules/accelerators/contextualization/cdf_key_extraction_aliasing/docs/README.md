# Documentation map — `cdf_key_extraction_aliasing`

All paths are relative to `modules/accelerators/contextualization/cdf_key_extraction_aliasing/` unless noted.

## Start here


| Document                                                   | Audience                      | Contents                                                                              |
| ---------------------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------- |
| [Key Discovery incremental state](#key-discovery-incremental-state-architecture-summary) | Everyone | Plan-aligned summary: FDM vs RAW split, checkpoint vs processing state, naming, deploy/fallback |
| [Module functional document](module_functional_document.md) | Everyone                      | End-to-end scope, behaviors, components, data flows, interfaces (points to deep specs) |
| [Module README](../README.md)                              | Everyone                      | What the module does, prerequisites, [Local runs (module.py)](../README.md#local-runs-modulepy), [Python API](../README.md#python-api), [custom handlers how-to](guides/howto_custom_handlers.md), pointers to deeper docs |
| [Quickstart — local `module.py`](guides/howto_quickstart.md) | Everyone                      | `.env` at repo root, `PYTHONPATH=.`, run `module.py`, read outputs under `tests/results/` |
| [Build configuration (YAML)](guides/howto_config_yaml.md)   | Authors                       | v1 scope files, `default.config.yaml` hierarchy, template alignment, `module.py build` / `run`, deploy pointers |
| [Build configuration (UI)](guides/howto_config_ui.md)      | Authors / operators           | Local operator UI + API: edit scope/template/triggers, run build and pipeline, `run_all` / `--all`, security notes |
| [Scoped deployment](guides/howto_scoped_deployment.md)     | Authors / operators           | `aliasing_scope_hierarchy`, `module.py build`, WorkflowTrigger `configuration` / instance spaces, local parity from a trigger, Cognite Toolkit `cdf build` / `cdf deploy` |
| [Logging (CDF functions)](guides/logging_cdf_functions.md) | Developers / workflow authors | `logLevel` / `verbose`, required logger methods, optional handler injection           |
| [Config layout](../config/README.md)                       | Authors                       | Module-root scope YAML (`workflow.local.config.yaml`), `default.config.yaml` `aliasing_scope_hierarchy`, `tag_patterns.yaml`, `config/examples/`, v1 scope shape, `build_scopes.py`, `module.py build` (create missing triggers), `build --clean` (remove generated `workflows/` YAML), `--check-workflow-triggers` |
| [Config examples](../config/examples/README.md)            | Authors / testers             | Demo folders, `--config-path` examples, progressive demo order                        |


## Key Discovery incremental state (architecture summary)

Incremental **listing cursor**, **per-record content hash**, and **prior classification** for cohort gating can live in **FDM** (CDM-aligned Key Discovery views) while the **work queue** stays on **RAW** — by design: moving every cohort row into data modeling would multiply instance creates/updates and hit consumption limits at scale.

| Responsibility | **FDM (Key Discovery)** | **RAW** (`raw_table_key`) |
|----------------|-------------------------|---------------------------|
| Global listing watermark (`lastUpdatedTime` cursor) | **`KeyDiscoveryScopeCheckpoint`** (`highWatermarkMs`) | Legacy **`scope_wm_*`** rows only when FDM is off or at runtime fallback |
| Per-source-record hash, status, retries | **`KeyDiscoveryProcessingState`** (`lastSeenHash`, `status`, …) | **`EXTRACTION_INPUTS_HASH`** scans only in legacy/fallback path — not dual-written when FDM is active |
| Cohort / workflow handoff (`RUN_ID`, `WORKFLOW_STATUS=detected` → downstream) | — | **Kept on RAW** (not migrated to FDM as a per-run queue) |

**Views** (`data_modeling/`): **`KeyDiscoveryProcessingState`** and **`KeyDiscoveryScopeCheckpoint`** both **implement** `cdf_cdm:CogniteDescribable` (container `requires` + describable payload on upsert). Deploy them with Cognite Toolkit alongside functions. At runtime, if views are missing or API calls fail, **`fn_dm_incremental_state_update`** and **`fn_dm_key_extraction`** **fall back** to RAW watermark + hash behavior.

**Naming (config vs legacy):** **`workflow_scope`** (same as leaf **`scope.id`**, injected by **`module.py build`**) groups FDM rows; **`source_view_fingerprint`** (deterministic hash of view + filters) disambiguates multiple source views under one scope; content digest is **`lastSeenHash`** (hash v2 payload may use `workflow_scope` / `source_view_fingerprint` in JSON). Optional per-view **`key_discovery_hash_property_paths`** controls which source properties feed the hash.

**See also:** [Module functional document](module_functional_document.md) (§3.4 Incremental processing), [Configuration guide](guides/configuration_guide.md) (subsection *Incremental mode, Key Discovery FDM, and RAW cohort*), [Workflows README](../workflows/README.md) (incremental state task), [module README](../README.md) (*Incremental cohort processing*), [Scoped deployment](guides/howto_scoped_deployment.md) (*Key Discovery data model*).


## Configuration (YAML, UI, and parameters)


| Document                                                                                    | Contents                                                                                             |
| ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [How to build configuration (YAML)](guides/howto_config_yaml.md)                          | File-based authoring: scope vs `default.config.yaml` vs template, build, run, deploy                 |
| [How to build configuration (UI)](guides/howto_config_ui.md)                               | Operator UI + API: dev setup, tabs, sync template, run targets, `run_all`                            |
| [Configuration guide](guides/configuration_guide.md)                                        | Pipeline YAML, `source_views`, filters, validation, aliasing parameters, `alias_mapping_table` (RAW) |
| [Workflow associations](guides/workflow_associations.md)                                    | Top-level `associations`, view→extraction bindings, Python reconcile, `compile_canvas_associations.py` |
| [Reference YAML (flat)](../config/examples/reference/config_example_complete.yaml)          | Exhaustive field listing                                                                             |
| [Reference scope YAML](../config/examples/reference/reference_key_extraction_aliasing.yaml) | Same fields as above in `key_extraction_aliasing` document shape                                     |
| Migration notes                                                                             | `[LEGACY_TO_NEW_*.md](../config/examples/reference/)` — old pipeline → new config                    |


## Extending the code (developers)


| Document                                                                     | Contents                                                                                                               |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [How to add custom handlers](guides/howto_custom_handlers.md)                | Subclass extraction / aliasing handlers, register enums and engine maps, tests, redeploy functions                    |


## CDF operations


| Document                                                                     | Contents                                                                                                               |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [Workflows README](../workflows/README.md)                                   | Single workflow `key_extraction_aliasing` (v4), generated YAML under `workflows/`, `workflow.input.configuration`, incremental Key Discovery vs RAW |
| [Key Discovery data modeling YAML](../data_modeling/)                        | `KeyDiscoveryProcessingState` / `KeyDiscoveryScopeCheckpoint` containers, views, datamodel — deploy with Toolkit; runtime falls back to RAW if missing |
| [Workflow diagram source](../workflow_template/workflow_diagram.md)                  | Mermaid diagram source (no committed PNG; lives in `workflow_template/`)                                                            |
| [fn_dm_key_extraction](../functions/fn_dm_key_extraction/README.md)          | Key extraction function I/O                                                                                            |
| [fn_dm_aliasing](../functions/fn_dm_aliasing/README.md)                      | Aliasing function I/O                                                                                                  |
| [fn_dm_reference_index](../functions/fn_dm_reference_index/README.md)      | RAW inverted index for FK + document reference strings                                                                 |
| [fn_dm_alias_persistence](../functions/fn_dm_alias_persistence/README.md) | Describable write-back, optional FK strings                                                                            |
| [ISA patterns (aliasing)](../functions/fn_dm_aliasing/ISA_PATTERNS_USAGE.md) | Pattern-based rules and example config                                                                                 |
| [Tag pattern library (paths)](../functions/fn_dm_aliasing/TAG_PATTERNS_LIBRARY.md) | Where `tag_patterns.yaml` lives and how registries load it                                                          |


## Specifications (behavior and options)


| Document                                                     | Contents                                     |
| ------------------------------------------------------------ | -------------------------------------------- |
| [Key extraction spec](specifications/1.%20key_extraction.md) | Methods, rules, extraction types, handlers, incremental / Key Discovery pointers   |
| [Aliasing spec](specifications/2.%20aliasing.md)             | Transformation types, rules, engine behavior |


## Generated reports and troubleshooting


| Document                                                              | Contents                                                                                                  |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [Key discovery / aliasing report](key_extraction_aliasing_report.md) | Current default scope, shared tag regex, aliasing stack; run-specific metrics live under `tests/results/` |
| [Common issues](troubleshooting/common_issues.md)                     | Troubleshooting                                                                                           |


## Tests


| Document                              | Contents                                         |
| ------------------------------------- | ------------------------------------------------ |
| [tests/README.md](../tests/README.md) | Layout, how to run `pytest` from repository root; `tests/unit/docs/` guards how-to guide files |


## Internal / analysis (optional)

Additional design notes may exist alongside this tree in git history or team wikis; nothing is required to operate the module.

## Package entry points (code)

- **CLI**: `module.py` — [Quickstart](guides/howto_quickstart.md), [module README](../README.md) (*Local runs*), [Scoped deployment](guides/howto_scoped_deployment.md) for `--build` / triggers; **UI**: [How to build configuration with the UI](guides/howto_config_ui.md)
- **Engines**: `functions/fn_dm_key_extraction/engine/`, `functions/fn_dm_aliasing/engine/`
- **CDF handlers**: `functions/fn_dm_key_extraction/handler.py`, `functions/fn_dm_aliasing/handler.py`, `functions/fn_dm_alias_persistence/handler.py`
- **Scope tooling**: `scripts/build_scopes.py`, `scripts/scope_build/registry.py`

