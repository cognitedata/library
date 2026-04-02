# Documentation map — `cdf_key_extraction_aliasing`

All paths are relative to `modules/accelerators/contextualization/cdf_key_extraction_aliasing/` unless noted.

## Start here


| Document                                                   | Audience                      | Contents                                                                              |
| ---------------------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------- |
| [Module functional document](module_functional_document.md) | Everyone                      | End-to-end scope, behaviors, components, data flows, interfaces (points to deep specs) |
| [Module README](../README.md)                              | Everyone                      | What the module does, prerequisites, [Local runs (main.py)](../README.md#local-runs-mainpy), [Python API](../README.md#python-api), pointers to deeper docs |
| [Logging (CDF functions)](guides/logging_cdf_functions.md) | Developers / workflow authors | `logLevel` / `verbose`, required logger methods, optional handler injection           |
| [Config layout](../config/README.md)                       | Authors                       | Module-root scope YAML (`workflow.local.config.yaml`), `default.config.yaml` `scope_hierarchy`, `tag_patterns.yaml`, `config/examples/`, v1 scope shape, `build_scopes.py`, `main.py --build` (create missing triggers only) / `--check-workflow-triggers` |
| [Config examples](../config/examples/README.md)            | Authors / testers             | Demo folders, `--config-path` examples, progressive demo order                        |


## Configuration (YAML and parameters)


| Document                                                                                    | Contents                                                                                             |
| ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [Configuration guide](guides/configuration_guide.md)                                        | Pipeline YAML, `source_views`, filters, validation, aliasing parameters, `alias_mapping_table` (RAW) |
| [Reference YAML (flat)](../config/examples/reference/config_example_complete.yaml)          | Exhaustive field listing                                                                             |
| [Reference scope YAML](../config/examples/reference/reference_key_extraction_aliasing.yaml) | Same fields as above in `key_extraction_aliasing` document shape                                     |
| Migration notes                                                                             | `[LEGACY_TO_NEW_*.md](../config/examples/reference/)` — old pipeline → new config                    |


## CDF operations


| Document                                                                     | Contents                                                                                                               |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [Workflows README](../workflows/README.md)                                   | Single workflow `key_extraction_aliasing` (v4), generated YAML under `workflows/`, `workflow.input.configuration` |
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
| [Key extraction spec](specifications/1.%20key_extraction.md) | Methods, rules, extraction types, handlers   |
| [Aliasing spec](specifications/2.%20aliasing.md)             | Transformation types, rules, engine behavior |


## Generated reports and troubleshooting


| Document                                                              | Contents                                                                                                  |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [Key extraction / aliasing report](key_extraction_aliasing_report.md) | Current default scope, shared tag regex, aliasing stack; run-specific metrics live under `tests/results/` |
| [Common issues](troubleshooting/common_issues.md)                     | Troubleshooting                                                                                           |


## Tests


| Document                              | Contents                                         |
| ------------------------------------- | ------------------------------------------------ |
| [tests/README.md](../tests/README.md) | Layout, how to run `pytest` from repository root |


## Internal / analysis (optional)

Additional design notes may exist alongside this tree in git history or team wikis; nothing is required to operate the module.

## Package entry points (code)

- **CLI**: `main.py` — see module [README](../README.md)
- **Engines**: `functions/fn_dm_key_extraction/engine/`, `functions/fn_dm_aliasing/engine/`
- **CDF handlers**: `functions/fn_dm_key_extraction/handler.py`, `functions/fn_dm_aliasing/handler.py`, `functions/fn_dm_alias_persistence/handler.py`
- **Scope tooling**: `scripts/build_scopes.py`, `scripts/scope_build/registry.py`

