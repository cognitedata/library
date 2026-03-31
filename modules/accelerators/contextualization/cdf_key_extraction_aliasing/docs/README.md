# Documentation map — `cdf_key_extraction_aliasing`

All paths are relative to `modules/accelerators/contextualization/cdf_key_extraction_aliasing/` unless noted.

## Start here

| Document | Audience | Contents |
|----------|----------|----------|
| [Module README](../README.md) | Everyone | What the module does, `main.py` quick start, CLI flags, pointers to deeper docs |
| [Quick start](guides/quick_start.md) | Developers | `PYTHONPATH`, `KeyExtractionEngine` / `AliasingEngine`, example config paths |
| [Config layout](../config/README.md) | Authors | `scopes/` vs `examples/`, v1 scope document shape, `build_scopes.py`, `main.py` flags |
| [Config examples](../config/examples/README.md) | Authors / testers | Demo folders, `--config-path` examples, progressive demo order |

## Configuration (YAML and parameters)

| Document | Contents |
|----------|----------|
| [Configuration guide](guides/configuration_guide.md) | Pipeline YAML, `source_views`, filters, validation, aliasing parameters, `alias_mapping_table` (RAW) |
| [Reference YAML (flat)](../config/examples/reference/config_example_complete.yaml) | Exhaustive field listing |
| [Reference scope YAML](../config/examples/reference/reference_key_extraction_aliasing.yaml) | Same fields as above in `key_extraction_aliasing` document shape |
| Migration notes | [`LEGACY_TO_NEW_*.md`](../config/examples/reference/) — old pipeline → new config |

## CDF operations

| Document | Contents |
|----------|----------|
| [Workflows README](../workflows/README.md) | `cdf_key_extraction_aliasing_{{ scope_cdf_suffix }}` tasks, RAW handoff, fusion placeholders (`scope_cdf_suffix`, `scope_leaf_display_name`) |
| [Workflow diagram source](../workflows/workflow_diagram.md) | Diagram source (and related assets in `workflows/`) |
| [fn_dm_key_extraction](../functions/fn_dm_key_extraction/README.md) | Key extraction function I/O |
| [fn_dm_aliasing](../functions/fn_dm_aliasing/README.md) | Aliasing function I/O |
| [fn_dm_alias_persistence](../functions/fn_dm_alias_persistence/README.md) | Describable write-back, optional FK strings |
| [ISA patterns (aliasing)](../functions/fn_dm_aliasing/ISA_PATTERNS_USAGE.md) | Pattern-based rules and example config |

## Specifications (behavior and options)

| Document | Contents |
|----------|----------|
| [Key extraction spec](specifications/1.%20key_extraction.md) | Methods, rules, extraction types, handlers |
| [Aliasing spec](specifications/2.%20aliasing.md) | Transformation types, rules, engine behavior |

## Generated reports and troubleshooting

| Document | Contents |
|----------|----------|
| [Key extraction / aliasing report](key_extraction_aliasing_report.md) | Current default scope, shared tag regex, aliasing stack; run-specific metrics live under `tests/results/` |
| [Common issues](troubleshooting/common_issues.md) | Troubleshooting |

## Tests

| Document | Contents |
|----------|----------|
| [tests/README.md](../tests/README.md) | Layout, how to run `pytest` from repository root |

## Internal / analysis (optional)

Additional design notes may exist alongside this tree in git history or team wikis; nothing is required to operate the module.

## Package entry points (code)

- **CLI**: `main.py` — see module [README](../README.md)
- **Engines**: `functions/fn_dm_key_extraction/engine/`, `functions/fn_dm_aliasing/engine/`
- **CDF handlers**: `functions/fn_dm_key_extraction/handler.py`, `functions/fn_dm_aliasing/handler.py`, `functions/fn_dm_alias_persistence/handler.py`
- **Scope tooling**: `scripts/build_scopes.py`, `scripts/scope_build/registry.py`
