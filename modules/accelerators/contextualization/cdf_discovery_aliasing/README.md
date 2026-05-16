# Key Discovery and Aliasing

Library for extracting candidate keys and foreign-key references from entity metadata, and generating aliases for matching. It targets **Cognite Data Fusion (CDF)** (data model views, functions, workflows) and can run standalone in Python.

**Full documentation index:** [docs/README.md](docs/README.md)

## Prerequisites

- **Python** 3.11+
- **PYTHONPATH:** use the **repository root** (the directory that contains `modules/`) so imports like `modules.accelerators.contextualization.cdf_discovery_aliasing` resolve — for example `PYTHONPATH=. python …` from that root.
- **Dependencies:** install in your environment (this repository may not ship a root `pyproject.toml`; use your team’s setup or install required packages manually).
- **CDF:** set credentials in `.env` or the environment when calling CDF APIs or running `module.py run` without `--dry-run`.

**Example config paths (for `--config-path` or `load_config_from_yaml`):** `config/examples/key_extraction/`, `config/examples/aliasing/`, `config/examples/reference/` — see [config/examples/README.md](config/examples/README.md).

## Results report

[docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md) describes the **current default scope** and behavior (maintained with the module).

To **overwrite** that markdown with statistics from the newest `local_run_results/*_cdf_extraction.json` (older style, run-specific tables), run:

```bash
python modules/accelerators/contextualization/cdf_discovery_aliasing/scripts/generate_report.py
```

## Capabilities

- **Extraction handlers:** `regex_handler` only — configured per rule with a shared `fields` list and optional `validation` / `validation_rules`
- **Extraction types:** candidate keys, foreign key references, document references
- **Aliasing transformation types:** character substitution, prefix/suffix, regex, case, **semantic expansion** (`type: semantic_expansion`), related instruments, hierarchical expansion, document aliases, leading zero normalization, pattern recognition, pattern-based expansion, composite, **`alias_mapping_table`** (RAW-backed tag→alias catalog when configured)
- **Config:** v1 scope document at module root (`workflow.local.config.yaml`) and examples under `config/examples/` (see [config/README.md](config/README.md))
- **Tests:** `tests/` — see [tests/README.md](tests/README.md)

**Reference tables** for each extraction method and aliasing `type` value: [Key extraction spec](<docs/specifications/1. key_extraction.md>), [Aliasing spec](<docs/specifications/2. aliasing.md>).

**Custom handlers (new Python methods or transformation types):** [How to add a custom handler](docs/guides/howto_custom_handlers.md).

**Configuration authoring:** [How to build configuration with YAML](docs/guides/howto_config_yaml.md) (files, git, CLI), [How to build configuration with the UI](docs/guides/howto_config_ui.md) (local operator UI + API).

**Local quickstart and scoped Toolkit deploy:** [Quickstart — `module.py run` and `.env`](docs/guides/howto_quickstart.md), [Scoped deployment — hierarchy, triggers, `cdf deploy`](docs/guides/howto_scoped_deployment.md).

**Default CDM scope** (asset + file + timeseries, shared `alphanumeric_tag`): [`workflow.local.config.yaml`](workflow.local.config.yaml) at module root. Narrative: [docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md). Authoring detail: [Configuration guide — Default CDM scope](docs/guides/configuration_guide.md#default-cdm-scope). Deployed triggers embed the patched template from [`workflow_template/workflow.template.config.yaml`](workflow_template/workflow.template.config.yaml).

### Configuration entry points

- **Deployed workflows** (**v5**) pass **trimmed** **`configuration`** on **`workflow.input`** (no **`compiled_workflow`**); **WorkflowVersion** tasks carry **inlined** per-step fields on function **`data`**. Functions derive `config` from **`configuration`**. Authoring source in-repo: **`workflow.local.config.yaml`** (local default, **`canvas`** in-file or merged locally), **`workflow_template/workflow.template.config.yaml`**, and **`config/examples/**`**. Validation and parameters are enforced in discovery function code (`fn_dm_*` `config.py` layers) where applicable.
- **`config/configuration_manager.py`:** dataclasses + JSON Schema for integration-style tests (`tests/integration/contextualization/`). **Not** used by `fn_dm_*` handlers at runtime.

**Multi-site scopes:** edit **`aliasing_scope_hierarchy`** in [default.config.yaml](default.config.yaml), then **`module.py build`** as in [Local runs (module.py)](#local-runs-modulepy). Deep dive: [config/README.md](config/README.md) (*Scope hierarchy builder*), [workflows/README.md](workflows/README.md) (generated manifests), [workflow_template/README.md](workflow_template/README.md) (templates), [scripts/scope_build/registry.py](scripts/scope_build/registry.py).

## Roadmap

- [x] Key Discovery FDM state for incremental watermark/hash (`data_modeling/`; RAW cohort unchanged; RAW fallback if views not deployed)
- [x] RAW **inverted index** for FK + document refs (`fn_dm_inverted_index`; see [workflows/README.md](workflows/README.md))
- [ ] DM projection / sync for the inverted index (RAW remains source of truth)
- [x] Default scope rules differentiated per `entity_type` / source view (separate extraction, validation, and aliasing scopes in [`workflow.local.config.yaml`](workflow.local.config.yaml); asset vs file vs timeseries)
- [ ] Broader non-ISA tag testing
- [ ] Inverted-index relationships beyond token lookup

## Local runs (module.py)

Run **`module.py run`** from **repository root** with **`PYTHONPATH=.`** (see [Prerequisites](./README.md#prerequisites)). Invoking **`module.py`** with no subcommand prints help. For a focused first-run checklist (`.env`, sample commands, **`local_run_results/`**), see [Quickstart — local `module.py run`](docs/guides/howto_quickstart.md). For **`aliasing_scope_hierarchy`**, **`module.py build`** (legacy **`module.py --build`**), editing triggers, and Toolkit **`cdf deploy`**, see [Scoped deployment](docs/guides/howto_scoped_deployment.md).

### Edit multi-site scope layout (`default.config.yaml`)

Before **`module.py build`**, configure **`aliasing_scope_hierarchy`** in [default.config.yaml](default.config.yaml) at the module root (or pass another file via **`--hierarchy`** to `scripts/build_scopes.py`):

- **`aliasing_scope_hierarchy.levels`** — Ordered labels for path tiers (same naming style as **`cdf_access_control`** `dimensions.*.levels` under **`type: hierarchy`**, e.g. `site` → `unit` → `area` → `system`). You do not have to reach every tier; shallow trees are valid, and deeper paths use synthetic names such as `level_5` when you run past the end of `levels`.
- **`aliasing_scope_hierarchy.locations`** — Root list of scope nodes. Each node should have a stable **`id`** (used in trigger `externalId` suffixes and `scope_id`) and optional **`name`** / **`description`**. **Children** of a node are listed under another **`locations`** key on that node (same key name as the root list). A **leaf** is a node with no child list or an empty **`locations: []`**; each leaf gets a scoped trio under **`workflows/<suffix>/`**, including **`key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml`**.

See the commented example under **`aliasing_scope_hierarchy.locations`** in `default.config.yaml` for a deeper tree.

### Workflow manifests (`build`)

**`module.py build`** creates missing scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** YAML under **`workflows/<suffix>/`** from **`workflow_template/`** and refreshes **`workflow_template/workflow.execution.graph.yaml`** from IR on every run. Use **`--force`** to overwrite **existing** scoped manifests after you edit **`workflow.template.config.yaml`** / canvas. **No CDF connection.** (Equivalent: **`module.py --build`**.)

```bash
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py build
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py build --check-workflow-triggers
```

The second form exits non-zero if required trigger files are missing or their content does not match the current templates (for CI). See [config/README.md](config/README.md) and [workflows/README.md](workflows/README.md).

### Remove generated workflow YAML (`build --clean`)

**`module.py build --clean`** (same flags on `scripts/build_scopes.py`) deletes Toolkit workflow artifacts under **`workflows/`** that match the **`workflow`** external id from your hierarchy file (per-suffix folders and a few legacy names). It prints a file list and a warning that the operation **cannot be undone**, then requires typing **`yes`** to proceed unless you pass **`--yes`** (required when stdin is not a TTY, for example in CI). **`--dry-run --clean`** shows what would be removed without deleting. **No build runs after a successful clean**—run **`module.py build`** again to recreate files.

This is **not** **`--clean-state`**: the latter drops incremental **RAW** tables for the pipeline scope; it does not remove **`workflows/*.yaml`**.

### Run the extraction / aliasing pipeline

```bash
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py run --dry-run
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py run --limit 50 --verbose
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py run --scope default
python modules/accelerators/contextualization/cdf_discovery_aliasing/module.py run --config-path modules/accelerators/contextualization/cdf_discovery_aliasing/workflow.local.config.yaml
```

If your project uses Poetry at repo root, prefix with `poetry run` as usual.

| Option | Description | Default |
|--------|-------------|---------|
| `build` | Subcommand: only run the scope builder — creates missing scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** under **`workflows/<suffix>/`**, and refreshes `workflow.execution.graph.yaml` from IR every run. **`--force`** overwrites existing scoped manifests and triggers. Forwards `--clean`, `--yes`, `--check-workflow-triggers`, `--dry-run`, `--hierarchy`, etc. Does **not** run the pipeline. Legacy: `--build` as first argument. | off |
| `--limit` | Max instances per view; `0` = all | `0` |
| `--verbose` | Verbose logging | false |
| `--dry-run` | Skip alias persistence to CDF | false |
| `--write-foreign-keys` | Persist FK reference strings (needs write-back property) | false |
| `--foreign-key-writeback-property` | DM property for FK strings | none |
| `--instance-space` | Keep `source_views` whose `instance_space` matches **or** whose `filters` constrain node `space` (`property_scope: node`, `EQUALS`/`IN`) | all |
| `--scope` | Only `default` supported without `--config-path` (loads module-root `workflow.local.config.yaml`) | `default` |
| `--config-path` | v1 scope document (overrides `--scope`) | none |
| `--clean-state` | Drop incremental RAW tables from the scope (key extraction state, inverted index, aliasing RAW), then run the pipeline | false |
| `--clean-state-only` | Same RAW drops as `--clean-state`, then exit (no pipeline) | false |
| `--skip-inverted-index` | Skip **`fn_dm_inverted_index`** during `module.py run` (workflow parity testing) | false |

**RAW clean does not** remove alias or FK values already written on data-model instances. For incremental runs, a typical full reprocess is `--clean-state --all`.

**Outputs:** timestamped JSON under `local_run_results/` (`*_cdf_extraction.json`, `*_cdf_aliasing.json`; same folder as optional `*_local_*` scripts). Without `--dry-run`, aliases are written to **`cdf_cdm:CogniteDescribable:v1`** (property configurable; default `aliases`). Optional FK strings use the configured FK write-back property.

**Write-back details:** [Configuration guide — Aliasing parameters](docs/guides/configuration_guide.md) and [workflows README](workflows/README.md).

## Incremental cohort processing (RAW cohort, CDM state)

When `parameters.incremental_change_processing` is true, **`fn_dm_view_query`** (and related query stages) advance listing watermarks / hashes via **Key Discovery** FDM views when deployed, and emit **cohort** rows on RAW with a per-run **`RUN_ID`** for downstream transform / validate / save tasks.

**Watermark and hash state:** When `parameters.key_discovery_instance_space` is set (and `workflow_scope` is set—scope build injects it from the hierarchy leaf), the global watermark and per-node hash/prior classification use **Key Discovery** FDM views under `data_modeling/` (`KeyDiscoveryScopeCheckpoint`, `KeyDiscoveryProcessingState`, CogniteDescribable-backed) **if those views are deployed** (verified via `data_modeling.views.retrieve`). If the views are missing or FDM calls fail, the pipeline **falls back** to the **RAW** watermark row (`scope_wm_*`) and RAW `EXTRACTION_INPUTS_HASH` scans. If `key_discovery_instance_space` is unset, only the RAW path is used.

**`parameters.incremental_skip_unchanged_source_inputs`** (default `true`): when enabled together with incremental processing, detection computes a SHA-256 digest of the same source fields and preprocessing as the query stage (optional per-view `key_discovery_hash_property_paths`), plus a fingerprint of `extraction_rules`. If it matches the latest stored hash for that node and scope, no new cohort row is emitted; **watermarks still advance** from `lastUpdatedTime` so unchanged noise updates do not re-list the same instances forever. With Key Discovery FDM, query upserts processing state instead of writing `EXTRACTION_INPUTS_HASH` on RAW rows. The `+1 ms` bound on `lastUpdatedTime` filters is unchanged. `run_all=true` still emits cohort rows for all matched instances regardless of prior hash/state.

## Alias write-back

- **Workflow:** `aliasWritebackProperty` / `alias_writeback_property` on the **`fn_dm_view_save`** task `data` (or the equivalent **`save_view`** canvas node).
- **`module.py` / scope YAML:** `aliasing.config.parameters.alias_writeback_property` in the loaded scope document.
- **Default property name on CogniteDescribable:** `aliases`.

See [Configuration guide](docs/guides/configuration_guide.md) (Aliasing `parameters`).

## Foreign key write-back

Enable with workflow flags, `module.py` / YAML, or env; set `foreign_key_writeback_property` when enabled. Requires that property on your target view. See [Configuration guide](docs/guides/configuration_guide.md).

## Python API

Programmatic extension points live in the **discovery** functions — primarily **`fn_dm_transform`**, **`fn_dm_validate`**, and **`fn_dm_view_save`** (plus optional **`fn_dm_raw_save`**, **`fn_dm_classic_save`**, and query/join stages per canvas). Author extraction / aliasing **libraries** in YAML under the v1 scope (`key_extraction`, `aliasing`) and reference them from canvas node **`data.config`**; see [How to add a custom handler](docs/guides/howto_custom_handlers.md).

### Config utilities

- **Scope document loading:** use `local_runner.config_loading.load_discovery_scope` / YAML loaders described in [Configuration guide](docs/guides/configuration_guide.md).
- **Typed / schema config (tests, tooling):** `config/configuration_manager.py` — not used by `fn_dm_*` handlers at runtime; see *Configuration entry points* above.

## Repository layout (this module)

```
cdf_discovery_aliasing/
├── module.py
├── workflow.local.config.yaml   # default v1 scope document (local CLI)
├── default.config.yaml     # module defaults + scope hierarchy for build_scopes.py
├── config/                 # tag_patterns.yaml, examples, configuration_manager — see config/README.md
├── functions/
│   ├── README.md           # index of all fn_dm_* executors and canvas kinds
│   ├── cdf_fn_common/      # shared workflow compile, scope, logging, …
│   ├── fn_dm_view_query/
│   ├── fn_dm_raw_query/
│   ├── fn_dm_classic_query/
│   ├── fn_dm_transform/
│   ├── fn_dm_validate/
│   ├── fn_dm_join/
│   ├── fn_dm_view_save/
│   ├── fn_dm_raw_save/
│   ├── fn_dm_classic_save/
│   ├── fn_dm_inverted_index/
│   ├── fn_dm_discovery_raw_cleanup/
│   └── functions.Function.yaml   # Toolkit externalIds
├── workflow_template/      # Authoring templates + diagram — see workflow_template/README.md
├── workflows/              # Generated Workflow YAML (--build) — see workflows/README.md
├── scripts/
├── tests/
└── docs/                   # see docs/README.md
```

| Path | Role |
|------|------|
| `workflow.local.config.yaml` | Default v1 scope document at module root (local CLI) |
| `functions/README.md` | Canvas kinds → **`fn_dm_*`** mapping and local pipeline entrypoints |
| `config/examples/` | Demos and reference YAML — [config/examples/README.md](config/examples/README.md) |
| `functions/fn_dm_*` | CDF functions + engines (`fn_dm_inverted_index` = RAW inverted index) |
| `workflow_template/` | `workflow.template.Workflow*.yaml`, `workflow.template.WorkflowTrigger.yaml`, `workflow.template.config.yaml`, diagram |
| `workflows/` | Generated Workflow / WorkflowVersion / WorkflowTrigger YAML |
| `tests/` | Pytest suite and `tests/results/` for test-harness JSON (distinct from `local_run_results/` pipeline outputs) |
| `ui/` | Operator UI (Vite) and `ui/server/` FastAPI for editing YAML and invoking `module.py` locally — [How to build configuration with the UI](docs/guides/howto_config_ui.md) |
| `docs/` | Specs, guides, report |

## Architecture (short)

- **Discovery canvas** — compiled **`canvas`** over **`fn_dm_*`** stages (default path often `fn_dm_view_query` → `fn_dm_transform` → `fn_dm_validate` → `fn_dm_view_save`; optional RAW/classic query/save, join, inverted index, cleanup) — [functions/README.md](functions/README.md), [workflows/README.md](workflows/README.md)

## Configuration details

Authoring and filters (`source_views[].filters`), validation, and field selection: **[docs/guides/configuration_guide.md](docs/guides/configuration_guide.md)** and **[config/README.md](config/README.md)**.

## CDF deployment

Deploy workflow **`key_extraction_aliasing`** (**v5**) once; use per-scope **`workflows/.../key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (embedded **trimmed** **`configuration`**). Creating or refreshing triggers: [Local runs (module.py)](#local-runs-modulepy), [workflows/README.md](workflows/README.md), and [Scoped deployment — Toolkit](docs/guides/howto_scoped_deployment.md).

## Testing

From **repository root**:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_discovery_aliasing/tests -q
```

See [tests/README.md](tests/README.md).

## Troubleshooting

- **Imports:** Run from repo root with `PYTHONPATH=.` including the `library` root (the parent of `modules/`).
- **No keys / no aliases:** Rules, `min_confidence`, `source_views`, and aliasing `enabled` flags — [docs/troubleshooting/common_issues.md](docs/troubleshooting/common_issues.md)
- **Workflow / RAW:** [workflows/README.md](workflows/README.md)

## Contributing

Add tests under `tests/` and update [docs/README.md](docs/README.md) or the relevant guide if you introduce new entry points or config shapes. If you add or rename operational docs under `docs/guides/`, extend [`tests/unit/docs/test_howto_guides.py`](tests/unit/docs/test_howto_guides.py) when those files should stay contract-tested.

## License

This repository is licensed under the Apache License 2.0 — see [`LICENSE`](../../../../LICENSE) at the repository root.

## Authors

- Darren Downtain
