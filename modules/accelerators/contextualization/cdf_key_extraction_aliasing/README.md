# Key Extraction and Aliasing

Library for extracting candidate keys and foreign-key references from entity metadata, and generating aliases for matching. It targets **Cognite Data Fusion (CDF)** (data model views, functions, workflows) and can run standalone in Python.

**Full documentation index:** [docs/README.md](docs/README.md)

## Prerequisites

- **Python** 3.11+
- **PYTHONPATH:** use the **repository root** (the directory that contains `modules/`) so imports like `modules.accelerators.contextualization.cdf_key_extraction_aliasing` resolve — for example `PYTHONPATH=. python …` from that root.
- **Dependencies:** install in your environment (this repository may not ship a root `pyproject.toml`; use your team’s setup or install required packages manually).
- **CDF:** set credentials in `.env` or the environment when calling CDF APIs or running `main.py` without `--dry-run`.

**Example config paths (for `--config-path` or `load_config_from_yaml`):** `config/examples/key_extraction/`, `config/examples/aliasing/`, `config/examples/reference/` — see [config/examples/README.md](config/examples/README.md).

## Results report

[docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md) describes the **current default scope** and behavior (maintained with the module).

To **overwrite** that markdown with statistics from the newest `tests/results/*_cdf_extraction.json` (older style, run-specific tables), run:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/scripts/generate_report.py
```

## Capabilities

- **Extraction methods:** passthrough (default when `method` is omitted), regex, fixed width, token reassembly, heuristic
- **Extraction types:** candidate keys, foreign key references, document references
- **Aliasing transformation types:** character substitution, prefix/suffix, regex, case, **semantic expansion** (`type: semantic_expansion`), related instruments, hierarchical expansion, document aliases, leading zero normalization, pattern recognition, pattern-based expansion, composite, **`alias_mapping_table`** (RAW-backed tag→alias catalog when configured)
- **Config:** v1 scope document at module root (`workflow.local.config.yaml`) and examples under `config/examples/` (see [config/README.md](config/README.md))
- **Tests:** `tests/` — see [tests/README.md](tests/README.md)

**Reference tables** for each extraction method and aliasing `type` value: [Key extraction spec](<docs/specifications/1. key_extraction.md>), [Aliasing spec](<docs/specifications/2. aliasing.md>).

**Default CDM scope** (asset + file + timeseries, shared `alphanumeric_tag`): [`workflow.local.config.yaml`](workflow.local.config.yaml) at module root. Narrative: [docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md). Authoring detail: [Configuration guide — Default CDM scope](docs/guides/configuration_guide.md#default-cdm-scope). Deployed triggers embed the patched template from [`workflow_template/workflow.template.config.yaml`](workflow_template/workflow.template.config.yaml).

### Configuration entry points

- **Deployed workflows** pass **`configuration`** on **`workflow.input`** (v4); functions derive `config` from it. Authoring source in-repo: **`workflow.local.config.yaml`** (local default), **`workflow_template/workflow.template.config.yaml`**, and **`config/examples/**`**. Validation: Pydantic in `functions/fn_dm_key_extraction/config.py`, `functions/fn_dm_aliasing/config.py`, and the `cdf_adapter` modules.
- **`config/configuration_manager.py`:** dataclasses + JSON Schema for integration-style tests (`tests/integration/contextualization/`). **Not** used by `fn_dm_*` handlers at runtime.

**Multi-site scopes:** edit **`scope_hierarchy`** in [default.config.yaml](default.config.yaml), then **`main.py --build`** as in [Local runs (main.py)](#local-runs-mainpy). Deep dive: [config/README.md](config/README.md) (*Scope hierarchy builder*), [workflows/README.md](workflows/README.md) (generated manifests), [workflow_template/README.md](workflow_template/README.md) (templates), [scripts/scope_build/registry.py](scripts/scope_build/registry.py).

## Roadmap

- [ ] State store for targets in CDM (reduce RAW-only patterns where applicable)
- [x] RAW inverted **reference index** for FK + document refs (`fn_dm_reference_index`; see [workflows/README.md](workflows/README.md))
- [ ] DM projection / sync for the reference index (RAW remains source of truth)
- [ ] Finer default rules per `entity_type`
- [ ] Broader non-ISA tag testing
- [ ] Reference-index relationships beyond inverted lookup

## Local runs (main.py)

Run **`main.py`** from **repository root** with **`PYTHONPATH=.`** (see [Prerequisites](./README.md#prerequisites)).

### Edit multi-site scope layout (`default.config.yaml`)

Before **`--build`**, configure **`scope_hierarchy`** in [default.config.yaml](default.config.yaml) at the module root (or pass another file via **`--hierarchy`** to `scripts/build_scopes.py`):

- **`scope_hierarchy.levels`** — Ordered labels for path tiers (for example site → plant → area → system). You do not have to reach every tier; shallow trees are valid, and deeper paths use synthetic names such as `level_3` when you run past the end of `levels`.
- **`scope_hierarchy.locations`** — Root list of scope nodes. Each node should have a stable **`id`** (used in trigger `externalId` suffixes and `scope_id`) and optional **`name`** / **`description`**. **Children** of a node are listed under another **`locations`** key on that node (same key name as the root list). A **leaf** is a node with no child list or an empty **`locations: []`**; leaves get one **`workflows/key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** each (or under **`workflows/<suffix>/`** when **`scope_build_mode: full`**).

See the commented example under **`scope_hierarchy.locations`** in `default.config.yaml` for a deeper tree.

### Create missing workflow triggers (`--build`)

**`--build`** creates **`workflows/key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (flat **`trigger_only`** layout) only when that file does not exist (embedded **`configuration`** per leaf). It does **not** overwrite existing triggers; delete a file first if you need to recreate it from templates. **No CDF connection.**

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --build
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --build --check-workflow-triggers
```

The second form exits non-zero if required trigger files are missing or their content does not match the current templates (for CI). See [config/README.md](config/README.md) and [workflows/README.md](workflows/README.md).

### Run the extraction / aliasing pipeline

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --dry-run
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --limit 50 --verbose
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --scope default
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --config-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflow.local.config.yaml
```

If your project uses Poetry at repo root, prefix with `poetry run` as usual.

| Option | Description | Default |
|--------|-------------|---------|
| `--build` | Only run the scope builder: **create missing** `workflows/.../key_extraction_aliasing.*.WorkflowTrigger.yaml` (and Workflow/WorkflowVersion per `scope_build_mode`) from `default.config.yaml` (does not overwrite existing). Forwards `--check-workflow-triggers`, `--dry-run`, `--hierarchy`, etc. Does **not** run the pipeline. | off |
| `--limit` | Max instances per view; `0` = all | `0` |
| `--verbose` | Verbose logging | false |
| `--dry-run` | Skip alias persistence to CDF | false |
| `--write-foreign-keys` | Persist FK reference strings (needs write-back property) | false |
| `--foreign-key-writeback-property` | DM property for FK strings | none |
| `--instance-space` | Keep `source_views` whose `instance_space` matches **or** whose `filters` constrain node `space` (`property_scope: node`, `EQUALS`/`IN`) | all |
| `--scope` | Only `default` supported without `--config-path` (loads module-root `workflow.local.config.yaml`) | `default` |
| `--config-path` | v1 scope document (overrides `--scope`) | none |
| `--clean-state` | Drop incremental RAW tables from the scope (key extraction state, reference index, aliasing RAW), then run the pipeline | false |
| `--clean-state-only` | Same RAW drops as `--clean-state`, then exit (no pipeline) | false |

**RAW clean does not** remove alias or FK values already written on data-model instances. For incremental runs, a typical full reprocess is `--clean-state --full-rescan`.

**Outputs:** timestamped JSON under `tests/results/` (`*_cdf_extraction.json`, `*_cdf_aliasing.json`). Without `--dry-run`, aliases are written to **`cdf_cdm:CogniteDescribable:v1`** (property configurable; default `aliases`). Optional FK strings use the configured FK write-back property.

**Write-back details:** [Configuration guide — Aliasing parameters](docs/guides/configuration_guide.md) and [workflows README](workflows/README.md).

## Incremental cohort processing (RAW)

When `parameters.incremental_change_processing` is true, `fn_dm_incremental_state_update` selects instances whose `node.lastUpdatedTime` is above a per-scope high watermark (stored in `raw_table_key`) and writes cohort entity rows with `WORKFLOW_STATUS=detected` for the downstream key-extraction step.

**`parameters.incremental_skip_unchanged_source_inputs`** (default `true`): when enabled together with incremental processing, detection computes a SHA-256 digest (`EXTRACTION_INPUTS_HASH`) of the same source fields and preprocessing as key extraction, plus a fingerprint of `extraction_rules`. If it matches the latest hash stored on a completed entity row for that node and scope (`WORKFLOW_STATUS` in `extracted`, `aliased`, or `persisted`), no new cohort row is emitted for that instance; **watermarks still advance** from `lastUpdatedTime` so unchanged noise updates do not re-list the same instances forever. `fn_dm_key_extraction` writes `EXTRACTION_INPUTS_HASH` on incremental entity rows when both flags are enabled. The `+1 ms` bound on `lastUpdatedTime` filters is unchanged (boundary semantics). `full_rescan=true` still emits cohort rows for all matched instances regardless of prior hash/state.

## Alias write-back

- **Workflow:** `aliasWritebackProperty` / `alias_writeback_property` on `fn_dm_alias_persistence` task `data`.
- **`main.py` / scope YAML:** `aliasing.config.parameters.alias_writeback_property` in the loaded scope document.
- **Default property name on CogniteDescribable:** `aliases`.

See [Configuration guide](docs/guides/configuration_guide.md) (Aliasing `parameters`).

## Foreign key write-back

Enable with workflow flags, `main.py` / YAML, or env; set `foreign_key_writeback_property` when enabled. Requires that property on your target view. See [Configuration guide](docs/guides/configuration_guide.md) and [fn_dm_alias_persistence README](functions/fn_dm_alias_persistence/README.md).

## Python API

### Key extraction engine (YAML config)

```python
from pathlib import Path

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    load_config_from_yaml,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)

repo_root = Path.cwd()
config_path = (
    repo_root
    / "modules/accelerators/contextualization/cdf_key_extraction_aliasing/config/examples/key_extraction/comprehensive_default.key_extraction_aliasing.yaml"
)

config_dict = load_config_from_yaml(str(config_path))
engine = KeyExtractionEngine(config_dict)

entity = {"name": "P-101", "description": "Main pump", "id": "asset-1"}
result = engine.extract_keys(entity, "asset")

print(f"Candidate keys: {[k.value for k in result.candidate_keys]}")
```

`extract_keys(entity_dict, entity_type)` matches the shape used by `main.py` and the CDF function (`asset`, `file`, `timeseries`, etc.).

### Aliasing engine

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)

aliasing_engine = AliasingEngine({"rules": [], "validation": {}})
out = aliasing_engine.generate_aliases("P-101", "asset")
print(out.aliases)
```

Production rules come from `workflow.local.config.yaml`, `--config-path`, or an example `*.key_extraction_aliasing.yaml` (e.g. `config/examples/aliasing/aliasing_default.key_extraction_aliasing.yaml`). For **`alias_mapping_table`** rules that load **RAW**, construct `AliasingEngine(..., client=cognite_client)`. Details: [Configuration guide](docs/guides/configuration_guide.md).

### Inline rule config (no YAML file)

```python
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import KeyExtractionEngine
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import AliasingEngine

extraction_config = {
    "extraction_rules": [{
        "name": "pump_tags", "method": "regex", "extraction_type": "candidate_key",
        "priority": 50, "enabled": True,
        "source_fields": [{"field_name": "name", "required": True}],
        "parameters": {"pattern": r"P[-_]?\d{1,6}[A-Z]?", "max_matches_per_field": 10},
    }],
    "validation": {"min_confidence": 0.5, "max_keys_per_type": 10}
}
extraction_engine = KeyExtractionEngine(extraction_config)
aliasing_engine = AliasingEngine({"rules": [], "validation": {}})

sample_data = {"id": "001", "name": "P-10001", "description": "Feed pump"}
result = extraction_engine.extract_keys(sample_data, "asset")
for key in result.candidate_keys:
    alias_result = aliasing_engine.generate_aliases(key.value, "asset")
    print(f"{key.value}: {alias_result.aliases}")
```

### Config utilities

- **YAML → key-extraction engine dict:** `load_config_from_yaml` in `functions/fn_dm_key_extraction/cdf_adapter.py` (also noted under [Configuration details](./README.md#configuration-details)).
- **Typed / schema config (tests, tooling):** `config/configuration_manager.py` — not used by `fn_dm_*` handlers at runtime; see *Configuration entry points* above.

## Repository layout (this module)

```
cdf_key_extraction_aliasing/
├── main.py
├── workflow.local.config.yaml   # default v1 scope document (local CLI)
├── default.config.yaml     # module defaults + scope hierarchy for build_scopes.py
├── config/                 # tag_patterns.yaml, examples, configuration_manager — see config/README.md
├── functions/
│   ├── fn_dm_key_extraction/
│   ├── fn_dm_aliasing/
│   └── fn_dm_alias_persistence/
├── workflow_template/      # Authoring templates + diagram — see workflow_template/README.md
├── workflows/              # Generated Workflow YAML (--build) — see workflows/README.md
├── scripts/
├── tests/
└── docs/                   # see docs/README.md
```

| Path | Role |
|------|------|
| `workflow.local.config.yaml` | Default v1 scope document at module root (local CLI) |
| `config/examples/` | Demos and reference YAML — [config/examples/README.md](config/examples/README.md) |
| `functions/fn_dm_*` | CDF functions + engines (`fn_dm_reference_index` = RAW reference index) |
| `workflow_template/` | `workflow.template.Workflow*.yaml`, `workflow.template.WorkflowTrigger.yaml`, `workflow.template.config.yaml`, diagram |
| `workflows/` | Generated Workflow / WorkflowVersion / WorkflowTrigger YAML |
| `tests/` | Pytest suite and `tests/results/` for JSON artifacts |
| `docs/` | Specs, guides, report |

## Architecture (short)

- **KeyExtractionEngine** — rules → handlers → `ExtractionResult`
- **AliasingEngine** — rules → transformers → `AliasingResult`
- **Alias persistence** — `fn_dm_alias_persistence` / `main.py` path applies aliases to CogniteDescribable (and optional FK property)
- **Workflow** — extraction → aliasing → persistence with RAW between steps — [workflows/README.md](workflows/README.md)

## Configuration details

Authoring and filters (`source_views[].filters`), validation, and field selection: **[docs/guides/configuration_guide.md](docs/guides/configuration_guide.md)** and **[config/README.md](config/README.md)**.

Loading in code: `load_config_from_yaml` in `functions/fn_dm_key_extraction/cdf_adapter.py`.

## CDF deployment

Deploy workflow **`key_extraction_aliasing`** (v4) once; use per-scope **`workflows/.../key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (embedded **`configuration`**). Creating or refreshing triggers: [Local runs (main.py)](#local-runs-mainpy) and [workflows/README.md](workflows/README.md).

## Testing

From **repository root**:

```bash
PYTHONPATH=. python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests -q
```

See [tests/README.md](tests/README.md).

## Troubleshooting

- **Imports:** Run from repo root with `PYTHONPATH=.` including the `library` root (the parent of `modules/`).
- **No keys / no aliases:** Rules, `min_confidence`, `source_views`, and aliasing `enabled` flags — [docs/troubleshooting/common_issues.md](docs/troubleshooting/common_issues.md)
- **Workflow / RAW:** [workflows/README.md](workflows/README.md)

## Contributing

Add tests under `tests/` and update [docs/README.md](docs/README.md) or the relevant guide if you introduce new entry points or config shapes.

## License

This repository is licensed under the Apache License 2.0 — see [`LICENSE`](../../../../LICENSE) at the repository root.

## Authors

- Darren Downtain
