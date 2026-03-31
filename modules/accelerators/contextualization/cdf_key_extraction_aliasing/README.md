# Key Extraction and Aliasing

Library for extracting candidate keys and foreign-key references from entity metadata, and generating aliases for matching. It targets **Cognite Data Fusion (CDF)** (data model views, functions, workflows) and can run standalone in Python.

**Full documentation index:** [docs/README.md](docs/README.md)

## Results report

[docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md) describes the **current default scope** and behavior (maintained with the module).

To **overwrite** that markdown with statistics from the newest `tests/results/*_cdf_extraction.json` (older style, run-specific tables), run:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/scripts/generate_report.py
```

## Capabilities

- **Extraction methods:** passthrough (default when `method` is omitted), regex, fixed width, token reassembly, heuristic
- **Extraction types:** candidate keys, foreign key references, document references
- **Aliasing transformation types:** character substitution, prefix/suffix, regex, case, equipment type expansion, related instruments, hierarchical expansion, document aliases, leading zero normalization, pattern recognition, pattern-based expansion, composite, **`alias_mapping_table`** (RAW-backed tag→alias catalog when configured)
- **Config:** v1 scope documents under `config/scopes/` and examples under `config/examples/` (see [config/README.md](config/README.md))
- **Tests:** `tests/` — see [tests/README.md](tests/README.md)

**Reference tables** for each extraction method and aliasing `type` value: [Key extraction spec](docs/specifications/1.%20key_extraction.md), [Aliasing spec](docs/specifications/2.%20aliasing.md).

**Default CDM scope** (asset + file + timeseries, shared `alphanumeric_tag`): [`config/scopes/default/key_extraction_aliasing.yaml`](config/scopes/default/key_extraction_aliasing.yaml). Narrative summary: [docs/key_extraction_aliasing_report.md](docs/key_extraction_aliasing_report.md). Deployed workflow YAML may inline different rules; keep it aligned with that scope when you intend the same behavior.

### Configuration entry points

- **Deployed workflows** pass `config` in each function’s task payload. Authoring source in-repo: **`config/scopes/<scope>/key_extraction_aliasing.yaml`** and **`config/examples/**`**. Validation: Pydantic in `functions/fn_dm_key_extraction/config.py`, `functions/fn_dm_aliasing/config.py`, and the `cdf_adapter` modules.
- **`config/configuration_manager.py`:** dataclasses + JSON Schema for integration-style tests (`tests/integration/contextualization/`). **Not** used by `fn_dm_*` handlers at runtime.

**Multi-site scopes:** define [scope_hierarchy.yaml](scope_hierarchy.yaml) and run **`scripts/build_scopes.py`**. Details: [config/README.md](config/README.md) (*Scope hierarchy builder*), [scripts/scope_build/registry.py](scripts/scope_build/registry.py).

## Roadmap

- [ ] State store for targets in CDM (reduce RAW-only patterns where applicable)
- [ ] Richer foreign-key / document-reference storage beyond optional Describable write-back
- [ ] Finer default rules per `entity_type`
- [ ] Broader non-ISA tag testing
- [ ] Reference-catalog reverse lookup and relationships

## Quick start (`main.py`)

From **repository root** (the directory that contains `modules/`):

**Environment:** Python 3.11+, dependencies available on your `PYTHONPATH` or virtualenv. Set CDF variables (e.g. `CDF_PROJECT`, `CDF_CLUSTER`, API key or OAuth) when not using `--dry-run`.

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --dry-run
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --limit 50 --verbose
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --scope default
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/main.py --config-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/config/scopes/default/key_extraction_aliasing.yaml
```

If your project uses Poetry at repo root, prefix with `poetry run` as usual.

| Option | Description | Default |
|--------|-------------|---------|
| `--limit` | Max instances per view; `0` = all | `0` |
| `--verbose` | Verbose logging | false |
| `--dry-run` | Skip alias persistence to CDF | false |
| `--write-foreign-keys` | Persist FK reference strings (needs write-back property) | false |
| `--foreign-key-writeback-property` | DM property for FK strings | none |
| `--instance-space` | Keep `source_views` whose `instance_space` matches **or** whose `filters` constrain node `space` (`property_scope: node`, `EQUALS`/`IN`) | all |
| `--scope` | Load `config/scopes/<scope>/key_extraction_aliasing.yaml` | `default` |
| `--config-path` | v1 scope document (overrides `--scope`) | none |

**Outputs:** timestamped JSON under `tests/results/` (`*_cdf_extraction.json`, `*_cdf_aliasing.json`). Without `--dry-run`, aliases are written to **`cdf_cdm:CogniteDescribable:v1`** (property configurable; default `aliases`). Optional FK strings use the configured FK write-back property.

**Write-back details:** [Configuration guide — Aliasing parameters](docs/guides/configuration_guide.md) and [workflows README](workflows/README.md).

## Incremental cohort processing (RAW)

When `parameters.incremental_change_processing` is true, `fn_dm_incremental_state_update` selects instances whose `node.lastUpdatedTime` is above a per-scope high watermark (stored in `raw_table_key`) and writes cohort entity rows with `WORKFLOW_STATUS=detected` for the downstream key-extraction step.

**`parameters.incremental_skip_unchanged_source_inputs`** (default `true`): when enabled together with incremental processing, detection computes a SHA-256 digest (`EXTRACTION_INPUTS_HASH`) of the same source fields and preprocessing as key extraction, plus a fingerprint of `extraction_rules`. If it matches the latest hash stored on a completed entity row for that node and scope (`WORKFLOW_STATUS` in `extracted`, `aliased`, or `persisted`), no new cohort row is emitted for that instance; **watermarks still advance** from `lastUpdatedTime` so unchanged noise updates do not re-list the same instances forever. `fn_dm_key_extraction` writes `EXTRACTION_INPUTS_HASH` on incremental entity rows when both flags are enabled. The `+1 ms` bound on `lastUpdatedTime` filters is unchanged (boundary semantics). `process_all=true` still emits cohort rows for all matched instances regardless of prior hash/state.

## Alias write-back

- **Workflow:** `aliasWritebackProperty` / `alias_writeback_property` on `fn_dm_alias_persistence` task `data`.
- **`main.py` / scope YAML:** `aliasing.config.parameters.alias_writeback_property` in the loaded scope document.
- **Default property name on CogniteDescribable:** `aliases`.

See [Configuration guide](docs/guides/configuration_guide.md) (Aliasing `parameters`).

## Foreign key write-back

Enable with workflow flags, `main.py` / YAML, or env; set `foreign_key_writeback_property` when enabled. Requires that property on your target view. See [Configuration guide](docs/guides/configuration_guide.md) and [fn_dm_alias_persistence README](functions/fn_dm_alias_persistence/README.md).

## Python API (minimal)

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

# For alias_mapping_table rules that read RAW, pass client= to AliasingEngine — see configuration_guide.md

sample_data = {"id": "001", "name": "P-10001", "description": "Feed pump"}
result = extraction_engine.extract_keys(sample_data, "asset")
for key in result.candidate_keys:
    alias_result = aliasing_engine.generate_aliases(key.value, "asset")
    print(f"{key.value}: {alias_result.aliases}")
```

More examples: [docs/guides/quick_start.md](docs/guides/quick_start.md).

## Repository layout (this module)

```
cdf_key_extraction_aliasing/
├── main.py
├── scope_hierarchy.yaml
├── config/                 # scopes, tag_patterns.yaml, examples, configuration_manager — see config/README.md
├── functions/
│   ├── fn_dm_key_extraction/
│   ├── fn_dm_aliasing/
│   └── fn_dm_alias_persistence/
├── workflows/              # CDF Workflow YAML — see workflows/README.md
├── scripts/
├── tests/
└── docs/                   # see docs/README.md
```

| Path | Role |
|------|------|
| `config/scopes/` | v1 scope document per scope (`key_extraction_aliasing.yaml`) |
| `config/examples/` | Demos and reference YAML — [config/examples/README.md](config/examples/README.md) |
| `functions/fn_dm_*` | CDF functions + engines |
| `workflows/` | Workflow manifests and diagram |
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

Workflow **`cdf_key_extraction_aliasing`** chains the three functions. Deploy using your CDF Toolkit / Fusion module layout (workflows, functions, data sets as defined in your package).

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

[Add license if applicable]

## Authors

- Darren Downtain
