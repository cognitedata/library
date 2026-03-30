# Configuration layout

Configs for this module are **not** read from disk by CDF at runtime. They are the **authoring source** for workflow task payloads (`parameters.function.data.config`) and for **`main.py`** / **`local_runner`**.

## Directories

| Path | Purpose |
|------|---------|
| **`scopes/<name>/key_extraction_aliasing.yaml`** | One **combined v1** document per scope: `key_extraction` (required) and optional `aliasing`. Default for local runs (`--scope` or `config/scopes/default/key_extraction_aliasing.yaml`). |
| **`examples/`** | Demos, migrated **split** `*key_extraction*.config.yaml` / `*aliasing*.config.yaml`, paired `.ExtractionPipeline.yaml`, `config_example_complete.yaml`, and **`LEGACY_TO_NEW_*.md`**. Not merged into production scopes by default. |

Python package code in this folder (`configuration_manager.py`, etc.) lives beside these data directories.

## Scope hierarchy builder

Authoring file **[`scope_hierarchy.yaml`](../scope_hierarchy.yaml)** (module root) declares `scope_hierarchy.levels` and a nested `locations` tree. Run **`scripts/build_scopes.py`** to validate the tree and create `config/scopes/<leaf_scope_id>/key_extraction_aliasing.yaml` from the default template for each leaf (skipped if that file already exists). Use `--dry-run`, `--list-builders`, `--only <name>`, and `--template` / `-f` as needed. Additional artifact types are added by implementing a builder and registering it in **`scripts/scope_build/registry.py`**.

## Combined scope YAML (v1)

- **`key_extraction`**: `externalId` + `config` with `parameters` and `data` — same shape as a standalone key-extraction `*.config.yaml` root.
- **`aliasing`** (optional): same for aliasing. If omitted, or `aliasing_rules` is missing or empty, the local loader uses **identity passthrough** (zero rules).
- **`extraction_rules`**: If omitted or `[]`, the loader injects **passthrough-on-`name`** rules, one per distinct `entity_type` in `source_views` (after defaulting empty `source_views` to CogniteAsset).

Optional: `schemaVersion: 1`, `scope: { name, description }` (informational).

## Local CLI (`main.py`)

| Flag | Behavior |
|------|----------|
| **`--config-path <file>`** | Load that file as v1 combined YAML (highest precedence). |
| **`--scope <name>`** | Load `config/scopes/<name>/key_extraction_aliasing.yaml` (default scope name is `default` when omitted). |
| Env **`CDF_KEY_EXTRACTION_LOCAL_CONFIG_MODE`** | Values like `merge`, `1`, `true` force **legacy merge**: glob only `config/examples/*key_extraction*.config.yaml` and `config/examples/*aliasing*.config.yaml`. |

If no `--config-path`, merge env is off, and `config/scopes/default/key_extraction_aliasing.yaml` is missing, the runner falls back to the same **legacy merge** under `config/examples/` and logs a warning.

## Workflows

Workflow YAML still carries **inline** `config` under each function task. Keep it aligned with the matching `key_extraction_aliasing.yaml` (copy, script, or comment pointing at the repo path).

## Reference docs

- Examples and narrative: `examples/PIPELINE_CONFIGURATIONS_README.md`
- Legacy mapping notes: `examples/LEGACY_TO_NEW_*.md`
