# Configuration layout

**Documentation index:** [docs/README.md](../docs/README.md) (maps specs, guides, examples, and workflows).

Configs for this module are **not** read from disk by CDF at runtime. They are the **authoring source** for workflow task payloads (`parameters.function.data.config`) and for **`main.py`** / **`local_runner`**.

## Directories

| Path | Purpose |
|------|---------|
| **`scopes/<name>/key_extraction_aliasing.yaml`** | **v1 scope document** per scope: required `key_extraction`, optional `aliasing` — single authoring shape aligned with `main.py` and workflows (not two independent configs). Default for local runs (`--scope` or `config/scopes/default/key_extraction_aliasing.yaml`). |
| **`tag_patterns.yaml`** | **Shared** tag + document regex library for **aliasing** (`tag_pattern_library`) and **authoring alignment** for key-extraction rules (e.g. `alphanumeric_tag` ↔ default scope `&alphanumeric_tag`). Loaded via [`tag_patterns_paths.py`](tag_patterns_paths.py). |
| **`examples/`** | **`key_extraction/`** and **`aliasing/`** example scope YAML (`*.key_extraction_aliasing.yaml`); **`reference/`** for complete YAML + migration notes (`LEGACY_TO_NEW_*.md`). Not used automatically by the local runner or CDF functions. |

Python package code in this folder (`configuration_manager.py`, etc.) lives beside these data directories.

## Scope hierarchy builder

Authoring file **[`scope_hierarchy.yaml`](../scope_hierarchy.yaml)** (module root) declares `scope_hierarchy.levels` and a nested `locations` tree. Run **`scripts/build_scopes.py`** to validate the tree and create `config/scopes/<leaf_scope_id>/key_extraction_aliasing.yaml` from the default template for each leaf (skipped if that file already exists). The **`key_extraction_aliasing`** builder prepends a default **node `space`** filter (`property_scope: node`, `target_property: space`, `EQUALS`) on **each** `source_views` row copied from the template, unless that view already has such a filter. The filter value is the leaf node's **`instance_space`** when set in the hierarchy; otherwise a placeholder `PLACEHOLDER_INSTANCE_SPACE_FOR_SCOPE__<suffix>` derived from the scope id (replace with your CDF instance space). Use `--dry-run`, `--list-builders`, `--only <name>`, and `--template` / `-f` as needed. Additional artifact types are added by implementing a builder and registering it in **`scripts/scope_build/registry.py`**.

## Scope YAML (v1)

- **`key_extraction`**: `externalId` + `config` with `parameters` and `data` — same shape as a standalone key-extraction `*.config.yaml` root.
- **`aliasing`** (optional): same for aliasing. If omitted, or `aliasing_rules` is missing or empty, the local loader uses **identity passthrough** (zero rules).
- **`extraction_rules`**: If omitted or `[]`, the loader injects **passthrough-on-`name`** rules, one per distinct `entity_type` in `source_views` (after defaulting empty `source_views` to CogniteAsset).

Optional: `schemaVersion: 1`, `scope: { name, description }` (informational).

## Local CLI (`main.py`)

| Flag | Behavior |
|------|----------|
| **`--config-path <file>`** | Load that file as a v1 scope document (highest precedence). |
| **`--scope <name>`** | Load `config/scopes/<name>/key_extraction_aliasing.yaml` (default scope name is `default` when omitted). |

If `--config-path` is omitted and `config/scopes/<scope>/key_extraction_aliasing.yaml` is missing, **`load_configs` raises `FileNotFoundError`**. Create that file (for example with **`scripts/build_scopes.py`**) or pass **`--config-path`**.

## Workflows

Workflow YAML still carries **inline** `config` under each function task. Keep it aligned with the matching `key_extraction_aliasing.yaml` (copy, script, or comment pointing at the repo path).

## Reference docs

- Examples and narrative: [`examples/README.md`](examples/README.md)
- Legacy mapping notes: `examples/reference/LEGACY_TO_NEW_*.md`
