# Configuration layout

**Documentation index:** [docs/README.md](../docs/README.md) (maps specs, guides, examples, and workflows).

Configs for this module are the **authoring source** for **`main.py`** / **`local_runner`** and for **CDF workflow** payloads. CDF functions receive **`workflow.input.scope_document`** (v1 scope mapping) from schedule triggers built by **`scripts/build_scopes.py`** — scope YAML is **not** uploaded as Cognite File for this pipeline.

## Directories

| Path | Purpose |
|------|---------|
| **`tag_patterns.yaml`** | **Shared** tag + document regex library for **aliasing** (`tag_pattern_library`) and **authoring alignment** for key-extraction rules. Loaded via [`tag_patterns_paths.py`](tag_patterns_paths.py). |
| **`examples/`** | **`key_extraction/`** and **`aliasing/`** example scope YAML (`*.key_extraction_aliasing.yaml`); **`reference/`** for complete YAML + migration notes (`LEGACY_TO_NEW_*.md`). Not used automatically by the local runner or CDF functions. |

**Local default v1 scope file:** [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at the **module root** (used when `--scope default` and no `--config-path`).

**Local incremental (workflow parity):** When `key_extraction.config.parameters.incremental_change_processing` is true, `local_runner/run.py` merges the filtered `source_views` into a v1 dict and passes **`scope_document`** plus **`instance_space`** on each step’s task payload, matching workflow v4 task inputs (see [`local_runner/workflow_payload.py`](../local_runner/workflow_payload.py)).

**Trigger-embedded template:** [`../workflows/_template/workflow.template.config.yaml`](../workflows/_template/workflow.template.config.yaml) — copied into each generated trigger’s **`input.scope_document`**, patched per hierarchy leaf (external ids, node `space` filters, `scope` block).

Python package code in this folder (`configuration_manager.py`, etc.) lives beside these data directories.

## Scope hierarchy builder

**[`default.config.yaml`](../default.config.yaml)** (module root) includes top-level **`scope_hierarchy`**: **`levels`** (tier labels) and **`locations`** (root node list; each node nests children under **`locations`**). **Leaves may be at any depth** — you do not need a full path for every declared level name; `levels` labels the first segments only, and tiers beyond that list get synthetic names (`level_3`, …) in build metadata. Run **`python main.py --build`** or **`scripts/build_scopes.py`** to validate the tree and **create missing** **`workflows/key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (one file per leaf that does not already exist; override with **`--scope-document`** / **`--workflow-trigger-template`**). Existing trigger files are not overwritten—delete a file to recreate it. Each file is one schedule trigger with **`input.scope_document`** built from **`workflows/_template/workflow.template.config.yaml`**. The **`workflow_triggers`** builder prepends a **node `space`** filter on each `source_views` row unless one exists; filter value is the leaf **`instance_space`** when set, else the toolkit placeholder **`{{instance_space}}`** (substituted at deploy from **`default.config.yaml`**). Use **`--check-workflow-triggers`** to fail CI if any trigger required by the current hierarchy is missing or out of date (extra **`key_extraction_aliasing.*.WorkflowTrigger.yaml`** files on disk are ignored; **`--build`** does not delete them). **`--workflow-trigger-template`** overrides the trigger shell. Use `--dry-run`, `--list-builders`, `--only workflow_triggers`, and `--hierarchy` as needed.

## Scope YAML (v1)

- **`key_extraction`**: `externalId` + `config` with `parameters` and `data` — same shape as a standalone key-extraction `*.config.yaml` root.
- **`aliasing`** (optional): same for aliasing. If omitted, or `aliasing_rules` is missing or empty, the local loader uses **identity passthrough** (zero rules).
- **`extraction_rules`**: If omitted or `[]`, the loader injects **passthrough-on-`name`** rules, one per distinct `entity_type` in `source_views` (after defaulting empty `source_views` to CogniteAsset).

Optional: `schemaVersion: 1`, `scope: { name, description }` (informational; **`scope`** is injected into trigger payloads by `build_scopes`).

## Local CLI (`main.py`)

| Flag | Behavior |
|------|----------|
| **`--config-path <file>`** | Load that file as a v1 scope document (highest precedence). |
| **`--scope <name>`** | Only **`default`** (or omitted) loads **`workflow.local.config.yaml`** at module root; other names require **`--config-path`**. |

## Workflows

Workflow **v4** ([`workflows/key_extraction_aliasing.WorkflowVersion.yaml`](../workflows/key_extraction_aliasing.WorkflowVersion.yaml)) passes **`scope_document`** on **`workflow.input`** into each function task. RAW table keys remain in **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside that object. See [`workflows/README.md`](../workflows/README.md).

## Reference docs

- Examples and narrative: [`examples/README.md`](examples/README.md)
- Legacy mapping notes: `examples/reference/LEGACY_TO_NEW_*.md`
