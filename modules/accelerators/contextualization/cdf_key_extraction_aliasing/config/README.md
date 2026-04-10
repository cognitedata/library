# Configuration layout

**Documentation index:** [docs/README.md](../docs/README.md) (maps specs, guides, examples, and workflows). **Scoped build and deploy walkthrough:** [Scoped deployment how-to](../docs/guides/howto_scoped_deployment.md).

Configs for this module are the **authoring source** for **`module.py`** / **`local_runner`** and for **CDF workflow** payloads. CDF functions receive **`workflow.input.configuration`** (v1 scope mapping) from schedule triggers built by **`scripts/build_scopes.py`** — scope YAML is **not** uploaded as Cognite File for this pipeline.

## Directories

| Path | Purpose |
|------|---------|
| **`tag_patterns.yaml`** | **Shared** tag + document regex library for **aliasing** (`tag_pattern_library`) and **authoring alignment** for key-extraction rules. Loaded via [`tag_patterns_paths.py`](tag_patterns_paths.py). |
| **`examples/`** | **`key_extraction/`** and **`aliasing/`** example scope YAML (`*.key_extraction_aliasing.yaml`); **`reference/`** for complete YAML + migration notes (`LEGACY_TO_NEW_*.md`). Not used automatically by the local runner or CDF functions. |

**Local default v1 scope file:** [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at the **module root** (used when `--scope default` and no `--config-path`).

**Local incremental (workflow parity):** When `key_extraction.config.parameters.incremental_change_processing` is true, `local_runner/run.py` sets top-level `source_views` on the merged v1 scope dict and passes **`configuration`** plus **`instance_space`** on each step’s task payload, matching workflow v4 task inputs (see [`local_runner/workflow_payload.py`](../local_runner/workflow_payload.py)).

**Trigger-embedded template:** [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml) — copied into each generated trigger’s **`input.configuration`**, patched per hierarchy leaf (external ids, node `space` filters, `scope` block).

Python package code in this folder (`configuration_manager.py`, etc.) lives beside these data directories.

## Scope hierarchy builder

**[`default.config.yaml`](../default.config.yaml)** (module root) includes top-level **`scope_hierarchy`**: **`levels`** (tier labels) and **`locations`** (root node list; each node nests children under **`locations`**). **Leaves may be at any depth** — you do not need a full path for every declared level name; `levels` labels the first segments only, and tiers beyond that list get synthetic names (`level_3`, …) in build metadata. Run **`python module.py --build`** or **`scripts/build_scopes.py`** to validate the tree and **create missing** **`workflows/key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (or under **`workflows/<suffix>/`** in **`full`** mode) (one file per leaf that does not already exist; override with **`--scope-document`** / **`--workflow-trigger-template`**). Existing trigger files are not overwritten—delete a file to recreate it, or use **`--build --clean`** to remove generated workflow YAML under **`workflows/`** in bulk (confirmation or **`--yes`**; **`--dry-run --clean`** to preview; no automatic rebuild—run **`--build`** again afterward). Each file is one schedule trigger with **`input.configuration`** built from **`workflow_template/workflow.template.config.yaml`**. The **`workflow_triggers`** builder prepends a **node `space`** filter on each top-level `source_views` row unless one exists; filter value is the leaf **`instance_space`** when set, else the toolkit placeholder **`{{instance_space}}`** (substituted at deploy from **`default.config.yaml`**). Use **`--check-workflow-triggers`** to fail CI if any trigger required by the current hierarchy is missing or out of date (extra **`key_extraction_aliasing.*.WorkflowTrigger.yaml`** files on disk are ignored; a normal **`--build`** does not delete them). **`--workflow-trigger-template`** overrides the trigger shell. Use `--dry-run`, `--list-builders`, `--only workflow_triggers`, and `--hierarchy` as needed.

## Scope YAML (v1)

- **`source_views`**: required non-empty list at the **document root** (sibling of `key_extraction`) — which DM views to query.
- **`key_extraction`**: `externalId` + `config` with `parameters` and `data` (`validation`, `extraction_rules`; no `source_views` here in v1 scope YAML).
- **`aliasing`** (optional): same for aliasing. If omitted, or `aliasing_rules` is missing or empty, the local loader uses **identity passthrough** (zero rules).
- **`extraction_rules`**: If omitted or `[]`, the loader injects **passthrough-on-`name`** rules, one per distinct `entity_type` in top-level `source_views`.

Optional: `schemaVersion: 1`, `scope: { name, description }` (informational; **`scope`** is injected into trigger payloads by `build_scopes`).

## Local CLI (`module.py`)

| Flag | Behavior |
|------|----------|
| **`--config-path <file>`** | Load that file as a v1 scope document (highest precedence). |
| **`--scope <name>`** | Only **`default`** (or omitted) loads **`workflow.local.config.yaml`** at module root; other names require **`--config-path`**. |

## Workflows

Workflow **v4** (generated **`workflows/.../key_extraction_aliasing*.WorkflowVersion.yaml`**) passes **`configuration`** on **`workflow.input`** into each function task. RAW table keys remain in **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside that object. See [`workflows/README.md`](../workflows/README.md).

## Reference docs

- Examples and narrative: [`examples/README.md`](examples/README.md)
- Legacy mapping notes: `examples/reference/LEGACY_TO_NEW_*.md`
