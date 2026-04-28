# Scoped deployment — hierarchy, workflow manifests, and Toolkit

Use this guide when you need **one workflow** (`key_extraction_aliasing`) with **per-scope schedule triggers** (different sites, plants, or instance spaces). You edit the **scope hierarchy** in [`default.config.yaml`](../../default.config.yaml), generate **CDF Toolkit** YAML under [`workflows/`](../../workflows/), optionally adjust **instance space** handling, validate locally with [`module.py`](../../module.py), and deploy from a **Cognite Toolkit** project that includes this module.

**Documentation index:** [docs/README.md](../README.md) · **Workflow task graph:** [workflows/README.md](../../workflows/README.md) · **Quickstart:** [howto_quickstart.md](howto_quickstart.md) · **Config authoring:** [howto_config_yaml.md](howto_config_yaml.md), [howto_config_ui.md](howto_config_ui.md)

## Flow overview

```mermaid
flowchart LR
  defaultConfig[default.config.yaml]
  buildRun["module.py build"]
  workflowsDir[workflows_manifests]
  editTokens[Edit_or_substitute_tokens]
  cdfDeploy["cdf build / cdf deploy"]
  cdf[CDF]

  defaultConfig --> buildRun
  buildRun --> workflowsDir
  workflowsDir --> editTokens
  editTokens --> cdfDeploy
  cdfDeploy --> cdf
```

## 1. Customize the scope hierarchy

Authoring lives in **[`default.config.yaml`](../../default.config.yaml)** at the module root.

- **`aliasing_scope_hierarchy.levels`** — Ordered labels for path tiers (same convention as **`cdf_access_control`** hierarchy dimensions: semantic names like `site`, `unit`, `area`, `system`, not `level_1`, `level_2`, …). You do not have to use every tier; deeper paths can use synthetic tier names when you exceed the list (see [config/README.md](../../config/README.md)).
- **`aliasing_scope_hierarchy.locations`** — Root list of nodes. Each node has a stable **`id`** (used in trigger `externalId` suffixes and `scope_id`). Nest children under another **`locations`** key on each node.
- **Leaves** — A leaf is a node with no child `locations` or **`locations: []`**. Each leaf gets its own scoped trio under **`workflows/<suffix>/`**, including **`key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml`**.
- **Optional `instance_space` on a leaf** — When set, the scope builder can emit **literal** node `space` filters in that leaf’s embedded **`input.configuration`** instead of the Toolkit placeholder `{{instance_space}}`. See *Instance spaces* below.

Commented examples in `default.config.yaml` show how to add sites and nested locations.

## 2. Generated layout under `workflows/`

Per leaf, the builder writes **`workflows/<suffix>/key_extraction_aliasing.<suffix>.Workflow.yaml`**, **`WorkflowVersion.yaml`**, and **`WorkflowTrigger.yaml`**. The legacy **`scope_build_mode: trigger_only`** (flat triggers and root Workflow pair) is **not** supported.

Templates always come from [`workflow_template/`](../../workflow_template/) (see [workflow_template/README.md](../../workflow_template/README.md)).

### Key Discovery data model (incremental state)

Incremental watermark and per-record hash state can live in **FDM** views shipped under [`data_modeling/`](../../data_modeling/) (`KeyDiscoveryScopeCheckpoint`, `KeyDiscoveryProcessingState`). Include those resources in your **Cognite Toolkit** project and deploy them to the same CDF project as the functions. **`workflow_scope`** on each generated trigger is set by scope build (same as **`scope.id`**). If the views are not deployed yet, pipeline code **falls back to RAW** watermarks and hash columns — see [workflows/README.md](../../workflows/README.md) and [configuration guide](configuration_guide.md#incremental-mode-key-discovery-fdm-and-raw-cohort).

## 3. Build commands (`module.py build`)

**`python module.py build`** does **not** connect to CDF. (Legacy: **`python module.py --build`**.) It runs the same orchestrator as [`scripts/build_scopes.py`](../../scripts/build_scopes.py). Forwarded flags include:

| Flag | Purpose |
|------|---------|
| *(none)* | Create missing scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** under **`workflows/<suffix>/`**; refresh **`workflow_template/workflow.execution.graph.yaml`** from IR (does not overwrite existing scoped flow YAML without **`--force`**). |
| **`--force`** | Overwrite **existing** scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** files from templates + IR. (The execution graph file is refreshed on every build without **`--force`**.) |
| **`--hierarchy <path>`** | Use a different hierarchy file instead of module-root `default.config.yaml`. |
| **`--scope-document <path>`** | Patch scope body from another YAML (default: `workflow_template/workflow.template.config.yaml`). |
| **`--workflow-trigger-template <path>`** | Custom trigger shell template. |
| **`--dry-run`** | Log actions without writing. |
| **`--check-workflow-triggers`** | Exit non-zero if required artifacts are missing or out of date vs templates (CI gate). **No writes.** |
| **`--list-builders`** | Print builder names. |
| **`--only <name>`** | Run only named builders (repeatable). |
| **`-v` / `--verbose`** | Debug logging. |
| **`--clean`** | Delete generated workflow YAML under **`workflows/`** for this module’s **`workflow`** id (with confirmation, or **`--yes`**). **`--dry-run --clean`** lists paths only. **No build runs after a successful clean** — run **`module.py build`** again to recreate. |
| **`--yes`** | With **`--clean`**, skip confirmation (needed when stdin is not a TTY). |

**Do not confuse** **`module.py build --clean`** (removes Toolkit manifest files under `workflows/`) with **`module.py run --clean-state`** (drops **RAW** pipeline tables for a scope). They are unrelated.

Example:

```bash
# From repository root
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py build
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py build --check-workflow-triggers
```

## 4. Instance spaces in generated triggers

Deployed workflows read **`workflow.input.configuration`** (v1 scope shape). The builder patches **`source_views`** (including node **`space`** filters) per leaf. You can control instance space in three ways:

**A — Leaf `instance_space` in the hierarchy**  
Set **`instance_space`** on the leaf node in `default.config.yaml`. Run **`module.py build`** (add **`--force`** if the trigger file already exists and must be rewritten). Generated filters can contain the **literal** space string instead of a template token.

**B — Toolkit placeholder `{{instance_space}}`**  
If the leaf has no baked-in space, triggers may keep **`{{instance_space}}`** inside **`input.configuration`** (for example on node `space` filters). **CDF Toolkit** substitutes that value at **`cdf build` / deploy** from your project’s variables (often aligned with keys in `default.config.yaml`). Substitution applies **inside the embedded configuration**, not only on `workflow.input` top-level fields. See [workflows/README.md](../../workflows/README.md).

**C — Manual edits after generation**  
You can edit **`workflows/.../*.WorkflowTrigger.yaml`**; the next plain **`module.py build`** will **not** overwrite an existing trigger—use **`module.py build --force`** to regenerate from **`workflow_template/`**. For durable per-leaf tweaks, change **`default.config.yaml`** / templates (or use **`copy-workflow-config`** between leaves).

Also ensure your Toolkit / project config supplies **`instance_space`** (or equivalent) for every deploy target where triggers still contain `{{instance_space}}`.

## 5. Run locally against one generated scope (trigger parity)

There is **no** CLI flag to point `module.py` directly at a `WorkflowTrigger.yaml` file. To mirror what CDF runs for **one** leaf:

1. Open the leaf’s **`...WorkflowTrigger.yaml`** under **`workflows/`**.
2. Copy the entire **`input.configuration`** mapping (the value of the **`configuration`** key under **`input`**).
3. Save it as a **standalone** v1 scope YAML file (for example `my_leaf.local.yaml`). The document root should match the workflow payload: top-level **`source_views`**, **`key_extraction`**, optional **`aliasing`**, etc.
4. Replace any **Toolkit placeholders** (`{{instance_space}}`, and any others your file contains) with **real** values for your CDF project. Unsubstituted `{{...}}` strings will break YAML parsing or DM queries.
5. Run from repository root with **`PYTHONPATH=.`**:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run \
  --config-path /path/to/my_leaf.local.yaml \
  --dry-run --limit 50
```

Use **`--dry-run`** and **`--limit`** until you are confident. Optional **`--instance-space`** filters views when your file lists multiple views; see [`module.py`](../../module.py).

## 6. Deploy to CDF

### 6a. Workflow shell only (`module.py deploy-scope` — Cognite SDK)

From this module (with the same **`.env` / credentials** as `module.py run`), **`python module.py deploy-scope --scope-suffix <leaf>`** validates the scoped trio under **`workflows/<suffix>/`**, optionally runs **`module.py build --scope-suffix`**, then **upserts** **`Workflow`**, **`WorkflowVersion`**, and **`WorkflowTrigger`** via the **Cognite Python SDK** (no **`cdf build`** / **`cdf deploy`**). It does **not** upload functions, RAW, data sets, or other Toolkit resources — deploy those separately if needed.

- Use **`--dry-run`** to print planned upserts without calling CDF.
- Generated trigger YAML may still contain **`{{…}}`** Toolkit placeholders; the CLI rejects those by default. Pass **`--allow-unresolved-placeholders`** only when you intend to push placeholders and fix them later (CDF may still reject invalid payloads).
- Resolve **`dataSetExternalId`** on the Workflow file to an existing data set in the project.

The operator UI **Run** tab uses the same API path when you click **Deploy**.

### 6b. Full module with Cognite Toolkit (optional)

This **library** repository does not ship a root **`cdf.toml`** or **`fusion.yaml`**. To deploy **functions**, **RAW**, **data models**, and workflows together, add or symlink this module into a **Cognite Toolkit** project.

1. **Artifacts** — Under this module, typical Toolkit resources include **`workflows/`**, **`functions/`**, **`data_modeling/`**, etc.
2. **Build** — From your Toolkit project root, run **`cdf build`** so templates and variables resolve.
3. **Deploy** — Run **`cdf deploy`**. Schedule cron, OAuth placeholders (`{{functionClientId}}`, `{{functionClientSecret}}`), and **`{{instance_space}}`** are resolved from your project’s configuration when you build/deploy, not by `module.py`.

Official Toolkit repository and docs: [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit). For workflow triggers and **`workflow.input`**, see Cognite’s data workflows documentation (linked from [workflows/README.md](../../workflows/README.md)).

## Related reading

- [How to build configuration with YAML](howto_config_yaml.md) — files, template alignment, build/run
- [How to build configuration with the UI](howto_config_ui.md) — edit triggers and run locally
- [config/README.md](../../config/README.md) — hierarchy builder details
- [Configuration guide](configuration_guide.md) — v1 scope shape, `source_views`, parameters
- [Quickstart](howto_quickstart.md) — `.env` and first `module.py` run
- [How to add a custom handler](howto_custom_handlers.md) — when YAML is not enough
