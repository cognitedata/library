# How to build configuration with YAML

This guide walks through **authoring and maintaining** the key discovery and aliasing configuration **using files only** (editor, git, CI). The same YAML shapes are what the **operator UI** reads and writes; see [How to build configuration with the UI](howto_config_ui.md) for the visual workflow.

**Prerequisites:** [Quickstart — local `module.py`](howto_quickstart.md) (repository root, `PYTHONPATH=.`, `.env` for CDF). **Field reference:** [Configuration guide](configuration_guide.md), [config/README.md](../../config/README.md), [config/examples/README.md](../../config/examples/README.md).

---

## 1. Know the three layers

| File (module root) | Role |
| -------------------- | ---- |
| **`workflow.local.config.yaml`** | **v1 scope document** for local `module.py run` when you use `--scope default` or pass this path as `--config-path`. Root keys: `source_views`, `key_extraction`, optional `aliasing`, optional `scope`. Same logical shape as **`workflow.input.configuration`** on deployed workflows. |
| **`default.config.yaml`** | **Module and hierarchy defaults**: Toolkit placeholders, schedules, and **`aliasing_scope_hierarchy`** (multi-site tree). Consumed by **`module.py build`** / `scripts/build_scopes.py`, not substituted wholesale into `module.py run` unless you merge by hand. |
| **`workflow_template/workflow.template.config.yaml`** | **Template** for **`input.configuration`** patched into each generated **`WorkflowTrigger`**. Keep it aligned with your intended deployed scope (often a copy of a tuned `workflow.local.config.yaml`). |

**Examples** under **`config/examples/`** are valid v1 scope documents for learning and tests; point **`--config-path`** at any of them.

---

## 2. Author or copy a v1 scope document

1. **Start from the committed default:** `workflow.local.config.yaml` (CDM-oriented asset / file / timeseries scope), **or** copy an example such as `config/examples/key_extraction/…` or `config/examples/aliasing/…` (see [config/examples README](../../config/examples/README.md)).
2. **Edit** top-level **`source_views`** (which DM views and filters), then **`key_extraction.config`** (`parameters`, `data.validation`, `data.extraction_rules`), then **`aliasing.config`** if you need transforms beyond identity passthrough.
3. **Validate mentally** against [Configuration guide](configuration_guide.md): incremental parameters (`incremental_change_processing`, Key Discovery spaces, `run_all` inside **`key_extraction.config.parameters`** for workflow semantics), RAW table names, alias write-back property.

For **incremental** runs, local **`module.py run`** follows **workflow parity** (state update → extraction → …) when `incremental_change_processing` is true; **`run_all: true`** in YAML (or **`module.py run --all`**) forces processing of the full filtered instance set—see [module README — Incremental cohort processing](../../README.md#incremental-cohort-processing-raw-cohort-cdm-state).

---

## 3. (Optional) Multi-site: hierarchy in `default.config.yaml`

If you need **one WorkflowTrigger per leaf** (per site / line / area):

1. Edit **`aliasing_scope_hierarchy`** in **`default.config.yaml`**: **`levels`** (tier labels) and **`locations`** (nested scope nodes with stable **`id`**).
2. Run **`module.py build`** to create missing manifests under **`workflows/`** (and **`module.py build --force`** after template edits to overwrite triggers). See [Scoped deployment](howto_scoped_deployment.md) and [config/README.md](../../config/README.md#scope-hierarchy-builder).
3. Keep **`workflow_template/workflow.template.config.yaml`** in sync with the scope you want embedded in new triggers (copy from your tuned **`workflow.local.config.yaml`** when appropriate).

---

## 4. Align the workflow template (before build / deploy)

Generated triggers patch from **`workflow.template.config.yaml`**. After you finalize rules in **`workflow.local.config.yaml`**, **copy** that file to **`workflow_template/workflow.template.config.yaml`** (or merge the sections you care about) so the next **`module.py build`** seeds correct **`input.configuration`**.

---

## 5. Generate workflow YAML

From **repository root** with `PYTHONPATH=.`:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py build
```

Optional: **`--check-workflow-triggers`** for CI, **`build --clean`** to remove generated workflow files (see [module README — Local runs](../../README.md#local-runs-modulepy)).

---

## 6. Run the pipeline locally

```bash
# Safe: no DM write-back
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --dry-run

# Default scope file
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --scope default

# Any v1 scope file
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run \
  --config-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflow.local.config.yaml
```

With **incremental** config, add **`--all`** to mirror **`run_all: true`** (process entire scope per filters). Outputs land under **`tests/results/`** as `*_cdf_extraction.json` and `*_cdf_aliasing.json`.

---

## 7. Deploy to CDF

Use Cognite Toolkit with the generated **`workflows/`** manifests: **`cdf build`** / **`cdf deploy`**. Details: [Scoped deployment](howto_scoped_deployment.md), [workflows README](../../workflows/README.md).

---

## Related links

| Topic | Document |
| ----- | -------- |
| Default scope narrative | [Key extraction / aliasing report](../key_extraction_aliasing_report.md) |
| End-to-end behavior | [Module functional document](../module_functional_document.md) |
| UI-based editing | [How to build configuration with the UI](howto_config_ui.md) |
