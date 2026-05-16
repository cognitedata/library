## Contextualization workflows

**Documentation index:** [docs/README.md](../docs/README.md).

**`--build` output** (Workflow / WorkflowVersion / WorkflowTrigger YAML for CDF) lives in **`workflows/`** at the module root. Deploy the trio with **`python module.py deploy-scope`** (Cognite SDK) or include them in a Cognite Toolkit **`cdf deploy`**. **Authoring inputs** (`workflow.template.Workflow.yaml`, `workflow.template.WorkflowTrigger.yaml`, **`workflow.template.config.yaml`** — unified v1 scope including embedded **`canvas`**) stay in **[`workflow_template/`](../workflow_template/)** — see [workflow_template/README.md](../workflow_template/README.md). The operator UI persists the graph under **`canvas`** inside the scope YAML. **`build_scopes`** loads the scope template only (root graph keys are normalized into **`canvas`** when present). **`WorkflowVersion.yaml`** under **`workflows/`** is generated at build time from the **canvas DAG** (in-memory **`compiled_workflow` IR** → per-task fields inlined into each function task’s **`data`**; not copied from `workflow.template.WorkflowVersion.yaml`, which is a human reference). The **Kahn execution graph** ([`workflow_template/workflow.execution.graph.yaml`](../workflow_template/workflow.execution.graph.yaml)) is refreshed from that same IR on **every** **`python module.py build`** (first scoped leaf; **no** **`--force`** required). It must match the IR implied by the scope template; validate with **`python scripts/validate_workflow_version_graph.py`** (also enforced by **`python module.py build --check-workflow-triggers`** when verifying workflow bundles).

This module ships a **discovery canvas** workflow (compiled from **`canvas`** to Cognite Function tasks) that typically runs:

1. **`fn_dm_view_query`** (and optional **`fn_dm_raw_query`** / **`fn_dm_classic_query`**) — list scoped instances, write **cohort** rows to the configured discovery RAW table with a per-run **`RUN_ID`** (Key Discovery FDM views under [`data_modeling/`](../data_modeling/) supply incremental watermarks when deployed).
2. **`fn_dm_transform`** — read predecessor RAW cohort payloads, apply transform rules, write transform sink RAW.
3. **`fn_dm_validate`** — run validation / confidence chains where the canvas wires match-definition nodes.
4. **`fn_dm_view_save`** (and optional **`fn_dm_raw_save`** / **`fn_dm_classic_save`**) — apply upstream payloads to the data model or RAW sinks (for example **`cdf_cdm:CogniteDescribable`** alias lists).
5. **`fn_dm_inverted_index`** (optional branch) — build or refresh an inverted index RAW table from predecessor task payloads (see task **`data`** / IR).
6. **`fn_dm_discovery_raw_cleanup`** (optional) — operator-configured post-run RAW cleanup.

Authoring source for rules and views is the v1 scope mapping embedded in each schedule trigger as **`input.configuration`** (template: [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)), patched per leaf by **`scripts/build_scopes.py`**. Local default scope (same v1 shape): [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at module root.

### Workflow: `key_extraction_aliasing` (version `v5`)

Each leaf has its own **scoped** workflow external id **`key_extraction_aliasing.<suffix>`** and matching trio under **`workflows/<suffix>/`** (for example **`workflows/default/key_extraction_aliasing.default.WorkflowTrigger.yaml`**). [`scripts/build_scopes.py`](../scripts/build_scopes.py) or **`python module.py build`** (legacy: **`python module.py --build`**) creates **missing** scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** from **`workflow_template/`**; use **`--force`** to overwrite **existing** copies after you change the scope template (unified config + **`canvas`**). **`workflow.execution.graph.yaml`** is refreshed from IR on every build (no **`--force`**). Each trigger calls the workflow and supplies **`input`** that becomes **`workflow.input`** at run time ([Cognite trigger static input](https://docs.cognite.com/cdf/data_workflows/triggers)). Toolkit placeholders (for example `{{functionClientId}}`, `{{ key_extraction_aliasing_schedule }}`) are resolved at deploy from [`default.config.yaml`](../default.config.yaml). **`{{instance_space}}`** is substituted **inside** each trigger’s embedded **`configuration`** (for example on **`source_views`**), not on **`workflow.input`**.

**Removing generated files:** **`python module.py build --clean`** deletes matching Workflow / WorkflowVersion / WorkflowTrigger YAML under **`workflows/`** (scoped by the hierarchy **`workflow`** id). It shows a summary, warns the change cannot be undone, and asks you to type **`yes`** unless you pass **`--yes`** (needed for non-interactive runs). **`--dry-run --clean`** lists paths only. **No rebuild runs after clean**—run **`module.py build`** again to regenerate. **`workflow_template/`** and **`workflows/README.md`** are not removed. Do not confuse this with **`module.py run --clean-state`**, which clears RAW pipeline tables, not these manifests.

**Configuration:** v5 passes a **trimmed** v1 scope document on **`workflow.input.configuration`** (subgraphs flattened, layout stripped, extraction/aliasing rule lists pruned to rules referenced by executable canvas nodes). There is **no** **`workflow.input.compiled_workflow`**. The **full** unified scope for each leaf is written alongside the trigger as **`workflows/<suffix>/key_extraction_aliasing.<suffix>.config.yaml`** (operator UI source of truth); the trigger embeds only the trimmed copy for CDF size limits. The compiled DAG is used **only at build time** to emit **`workflows/.../*.WorkflowVersion.yaml`**: each task has an **`externalId`** derived from the canvas (`kea__<executor_kind>__<name_slug>`, built from stage **`data.config`** such as **`description`** / **`view_external_id`**, with length limits and collision suffixes). **`task_id`** and **`pipeline_node_id`** match that **`externalId`**; **`canvas_node_id`** keeps the opaque authoring node id from the canvas. Functions resolve **`config`** from **`configuration`**; there is **no** Cognite Files download for scope YAML. **`workflow.input`** supplies optional **`run_id`** and optional **`run_all`**; DM **`instance_space`** for handlers is taken from **`configuration`** (first **`source_views[].instance_space`**, or a single-value node **`space`** filter) unless a task explicitly passes **`instance_space`** on function **`data`**.

Renaming a stage **description** or similar label can change task **`externalId`** on the next **`module.py build`** (new WorkflowVersion tasks). **`canvas_node_id`** is the stable key for correlating compiled tasks with canvas nodes.

For a leaf in [`default.config.yaml`](../default.config.yaml), **`key_extraction.externalId`** / **`aliasing.externalId`** and node **`space`** filters use **`cdf_external_id_suffix(scope_id)`** from [`scripts/scope_build/naming.py`](../scripts/scope_build/naming.py) when **`build_scopes`** patches each trigger’s **`configuration`**.

#### Workflow inputs (v5)

- **`run_all`** (bool, default `false`): when sent on **`workflow.input`**, overrides **`key_extraction.config.parameters.run_all`** after the scope document is applied; query stages use it with cohort / watermark semantics (see configuration guide).
- **`run_id`** (string, optional): reserved for operator/trigger use when wiring task outputs; when unset, downstream tasks may use `incremental_auto_run_id` to discover a single active `RUN_ID` in RAW (single-run deployments only).
- **`configuration`**: v1 scope mapping (`key_extraction`, `aliasing`, optional `scope` metadata, embedded **`canvas`**) — **required** for deployed runs; generated triggers embed the **trimmed** document per leaf (see **Configuration** above). **`instance_space`** for DM/RAW is derived from top-level **`source_views`** in **`configuration`** when not set on task **`data`**.
- **RAW keys** (extraction, aliasing, inverted index): authored under **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`configuration`** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`; optional **`inverted_index_raw_table_key`**, otherwise derived from `raw_table_key` by replacing the `_key_extraction_state` suffix with `_inverted_index`).

#### Discovery stages (canvas order)

- **Query (`fn_dm_view_query`, …)** — **`key_extraction.config.parameters`** supply **`raw_table_key`**, Key Discovery / watermark fields (**`key_discovery_instance_space`**, **`workflow_scope`**, **`incremental_skip_unchanged_source_inputs`**, …), and DM list filters. Writes cohort RAW rows keyed by **`RUN_ID`**, scope, and instance identifiers.
- **Transform (`fn_dm_transform`)** — reads predecessor RAW tables; transform rules live on each canvas node’s **`data.config`** (merged from scope **`key_extraction`** / **`aliasing`** libraries where referenced).
- **Validation (`fn_dm_validate`)** — reads predecessor RAW; match-definition and confidence rules are inlined like other discovery tasks.
- **Save (`fn_dm_view_save`, …)** — applies predecessor payloads to DM (for example **`instances.apply`** on **`cdf_cdm:CogniteDescribable`**). Configure write-back property names on the **`save_view`** task **`data`** / canvas node.
- **Inverted index (`fn_dm_inverted_index`)** — optional; see [`functions/README.md`](../functions/README.md) and [`functions/fn_dm_inverted_index/handler.py`](../functions/fn_dm_inverted_index/handler.py).
- **RAW cleanup (`fn_dm_discovery_raw_cleanup`)** — optional post-run operator cleanup.

### Interpreting persisted aliases

Alias lists are produced by upstream transform/validate stages and applied in **`fn_dm_view_save`**. Multiple upstream rows that reference the same entity are merged according to the save handler’s merge rules.

### Workflow manifests (CDF Toolkit)

Generated **`WorkflowVersion.yaml`** omits **`workflowDefinition.input`**: the CDF workflow API rejects that field, and Cognite Toolkit **`cdf deploy`** sends manifests as authored, so **`workflow.input`** (`run_all`, `run_id`, `configuration`) is supplied only by each **WorkflowTrigger**’s static **`input`**, while function tasks keep **`${workflow.input.*}`** references in **`parameters.function.data`**.

| File | Role |
|------|------|
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.Workflow.yaml`** | Scoped workflow container (`externalId` = `key_extraction_aliasing.<suffix>`). |
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.WorkflowVersion.yaml`** | Version **`v5`**. Each task’s **`data`** includes **`${workflow.input.configuration}`** plus inlined IR fields (`task_id`, payloads, persistence, etc.). |
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml`** (one per leaf) | **Created** if missing from [`../workflow_template/workflow.template.WorkflowTrigger.yaml`](../workflow_template/workflow.template.WorkflowTrigger.yaml) + **trimmed** scope from [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml). Use **`module.py build --force`** to overwrite existing trigger + refresh the sibling **full** `*.config.yaml` after template edits. Resource **`externalId`**: `key_extraction_aliasing.<suffix>` (`__KEA_CDF_SUFFIX__` → `cdf_external_id_suffix`). **`build`** **does not delete** orphaned per-suffix folders during a normal build; use **`module.py build --clean`** to remove generated manifests in bulk (see paragraph above). Override scope body with **`--scope-document`**, trigger shell with **`--workflow-trigger-template`**. Verify with **`python module.py build --check-workflow-triggers`**: required trigger **and** leaf `*.config.yaml` must exist and match templates; **extra** trigger files on disk are ignored. |
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.config.yaml`** | **Full** unified v1 scope for that leaf (same shape as `workflow.template.config.yaml` after leaf patches). Always (re)written on **`module.py build`** even when the WorkflowTrigger file is skipped without **`--force`**. The operator UI loads this file as the pipeline / canvas source of truth and saves back trimmed **`input.configuration`** into the trigger via **`POST /api/scoped-workflow-publish`**. |

**Migration:** Deployed **v5** triggers embed **trimmed** **`input.configuration`** only (no **`compiled_workflow`** on **`workflow.input`**). After pulling template changes, run **`python module.py build`** (refreshes **`workflow.execution.graph.yaml`**) and **`python module.py build --force`** when you need to overwrite existing scoped **`workflows/`** manifests and regenerated **WorkflowVersion** task **`data`**.

### Alias write-back property

By default, **`fn_dm_view_save`** writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via **`aliasWritebackProperty`** / **`alias_writeback_property`** on the **`save_view`** task **`data`** (workflow), or **`alias_writeback_property`** under `aliasing.config.parameters` in the v1 scope document used by `module.py` (`workflow.local.config.yaml` at module root or `--config-path`).

See the module [README](../README.md#alias-write-back) for the full table.

### Foreign key write-back

Enable **`write_foreign_key_references`** and **`foreign_key_writeback_property`** on the **`save_view`** task **`data`** when your view exposes a suitable list property, and point **`source_raw_*`** at the RAW table that carries FK reference payloads. See [module README — Foreign key write-back](../README.md#foreign-key-write-back).

### `alias_mapping_table` rules

Aliasing configs may include rules with `type: alias_mapping_table` that load rows from a Cognite **RAW** table (see [configuration guide](../docs/guides/configuration_guide.md)). The engine hydrates these rules at startup when a Cognite client is available.

- RAW mapping format supports either:
  - multiple alias columns via `alias_columns: [alias_1, alias_2, ...]`, or
  - a single alias column (e.g. `alias_columns: [aliases]`) parsed with `alias_delimiter` (for example `","` for comma-separated aliases).

### Directory layout

**`workflow_template/`** (authoring inputs — never overwritten by **`build`**; **`build`** writes *from* here into **`workflows/`** when creating missing artifacts or when **`--force`** overwrites existing scoped manifests; **`workflow.execution.graph.yaml`** is refreshed from IR on every build):

```
workflow_template/
├── workflow.template.config.yaml
├── workflow.template.WorkflowTrigger.yaml
├── workflow.template.Workflow.yaml
├── workflow.template.WorkflowVersion.yaml
├── workflow_diagram.md
└── README.md
```

**`workflows/`** (**`--build`** output for Toolkit deploy; **no** per-leaf **`*.canvas.yaml`** — the graph lives under **`canvas`** inside **`input.configuration`**):

```
workflows/
├── <suffix>/                                       # per leaf: scoped Workflow + WorkflowVersion + WorkflowTrigger
│   ├── key_extraction_aliasing.<suffix>.Workflow.yaml
│   ├── key_extraction_aliasing.<suffix>.WorkflowVersion.yaml
│   └── key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; each discovery stage reads/writes the cohort tables configured on **`key_extraction.config.parameters`** / per-task **`data`**.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Local default scope: [`../workflow.local.config.yaml`](../workflow.local.config.yaml); CDF: trigger **`input.configuration`** (see [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)).

### Related documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md)
- [Quickstart — local `module.py`](../docs/guides/howto_quickstart.md)
- [Build configuration with YAML](../docs/guides/howto_config_yaml.md)
- [Build configuration with the UI](../docs/guides/howto_config_ui.md)
- [Scoped deployment — hierarchy and Toolkit](../docs/guides/howto_scoped_deployment.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)
