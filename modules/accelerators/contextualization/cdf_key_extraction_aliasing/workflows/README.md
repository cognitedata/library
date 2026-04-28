## Contextualization workflows

**Documentation index:** [docs/README.md](../docs/README.md).

**`--build` output** (Workflow / WorkflowVersion / WorkflowTrigger YAML for CDF) lives in **`workflows/`** at the module root. Deploy the trio with **`python module.py deploy-scope`** (Cognite SDK) or include them in a Cognite Toolkit **`cdf deploy`**. **Authoring inputs** (`workflow.template.Workflow.yaml`, `workflow.template.WorkflowTrigger.yaml`, `workflow.template.config.yaml`, optional sibling `workflow.template.canvas.yaml`) stay in **[`workflow_template/`](../workflow_template/)** — see [workflow_template/README.md](../workflow_template/README.md). **`WorkflowVersion.yaml`** under **`workflows/`** is generated from the **canvas `compiled_workflow` IR** (not copied from `workflow.template.WorkflowVersion.yaml`, which is kept as a human reference). The **Kahn execution graph** ([`workflow_template/workflow.execution.graph.yaml`](../workflow_template/workflow.execution.graph.yaml)) is refreshed from that same IR on **every** **`python module.py build`** (first scoped leaf’s compiled workflow; **no** **`--force`** required). It must match the IR implied by the scope template; validate with **`python scripts/validate_workflow_version_graph.py`** (also enforced by **`python module.py build --check-workflow-triggers`** when verifying workflow bundles).

This module ships a workflow that:
1) (**incremental / default in WorkflowVersion**) runs **`fn_dm_incremental_state_update`** to detect scoped instance changes, advance the **listing watermark** and **skip-unchanged hash state** (prefer **Key Discovery** FDM views under [`data_modeling/`](../data_modeling/) when deployed — `KeyDiscoveryScopeCheckpoint`, `KeyDiscoveryProcessingState`; otherwise **RAW** `scope_wm_*` + `EXTRACTION_INPUTS_HASH`), and write **cohort** entity rows in the unified key-extraction RAW table with **`WORKFLOW_STATUS=detected`** and a per-run **`RUN_ID`**,
2) runs **`fn_dm_key_extraction`** on that cohort (reads **`RUN_ID` + `WORKFLOW_STATUS=detected`**, then sets **`extracted`** on success or **`failed`** on row failure),
3) runs **`fn_dm_reference_index`** in parallel with aliasing (depends only on key extraction): reads **`FOREIGN_KEY_REFERENCES_JSON`** and **`DOCUMENT_REFERENCES_JSON`** from key-extraction RAW and maintains an inverted **reference index** RAW table whose key comes from the scope document (`key_extraction.config.parameters.reference_index_raw_table_key`, or the `*_key_extraction_state` → `*_reference_index` convention) — candidate keys are **not** sent through this task,
4) runs **`fn_dm_aliasing`** reading **candidate keys** from RAW (optionally filtered by run + status), writes alias rows, then advances **`WORKFLOW_STATUS`** to **`aliased`** for that cohort where applicable,
5) runs **`fn_dm_alias_persistence`** and advances **`WORKFLOW_STATUS`** to **`persisted`** (or leaves failures for operator review).

For deployments that omit incremental mode, the same pipeline can be described as: key extraction → (reference index ∥ aliasing) → alias persistence. Authoring source for rules and views is the v1 scope mapping embedded in each schedule trigger as **`input.configuration`** (template: [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)), patched per leaf by **`scripts/build_scopes.py`**. Local default scope (same v1 shape): [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at module root.

### Workflow: `key_extraction_aliasing` (version `v5`)

Each leaf has its own **scoped** workflow external id **`key_extraction_aliasing.<suffix>`** and matching trio under **`workflows/<suffix>/`** (for example **`workflows/default/key_extraction_aliasing.default.WorkflowTrigger.yaml`**). [`scripts/build_scopes.py`](../scripts/build_scopes.py) or **`python module.py build`** (legacy: **`python module.py --build`**) creates **missing** scoped **Workflow** / **WorkflowVersion** / **WorkflowTrigger** from **`workflow_template/`**; use **`--force`** to overwrite **existing** copies after you change the scope template or sibling **`*.canvas.yaml`**. **`workflow.execution.graph.yaml`** is refreshed from IR on every build (no **`--force`**). Each trigger calls the workflow and supplies **`input`** that becomes **`workflow.input`** at run time ([Cognite trigger static input](https://docs.cognite.com/cdf/data_workflows/triggers)). Toolkit placeholders (for example `{{functionClientId}}`, `{{ key_extraction_aliasing_schedule }}`) are resolved at deploy from [`default.config.yaml`](../default.config.yaml). **`{{instance_space}}`** is substituted **inside** each trigger’s embedded **`configuration`** (for example on **`source_views`**), not on **`workflow.input`**.

**Removing generated files:** **`python module.py build --clean`** deletes matching Workflow / WorkflowVersion / WorkflowTrigger YAML under **`workflows/`** (scoped by the hierarchy **`workflow`** id). It shows a summary, warns the change cannot be undone, and asks you to type **`yes`** unless you pass **`--yes`** (needed for non-interactive runs). **`--dry-run --clean`** lists paths only. **No rebuild runs after clean**—run **`module.py build`** again to regenerate. **`workflow_template/`** and **`workflows/README.md`** are not removed. Do not confuse this with **`module.py run --clean-state`**, which clears RAW pipeline tables, not these manifests.

**Configuration:** v5 passes the full v1 scope document on **`workflow.input.configuration`** and a compiled DAG on **`workflow.input.compiled_workflow`** into every function task (see generated **`workflows/.../*.WorkflowVersion.yaml`** after **`--build`**). Each task has a stable **`externalId`** (`kea__*`) and passes **`task_id`** plus **`compiled_workflow: ${workflow.input.compiled_workflow}`**; per-step RAW/DM targets are merged from the IR at runtime. Functions resolve **`config`** from **`configuration`**; there is **no** Cognite Files download for scope YAML. **`workflow.input`** supplies optional **`run_id`** and optional **`run_all`**; DM **`instance_space`** for handlers is taken from **`configuration`** (first **`source_views[].instance_space`**, or a single-value node **`space`** filter) unless a task explicitly passes **`instance_space`** on function **`data`**.

For a leaf in [`default.config.yaml`](../default.config.yaml), **`key_extraction.externalId`** / **`aliasing.externalId`** and node **`space`** filters use **`cdf_external_id_suffix(scope_id)`** from [`scripts/scope_build/naming.py`](../scripts/scope_build/naming.py) when **`build_scopes`** patches each trigger’s **`configuration`**.

#### Workflow inputs (v5)

- **`run_all`** (bool, default `false`): when sent on **`workflow.input`**, overrides **`key_extraction.config.parameters.run_all`** after the scope document is applied; same semantics as before for incremental + key extraction (see configuration guide).
- **`run_id`** (string, optional): reserved for operator/trigger use when wiring task outputs; when unset, downstream tasks may use `incremental_auto_run_id` to discover a single active `RUN_ID` in RAW (single-run deployments only).
- **`configuration`**: v1 scope mapping (`key_extraction`, `aliasing`, optional `scope` metadata) — **required** for deployed runs; generated triggers embed the full tree per leaf. **`instance_space`** for DM/RAW is derived from top-level **`source_views`** in **`configuration`** when not set on task **`data`**.
- **RAW keys** (extraction, aliasing, reference index): authored under **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`configuration`** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`; optional **`reference_index_raw_table_key`**, otherwise derived from `raw_table_key` by replacing the `_key_extraction_state` suffix with `_reference_index`).

#### Task 1 — Incremental state (`fn_dm_incremental_state_update`)

- **Watermark / hash state:** Configured under **`key_extraction.config.parameters`** in each trigger’s **`configuration`**: **`key_discovery_instance_space`**, **`workflow_scope`** (per-leaf, from scope build), **`key_discovery_schema_space`**, **`key_discovery_dm_version`**, **`cdm_view_version`**, **`incremental_skip_unchanged_source_inputs`**. Deploy Key Discovery views from **`data_modeling/`**; if views are absent or calls fail, **RAW** watermark and hash scans apply automatically.
- **Writes** per-scope **RAW** watermark rows (legacy path) and per-instance **cohort** rows with **`WORKFLOW_STATUS=detected`**, **`RUN_ID`**, **`SCOPE_KEY`**, **`NODE_INSTANCE_ID`**, and **`RAW_ROW_KEY`** (row key `RUN_ID:SCOPE_KEY:instance_id`).

#### Task 2 — Key extraction
- **Function**: `fn_dm_key_extraction`
- **Writes RAW** (configured by the workflow task config) to **`db_key_extraction/<raw_table_key>`** (from **`key_extraction.config.parameters`** in the scope file):
  - **Entity rows** (`RECORD_KIND=entity`): with incremental mode, row key is the cohort **`RAW_ROW_KEY`** (stable per run + scope + instance); otherwise row key is typically the node **external id**. Columns include extraction payloads; **`WORKFLOW_STATUS`** moves to **`extracted`** (or **`failed`**); `RULES_USED_JSON`; optional `FOREIGN_KEY_REFERENCES_JSON` and **`DOCUMENT_REFERENCES_JSON`**; `EXTRACTION_STATUS`, `UPDATED_AT`, `RUN_ID`
  - **Run summary rows** (`RECORD_KIND=run`): timestamp row key; counts, durations, `rules_used_counts_json`, `skip_entity_policy`, etc.

#### Task 3 — Reference index (optional branch)
- **Function**: `fn_dm_reference_index`
- **Reads** key-extraction RAW (`FOREIGN_KEY_REFERENCES_JSON`, `DOCUMENT_REFERENCES_JSON`); **does not** read candidate-key list columns.
- **Writes** inverted index RAW (`db_key_extraction/<reference_index_raw_table>` resolved from scope parameters) — see [`functions/fn_dm_reference_index/README.md`](../functions/fn_dm_reference_index/README.md).

#### Task 4 — Key aliasing
- **Function**: `fn_dm_aliasing`
- **Reads RAW** (key extraction output):
  - `db_key_extraction/<raw_table_key>` (scope **`key_extraction.config.parameters.raw_table_key`**)
- **Writes RAW**:
  - `db_tag_aliasing/<raw_table_aliases>` (scope **`aliasing.config.parameters.raw_table_aliases`**)
    - Row key: `original_tag`
    - Columns:
      - `aliases` (list-like)
      - `total_aliases`
      - `metadata_json`
      - `entities_json` (mapping of tag → entity nodes)

#### Task 5 — Alias persistence
- **Function**: `fn_dm_alias_persistence`
- **Reads RAW**:
  - `db_tag_aliasing/<raw_table_aliases>` (alias rows; resolved from **`configuration`** when present on the task payload)
  - When **`write_foreign_key_references`** is true: key-extraction RAW for `FOREIGN_KEY_REFERENCES_JSON`, via `source_raw_db` / `source_raw_table_key` (resolved from **`configuration`** when present)
- **Writes back to data model**
  - Updates each referenced node on **`cdf_cdm:CogniteDescribable:v1`** with the configured alias property (default `aliases`)
  - Optionally writes foreign-key reference strings to **`foreign_key_writeback_property`** (e.g. `references_found`) when enabled — only if that property exists in your data model

### Interpreting “aliases persisted”

Aliases are aggregated **per entity**. If one entity is referenced by multiple tag rows (for example a scope-specific key and a `PP*` key), the entity will receive the union of aliases from those rows.

### Workflow manifests (CDF Toolkit)

| File | Role |
|------|------|
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.Workflow.yaml`** | Scoped workflow container (`externalId` = `key_extraction_aliasing.<suffix>`). |
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.WorkflowVersion.yaml`** | Version **`v5`**. Tasks pass **`configuration`**, **`compiled_workflow`**, and **`task_id`** per step. |
| **`workflows/<suffix>/key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml`** (one per leaf) | **Created** if missing from [`../workflow_template/workflow.template.WorkflowTrigger.yaml`](../workflow_template/workflow.template.WorkflowTrigger.yaml) + [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml) (with sibling canvas merge). Use **`module.py build --force`** to overwrite existing files after template edits. Resource **`externalId`**: `key_extraction_aliasing.<suffix>` (`__KEA_CDF_SUFFIX__` → `cdf_external_id_suffix`). **`build`** **does not delete** orphaned per-suffix folders during a normal build; use **`module.py build --clean`** to remove generated manifests in bulk (see paragraph above). Override scope body with **`--scope-document`**, trigger shell with **`--workflow-trigger-template`**. Verify with **`python module.py build --check-workflow-triggers`**: required files must exist and match templates; **extra** trigger files on disk are ignored. |

**Migration:** New deploys use workflow **v5** and trigger-embedded **`configuration`** plus **`compiled_workflow`**. After pulling template changes, run **`python module.py build`** (refreshes **`workflow.execution.graph.yaml`**) and **`python module.py build --force`** when you need to overwrite existing scoped **`workflows/`** manifests.

### Alias write-back property

By default, alias persistence writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via:

- **`aliasWritebackProperty`** or **`alias_writeback_property`** in the `fn_dm_alias_persistence` task `data` (workflow), or
- **`alias_writeback_property`** under `aliasing.config.parameters` in the v1 scope document used by `module.py` (`workflow.local.config.yaml` at module root or `--config-path`)

See the module [README](../README.md#alias-write-back) for the full table.

### Foreign key write-back

The workflow version files set **`write_foreign_key_references: false`** by default on `fn_dm_alias_persistence`. Set it to **`true`** and configure **`foreign_key_writeback_property`** (and keep **`source_raw_*`** pointing at the key-extraction table) when your CogniteDescribable view exposes a suitable list property. See [module README — Foreign key write-back](../README.md#foreign-key-write-back).

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

**`workflows/`** (**`--build`** output for Toolkit deploy):

```
workflows/
├── <suffix>/                                       # per leaf: scoped Workflow + WorkflowVersion + WorkflowTrigger
│   ├── key_extraction_aliasing.<suffix>.Workflow.yaml
│   ├── key_extraction_aliasing.<suffix>.WorkflowVersion.yaml
│   └── key_extraction_aliasing.<suffix>.WorkflowTrigger.yaml
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; key extraction writes extracted keys to RAW, aliasing reads them and writes alias rows, persistence reads alias rows.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Legacy → new config mapping references live under [`config/examples/reference/`](../config/examples/reference/) (`LEGACY_TO_NEW_*.md`). Local default scope: [`../workflow.local.config.yaml`](../workflow.local.config.yaml); CDF: trigger **`input.configuration`** (see [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)).

### Related documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md)
- [Quickstart — local `module.py`](../docs/guides/howto_quickstart.md)
- [Build configuration with YAML](../docs/guides/howto_config_yaml.md)
- [Build configuration with the UI](../docs/guides/howto_config_ui.md)
- [Scoped deployment — hierarchy and Toolkit](../docs/guides/howto_scoped_deployment.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)
