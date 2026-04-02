## Contextualization workflows

**Documentation index:** [docs/README.md](../docs/README.md).

**`--build` output** (Workflow / WorkflowVersion / WorkflowTrigger YAML for CDF Toolkit) lives in **`workflows/`** at the module root. **Authoring inputs** (`workflow.template.Workflow*.yaml`, `workflow.template.WorkflowTrigger.yaml`, `workflow.template.config.yaml`) and the Mermaid diagram source stay in **[`workflow_template/`](../workflow_template/)** — see [workflow_template/README.md](../workflow_template/README.md).

This module ships a workflow that:
1) (**incremental / default in WorkflowVersion**) runs **`fn_dm_incremental_state_update`** to detect scoped instance changes, write **scope watermarks** and **cohort** entity rows in the unified key-extraction RAW table with **`WORKFLOW_STATUS=detected`** and a per-run **`RUN_ID`**,
2) runs **`fn_dm_key_extraction`** on that cohort (reads **`RUN_ID` + `WORKFLOW_STATUS=detected`**, then sets **`extracted`** on success or **`failed`** on row failure),
3) runs **`fn_dm_reference_index`** in parallel with aliasing (depends only on key extraction): reads **`FOREIGN_KEY_REFERENCES_JSON`** and **`DOCUMENT_REFERENCES_JSON`** from key-extraction RAW and maintains an inverted **reference index** RAW table whose key comes from the scope document (`key_extraction.config.parameters.reference_index_raw_table_key`, or the `*_key_extraction_state` → `*_reference_index` convention) — candidate keys are **not** sent through this task,
4) runs **`fn_dm_aliasing`** reading **candidate keys** from RAW (optionally filtered by run + status), writes alias rows, then advances **`WORKFLOW_STATUS`** to **`aliased`** for that cohort where applicable,
5) runs **`fn_dm_alias_persistence`** and advances **`WORKFLOW_STATUS`** to **`persisted`** (or leaves failures for operator review).

For deployments that omit incremental mode, the same pipeline can be described as: key extraction → (reference index ∥ aliasing) → alias persistence. Authoring source for rules and views is the v1 scope mapping embedded in each schedule trigger as **`input.configuration`** (template: [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)), patched per leaf by **`scripts/build_scopes.py`**. Local default scope (same v1 shape): [`../workflow.local.config.yaml`](../workflow.local.config.yaml) at module root.

### Workflow: `key_extraction_aliasing` (version `v4`)

There is **one** workflow external id for all scopes (in **`trigger_only`** mode). **Per-leaf schedule triggers** live in separate files **`key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** under **`workflows/`** (for example `key_extraction_aliasing.default.WorkflowTrigger.yaml`). In **`full`** mode, each leaf’s Workflow, WorkflowVersion, and trigger sit under **`workflows/<suffix>/`**. [`scripts/build_scopes.py`](../scripts/build_scopes.py) or **`python main.py --build`** **creates missing** artifacts only (does not overwrite existing, unless **`--force`**). Each trigger calls the workflow and supplies **`input`** that becomes **`workflow.input`** at run time ([Cognite trigger static input](https://docs.cognite.com/cdf/data_workflows/triggers)). Toolkit placeholders (for example `{{functionClientId}}`, `{{ key_extraction_aliasing_schedule }}`) are resolved at deploy from [`default.config.yaml`](../default.config.yaml). **`{{instance_space}}`** is substituted **inside** each trigger’s embedded **`configuration`** (for example on **`source_views`**), not on **`workflow.input`**.

**Configuration:** v4 passes the full v1 scope document on **`workflow.input.configuration`** into every function task (see generated **`workflows/.../key_extraction_aliasing*.WorkflowVersion.yaml`** after **`--build`**). Functions resolve **`config`** from that object; there is **no** Cognite Files download for scope YAML. **RAW table keys** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`, reference index target) are read from **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`configuration`**. **`workflow.input`** supplies optional **`run_id`** and optional **`full_rescan`**; DM **`instance_space`** for handlers is taken from **`configuration`** (first **`source_views[].instance_space`**, or a single-value node **`space`** filter) unless a task explicitly passes **`instance_space`** on function **`data`**.

For a leaf in [`default.config.yaml`](../default.config.yaml), **`key_extraction.externalId`** / **`aliasing.externalId`** and node **`space`** filters use **`cdf_external_id_suffix(scope_id)`** from [`scripts/scope_build/naming.py`](../scripts/scope_build/naming.py) when **`build_scopes`** patches each trigger’s **`configuration`**.

#### Workflow inputs (v4)

- **`full_rescan`** (bool, default `false`): when sent on **`workflow.input`**, overrides **`key_extraction.config.parameters.full_rescan`** after the scope document is applied; same semantics as before for incremental + key extraction (see configuration guide).
- **`run_id`** (string, optional): reserved for operator/trigger use when wiring task outputs; when unset, downstream tasks may use `incremental_auto_run_id` to discover a single active `RUN_ID` in RAW (single-run deployments only).
- **`configuration`**: v1 scope mapping (`key_extraction`, `aliasing`, optional `scope` metadata) — **required** for deployed runs; generated triggers embed the full tree per leaf. **`instance_space`** for DM/RAW is derived from **`key_extraction.config.data.source_views`** (see above) when not set on task **`data`**.
- **RAW keys** (extraction, aliasing, reference index): authored under **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`configuration`** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`; optional **`reference_index_raw_table_key`**, otherwise derived from `raw_table_key` by replacing the `_key_extraction_state` suffix with `_reference_index`).

#### Task 1 — Incremental state (`fn_dm_incremental_state_update`)

- **Writes** per-scope watermark rows and per-instance cohort rows with **`WORKFLOW_STATUS=detected`**, **`RUN_ID`**, **`SCOPE_KEY`**, **`NODE_INSTANCE_ID`**, and **`RAW_ROW_KEY`** (row key `RUN_ID:SCOPE_KEY:instance_id`).

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
| **`key_extraction_aliasing.Workflow.yaml`** (under **`workflows/`** root in **trigger_only**, or **`workflows/<suffix>/`** in **full**) | Workflow container. |
| **`key_extraction_aliasing.WorkflowVersion.yaml`** (same layout) | Version **`v4`**. Tasks pass **`configuration: ${workflow.input.configuration}`** plus wiring. |
| **`key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (one per leaf) | **Created** from [`../workflow_template/workflow.template.WorkflowTrigger.yaml`](../workflow_template/workflow.template.WorkflowTrigger.yaml) + [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml). Resource **`externalId`**: `key_extraction_aliasing.<suffix>` (`__KEA_CDF_SUFFIX__` → `cdf_external_id_suffix`). **`python main.py --build`** or **`python scripts/build_scopes.py`** creates **missing** files only (use **`--force`** to overwrite from templates). **`--build`** **does not delete** other `key_extraction_aliasing.*.WorkflowTrigger.yaml` files (remove orphans manually if needed). Override scope body with **`--scope-document`**, trigger shell with **`--workflow-trigger-template`**. Verify with **`python main.py --build --check-workflow-triggers`**: required files must exist and match templates; **extra** trigger files on disk are ignored. |

**Migration:** New deploys use workflow **v4** and trigger-embedded **`configuration`**. Older v3 runs that used CogniteFile + **`scope_config_file_external_id`** must be redeployed with v4 schedule triggers (create via **`--build`** where missing) and workflow version **v4**.

### Alias write-back property

By default, alias persistence writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via:

- **`aliasWritebackProperty`** or **`alias_writeback_property`** in the `fn_dm_alias_persistence` task `data` (workflow), or
- **`alias_writeback_property`** under `aliasing.config.parameters` in the v1 scope document used by `main.py` (`workflow.local.config.yaml` at module root or `--config-path`)

See the module [README](../README.md#alias-write-back) for the full table.

### Foreign key write-back

The workflow version files set **`write_foreign_key_references: false`** by default on `fn_dm_alias_persistence`. Set it to **`true`** and configure **`foreign_key_writeback_property`** (and keep **`source_raw_*`** pointing at the key-extraction table) when your CogniteDescribable view exposes a suitable list property. See [module README — Foreign key write-back](../README.md#foreign-key-write-back).

### `alias_mapping_table` rules

Aliasing configs may include rules with `type: alias_mapping_table` that load rows from a Cognite **RAW** table (see [configuration guide](../docs/guides/configuration_guide.md)). The engine hydrates these rules at startup when a Cognite client is available.

- RAW mapping format supports either:
  - multiple alias columns via `alias_columns: [alias_1, alias_2, ...]`, or
  - a single alias column (e.g. `alias_columns: [aliases]`) parsed with `alias_delimiter` (for example `","` for comma-separated aliases).

### Directory layout

**`workflow_template/`** (inputs — not overwritten by default **`--build`** except when using **`--force`** only on *generated* paths under **`workflows/`**; templates themselves are never overwritten by build):

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
├── key_extraction_aliasing.Workflow.yaml          # trigger_only (root)
├── key_extraction_aliasing.WorkflowVersion.yaml
├── key_extraction_aliasing.<scope>.WorkflowTrigger.yaml
├── <suffix>/                                       # full mode: scoped trio + trigger per leaf
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; key extraction writes extracted keys to RAW, aliasing reads them and writes alias rows, persistence reads alias rows.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Legacy → new config mapping references live under [`config/examples/reference/`](../config/examples/reference/) (`LEGACY_TO_NEW_*.md`). Local default scope: [`../workflow.local.config.yaml`](../workflow.local.config.yaml); CDF: trigger **`input.configuration`** (see [`../workflow_template/workflow.template.config.yaml`](../workflow_template/workflow.template.config.yaml)).

### Related documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)
