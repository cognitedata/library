## Contextualization workflows

**Documentation index:** [docs/README.md](../docs/README.md).

This module ships a workflow that:
1) (**incremental / default in WorkflowVersion**) runs **`fn_dm_incremental_state_update`** to detect scoped instance changes, write **scope watermarks** and **cohort** entity rows in the unified key-extraction RAW table with **`WORKFLOW_STATUS=detected`** and a per-run **`RUN_ID`**,
2) runs **`fn_dm_key_extraction`** on that cohort (reads **`RUN_ID` + `WORKFLOW_STATUS=detected`**, then sets **`extracted`** on success or **`failed`** on row failure),
3) runs **`fn_dm_reference_index`** in parallel with aliasing (depends only on key extraction): reads **`FOREIGN_KEY_REFERENCES_JSON`** and **`DOCUMENT_REFERENCES_JSON`** from key-extraction RAW and maintains an inverted **reference index** RAW table whose key comes from the scope document (`key_extraction.config.parameters.reference_index_raw_table_key`, or the `*_key_extraction_state` → `*_reference_index` convention) — candidate keys are **not** sent through this task,
4) runs **`fn_dm_aliasing`** reading **candidate keys** from RAW (optionally filtered by run + status), writes alias rows, then advances **`WORKFLOW_STATUS`** to **`aliased`** for that cohort where applicable,
5) runs **`fn_dm_alias_persistence`** and advances **`WORKFLOW_STATUS`** to **`persisted`** (or leaves failures for operator review).

For deployments that omit incremental mode, the same pipeline can be described as: key extraction → (reference index ∥ aliasing) → alias persistence. Authoring source for rules and views is the v1 scope mapping embedded in each schedule trigger as **`input.scope_document`** (template: [`_template/key_extraction_aliasing.scope_document.yaml`](_template/key_extraction_aliasing.scope_document.yaml)), patched per leaf by **`scripts/build_scopes.py`**. Local reference copy: [`../key_extraction_aliasing.yaml`](../key_extraction_aliasing.yaml) at module root.

### Workflow: `cdf_key_extraction_aliasing` (version `v4`)

There is **one** workflow external id for all scopes. **Per-leaf schedule triggers** are generated as separate files **`cdf_key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** under [`workflows/`](.) (for example `cdf_key_extraction_aliasing.default.WorkflowTrigger.yaml`), produced by [`scripts/build_scopes.py`](../scripts/build_scopes.py) or **`python main.py --build`**. Each calls the same workflow and supplies **`input`** that becomes **`workflow.input`** at run time ([Cognite trigger static input](https://docs.cognite.com/cdf/data_workflows/triggers)). Toolkit placeholders (for example `{{functionClientId}}`, `{{ key_extraction_aliasing_schedule }}`) are resolved at deploy from [`default.config.yaml`](../default.config.yaml). **`{{instance_space}}`** is substituted **inside** each trigger’s embedded **`scope_document`** (for example on **`source_views`**), not on **`workflow.input`**.

**Configuration:** v4 passes the full v1 scope document on **`workflow.input.scope_document`** into every function task (see [`cdf_key_extraction_aliasing.WorkflowVersion.yaml`](cdf_key_extraction_aliasing.WorkflowVersion.yaml)). Functions resolve **`config`** from that object; there is **no** Cognite Files download for scope YAML. **RAW table keys** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`, reference index target) are read from **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`scope_document`**. **`workflow.input`** supplies optional **`run_id`** and optional **`full_rescan`**; DM **`instance_space`** for handlers is taken from **`scope_document`** (first **`source_views[].instance_space`**, or a single-value node **`space`** filter) unless a task explicitly passes **`instance_space`** on function **`data`**.

For a leaf in [`default.config.yaml`](../default.config.yaml), **`key_extraction.externalId`** / **`aliasing.externalId`** and node **`space`** filters use **`cdf_external_id_suffix(scope_id)`** from [`scripts/scope_build/naming.py`](../scripts/scope_build/naming.py) when **`build_scopes`** patches each trigger’s **`scope_document`**.

#### Workflow inputs (v4)

- **`full_rescan`** (bool, default `false`): when sent on **`workflow.input`**, overrides **`key_extraction.config.parameters.full_rescan`** after the scope document is applied; same semantics as before for incremental + key extraction (see configuration guide).
- **`run_id`** (string, optional): reserved for operator/trigger use when wiring task outputs; when unset, downstream tasks may use `incremental_auto_run_id` to discover a single active `RUN_ID` in RAW (single-run deployments only).
- **`scope_document`**: v1 scope mapping (`key_extraction`, `aliasing`, optional `scope` metadata) — **required** for deployed runs; generated triggers embed the full tree per leaf. **`instance_space`** for DM/RAW is derived from **`key_extraction.config.data.source_views`** (see above) when not set on task **`data`**.
- **RAW keys** (extraction, aliasing, reference index): authored under **`key_extraction.config.parameters`** / **`aliasing.config.parameters`** inside **`scope_document`** (`raw_table_key`, `raw_table_aliases`, `raw_table_state`; optional **`reference_index_raw_table_key`**, otherwise derived from `raw_table_key` by replacing the `_key_extraction_state` suffix with `_reference_index`).

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
  - `db_tag_aliasing/<raw_table_aliases>` (alias rows; resolved from **`scope_document`** when present on the task payload)
  - When **`write_foreign_key_references`** is true: key-extraction RAW for `FOREIGN_KEY_REFERENCES_JSON`, via `source_raw_db` / `source_raw_table_key` (resolved from **`scope_document`** when present)
- **Writes back to data model**
  - Updates each referenced node on **`cdf_cdm:CogniteDescribable:v1`** with the configured alias property (default `aliases`)
  - Optionally writes foreign-key reference strings to **`foreign_key_writeback_property`** (e.g. `references_found`) when enabled — only if that property exists in your data model

### Interpreting “aliases persisted”

Aliases are aggregated **per entity**. If one entity is referenced by multiple tag rows (for example a scope-specific key and a `PP*` key), the entity will receive the union of aliases from those rows.

### Workflow manifests (CDF Toolkit)

| File | Role |
|------|------|
| [`cdf_key_extraction_aliasing.Workflow.yaml`](cdf_key_extraction_aliasing.Workflow.yaml) | Workflow container: fixed `externalId: cdf_key_extraction_aliasing`. |
| [`cdf_key_extraction_aliasing.WorkflowVersion.yaml`](cdf_key_extraction_aliasing.WorkflowVersion.yaml) | `workflowExternalId: cdf_key_extraction_aliasing`, version **`v4`**. Tasks pass **`scope_document: ${workflow.input.scope_document}`** plus wiring. |
| **`cdf_key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (one per leaf) | **Generated** from [`_template/cdf_key_extraction_aliasing_scope_trigger.WorkflowTrigger.yaml.template`](_template/cdf_key_extraction_aliasing_scope_trigger.WorkflowTrigger.yaml.template) + [`_template/key_extraction_aliasing.scope_document.yaml`](_template/key_extraction_aliasing.scope_document.yaml). Resource **`externalId`**: `cdf_key_extraction_aliasing.<suffix>` (`__KEA_CDF_SUFFIX__` → `cdf_external_id_suffix`). Regenerate with **`python main.py --build`** or **`python scripts/build_scopes.py`**. **`--build`** updates only triggers for leaves in the current hierarchy; it **does not delete** other `cdf_key_extraction_aliasing.*.WorkflowTrigger.yaml` files (remove orphans manually if needed). Override scope body with **`--scope-document`**, trigger shell with **`--workflow-trigger-template`**. Verify with **`python main.py --build --check-workflow-triggers`**: required files must exist and match templates; **extra** trigger files on disk are ignored. |

**Migration:** New deploys use workflow **v4** and trigger-embedded **`scope_document`**. Older v3 runs that used CogniteFile + **`scope_config_file_external_id`** must be redeployed with regenerated triggers and workflow version **v4**.

### Alias write-back property

By default, alias persistence writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via:

- **`aliasWritebackProperty`** or **`alias_writeback_property`** in the `fn_dm_alias_persistence` task `data` (workflow), or
- **`alias_writeback_property`** under `aliasing.config.parameters` in the v1 scope document used by `main.py` (`key_extraction_aliasing.yaml` at module root or `--config-path`)

See the module [README](../README.md#alias-write-back) for the full table.

### Foreign key write-back

The workflow version files set **`write_foreign_key_references: false`** by default on `fn_dm_alias_persistence`. Set it to **`true`** and configure **`foreign_key_writeback_property`** (and keep **`source_raw_*`** pointing at the key-extraction table) when your CogniteDescribable view exposes a suitable list property. See [module README — Foreign key write-back](../README.md#foreign-key-write-back).

### `alias_mapping_table` rules

Aliasing configs may include rules with `type: alias_mapping_table` that load rows from a Cognite **RAW** table (see [configuration guide](../docs/guides/configuration_guide.md)). The engine hydrates these rules at startup when a Cognite client is available.

- RAW mapping format supports either:
  - multiple alias columns via `alias_columns: [alias_1, alias_2, ...]`, or
  - a single alias column (e.g. `alias_columns: [aliases]`) parsed with `alias_delimiter` (for example `","` for comma-separated aliases).

### Workflow file layout

```
workflows/
├── _template/
│   └── cdf_key_extraction_aliasing_scope_trigger.WorkflowTrigger.yaml.template
├── cdf_key_extraction_aliasing.Workflow.yaml
├── cdf_key_extraction_aliasing.WorkflowVersion.yaml
├── cdf_key_extraction_aliasing.<scope>.WorkflowTrigger.yaml  # generated, one per leaf
├── workflow_diagram.md
├── workflow_diagram.png
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; key extraction writes extracted keys to RAW, aliasing reads them and writes alias rows, persistence reads alias rows.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Legacy → new config mapping references live under [`config/examples/reference/`](../config/examples/reference/) (`LEGACY_TO_NEW_*.md`). Local default scope: [`../key_extraction_aliasing.yaml`](../key_extraction_aliasing.yaml); CDF: trigger **`input.scope_document`** (see [`_template/key_extraction_aliasing.scope_document.yaml`](_template/key_extraction_aliasing.scope_document.yaml)).

### Related documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)

