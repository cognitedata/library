## Contextualization workflows

**Documentation index:** [docs/README.md](../docs/README.md).

This module ships a workflow that:
1) (**incremental / default in WorkflowVersion**) runs **`fn_dm_incremental_state_update`** to detect scoped instance changes, write **scope watermarks** and **cohort** entity rows in the unified key-extraction RAW table with **`WORKFLOW_STATUS=detected`** and a per-run **`RUN_ID`**,
2) runs **`fn_dm_key_extraction`** on that cohort (reads **`RUN_ID` + `WORKFLOW_STATUS=detected`**, then sets **`extracted`** on success or **`failed`** on row failure),
3) runs **`fn_dm_aliasing`** reading candidate keys from RAW (optionally filtered by run + status), writes alias rows, then advances **`WORKFLOW_STATUS`** to **`aliased`** for that cohort where applicable,
4) runs **`fn_dm_alias_persistence`** and advances **`WORKFLOW_STATUS`** to **`persisted`** (or leaves failures for operator review).

For deployments that omit incremental mode, the same pipeline can be described as three user-visible steps: key extraction → aliasing → persistence. The **default scope** in `config/scopes/default/key_extraction_aliasing.yaml` includes CogniteAsset and CogniteTimeSeries; embedded site workflow configs are often CogniteFile-only.

### Workflow: `cdf_key_extraction_aliasing_{{ site_abbreviation }}` (version `v1`)

#### Workflow inputs (v1)

- **`process_all`** (bool, default `false`): passed to `fn_dm_incremental_state_update` as `process_all`; when `true`, scope watermarks are cleared and the run lists the full scoped set (no `lastUpdatedTime` range) before re-seeding watermarks.
- **`run_id`** (string, optional): reserved for operator/trigger use when wiring task outputs; when unset, downstream tasks may use `incremental_auto_run_id` to discover a single active `RUN_ID` in RAW (single-run deployments only).

#### Task 1 — Incremental state (`fn_dm_incremental_state_update`)

- **Writes** per-scope watermark rows and per-instance cohort rows with **`WORKFLOW_STATUS=detected`**, **`RUN_ID`**, **`SCOPE_KEY`**, **`NODE_INSTANCE_ID`**, and **`RAW_ROW_KEY`** (row key `RUN_ID:SCOPE_KEY:instance_id`).

#### Task 2 — Key extraction
- **Function**: `fn_dm_key_extraction`
- **Writes RAW** (configured by the workflow input config) to **`db_key_extraction/{{ site_abbreviation }}_key_extraction_state`**:
  - **Entity rows** (`RECORD_KIND=entity`): with incremental mode, row key is the cohort **`RAW_ROW_KEY`** (stable per run + scope + instance); otherwise row key is typically the node **external id**. Columns include extraction payloads; **`WORKFLOW_STATUS`** moves to **`extracted`** (or **`failed`**); `RULES_USED_JSON`; optional `FOREIGN_KEY_REFERENCES_JSON`; `EXTRACTION_STATUS`, `UPDATED_AT`, `RUN_ID`
  - **Run summary rows** (`RECORD_KIND=run`): timestamp row key; counts, durations, `rules_used_counts_json`, `skip_entity_policy`, etc.

#### Task 3 — Key aliasing
- **Function**: `fn_dm_aliasing`
- **Reads RAW** (key extraction output):
  - `db_key_extraction/{{ site_abbreviation }}_key_extraction_state`
- **Writes RAW**:
  - `db_tag_aliasing/{{ site_abbreviation }}_aliases`
    - Row key: `original_tag`
    - Columns:
      - `aliases` (list-like)
      - `total_aliases`
      - `metadata_json`
      - `entities_json` (mapping of tag → entity nodes)

#### Task 4 — Alias persistence
- **Function**: `fn_dm_alias_persistence`
- **Reads RAW**:
  - `db_tag_aliasing/{{ site_abbreviation }}_aliases` (alias rows)
  - When **`write_foreign_key_references`** is true: key-extraction RAW (e.g. `db_key_extraction/{{ site_abbreviation }}_key_extraction_state`) for `FOREIGN_KEY_REFERENCES_JSON`, via `source_raw_db` / `source_raw_table_key` on the task `data`
- **Writes back to data model**
  - Updates each referenced node on **`cdf_cdm:CogniteDescribable:v1`** with the configured alias property (default `aliases`)
  - Optionally writes foreign-key reference strings to **`foreign_key_writeback_property`** (e.g. `references_found`) when enabled — only if that property exists in your data model

### Interpreting “aliases persisted”

Aliases are aggregated **per entity**. If one entity is referenced by multiple tag rows (for example a `{{ site_abbreviation }}-*` key and a `PP*` key), the entity will receive the union of aliases from those rows.

### Generic workflow: `cdf_key_extraction_aliasing` (version `v1`)

For deployments that use a **single** workflow external id (no site suffix on the workflow container), use:

- [`cdf_key_extraction_aliasing.Workflow.yaml`](cdf_key_extraction_aliasing.Workflow.yaml) — `externalId: cdf_key_extraction_aliasing`
- [`cdf_key_extraction_aliasing.WorkflowVersion.yaml`](cdf_key_extraction_aliasing.WorkflowVersion.yaml) — same RAW handoff pattern as the site-templated version; task `externalId` values are `fn_dm_key_extraction`, `fn_dm_aliasing`, and `fn_dm_alias_persistence` (no per-site suffix on task ids)
- [`cdf_key_extraction_aliasing.WorkflowTrigger.yaml`](cdf_key_extraction_aliasing.WorkflowTrigger.yaml) — schedule trigger for `cdf_key_extraction_aliasing` / `v1`

Embedded config in the version file may still use CDF Toolkit placeholders such as `{{ site_abbreviation }}`, `{{ site_name }}`, and `{{ instance_space }}` for RAW table names and pipeline config external ids.

### Site-templated workflow: `cdf_key_extraction_aliasing_{{ site_abbreviation }}`

Optional parallel manifests (per-site workflow **container** id):

- [`cdf_key_extraction_aliasing_site.Workflow.yaml`](cdf_key_extraction_aliasing_site.Workflow.yaml)
- [`cdf_key_extraction_aliasing_site.WorkflowVersion.yaml`](cdf_key_extraction_aliasing_site.WorkflowVersion.yaml)
- [`cdf_key_extraction_aliasing_site-01.WorkflowTrigger.yaml`](cdf_key_extraction_aliasing_site-01.WorkflowTrigger.yaml)

**Keeping generic and site workflows in sync:** [`cdf_key_extraction_aliasing_site.WorkflowVersion.yaml`](cdf_key_extraction_aliasing_site.WorkflowVersion.yaml) is generated from [`cdf_key_extraction_aliasing.WorkflowVersion.yaml`](cdf_key_extraction_aliasing.WorkflowVersion.yaml) (only workflow/task `externalId` lines differ). After editing the canonical file, regenerate the site manifest:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflows/generate_site_workflow_version.py
```

The repository’s **Build and Release Packages** workflow runs `--check` on every push and pull request so the committed site file cannot drift from the generator. Locally:

```bash
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflows/generate_site_workflow_version.py --check
```

### Alias write-back property

By default, alias persistence writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via:

- **`aliasWritebackProperty`** or **`alias_writeback_property`** in the `fn_dm_alias_persistence` task `data` (workflow), or
- **`alias_writeback_property`** under `aliasing.config.parameters` in the v1 scope document used by `main.py` (`config/scopes/.../key_extraction_aliasing.yaml` or `--config-path`)

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
├── cdf_key_extraction_aliasing.Workflow.yaml
├── cdf_key_extraction_aliasing.WorkflowVersion.yaml
├── cdf_key_extraction_aliasing.WorkflowTrigger.yaml
├── cdf_key_extraction_aliasing_site.Workflow.yaml
├── cdf_key_extraction_aliasing_site.WorkflowVersion.yaml
├── cdf_key_extraction_aliasing_site-01.WorkflowTrigger.yaml
├── workflow_diagram.md
├── workflow_diagram.png
├── generate_site_workflow_version.py
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; key extraction writes extracted keys to RAW, aliasing reads them and writes alias rows, persistence reads alias rows.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Legacy → new config mapping references live under [`config/examples/reference/`](../config/examples/reference/) (`LEGACY_TO_NEW_*.md`). Authoring scope files: [`config/scopes/<scope>/key_extraction_aliasing.yaml`](../config/scopes/default/key_extraction_aliasing.yaml).

### Related documentation

- [Documentation map](../docs/README.md)
- [Module README](../README.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)

