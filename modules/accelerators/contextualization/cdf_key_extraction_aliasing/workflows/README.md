## Contextualization workflows

This module ships a 3-step workflow that:
1) extracts candidate keys from files,
2) generates alias variants for those keys,
3) persists the aliases back onto the source nodes.

### Workflow: `cdf_key_extraction_aliasing_{{ site_abbreviation }}` (version `v1`)

#### Task 1 — Key extraction
- **Function**: `fn_dm_key_extraction`
- **Writes RAW** (configured by the workflow input config):
  - **Keys table**: `db_key_extraction/{{ site_abbreviation }}_extracted_keys`
    - Row key: file external id (node external id)
    - Columns: extracted keys grouped by source field (for example `NAME`, `DESCRIPTION`, `METADATA`)
    - Extra provenance column: `RULES_USED_JSON` (JSON list of rule names that produced at least one key for that entity)
  - **State table**: `db_key_extraction/key_extraction_state_{{ site_abbreviation }}`
    - One row per run with counts, timestamps, run duration, and `rules_used_counts_json`

#### Task 2 — Key aliasing
- **Function**: `fn_dm_aliasing`
- **Reads RAW** (key extraction output):
  - `db_key_extraction/{{ site_abbreviation }}_extracted_keys`
- **Writes RAW**:
  - `db_tag_aliasing/{{ site_abbreviation }}_aliases`
    - Row key: `original_tag`
    - Columns:
      - `aliases` (list-like)
      - `total_aliases`
      - `metadata_json`
      - `entities_json` (mapping of tag → entity nodes)

#### Task 3 — Alias persistence
- **Function**: `fn_dm_alias_persistence`
- **Reads RAW**:
  - `db_tag_aliasing/{{ site_abbreviation }}_aliases`
- **Writes back to data model**
  - Updates each referenced node by writing `aliases` to:
    - `cdf_cdm:CogniteDescribable:v1`

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

### Alias write-back property

By default, alias persistence writes the alias list to the **`aliases`** property on `cdf_cdm:CogniteDescribable:v1`. Override via:

- **`aliasWritebackProperty`** or **`alias_writeback_property`** in the `fn_dm_alias_persistence` task `data` (workflow), or
- **`alias_writeback_property`** under `config.parameters` in the first `*aliasing*.config.yaml` consumed by `main.py`

See the module [README](../README.md#alias-write-back) for the full table.

### `alias_mapping_table` rules

Aliasing configs may include rules with `type: alias_mapping_table` that load rows from a Cognite **RAW** table (see [configuration guide](../docs/guides/configuration_guide.md)). The engine hydrates these rules at startup when a Cognite client is available.

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
└── README.md
```

### Notes

- RAW is used **between** workflow tasks because CDF Workflows do not automatically pass function outputs to the next task; key extraction writes extracted keys to RAW, aliasing reads them and writes alias rows, persistence reads alias rows.
- Enable **`logLevel: DEBUG`** in task `data` for verbose function logs.
- Legacy → new config mapping references live under [`pipelines/`](../pipelines/) (`LEGACY_TO_NEW_*.md`).

### Related documentation

- [Module README](../README.md)
- [Configuration guide](../docs/guides/configuration_guide.md)
- [CDF Toolkit](https://github.com/cognitedata/cdf-toolkit)

