## Contextualization workflows

This module ships a 3-step workflow that:
1) extracts candidate keys from files,
2) generates alias variants for those keys,
3) persists the aliases back onto the source nodes.

### Workflow: `cdf_key_extraction_aliasing_GEL` (version `v1`)

#### Task 1 — Key extraction
- **Function**: `fn_dm_key_extraction`
- **Writes RAW** (configured by the workflow input config):
  - **Keys table**: `db_key_extraction/GEL_extracted_keys`
    - Row key: file external id (node external id)
    - Columns: extracted keys grouped by source field (for example `NAME`, `DESCRIPTION`, `METADATA`)
    - Extra provenance column: `RULES_USED_JSON` (JSON list of rule names that produced at least one key for that entity)
  - **State table**: `db_key_extraction/key_extraction_state_GEL`
    - One row per run with counts, timestamps, run duration, and `rules_used_counts_json`

#### Task 2 — Key aliasing
- **Function**: `fn_dm_aliasing`
- **Reads RAW** (key extraction output):
  - `db_key_extraction/GEL_extracted_keys`
- **Writes RAW**:
  - `db_tag_aliasing/GEL_aliases`
    - Row key: `original_tag`
    - Columns:
      - `aliases` (list-like)
      - `total_aliases`
      - `metadata_json`
      - `entities_json` (mapping of tag → entity nodes)

#### Task 3 — Alias persistence
- **Function**: `fn_dm_alias_persistence`
- **Reads RAW**:
  - `db_tag_aliasing/GEL_aliases`
- **Writes back to data model**
  - Updates each referenced node by writing `aliases` to:
    - `cdf_cdm:CogniteDescribable:v1`

### Interpreting “aliases persisted”

Aliases are aggregated **per entity**. If one entity is referenced by multiple tag rows (for example a `GEL-*` key and a `PP*` key), the entity will receive the union of aliases from those rows.

