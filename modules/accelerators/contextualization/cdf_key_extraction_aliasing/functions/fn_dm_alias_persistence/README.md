## fn_dm_alias_persistence

Reads aliasing results (from task input or RAW) and persists generated **aliases** back to data model instances on **`cdf_cdm:CogniteDescribable:v1`**. Optionally persists **foreign key reference strings** from key extraction to a second configurable property (same default view or another view).

### Inputs (workflow task `data` or local `handle` `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **Aliases — either** provide **`aliasing_results`** directly **or** point to RAW:
  - **`raw_db`**, **`raw_table_aliases`** (or `raw_table`), optional **`raw_read_limit`**
- **Alias property name** (optional): **`aliasWritebackProperty`** or **`alias_writeback_property`** (default `aliases`)
- **Foreign keys** (optional):
  - **`writeForeignKeyReferences`** or **`write_foreign_key_references`**: enable FK string write-back
  - **`foreignKeyWritebackProperty`** or **`foreign_key_writeback_property`**: required when FK write is enabled
  - **`source_raw_db`**, **`source_raw_table_key`**, optional **`source_raw_read_limit`**: load `FOREIGN_KEY_REFERENCES_JSON` from key-extraction RAW (workflow pattern)
  - **`entities_keys_extracted`**: optional in-memory map with per-entity `foreign_key_references` (local `main.py` passes this)
  - **`source_instance_space`**, **`source_view_space`**, **`source_view_external_id`**, **`source_view_version`**: used when resolving entities that appear only in FK data
- **FK target view** (optional; defaults match CogniteDescribable): **`foreignKeyWritebackViewSpace`** / **`foreign_key_writeback_view_space`**, **`foreignKeyWritebackViewExternalId`** / **`foreign_key_writeback_view_external_id`**, **`foreignKeyWritebackViewVersion`** / **`foreign_key_writeback_view_version`**

### What it updates

For each entity referenced by alias rows (`entities_json` / normalized entity list):

- Applies `instances.apply()` with `sources` pointing at **`cdf_cdm:CogniteDescribable:v1`** for the alias list (property name as configured).
- If FK write-back is enabled and values exist for that entity, the FK property is set on the same apply when the FK target view matches the alias view; otherwise a second `NodeOrEdgeData` source is used for the FK view.

### Outputs

Handler return value (JSON-safe summary):

- **`summary.entities_updated`**, **`summary.entities_failed`**
- **`summary.aliases_persisted`**
- **`summary.foreign_keys_persisted`**, **`summary.entities_fk_updated`**
- **`summary.aliasing_results_loaded_from_raw`** (when RAW was used for aliases)

The pipeline also writes diagnostic counts onto the mutable **`data`** dict (e.g. `aliases_persisted`, `foreign_keys_persisted`).

### How to run locally

Run `handler.py:run_locally()` and point it at the RAW table you want to read:

- `raw_db = db_tag_aliasing`
- `raw_table_aliases = {{ site_abbreviation }}_aliases`

Add FK-related keys to `data` if testing FK persistence (and ensure extraction RAW or `entities_keys_extracted` is populated).

### How it runs in the workflow

In **`cdf_key_extraction_aliasing`** / **`cdf_key_extraction_aliasing_{{ site_abbreviation }}`** (v1):

- Reads alias rows from `db_tag_aliasing/{{ site_abbreviation }}_aliases` (unless `aliasing_results` is passed inline).
- When **`write_foreign_key_references`** is true, reads key-extraction RAW (`source_raw_db` / `source_raw_table_key`) for `FOREIGN_KEY_REFERENCES_JSON` and merges with any inline extraction payload.
- Updates referenced nodes in their **`instance_space`**.

See the module [README](../../README.md) (**Alias write-back**, **Foreign key write-back**) and [workflows README](../../workflows/README.md).

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.
