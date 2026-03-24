## fn_dm_alias_persistence

Reads aliasing results from RAW and persists the aliases back to data model instances by writing to:

- **`cdf_cdm:CogniteDescribable:v1`** (`aliases` property)

### Inputs (workflow task `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **Either** provide `aliasing_results` directly **or** point to RAW:
  - **`raw_db`**
  - **`raw_table_aliases`**
  - **`raw_read_limit`** (optional)

### What it updates

For each entity referenced by `entities_json` in the alias rows:
- applies `instances.apply()` with `sources=[cdf_cdm:CogniteDescribable:v1]`
- sets the `aliases` property to the deduplicated alias list for that entity

### Outputs

- **Function return**: JSON-safe summary only
  - `entities_updated`
  - `entities_failed`
  - `aliases_planned`
  - `aliases_persisted`

### How to run locally

Run `handler.py:run_locally()` and point it at the RAW table you want to read:
- `raw_db = db_tag_aliasing`
- `raw_table_aliases = GEL_aliases`

### How it runs in the workflow

In `cdf_key_extraction_aliasing_GEL` (v1):
- task `fn_dm_alias_persistence_gel` reads from:
  - `db_tag_aliasing/GEL_aliases`
- and updates the referenced nodes in their `instance_space`.

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

