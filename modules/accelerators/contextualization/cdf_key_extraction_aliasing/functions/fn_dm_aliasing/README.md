## fn_dm_aliasing

CDF function in **`cdf_key_extraction_aliasing_{{ scope_cdf_suffix }}`**. Pipeline context: [workflows/README.md](../../workflows/README.md). Documentation map: [docs/README.md](../../docs/README.md).

Generates alias variants for tags/candidate keys (OCR variants, separator variants, regex-based transforms, etc.), writes aliases to RAW, and includes an `entities_json` mapping so downstream steps can update the correct nodes.

### Inputs (workflow task `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **`config`**: workflow-shaped aliasing config payload
- **Key-extraction RAW fallback inputs** (used in the workflow):
  - **`source_raw_db`** / **`source_raw_table_key`**
  - **`source_instance_space`**
  - **`source_view_space`** / **`source_view_external_id`** / **`source_view_version`**
  - **`source_entity_type`**
  - Note: these are only required when aliasing falls back to reading candidate keys from RAW (no `entities_keys_extracted` payload).

### `alias_mapping_table` RAW format

For rules with `type: alias_mapping_table`, RAW mapping rows can provide aliases as:

- multiple columns listed in `alias_columns` (for example `alias_1`, `alias_2`), or
- one column (for example `aliases`) with delimiter splitting via `alias_delimiter` (for example `","`).

Optional `alias_strip_quotes` (default `true`) removes surrounding single/double quotes on split tokens.

### Outputs

- **RAW aliases table** (`raw_db` / `raw_table_aliases`, configured via `config.parameters`)
  - row key = `original_tag`
  - columns:
    - `aliases` (list-like)
    - `total_aliases`
    - `metadata_json`
    - `entities_json` (tag → nodes mapping)
- **Function return**: JSON-safe summary only (`total_tags_processed`, `total_aliases_generated`, RAW pointers)

### How to run locally

Run `handler.py:run_locally()` (requires `.env` + CDF credentials).

### How it runs in the workflow

In `cdf_key_extraction_aliasing_{{ scope_cdf_suffix }}` (v1):
- task `fn_dm_aliasing_{{ scope_cdf_suffix }}` reads candidate keys back from RAW:
  - `db_key_extraction/{{ scope_cdf_suffix }}_key_extraction_state`
- generates aliases and writes to RAW:
  - `db_tag_aliasing/{{ scope_cdf_suffix }}_aliases`

### Notes on “how many aliases get persisted”

Persistence aggregates aliases **per entity**, so if one entity is referenced by multiple alias rows (for example one `{{ scope_cdf_suffix }}-*` key and one `PP*` key), that entity will receive the union of aliases from those rows.

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

