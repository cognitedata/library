## fn_dm_aliasing

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

In `cdf_key_extraction_aliasing_{{ site_abbreviation }}` (v1):
- task `fn_dm_aliasing_{{ site_abbreviation }}` reads candidate keys back from RAW:
  - `db_key_extraction/{{ site_abbreviation }}_extracted_keys`
- generates aliases and writes to RAW:
  - `db_tag_aliasing/{{ site_abbreviation }}_aliases`

### Notes on “how many aliases get persisted”

Persistence aggregates aliases **per entity**, so if one entity is referenced by multiple alias rows (for example one `{{ site_abbreviation }}-*` key and one `PP*` key), that entity will receive the union of aliases from those rows.

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

