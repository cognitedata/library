# fn_dm_reference_index

CDF function that maintains a **RAW inverted index** of foreign-key and document-reference strings from key-extraction output.

- **Does not** index candidate keys (those stay on the aliasing path only).
- Expands each referenced value with an **inline** `AliasingEngine` (same config shape as `fn_dm_aliasing`).
- Writes:
  - **Inverted rows** (`t_<sha256>`): `lookup_token`, `postings_json`, `updated_at`
  - **Source snapshot rows** (`ssrc_<sha256>`): `inverted_keys_json` for tombstoning prior tokens when refs change

## Task `data` parameters

| Parameter | Description |
|-----------|-------------|
| `source_raw_db`, `source_raw_table_key` | Key-extraction state table |
| `reference_index_raw_table` | Target table (required), e.g. `{site}_reference_index` |
| `reference_index_raw_db` | Optional; defaults to `source_raw_db` |
| `config` | Aliasing config (`aliasing_rules`, `validation`) — same as `fn_dm_aliasing` (filled from `configuration` when reference index is enabled) |
| `configuration` | v4: v1 scope mapping; sets `enable_reference_index` from `key_extraction.config.parameters`, fills `config` from `aliasing` when enabled |
| `instance_space` | Required with `configuration` for merging into resolved config |
| `source_instance_space`, `source_view_*` | Fallbacks if cohort columns are missing on rows |
| `reference_index_fk_entity_type` | Default `asset` — passed to `generate_aliases` for FK refs |
| `reference_index_document_entity_type` | Default `file` — for document refs |
| `source_raw_read_limit` | Default `10000` |
| `incremental_auto_run_id`, `source_run_id`, `source_workflow_status` | Optional RAW filters |

RAW columns read: `FOREIGN_KEY_REFERENCES_JSON`, `DOCUMENT_REFERENCES_JSON` (written by `fn_dm_key_extraction`).
