## fn_dm_key_extraction

CDF function in **`cdf_key_extraction_aliasing_{{ scope_cdf_suffix }}`**. Pipeline context: [workflows/README.md](../../workflows/README.md). Documentation map: [docs/README.md](../../docs/README.md).

Extracts candidate keys, foreign key references, and document references from **configured `source_views`** (default scope: CogniteAsset, CogniteFile, CogniteTimeSeries — see `config/scopes/default/key_extraction_aliasing.yaml`), writes results to RAW, and writes a per-run state row for auditability.

### Inputs (workflow task `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **`config`**: workflow-shaped config payload (the content embedded in `WorkflowVersion.yaml` under `data.config`)

Key config fields used:
- **`config.parameters.raw_db`**
- **`config.parameters.raw_table_key`**: RAW table for entity payloads, `EXTRACTION_STATUS`, and run-summary rows (`RECORD_KIND=run`)
- **`config.parameters.skip_entity_policy`**, **`write_empty_extraction_rows`**, **`raw_skip_scan_chunk_size`**: control instance listing when `full_rescan` is false (see configuration guide)
- **`config.parameters.max_files`**: optional limit for testing
- **`config.data.source_views`**: what view(s) to query; optional per-view **`instance_space`** (API `space` argument), optional **`filters`** including **`property_scope: node`** for `("node", "space")` style filters when `instance_space` is omitted or for extra narrowing

### Outputs

- **RAW extraction table** (`raw_db` / `raw_table_key`)
  - **Entity rows:** row key = instance external id; field columns, `RULES_USED_JSON`, optional `FOREIGN_KEY_REFERENCES_JSON`, plus `RECORD_KIND=entity`, `EXTRACTION_STATUS`, `UPDATED_AT`, `RUN_ID`, and `LAST_ERROR` on failures
  - **Run rows:** timestamp key; `RECORD_KIND=run` and run-level metrics (counts, durations, `skip_entity_policy`, `run_id`, etc.)
- **Function return**: JSON-safe summary (`keys_extracted`, `status`, `message`, `run_id`, …) plus in-memory `entities_keys_extracted` for callers that need it

### How to run locally

This function is designed to run in CDF, but you can run it locally by calling `fn_dm_key_extraction/handler.py:run_locally()` and providing:
- standard CDF env vars (`CDF_PROJECT`, `CDF_CLUSTER`, `IDP_*`)
- **`KEY_EXTRACTION_CONFIG_PATH`**: path to a workflow-shaped YAML config file

### How it runs in the workflow

In `cdf_key_extraction_aliasing_{{ scope_cdf_suffix }}` (v1):
- task `fn_dm_key_extraction_{{ scope_cdf_suffix }}` runs this function and writes entity and run rows to `db_key_extraction/{{ scope_cdf_suffix }}_key_extraction_state`

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

