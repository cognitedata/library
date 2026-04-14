## fn_dm_key_extraction

CDF function in workflow **`key_extraction_aliasing`** (v4). Pipeline context: [workflows/README.md](../../workflows/README.md). Documentation map: [docs/README.md](../../docs/README.md).

Extracts candidate keys, foreign key references, and document references from **configured `source_views`** (default scope: CogniteAsset, CogniteFile, CogniteTimeSeries — see `workflow.local.config.yaml` at module root), writes results to RAW, and writes a per-run state row for auditability.

### Inputs (workflow task `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **`config`**: workflow-shaped config payload **or** omit when **`configuration`** is set (v4: v1 scope mapping on task payload; **`raw_table_key`** must be set under **`key_extraction.config.parameters`** in that document). **`instance_space`** is optional on task **`data`** when it can be derived from **`source_views`** (`instance_space` field or single-value node **`space`** filter).

Key config fields used:
- **`config.parameters.raw_db`**
- **`config.parameters.raw_table_key`**: RAW table for entity payloads, `EXTRACTION_STATUS`, and run-summary rows (`RECORD_KIND=run`)
- **`config.parameters.skip_entity_policy`**, **`write_empty_extraction_rows`**, **`raw_skip_scan_chunk_size`**: control instance listing when `full_rescan` is false (see configuration guide)
- **Incremental + Key Discovery (optional):** when **`incremental_change_processing`** is true and **`key_discovery_instance_space`** is set, successful extraction can upsert **`KeyDiscoveryProcessingState`** in FDM instead of writing **`EXTRACTION_INPUTS_HASH`** on RAW for skip/hash parity — see **`workflow_scope`**, **`key_discovery_schema_space`**, **`key_discovery_dm_version`**, **`cdm_view_version`**, and [configuration guide](../../../docs/guides/configuration_guide.md#incremental-mode-key-discovery-fdm-and-raw-cohort). If Key Discovery views are not deployed, the function falls back to RAW hash columns.
- **`config.parameters.max_files`**: optional limit for testing
- **`config.data.source_views`** (populated from top-level **`configuration.source_views`** on the v1 scope document before the function runs): what view(s) to query; optional per-view **`instance_space`** (API `space` argument), optional **`filters`** including **`property_scope: node`** for `("node", "space")` style filters when `instance_space` is omitted or for extra narrowing; optional **`key_discovery_hash_property_paths`** for incremental hash field selection

### Outputs

- **RAW extraction table** (`raw_db` / `raw_table_key`)
  - **Entity rows:** row key = instance external id; per–source-field **candidate key list** columns named **`source_field` uppercased** (dots preserved, e.g. `metadata.code` → `METADATA.CODE`); `RULES_USED_JSON`; optional `FOREIGN_KEY_REFERENCES_JSON` / `DOCUMENT_REFERENCES_JSON`; plus `RECORD_KIND=entity`, `EXTRACTION_STATUS`, `UPDATED_AT`, `RUN_ID`, and `LAST_ERROR` on failures
  - **Run rows:** timestamp key; `RECORD_KIND=run` and run-level metrics (counts, durations, `skip_entity_policy`, `run_id`, etc.)
- **Function return**: JSON-safe summary (`keys_extracted`, `status`, `message`, `run_id`, …) plus in-memory `entities_keys_extracted` for callers that need it

### How to run locally

This function is designed to run in CDF, but you can run it locally by calling `fn_dm_key_extraction/handler.py:run_locally()` and providing:
- standard CDF env vars (`CDF_PROJECT`, `CDF_CLUSTER`, `IDP_*`)
- **`KEY_EXTRACTION_CONFIG_PATH`**: path to a workflow-shaped YAML config file

### How it runs in the workflow

In `key_extraction_aliasing` (v4):
- task `fn_dm_key_extraction` runs this function and writes entity and run rows to `db_key_extraction/<raw_table_key>` (from the scope YAML **`key_extraction.config.parameters.raw_table_key`**)

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

