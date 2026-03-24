## fn_dm_key_extraction

Extracts candidate keys from `cdf_cdm:CogniteFile:v1` (or other configured views), writes results to RAW, and writes a per-run state row for auditability.

### Inputs (workflow task `data`)

- **`logLevel`**: `DEBUG|INFO|WARNING|ERROR`
- **`verbose`**: `true|false`
- **`config`**: workflow-shaped config payload (the content embedded in `WorkflowVersion.yaml` under `data.config`)

Key config fields used:
- **`config.parameters.raw_db`**
- **`config.parameters.raw_table_key`**
- **`config.parameters.raw_table_state`**
- **`config.parameters.max_files`**: optional limit for testing
- **`config.data.source_views`**: what view(s)/space(s) to query

### Outputs

- **RAW keys table** (`raw_db` / `raw_table_key`)
  - one row per entity (row key = entity external id)
  - columns contain extracted keys grouped by field (for example `NAME`, `DESCRIPTION`, `METADATA`)
- **RAW state table** (`raw_db` / `raw_table_state`)
  - one row per run with counts, timestamps, and `run_duration_s/ms`
- **Function return**: JSON-safe summary only (`entities_processed`, `workflow_config_external_id`)

### How to run locally

This function is designed to run in CDF, but you can run it locally by calling `fn_dm_key_extraction/handler.py:run_locally()` and providing:
- standard CDF env vars (`CDF_PROJECT`, `CDF_CLUSTER`, `IDP_*`)
- **`KEY_EXTRACTION_CONFIG_PATH`**: path to a workflow-shaped YAML config file

### How it runs in the workflow

In `cdf_key_extraction_aliasing_GEL` (v1):
- task `fn_dm_key_extraction_gel` runs this function and writes to:
  - `db_key_extraction/GEL_extracted_keys`
  - `db_key_extraction/key_extraction_state_GEL`

### Change history

See `modules/models/Contextualization/CHANGELOG.md`.

