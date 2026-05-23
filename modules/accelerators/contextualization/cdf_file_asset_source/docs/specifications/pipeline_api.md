# Pipeline API — `cdf_file_asset_source`

Function boundaries, RAW storage, and workflow task chain. **Overview:** [MODULE_SPECIFICATION.md](../MODULE_SPECIFICATION.md).

---

## Workflow

**External id:** `create_asset_hierarchy_from_files` (from `workflow` in `default.config.yaml`).

| Order | Function | Task purpose |
| ----- | -------- | ------------ |
| 1 | `fn_dm_extract_assets_by_pattern` | Diagram/file pattern extraction → RAW results |
| 2 | `fn_dm_create_asset_hierarchy` | Match files to scope leaves, build hierarchy → RAW assets |
| 3 | `fn_dm_write_asset_hierarchy` | Upsert `CogniteAsset` instances to data modeling |

Manifests: [workflows/README.md](../../workflows/README.md). Sync trigger `input.configuration` with **`python module.py build`**.

---

## RAW storage (defaults)

| Key | Typical value | Used by |
| --- | ------------- | ------- |
| `raw_db` | `db_file_asset_extract` | All three steps |
| `raw_table_state` | `file_asset_extract_state` | Extract, create (incremental state) |
| `raw_table_results` | (optional / null) | Extract output when configured |
| `raw_table_assets` | `file_asset_extract_assets` | Create output, write input |
| `results_field` | `results` | Column/field name for extraction payloads |

Configure under `file_asset_source.<step>.parameters`.

---

## Function: `fn_dm_extract_assets_by_pattern`

**Inputs (config):** `file_asset_source.extract` — `patterns`, `instance_space`, `limit`, `batch_size`, `diagram_detect_config`, `mime_type`, …

**Behavior:**

- Lists CDF files matching filters
- Runs diagram detection / text extraction
- Pattern-matches asset tags; writes rows to RAW
- Tracks state in `raw_table_state`

**Deploy requirements:** see `functions/fn_dm_extract_assets_by_pattern/requirements.txt` (`cognite-sdk`, `requests`, `cognite-extractor-utils`, …).

---

## Function: `fn_dm_create_asset_hierarchy`

**Inputs:** `file_asset_source.create` — `scope_hierarchy` (via module config), `pattern_config_path`, `output_file`, `raw_table_assets`, …

**Behavior:**

- Reads extraction results from RAW
- Maps files on scope **leaf** nodes (`files: []`) to systems
- Builds hierarchical asset structure (optional ISA/classifier patterns)
- Writes asset rows to RAW and optional YAML (`output_file`)

---

## Function: `fn_dm_write_asset_hierarchy`

**Inputs:** `file_asset_source.write` — `hierarchy_file` or RAW assets table, `view_space` / `view_external_id` / `view_version`, `batch_size`, `dry_run`

**Behavior:**

- Loads hierarchy from RAW or file
- Batches upserts to CDF data modeling (`CogniteAsset` by default)

---

## Local runner

| CLI | Maps to |
| --- | ------- |
| `module.py run --step extract` | Extract only |
| `module.py run --step create` | Create only |
| `module.py run --step write` | Write only |
| `module.py run --step all` | Full pipeline |

Results: JSON under `local_run_results/` (`*_pipeline_extract.json`, etc.).

---

## Error handling

- Functions use structured logging (`log_level` per step)
- `max_attempts` on extract for transient failures
- `validate` CLI runs config schema checks and optional accelerator compliance gates before CDF calls

---

## Related

| Document | Contents |
| -------- | -------- |
| [config_schema.md](config_schema.md) | YAML fields |
| [patterns/validation_rules_examples.md](../../patterns/validation_rules_examples.md) | Tag validation examples |
