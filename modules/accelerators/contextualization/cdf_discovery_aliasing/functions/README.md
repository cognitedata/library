# Discovery CDF functions (`fn_dm_*`)

Cognite Functions for the **`key_extraction_aliasing`** workflow (**v5**). Toolkit **`externalId`** values and metadata live in [`functions.Function.yaml`](functions.Function.yaml).

## Canvas kinds → functions

The UI **`canvas`** compiles to tasks whose **`externalId`** matches the folder name under `functions/`.

| Canvas node kind | `externalId` | Role |
|------------------|--------------|------|
| `query_view` | `fn_dm_view_query` | DM `instances.list` → discovery RAW cohort (`RUN_ID`). |
| `query_raw` | `fn_dm_raw_query` | RAW-backed cohort reads → sink RAW. |
| `query_classic` | `fn_dm_classic_query` | Classic / non-DM cohort reads → sink RAW. |
| `transform` | `fn_dm_transform` | Extraction + aliasing transforms on RAW rows (handler registry). |
| `validation` | `fn_dm_validate` | Validation / confidence on RAW payloads. |
| `filter` | `fn_dm_filter` | Exclude cohort rows using query-style `filters` (and/or/not, operators). |
| `join` | `fn_dm_join` | Merge two predecessor cohorts (`in__left` / `in__right`). |
| `save_view` | `fn_dm_view_save` | Data model `instances.apply` (e.g. **`cdf_cdm:CogniteDescribable`** aliases). |
| `save_raw` | `fn_dm_raw_save` | RAW upsert from predecessor payloads. |
| `save_classic` | `fn_dm_classic_save` | Classic write path from predecessor payloads. |
| `inverted_index` | `fn_dm_inverted_index` | Expand FK/document reference strings → inverted-index RAW. |
| `discovery_raw_cleanup` | `fn_dm_discovery_raw_cleanup` | Post-run purge of inter-node cohort RAW (current `run_id` + rows older than 72h by default); optional `truncate_tables`. Does not touch inverted index or aliasing stores. |

**Shared library:** `cdf_fn_common/` — canvas → IR compile, scope trim, incremental scope, logging, merge helpers, workflow associations, etc.

## Local execution

`module.py run` resolves each function’s Python entry via **`discovery_local_pipeline_specs()`** in `cdf_fn_common/workflow_compile/canvas_dag.py` (`<externalId>.pipeline` module + exec kind).

## See also

- [Workflows README](../workflows/README.md) — v5 manifests, `workflow.input.configuration`, RAW handoff.
- [Documentation map](../docs/README.md)
- [Workflow diagram source](../workflow_template/workflow_diagram.md)
