# Workflow execution channel contracts

This document describes **data edges** between macro pipeline stages ([`workflow.execution.graph.yaml`](workflow.execution.graph.yaml)). Cognite Workflows do not stream arbitrary payloads between function tasks; handoffs use **RAW tables**, **`RUN_ID`**, **`workflow.input.configuration`**, and task `data` fields.

| Edge (from → to) | `channel` id | Contract |
|------------------|----------------|----------|
| `fn_dm_incremental_state_update` → `fn_dm_key_extraction` | `cohort_raw_and_run_id` | Incremental task writes cohort rows to key-extraction RAW with `RUN_ID` and `WORKFLOW_STATUS`; key extraction reads `run_id` / `run_all` and scope via `configuration` (same as workflow task `data`). |
| `fn_dm_key_extraction` → `fn_dm_reference_index` | `key_extraction_raw_fk_doc_json` | Reference index reads key-extraction RAW rows for the run; consumes `FOREIGN_KEY_REFERENCES_JSON` / `DOCUMENT_REFERENCES_JSON` (and related columns) per scope `enable_reference_index`. |
| `fn_dm_key_extraction` → `fn_dm_aliasing` | `key_extraction_raw_candidate_keys` | Aliasing reads candidate keys from key-extraction RAW (and in local runs may use in-memory `entities_keys_extracted` mirroring that RAW shape). |
| `fn_dm_aliasing` → `fn_dm_alias_persistence` | `tag_aliasing_raw` | Persistence reads tag-aliasing RAW (and optional FK JSON from key-extraction RAW when configured) and writes to data model. |

Parallelism: after key extraction, **`fn_dm_reference_index`** and **`fn_dm_aliasing`** are independent (both depend only on `fn_dm_key_extraction`). Deployed CDF workflow runs them in parallel; the local runner executes them concurrently to match.

Inside **`fn_dm_aliasing`**, optional **`aliasing.config.data.pathways`** defines sequential and parallel *rule* execution; top-level **`aliasing_rule_definitions`** / **`aliasing_rule_sequences`** are expanded onto both **`extraction_rules[].aliasing_pipeline`** and pathway rule lists before the function runs (see `cdf_fn_common.aliasing_rule_refs`).

See also: [`workflow_diagram.md`](workflow_diagram.md), [`workflows/README.md`](../workflows/README.md).
