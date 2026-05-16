# Workflow execution channel contracts

This document describes **data edges** between macro pipeline stages ([`workflow.execution.graph.yaml`](workflow.execution.graph.yaml)). Cognite Workflows do not stream arbitrary payloads between function tasks; handoffs use **RAW tables**, **`RUN_ID`**, **`workflow.input.configuration`**, and task `data` fields.

**Query / save variants:** When the canvas uses **`fn_dm_raw_query`** or **`fn_dm_classic_query`** instead of **`fn_dm_view_query`**, or **`fn_dm_raw_save`** / **`fn_dm_classic_save`** instead of **`fn_dm_view_save`**, the same RAW + `RUN_ID` contracts apply; only the Cognite APIs invoked by each stage differ. See [`functions/README.md`](../functions/README.md).

| Edge (from → to) | `channel` id | Contract |
|------------------|----------------|----------|
| `fn_dm_view_query` → `fn_dm_transform` | `query_sink_to_transform` | View query writes cohort rows / payloads to its discovery RAW sink; transform reads the same **`RUN_ID`** and predecessor task ids from **`compiled_workflow`** / task **`data`**. |
| `fn_dm_transform` → `fn_dm_validate` | `transform_to_validate` | Transform writes its sink RAW; validate reads predecessor RAW for the run. |
| `fn_dm_validate` → `fn_dm_view_save` | `validate_to_save` | Validated payloads are consumed by **`fn_dm_view_save`** for **`instances.apply`**. |
| `fn_dm_validate` → `fn_dm_inverted_index` | `validate_to_inverted_index` | Inverted index may consume predecessor task snapshots (IR **`upstream_compiled_task_ids`**) rather than a single fixed RAW column set. |
| `fn_dm_view_save` / `fn_dm_inverted_index` → `fn_dm_discovery_raw_cleanup` | `post_run_cleanup` | Cleanup uses **`run_id`** and configured tables to delete cohort keys or truncate state. |

Parallelism: **`fn_dm_inverted_index`** may run in parallel with branches that do not depend on its outputs; the compiled **`depends_on`** graph is authoritative.

Inside **`fn_dm_transform`**, rules are declared on canvas nodes under **`data.config`**; scope-level **`key_extraction`** / **`aliasing`** libraries are merged where referenced (see `cdf_fn_common.aliasing_rule_refs`).

See also: [`workflow_diagram.md`](workflow_diagram.md), [`workflows/README.md`](../workflows/README.md).
