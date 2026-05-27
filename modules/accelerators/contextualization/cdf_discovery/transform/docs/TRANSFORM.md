# ETL transform stage

The **transform** canvas node applies string transforms to cohort row properties using the same config contract as discovery aliasing (no runtime import from `cdf_discovery_aliasing`).

## Config keys

| Key | Purpose |
|-----|---------|
| `fields[]` | `field_name`, optional `regex`, `regex_options`, `max_matches_per_field`, `priority` |
| `output_template` | `{field}` substitution with delimiter cleanup |
| `output_field` | Target property on the row (required) |
| `output_mode` | `append` or `overwrite` |
| `output_field_type` | `auto`, `string`, `int`, `float`, `bool`, `list`, `object`, `json` |
| `handler_id` | Handler id (see catalog below) |
| `{handler_id}` | Handler-specific block (e.g. `trim_whitespace`, `regex_substitution`) |
| `steps[]` | Multi-step pipeline |
| `execution.mode` | `ordered` or `parallel` |
| `field_policies` | Parallel merge policies (`merge_list`, `tie_break`) |

## Handler catalog

**Core:** `regex_substitution`, `leading_zero_normalize`, `sequential_literal_replace`, `substitution_variants`

**ELT:** `trim_whitespace`, `change_case`, `coerce_scalar`, `default_if_empty`, `split_string`, `split_join`, `parse_json_extract`, `format_datetime`, `hash_stable`, `mask_string`, `static_lookup_map`, `heuristic_sampler`

## Python modules

- Engine: `functions/cdf_fn_common/etl_transform/`
- Facade: `functions/cdf_fn_common/etl_transform_api.py`
- Handler: `functions/fn_etl_transform/handler.py`
- Steps / merge: `etl_pipeline_steps.py`, `etl_property_merge.py`

## Predecessor handoff (in-memory vs cohort RAW)

| Mode | When | Behavior |
|------|------|----------|
| `in_memory` | Default for `local_runner` | Upstream tasks set `_predecessor_rows`; transform reads/writes that buffer |
| `cohort` | Deployed workflows; local `--predecessor-mode cohort` | Query tasks flush cohort rows to RAW; transform reads predecessors via `etl_transform_orchestration` |

Resolution order: `data.local_predecessor_mode` → `configuration.parameters.local_predecessor_mode` → `ETL_LOCAL_PREDECESSOR_MODE` → task `_use_cohort_predecessors` / `_use_in_memory_predecessors` → `ETL_TRANSFORM_IN_MEMORY` (`0` → cohort).

```bash
python -m local_runner.run --predecessor-mode cohort --instance discovery_etl_default
```

## Local execution

```bash
cd modules/accelerators/contextualization/cdf_discovery/transform
python -m pytest tests/unit/test_etl_transform_handlers.py tests/unit/test_fn_etl_transform.py -q
```

## UI

Transform nodes in the discovery ETL flow editor use structured fields (`EtlTransformNodeConfigFields`) — handler, fields, regex, output template, multi-step.

## Scoring vs transform

Use the **`score`** node (not `transform`) for `scoring_rules` / validation-style scoring. See [SCORING.md](./SCORING.md).

## File annotation and fan-out planner

| Canvas kind | Function | Role |
|-------------|----------|------|
| **`file_annotation`** | `fn_etl_file_annotation` | Diagram detect on wired **`in__entities`** + **`in__files`** |
| **`workflow_fanout_plan`** | `fn_etl_workflow_fanout_plan` | Profile-driven dynamic task generator (`fanout_profile`, v1: `file_annotation`) |
| **`dynamic_fanout`** | — | Expands planner `tasks[]` at run time |

Wire **`in__input_a`** (search context cohort) and **`in__input_b`** (files to scan) on the fan-out planner. Files must be explicit — no project-wide file listing.

Building blocks: `etl_file_annotation/` (entities, files, packing, run_pack, sink, state), `etl_fanout_plan/profiles/file_annotation.py`.

**JSON mapping (CDF `jsonMapping`)**: Canvas `json_mapping` nodes compile to native workflow tasks (`type: jsonMapping`) with `parameters.jsonMapping.input` and `expression` (Kuiper). Use `${upstream_task_id.output}` in `input` for cross-task references. Local runs use the same engine via [`cognite-kuiper`](https://pypi.org/project/cognite-kuiper/) (`pip install -r requirements-local.txt`).

**Diagram annotation mapping**: `mapper_kind` `diagram_detect_to_dm` / `diagram_detect_to_classic` deploy as `jsonMapping` (default expression `input.rows`). Locally, cohort rows are loaded from the wired predecessor, expanded to staging rows, evaluated with Kuiper, then materialized into the task cohort RAW table for `save_raw`. Python reference: `etl_annotation_map/` (row expansion), `kuiper_eval.py`, `etl_json_mapping_sink.py`.

## Records and streams

Canvas kinds `query_records`, `save_records`, and `save_stream` use the CDF Streams/Records REST API (not Transformations SQL). See [RECORDS_STREAMS.md](RECORDS_STREAMS.md).

## RAW cleanup (`raw_cleanup` / `end`)

Post-run **`fn_etl_raw_cleanup`** drops ephemeral per-run cohort RAW tables (`{base_table_key}__{run_segment}__{canvas_node}`) created during the pipeline. It does **not** delete stable tables such as `{base}__incremental` (watermarks / hashes) or `{base}__file_state`.

| Config key | Default | Purpose |
|------------|---------|---------|
| `purge_stale` | `true` | Also delete other runs’ node tables older than `retention_hours` |
| `retention_hours` | `72` | Stale-run age threshold |
| `raw_tables` | — | Optional list of `{raw_db, raw_table_key}` overrides |
| `dry_run` | `false` | Skip deletes; handler returns `status: ok` |

Python: `cdf_fn_common/etl_raw_purge.py`, `etl_run_retention.py`, `fn_etl_raw_cleanup/handler.py`.

Deployed workflows use **`onFailure: skipTask`** for cleanup (see `workflow_task_policy.py`). Rebuild workflow YAML after policy changes so generated `WorkflowVersion` tasks pick up the policy.
