# ETL transform stage

The **transform** canvas node applies string transforms to cohort row properties using the same config contract as discovery aliasing (no runtime import from `cdf_discovery_aliasing`).

## Config keys

| Key | Purpose |
|-----|---------|
| `fields[]` | `field_name`, optional `regex`, `regex_options`, `max_matches_per_field`, `priority` |
| `output_template` | `{field}` substitution with delimiter cleanup |
| `output_field` | Target property on the row (`aliases`, `indexKey`, …) |
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
