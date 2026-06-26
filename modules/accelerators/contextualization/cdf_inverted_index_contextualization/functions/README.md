# Inverted Index CDF Functions

Deployable Cognite Functions for [`cdf_inverted_index_contextualization`](../README.md). Registry: [`functions.Function.yaml`](functions.Function.yaml).

## Functions

| externalId | Handler | Library entry point |
|------------|---------|---------------------|
| `fn_idx_build_metadata` | [`fn_idx_build_metadata/handler.py`](fn_idx_build_metadata/handler.py) | `build_metadata_index` |
| `fn_idx_build_annotations` | [`fn_idx_build_annotations/handler.py`](fn_idx_build_annotations/handler.py) | `build_diagram_annotation_index` |
| `fn_idx_target_driven` | [`fn_idx_target_driven/handler.py`](fn_idx_target_driven/handler.py) | `process_target_driven_contextualization` |
| `fn_idx_handle_subscription` | [`fn_idx_handle_subscription/handler.py`](fn_idx_handle_subscription/handler.py) | `handle_aliases_subscription_event` |
| `fn_idx_score` | [`fn_idx_score/handler.py`](fn_idx_score/handler.py) | `calculate_contextualization_score` |
| `fn_idx_deltas` | [`fn_idx_deltas/handler.py`](fn_idx_deltas/handler.py) | detection mode deltas |

Dataset: `ds_inverted_index_all`. Runtime: `py311`.

## Handler payload

Handlers accept a JSON `data` dict. Defaults come from [`default.config.yaml`](../default.config.yaml) via `cdf_fn_common.fn_runtime.resolve_handler_payload`.

Common fields:

| Field | Used by | Description |
|-------|---------|-------------|
| `dry_run` | build, target-driven, subscription | Skip writes |
| `filter_updated_after` | build handlers | ISO timestamp; incremental watermark on `lastUpdatedTime` |
| `instance_spaces` | build handlers | Restrict DM query to spaces |
| `file_external_id` / `file_id` | score, deltas | Target file |
| `instance_external_id` / `instance_id` | target-driven | Target asset/file/equipment/timeseries |
| `instance_type` | target-driven | `asset`, `file`, `equipment`, `timeseries` |
| `match_scope_key` | score, deltas, query | Scope partition key |
| `detection_mode` | build-annotations | `standard`, `pattern`, or `all` |
| `delta_mode` | deltas | `both`, `pattern_not_standard`, `standard_not_pattern` |
| `event` | subscription | Instance subscription event (or pass event fields at top level) |

## Local invoke

From module root (with `.env` credentials — see [module README](../README.md#prerequisites)):

```bash
python module.py whoami
python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
python module.py invoke-fn fn_idx_target_driven --data '{"instance_external_id":"ASSET_P101","dry_run":true}'
```

## DM reads

Source enumeration and DM index lookups use **`instances.query`** with server-side filters (`inverted_index/dm_query.py`). RAW index storage (default) still uses `rows.retrieve` per term.

## Deploy

Register via Cognite Toolkit module [`module.toml`](../module.toml) `extra_resources`. Deploy with your project's `cdf build` / deploy pipeline for functions and dependent RAW/DM resources.
