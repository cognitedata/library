# Inverted Index CDF Functions

Deployable Cognite Functions for [`cdf_inverted_index_contextualization`](../README.md). Registry: [`functions.Function.yaml`](functions.Function.yaml).

## Functions

| externalId | Handler | Library entry point |
|------------|---------|---------------------|
| `fn_idx_build_metadata` | [`fn_idx_build_metadata/handler.py`](fn_idx_build_metadata/handler.py) | `build_metadata_index` |
| `fn_idx_build_annotations` | [`fn_idx_build_annotations/handler.py`](fn_idx_build_annotations/handler.py) | `build_diagram_annotation_index` |
| `fn_idx_target_driven` | [`fn_idx_target_driven/handler.py`](fn_idx_target_driven/handler.py) | `process_target_driven_contextualization` / `run_target_driven_for_instance_ids` |
| `fn_idx_handle_subscription` | [`fn_idx_handle_subscription/handler.py`](fn_idx_handle_subscription/handler.py) | `handle_aliases_subscription_event` |
| `fn_idx_score` | [`fn_idx_score/handler.py`](fn_idx_score/handler.py) | `calculate_contextualization_score` |
| `fn_idx_deltas` | [`fn_idx_deltas/handler.py`](fn_idx_deltas/handler.py) | detection mode deltas |
| `fn_idx_upsert_detections` | [`fn_idx_upsert_detections/handler.py`](fn_idx_upsert_detections/handler.py) | `upsert_diagram_detections` |
| `fn_idx_index_metadata_instance` | [`fn_idx_index_metadata_instance/handler.py`](fn_idx_index_metadata_instance/handler.py) | `build_metadata_index_for_instance` |
| `fn_idx_virtual_tags` | [`fn_idx_virtual_tags/handler.py`](fn_idx_virtual_tags/handler.py) | `run_virtual_tag_creation` |

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
| `instance_external_id` / `instance_id` | target-driven | Single target instance (incremental) |
| `instance_external_ids` | target-driven | Comma-separated or JSON list of instance IDs (incremental batch) |
| `incoming_view_key` | target-driven | View key from `direct_relation_config.views` (e.g. `asset`, `file`) |
| `view_external_id` / `view_space` | target-driven, index-metadata-instance | DM view lookup when `incoming_view_key` is omitted |
| `query_property` | target-driven, subscription | Override `target_driven.query_property` (default `aliases`) |
| `force` | target-driven, subscription | Bypass cooldown dedupe (`terms_hash`) |
| `match_scope_key` / `match_scope_keys` | score, deltas, query, target-driven | Scope partition key(s) |
| `scope_lookup_override` | target-driven | When true with `match_scope_keys`, query those scopes instead of instance-resolved scope |
| `detection_mode` | build-annotations, upsert-detections | `standard`, `pattern`, or `all` (build only) |
| `write_mode` | upsert-detections, index-metadata-instance | `upsert` or `replace` (default `replace`) |
| `detections` | upsert-detections | JSON array of index-only detection dicts |
| `annotations` | upsert-detections | Optional edge-shaped annotation dicts |
| `view_external_id` | index-metadata-instance | DM view for `index_field_config` lookup |
| `delta_mode` | deltas | `both`, `pattern_not_standard`, `standard_not_pattern` |
| `event` | subscription | Instance subscription event (or pass event fields at top level) |
| `all_scopes` | virtual-tags | Scan every scope in the partition registry |
| `term_selection_mode` | virtual-tags | `all` or `missing_tags_only` (override config) |
| `limit` | virtual-tags | Max eligible terms to process (0 = no cap) |

`fn_idx_target_driven` requires `instance_external_id` or `instance_external_ids`. Fleet backfill (no instance IDs) is CLI/UI only — see `run_target_driven_backfill` in the function spec.

`fn_idx_virtual_tags` requires `all_scopes: true` or at least one `match_scope_key` / `match_scope_keys`. Set `virtual_tag_creation.enabled: true` in config (handler forces enabled when omitted).

## Local invoke

From module root (with `.env` credentials — see [module README](../README.md#prerequisites)):

```bash
python module.py whoami
python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
python module.py invoke-fn fn_idx_target_driven --data '{"instance_external_id":"ASSET_P101","incoming_view_key":"asset","dry_run":true}'
python module.py invoke-fn fn_idx_target_driven --data '{"instance_external_ids":["A1","A2"],"query_property":"aliases","force":false,"dry_run":true}'
python module.py invoke-fn fn_idx_upsert_detections --data '{"dry_run":true,"detection_mode":"pattern","file_external_id":"FILE_PID_12","detections":[{"file_external_id":"FILE_PID_12","text":"P-101A","page":1,"bbox":[0.1,0.2,0.3,0.4]}]}'
python module.py invoke-fn fn_idx_index_metadata_instance --data '{"dry_run":true,"instance_external_id":"EQ-1001","view_external_id":"CogniteEquipment"}'
python module.py invoke-fn fn_idx_virtual_tags --data '{"dry_run":true,"all_scopes":true,"term_selection_mode":"missing_tags_only"}'
```

## DM reads

Source enumeration and DM index lookups use **`instances.query`** with server-side filters (`inverted_index/dm_query.py`). RAW index storage (default) still uses `rows.retrieve` per term.

## Deploy

Register via Cognite Toolkit module [`module.toml`](../module.toml) `extra_resources`. Deploy with your project's `cdf build` / deploy pipeline for functions and dependent RAW/DM resources.
