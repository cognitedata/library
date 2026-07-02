# Quickstart — Inverted Index Contextualization

Get from zero to scoped index lookups and target-driven linking in a CDF project. This guide covers the **pilot defaults** (RAW index, `global` scope, CDM views). For full configuration and API detail, see [cdf_inverted_index_function_spec.md](cdf_inverted_index_function_spec.md). For the end-to-end pipeline diagram, see [target_driven_contextualization_flow.md](target_driven_contextualization_flow.md).

## What you are building

The module maintains an **inverted index** that maps terms (asset tags, file names, diagram detections) to DM references, then uses it for **target-driven contextualization**: when a real asset arrives with query terms (typically `aliases` from an external aliasing step), the index finds matching files and diagram detections and writes CDM links.

```text
Index build  →  inverted index (term + scope → file / metadata refs)
Aliasing     →  writes aliases on CogniteAsset
Target-driven →  scoped lookup → CogniteFile.assets, diagram annotations, etc.
```

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| CDF project with CDM instances | At minimum: files, assets, and/or diagram annotations in `cdf_cdm` (or your configured spaces) |
| Cognite Toolkit | Deploy functions, RAW database, and optional workflow trigger |
| `.env` at repo root or module dir | See [README](../README.md#prerequisites) for `COGNITE_*` / OAuth variables |
| External query terms (production) | e.g. [`cdf_discovery`](../../cdf_discovery/) — populates `aliases` before target-driven runs |

```bash
cd modules/accelerators/contextualization/cdf_inverted_index_contextualization
pip install -r requirements.txt
python module.py whoami
```

## 1. Configure the module

Copy or merge [`default.config.yaml`](../default.config.yaml) into your Toolkit project config. Key pilot defaults:

| Setting | OOTB value | Purpose |
|---------|------------|---------|
| `index_storage_backend` | `raw` | Postings in `db_contextualization_idx` |
| `scope.levels` | `[]` | Single `global` partition (no site/unit isolation) |
| `target_driven.query_property` | `aliases` | Property used for index lookup |
| `direct_relation_config.min_confidence` | `0.6` | Diagram hit threshold |

Adjust `index_field_config` if your tag patterns or views differ. Use **scope-specific extraction overrides** (`properties_by_scope` on each view) when regex rules differ by site/unit; the config editor supports merge/replace modes and wildcard scope keys. For multi-unit sites, enable scope from [`config/scope.example.yaml`](../config/scope.example.yaml) (see [step 6](#6-optional-enable-site--unit-scope)).

## 2. Deploy to CDF

Enable the module in your Toolkit config and deploy:

```bash
# From your Toolkit project root (with this module enabled)
cdf build
cdf deploy
```

Deployable resources (see [`module.toml`](../module.toml)):

- RAW database and partition registry
- CDF Functions (`fn_idx_build_metadata`, `fn_idx_build_annotations`, `fn_idx_target_driven`, `fn_idx_handle_subscription`, …)
- Optional: `wf_idx_target_driven_incremental` workflow + `dataModeling` trigger

Verify a function locally before deploy:

```bash
python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
```

## 3. Build the index

Run a full metadata and diagram annotation index build. Start with **dry run** to validate extraction counts.

```bash
python module.py build-metadata --dry-run
python module.py build-annotations --dry-run

# Live build
python module.py build-metadata
python module.py build-annotations
```

**What gets indexed (pilot config):**

- **Metadata** — regex tag / file-ref extraction from `name` and `description` on `CogniteFile`, `CogniteEquipment`, `CogniteTimeSeries`, and related views
- **Diagram annotations** — `CogniteDiagramAnnotation` edges (`startNodeText`, confidence, bbox, page); stored as file-as-reference rows

After upgrading from legacy index shapes, run `python module.py migrate` once (purge + rebuild).

### Steady-state index maintenance (production)

Deploy the shipped index maintenance workflows (see [README § Index maintenance wiring](../README.md#index-maintenance-wiring)):

1. **Event-driven metadata** — `wf_idx_source_metadata_incremental` + `dataModeling` trigger on all `index_field_config` views → `fn_idx_handle_source_metadata`
2. **Scheduled watermark** — `wf_idx_source_index_watermark` + cron trigger → `fn_idx_build_watermark_incremental` (metadata + annotation catch-up)

Merge [`workflows/source_index_incremental.config.yaml`](../workflows/source_index_incremental.config.yaml) into project config. Configure `source_index:` in [`default.config.yaml`](../default.config.yaml).

Alternatively, call `fn_idx_index_metadata_instance` or `fn_idx_upsert_detections` from upstream pipelines, or schedule `fn_idx_build_metadata` / `fn_idx_build_annotations` with `filter_updated_after`.

## 4. Verify with a query

Confirm terms from your data appear in the index:

```bash
python module.py query --terms P-101A --scope-key global
python module.py list-by-file --file-id MY_FILE --scope-key global
```

Structured output includes `hits` and optional `reuse_metrics`. Use `--hits-only` for scripting.

**Offline demo** (no CDF connection):

```bash
python module.py demo
```

## 5. Run target-driven contextualization

Target-driven does **not** run on raw asset ingest alone. It expects query terms on the instance (default: `aliases`), usually written by an aliasing pipeline **before** the trigger fires.

### Dry run (single asset)

```bash
python module.py target-driven \
  --instance-id ASSET_P101 \
  --type asset \
  --dry-run
```

If `aliases` is empty, try fallbacks or override the query property:

```bash
python module.py target-driven \
  --instance-id ASSET_P101 \
  --query-property name \
  --dry-run
```

Review output for `references_found`, `match_scope_key`, and skip reasons (`no_query_terms`, `scope_unresolved`, `scope_filtered`).

### Live run (single asset)

Remove `--dry-run` when results look correct:

```bash
python module.py target-driven --instance-id ASSET_P101 --type asset
```

Or invoke the deployed function:

```bash
python module.py invoke-fn fn_idx_target_driven \
  --data '{"instance_external_ids":["ASSET_P101"],"dry_run":false}'
```

### Fleet backfill (one-time)

After the index is populated, backfill links for all assets with query terms:

```bash
python module.py target-driven --dry-run --max-assets 50
python module.py target-driven --max-assets 500 --progress-interval 100
```

Omit `--instance-id` for fleet mode. Use `--force` to bypass cooldown dedupe. Prefer explicit instance IDs for steady-state incremental runs.

## 6. Wire the production trigger

Recommended path: deploy the incremental workflow and trigger shipped with the module.

1. Merge variables from [`workflows/target_driven_incremental.config.yaml`](../workflows/target_driven_incremental.config.yaml) into project config (`workflowClientId`, `workflowClientSecret`, batch settings).
2. Deploy `wf_idx_target_driven_incremental` + `dataModeling` WorkflowTrigger.
3. Ensure aliasing writes `aliases` (or your configured `watch_property`) on watched views (`CogniteAsset`, `CogniteFile` by default).

Flow:

```text
Asset updated → aliasing writes aliases → WorkflowTrigger → fn_idx_handle_subscription → target-driven
```

Local replay:

```bash
python module.py handle-subscription --event-file config/sample_subscription_event.json
```

Handler config: `subscription:` and `target_driven:` in [`default.config.yaml`](../default.config.yaml). See [`config/subscription.example.yaml`](../config/subscription.example.yaml).

## 7. Optional: enable site + unit scope

For multi-unit sites where the same tag string is reused, configure scope so index build and target-driven queries use the same `match_scope_key`:

1. Copy [`config/scope.example.yaml`](../config/scope.example.yaml) into project config under `scope:`.
2. Map `resolve_from` to your DM property paths (`sourceContext`, `sourceId`, etc.).
3. Set `strict_scope: true` and rebuild the index.
4. Target-driven resolves scope on the incoming asset automatically.

Details: [target_driven_contextualization_flow.md — Scope isolation](target_driven_contextualization_flow.md#scope-isolation-and-target-driven-matching).

Scoped CLI example:

```bash
python module.py target-driven \
  --instance-id ASSET_P101 \
  --scope-key 'site:Rotterdam|unit:U100' \
  --dry-run
```

## 8. Operator UI (local workbench)

For interactive config, builds, queries, and target-driven runs:

```bash
cd ui && npm install && cd ..
python module.py ui
```

- API: `http://127.0.0.1:8787`
- UI: `http://127.0.0.1:5195`

Trusted workstation only — no authentication on the local API.

## Checklist

| Step | Command / action | Done |
|------|------------------|------|
| Auth works | `python module.py whoami` | ☐ |
| Module deployed | `cdf deploy` | ☐ |
| Metadata index built | `python module.py build-metadata` | ☐ |
| Annotation index built | `python module.py build-annotations` | ☐ |
| Query returns hits | `python module.py query --terms … --scope-key global` | ☐ |
| Aliasing populates query property | External pipeline or manual `aliases` write | ☐ |
| Target-driven dry run OK | `python module.py target-driven --instance-id … --dry-run` | ☐ |
| Workflow trigger deployed | `wf_idx_target_driven_incremental` | ☐ |
| Scope configured (multi-unit) | `config/scope.example.yaml` merged | ☐ |

## Troubleshooting

| Symptom | Likely cause | What to try |
|---------|--------------|-------------|
| `references_found: 0` | Empty `aliases` / query property | Check aliasing; use `--query-property name` for testing |
| `reason: no_query_terms` | Primary + fallback properties empty | Populate `aliases` or adjust `target_driven.query_property_fallbacks` |
| `reason: scope_unresolved` | Strict scope enabled but site/unit missing on instance | Fix DM properties or use OOTB `global` scope for pilots |
| Hits in query but not target-driven | Confidence or source_type filter | Lower `min_confidence`; check `direct_relation_config.source_types` |
| Stale links after index rebuild | Index newer than last target-driven run | Re-run target-driven with `--force` |
| Cross-unit false positives | Scope disabled (`global` only) | Enable site + unit scope and rebuild index |

## Next steps

| Topic | Document |
|-------|----------|
| Full function spec and config schema | [cdf_inverted_index_function_spec.md](cdf_inverted_index_function_spec.md) |
| Pipeline flow and scope behaviour | [target_driven_contextualization_flow.md](target_driven_contextualization_flow.md) |
| CLI reference and CDF Functions | [README](../README.md) |
| Query terms upstream | [`cdf_discovery`](../../cdf_discovery/) |
| Virtual tag creation (UC4) | [README — Virtual tag creation](../README.md#virtual-tag-creation-uc4) |
| Unit tests | `python -m pytest tests/unit/ -q` |
