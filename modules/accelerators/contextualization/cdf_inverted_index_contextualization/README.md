# Inverted Index Contextualization

Implementation per [cdf_inverted_index_function_spec.md](docs/cdf_inverted_index_function_spec.md). See [target_driven_contextualization_flow.md](docs/target_driven_contextualization_flow.md) for an end-to-end flow diagram (index build → matching → links → virtual tags). **RAW** scoped postings index; DM source reads use **`instances.query`** with server-side filters; contextualization outputs via CDM data modeling.

## CDF Functions

Deployable handlers live under [`functions/`](functions/) (`functions.Function.yaml`):

| externalId | Purpose |
|------------|---------|
| `fn_idx_build_metadata` | Metadata index build |
| `fn_idx_build_annotations` | Diagram annotation index build |
| `fn_idx_target_driven` | Target-driven contextualization |
| `fn_idx_handle_subscription` | `aliases` / `watch_property` subscription handler |
| `fn_idx_score` | File contextualization score |
| `fn_idx_deltas` | Pattern vs standard detection deltas |
| `fn_idx_upsert_detections` | Incremental diagram detection index writes |
| `fn_idx_index_metadata_instance` | Incremental metadata index for one or more DM instances |
| `fn_idx_virtual_tags` | Virtual CogniteAsset tag creation from scoped index terms (UC4) |

Local invoke: `python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'`

## Pilot configuration (current defaults)

| Decision | Setting |
|----------|---------|
| Index sources | `CogniteFile`, `CogniteEquipment`, `CogniteTimeSeries` — `name` + `description` (regex tag extraction; see `default.config.yaml`) |
| Index storage | **RAW** (`db_contextualization_idx`) |
| Scope | **Disabled OOTB** → `global` partition; see `config/scope.example.yaml` |
| Annotations | CDM **edge** `CogniteDiagramAnnotation` — `startNodeText`, `confidence`, `status`, `startNodePageNumber`, bbox via `startNode*Min/Max` |
| Target-driven trigger | **Instance subscription** on configured `watch_property` (default `aliases`) — `inverted_index/subscription.py` |
| CDM writes | **Suggested** and **Approved** annotations (`allowed_annotation_statuses`); per-link `write_modes` default to `direct_relation` only |
| Virtual tag creation (UC4) | **Disabled** OOTB (`virtual_tag_creation.enabled: false`); requires `scope.enabled` when enabled |

**Index migration:** Diagram index entries now use `reference_type: CogniteFile` (annotation id in `additional_metadata`). Rebuild metadata and annotation indexes after upgrading — legacy `CogniteDiagramAnnotation` reference rows are not read.

## Prerequisites

Place a `.env` file at the **repo root** or in this module directory. `python module.py whoami` verifies the connection.

**API key auth**

| Variable | Aliases |
|----------|---------|
| `COGNITE_PROJECT` | `CDF_PROJECT`, `PROJECT` |
| `COGNITE_BASE_URL` | `CDF_URL`, `CDF_BASE_URL`, `BASE_URL` — or set `CDF_CLUSTER` (e.g. `greenfield`) |
| `COGNITE_API_KEY` | `CDF_API_KEY`, `API_KEY` |

**OAuth (client credentials)**

| Variable | Aliases |
|----------|---------|
| `COGNITE_PROJECT` | `CDF_PROJECT`, `PROJECT` |
| `COGNITE_BASE_URL` / `CDF_CLUSTER` | as above |
| `COGNITE_TENANT_ID` | `IDP_TENANT_ID`, `TENANT_ID` |
| `COGNITE_CLIENT_ID` | `IDP_CLIENT_ID`, `CLIENT_ID` |
| `COGNITE_CLIENT_SECRET` | `IDP_CLIENT_SECRET`, `CLIENT_SECRET` |
| `COGNITE_TOKEN_URL` | `IDP_TOKEN_URL`, `TOKEN_URL` |
| `COGNITE_SCOPES` | `IDP_SCOPES`, `SCOPES` (space-separated; defaults to `{base_url}/.default`) |

```bash
pip install -r requirements.txt
python module.py whoami
```

## Operator UI

Local workbench (FastAPI + Vite) for configuration, index builds, queries, file scoring, target-driven runs, virtual tag batch runs (CLI), and tag-reuse audits:

```bash
pip install -r requirements.txt
cd ui && npm install && cd ..
python module.py ui
```

- API default: `http://127.0.0.1:8787`
- UI default: `http://127.0.0.1:5195`
- Use `--no-browser` to skip opening a tab.
- **Trusted workstation only** — the local API has no authentication.
- Long-running operations stream progress over SSE (`/api/inverted-index/*/stream`) and support cooperative cancel.
- Workspace state persists in `.ui_workspace.json` (config path, last-open panes).

## CLI

```bash
python module.py build-metadata
python module.py build-annotations [--file-id FILE]
python module.py migrate   # purge RAW partitions + full rebuild (post file-as-reference upgrade)
python module.py partition-health   # row counts + reshard recommendations (term sub-partitioning)
python module.py reshard-scope --scope-key 'site:Rotterdam|unit:U100'   # unified → term buckets

# Single-scope query (structured JSON with reuse_metrics)
python module.py query --terms P-101A --scope-key global

# Multi-scope query — detect tag reuse across units
python module.py query --terms P-101A --scope-key 'site:Rotterdam|unit:U100' --scope-key 'site:Rotterdam|unit:U200'
python module.py query --terms P-101A --all-scopes

# Index-wide cross-scope duplicate audit (admin scan)
python module.py tag-reuse-audit --all-scopes
python module.py tag-reuse-audit --scope-key 'site:A|unit:1' --scope-key 'site:A|unit:2' --min-scope-count 2

python module.py virtual-tags --all-scopes --dry-run
python module.py virtual-tags --scope-key 'site:Rotterdam|unit:U100' --term-selection-mode missing_tags_only

python module.py target-driven --instance-id ASSET_P101 --type asset --dry-run
python module.py target-driven --instance-id ASSET_P101 --query-property name --force --dry-run
# Fleet backfill only (omit --instance-id); steady state should pass explicit instance IDs
python module.py target-driven --dry-run --max-assets 50 --progress-interval 100
python module.py target-driven --scope-key 'site:A|unit:1' --scope-override --dry-run
python module.py invoke-fn fn_idx_target_driven --data '{"instance_external_ids":["ASSET_P101"],"dry_run":true}'
python module.py invoke-fn fn_idx_virtual_tags --data '{"dry_run":true,"all_scopes":true}'
```

### Virtual tag creation (UC4)

Synthesize virtual `CogniteAsset` instances from scoped inverted-index terms (`asset_metadata` and `diagram_annotation_pattern` only). Structural hierarchy nodes follow `scope.levels` (e.g. site → unit); leaf `asset_tag` nodes hold detected tag text with `aliases` for downstream target-driven contextualization.

**Enable**

1. Set `scope.enabled: true` with `levels` and `resolve_from` (see `config/scope.example.yaml`).
2. In `default.config.yaml`, set `virtual_tag_creation.enabled: true`.
3. Optionally tune `term_selection_mode`:
   - `missing_tags_only` (default) — diagram-detected terms with no matching real `CogniteAsset`
   - `all` — every eligible term in the scope partition
4. Run batch: `python module.py virtual-tags --all-scopes` or schedule `fn_idx_virtual_tags`.
5. Incremental: keep `incremental_enabled: true` so index upserts (`build-*`, `fn_idx_upsert_detections`, `fn_idx_index_metadata_instance`) materialize virtual tags for touched terms.

**Example hierarchy** for scope `site:Rotterdam|unit:U100` and term `P-101A`:

| Node | External ID |
|------|-------------|
| Site | `site_rotterdam` |
| Unit | `unit_rotterdam_u100` |
| Virtual tag leaf | `asset_tag_rotterdam_u100_p101a` |

Virtual assets are written to `virtual_tag_creation.instance_space` (default `inst_virtual_tags`). Wire UC3 (`fn_idx_target_driven` or subscription on `aliases`) to contextualize them after creation.

See [spec §3.5](docs/cdf_inverted_index_function_spec.md) for full config schema and `missing_tag_criteria`.

### Target-driven execution profiles

| Profile | When | How |
|---------|------|-----|
| **Incremental** (default) | Aliases or other query property updated on one or more instances | `fn_idx_handle_subscription`, `fn_idx_target_driven` with `instance_external_ids`, or `--instance-id` |
| **Backfill** (one-time) | Initial link population after index build | Omit `--instance-id` (fleet scan with `Exists(aliases)` or fallback properties) |

Configure query terms via `target_driven` in `default.config.yaml` (default: `aliases` with `name` fallback). Set `exclude_empty_aliases: true` to skip instances without aliases. Override per run with `--query-property`. Use `--force` to bypass cooldown dedupe. Subscription `watch_property` should match the deployed CDF subscription filter (defaults to `query_property` when omitted).

```bash
python module.py score --file-id MY_FILE
python module.py list-by-file --file-id MY_FILE [--scope-key global]
python module.py deltas --file-id MY_FILE
python module.py handle-subscription --event-file config/sample_subscription_event.json
python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
python module.py invoke-fn fn_idx_index_metadata_instance --data '{"dry_run":true,"instance_external_ids":["EQ-1001","EQ-1002"],"view_external_id":"CogniteEquipment"}'
python module.py demo   # offline memory backend
```

### Query output

`module.py query` returns structured JSON:

```json
{
  "scopes_queried": ["site:Rotterdam|unit:U100", "site:Rotterdam|unit:U200"],
  "terms_queried": ["p101a"],
  "hits": [ ... ],
  "reuse_metrics": {
    "terms_with_hits": 1,
    "cross_scope_duplicate_count": 1,
    "cross_scope_duplicate_rate": 1.0,
    "by_term": [ ... ]
  }
}
```

Use `--hits-only` for a flat hit list (script compatibility). Use `--reuse-only` to filter `by_term` to cross-scope duplicates only.

Cross-scope query is for **reuse analysis and ops** — target-driven contextualization still uses single-scope lookups to avoid false positives when tags are reused across units.

## Enabling site + unit scope

Copy `config/scope.example.yaml` into `default.config.yaml` under `scope:` and set `enabled: true` once your DM views expose site/unit fields.

## Subscription wiring

After `cdf_discovery_aliasing` (or equivalent) writes the configured query property on `CogniteAsset`, a CDF instance subscription filtered to `subscription.watch_property` (default `aliases`) should invoke:

```python
from inverted_index.subscription import handle_aliases_subscription_event
handle_aliases_subscription_event(client, event)
```

See `config/subscription.example.yaml` for `target_driven` / `subscription` config, event shape, and dedupe behaviour (`terms_hash` cooldown; pass `force=True` to bypass).

## Tests

```bash
python -m pytest tests/unit/ -q
```
