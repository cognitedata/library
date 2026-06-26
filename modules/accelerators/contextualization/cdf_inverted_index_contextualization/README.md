# Inverted Index Contextualization

Implementation per [cdf_inverted_index_function_spec.md](docs/cdf_inverted_index_function_spec.md). **RAW** scoped postings index; DM source reads use **`instances.query`** with server-side filters; contextualization outputs via CDM data modeling.

## CDF Functions

Deployable handlers live under [`functions/`](functions/) (`functions.Function.yaml`):

| externalId | Purpose |
|------------|---------|
| `fn_idx_build_metadata` | Metadata index build |
| `fn_idx_build_annotations` | Diagram annotation index build |
| `fn_idx_target_driven` | Target-driven contextualization |
| `fn_idx_handle_subscription` | `aliases` subscription handler |
| `fn_idx_score` | File contextualization score |
| `fn_idx_deltas` | Pattern vs standard detection deltas |

Local invoke: `python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'`

## Pilot configuration (current defaults)

| Decision | Setting |
|----------|---------|
| Index sources | `CogniteFile`, `CogniteEquipment`, `CogniteTimeSeries` — `name` + `description` (regex tag extraction; see `default.config.yaml`) |
| Index storage | **RAW** (`db_contextualization_idx`) |
| Scope | **Disabled OOTB** → `global` partition; see `config/scope.example.yaml` |
| Annotations | CDM **edge** `CogniteDiagramAnnotation` — `startNodeText`, `confidence`, `status`, `startNodePageNumber`, bbox via `startNode*Min/Max` |
| Target-driven trigger | **Instance subscription** on `aliases` changes — `inverted_index/subscription.py` |
| CDM writes | **Suggested** and **Approved** annotations (`allowed_annotation_statuses`); per-link `write_modes` default to `direct_relation` only |

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

Local workbench (FastAPI + Vite) for configuration, index builds, queries, file scoring, target-driven runs, and tag-reuse audits:

```bash
pip install -r requirements.txt
cd ui && npm install && cd ..
python module.py ui
```

- API default: `http://127.0.0.1:8786`
- UI default: `http://127.0.0.1:5194`
- Use `--no-browser` to skip opening a tab.
- **Trusted workstation only** — the local API has no authentication.
- Long-running operations stream progress over SSE (`/api/inverted-index/*/stream`) and support cooperative cancel.
- Workspace state persists in `.ui_workspace.json` (config path, last-open panes).

## CLI

```bash
python module.py build-metadata
python module.py build-annotations [--file-id FILE]
python module.py migrate   # purge RAW partitions + full rebuild (post file-as-reference upgrade)

# Single-scope query (structured JSON with reuse_metrics)
python module.py query --terms P-101A --scope-key global

# Multi-scope query — detect tag reuse across units
python module.py query --terms P-101A --scope-key 'site:Rotterdam|unit:U100' --scope-key 'site:Rotterdam|unit:U200'
python module.py query --terms P-101A --all-scopes

# Index-wide cross-scope duplicate audit (admin scan)
python module.py tag-reuse-audit --all-scopes
python module.py tag-reuse-audit --scope-key 'site:A|unit:1' --scope-key 'site:A|unit:2' --min-scope-count 2

python module.py target-driven --instance-id ASSET_P101 --type asset --dry-run
# Batch all assets (omit --instance-id); optional caps and progress on stderr
python module.py target-driven --dry-run --max-assets 50 --progress-interval 100
python module.py target-driven --scope-key 'site:A|unit:1' --scope-override --dry-run

python module.py score --file-id MY_FILE
python module.py list-by-file --file-id MY_FILE [--scope-key global]
python module.py deltas --file-id MY_FILE
python module.py handle-subscription --event-file config/sample_subscription_event.json
python module.py invoke-fn fn_idx_build_metadata --data '{"dry_run":true}'
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

After `cdf_discovery_aliasing` writes `aliases` on `CogniteAsset`, a CDF instance subscription should invoke:

```python
from inverted_index.subscription import handle_aliases_subscription_event
handle_aliases_subscription_event(client, event)
```

See `config/subscription.example.yaml` for event shape and dedupe behaviour.

## Tests

```bash
python -m pytest tests/unit/ -q
```
