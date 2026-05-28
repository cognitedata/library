# Module specification — `cdf_discovery`

Canonical description of what the module does, its boundaries, configuration, API, and security. Procedural steps: [Operator UI guide](guides/howto_operator_ui.md). Install and launch: [Module README](../README.md).

---

## 1. Purpose and scope

### 1.1 Business intent

`cdf_discovery` is a **local workstation tool** for browsing Cognite Data Fusion (CDF) resources: data (RAW, data modeling, classic assets, records streams, saved queries), integration (workflows, pipelines, functions, transformations), and governance (live CDF spaces/groups plus **declared access control** config/build/artifacts). It provides SSMS-style navigation (Discovery object tree, document tabs, Properties panel) and SQL preview against CDF. Declared governance edits local Toolkit YAML only. CDF writes from the operator UI are limited to **Streams/Records** APIs (`/api/cdf/streams`, record ingest/upsert/delete) and SQL preview remains read-only otherwise.

### 1.2 Technical boundaries

| In scope | Out of scope |
| -------- | ------------ |
| Read-only CDF browse and SQL preview (except Streams/Records APIs and local ETL runs) | Hosted multi-user SaaS deployment |
| Declared access control (`governance/default.config.yaml`, build, generated `spaces/` / `auth/`) | Runtime Entra provisioning |
| Operator prefs (`discovery.local.config.yaml`) | Production CDF deploy orchestration beyond optional `transform deploy-scope` |
| ETL workflow authoring (`transform/workflow_definitions/`), build to `workflows/`, local DAG run | Replacing CDF Transformations product authoring |
| Cognite Functions under `functions/` (Toolkit YAML + Python handlers) | |
| FastAPI + Vite operator UI (Discovery tree, governance, Fusion / Transform canvas) | |
| Local DuckDB queries over downloaded file content | |

**Module type:** Toolkit utility (config family **D** in [ACCELERATOR_CONFIG_CONVENTIONS.md](../../ACCELERATOR_CONFIG_CONVENTIONS.md)).

---

## 2. Actors

| Actor | Role |
| ----- | ---- |
| **Data engineer / operator** | Explores CDF, runs ad hoc SQL preview, saves queries and workspace layout locally |
| **Maintainer** | Extends browse handlers and UI; no CDF deploy pipeline for this module |

---

## 3. Capabilities

- **Object tree** — lazy-loaded tree for Data, Integration, Governance; filter and favorites
- **Governance (declared)** — **Governance** node: scope hierarchy + dimensions; **Spaces** / **Groups** nodes: configure, scoped build, generated artifacts; live CDF leaves open detail tabs
- **SQL query tabs** — CDF run-query preview with export (JSON, YAML, CSV, Excel, Parquet)
- **File content queries** — DuckDB `SELECT` over parquet/CSV/JSON CDF Files (cached under `.cache/file_content/`)
- **Data model diagram** — React Flow graph for a data model; view properties panel
- **Workflow diagram** — task DAG from workflow version
- **Transformation / function detail** — definition tabs from tree double-click
- **Workspace persistence** — open tabs, active tab, saved queries, starred nodes in local config
- **Fusion / Transform canvas** — ETL pipeline and template editors (React Flow): palette, merge/explode/optimize, build, local run, preview nodes
- **ETL build** — `module.py transform build` emits Toolkit workflow YAML under `workflows/`

---

## 4. Configuration

| File | Role |
| ---- | ---- |
| `discovery.config.template.yaml` | Committed template (`stars`, `workspace`, `saved_queries`) |
| `discovery.local.config.yaml` | Gitignored operator prefs (copy from template) |
| `default.config.yaml` | ETL scope: `workflow`, `dataset`, `workflow_definitions` paths |
| `governance/default.config.yaml` | Declared spaces/groups build config |

Copy `discovery.config.template.yaml` → `discovery.local.config.yaml` for operator prefs. ETL authoring paths default to `transform/workflow_definitions/` (see `default.config.yaml`).

**Discovery tree roots:** `data` (Saved Queries at `data:sq`, RAW `raw`, Data Modeling `dm`, Classic `classic`), `integration` (Workflows `wf`, Pipelines `ep`, Functions `fn`, Transformations `tx`), `gov` (spaces, groups). Legacy starred ids (`sq`, `orch`, …) are migrated on load (`ui/server/tree_node_ids.py`).

---

## 5. CLI surface

| Command | Purpose |
| ------- | ------- |
| `module.py ui` | Start FastAPI + Vite dev server |
| `module.py build` | Generate Space/Group YAML under `governance/` (`--spaces-only` / `--groups-only`; compliance gates after write) |
| `module.py transform build` | Compile workflow canvas → `workflows/` (see `transform/docs/BUILD.md`) |
| `module.py transform run` | Local ETL DAG (`transform/local_runner/`; `--dry-run`, `--instance`, `--predecessor-mode`) |
| `module.py transform deploy-scope` | Deploy scoped workflows/functions to CDF |

Flags: `--api-host`, `--api-port` (default **8785**), `--vite-port` (default **5193**), `--no-browser`, `--no-reload`. Env: `CDF_DISCOVERY_ROOT`.

---

## 6. Operator API

FastAPI app: `ui.server.main:app`. Base URL default `http://127.0.0.1:8785`.

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/health` | Health check (`{ "ok": true }`) |
| GET | `/api/connection` | Project and auth mode |
| GET | `/api/cdf/discovery/config` | Operator config (`stars`, `workspace`, `saved_queries`) |
| PUT | `/api/cdf/discovery/config/stars` | Update starred node ids |
| PUT | `/api/cdf/discovery/config/workspace` | Update open tabs and active tab id |
| PUT | `/api/cdf/discovery/config/saved-queries` | Update saved SQL queries |
| GET | `/api/cdf/discovery/children?node_id=` | Lazy discovery tree children |
| GET | `/api/cdf/data-modeling/data-model/graph` | Data model diagram (space, external_id, version) |
| GET | `/api/cdf/transformations/detail?id=` | Transformation query and definition |
| GET | `/api/cdf/functions/detail?id=` | Function definition |
| GET | `/api/cdf/workflows/graph` | Workflow task DAG |
| POST | `/api/cdf/sql/run` | SQL preview (`query`, `limit`, optional `source_limit`, `timeout`) |
| POST | `/api/cdf/file-content/sql/run` | DuckDB query on CDF File content |
| GET | `/api/cdf/governance/spaces/detail?space=` | Live space metadata |
| GET | `/api/cdf/governance/groups/detail?id=` | Live group capabilities |
| GET/PUT | `/api/governance/declared/config/model` | Declared `default.config.yaml` document |
| POST | `/api/governance/declared/config/mirror` | Mirror config slice to env files |
| POST | `/api/governance/declared/build` | Build (`target`: `spaces` \| `groups` \| `all`) |
| GET | `/api/governance/declared/artifacts?kind=` | List generated YAML paths |
| GET/PUT | `/api/governance/declared/file?rel=` | Read/write generated artifact |

Declared root: `CDF_DISCOVERY_GOVERNANCE_ROOT` or `governance.declared_root` in `discovery.local.config.yaml` (default: `governance/` for config and templates; generated `spaces/` and `auth/` at module root).

Vite proxies `/api` to the API via `VITE_API_PROXY` (set by `module.py ui`).

---

## 7. Security and NFRs

- **No API authentication** — intended for `127.0.0.1` on a trusted workstation only
- **Read-only browse** toward CDF except Streams/Records APIs, optional `transform deploy-scope`, and local ETL runs that may write cohort RAW when using cohort predecessor mode
- **Credentials** — repository-root `.env` (`COGNITE_*` / `CDF_*` / `IDP_*`), same as other accelerators
- SQL preview requires CDF ACLs (e.g. `transformationsAcl:READ` for run-query)

---

## 8. Dependencies (summary)

Python: see [requirements.txt](../requirements.txt) (`cognite-sdk`, `fastapi`, `uvicorn`, `duckdb`, `pyyaml`, …). Operator UI requires **Node.js 18+** and `npm` ([ui/package.json](../ui/package.json)). Full table: [README — Dependencies](../README.md#dependencies).
