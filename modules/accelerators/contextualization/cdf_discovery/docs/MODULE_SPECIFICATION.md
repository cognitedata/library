# Module specification — `cdf_discovery`

Canonical description of what the module does, its boundaries, configuration, API, and security. Procedural steps: [Operator UI guide](guides/howto_operator_ui.md). Install and launch: [Module README](../README.md).

---

## 1. Purpose and scope

### 1.1 Business intent

`cdf_discovery` is a **local workstation tool** for browsing Cognite Data Fusion (CDF) resources: data (RAW, data modeling, classic assets, saved queries), integration (workflows, pipelines, functions, transformations), and governance (live CDF spaces/groups plus **declared access control** config/build/artifacts). It provides SSMS-style navigation (Discovery object tree, document tabs, Properties panel) and SQL preview against CDF. Declared governance edits local Toolkit YAML only; CDF remains read-only except for SQL preview.

### 1.2 Technical boundaries

| In scope | Out of scope |
| -------- | ------------ |
| Read-only CDF browse and SQL preview | Writing or deploying CDF resources from this module |
| Declared access control (offline `default.config.yaml`, build, generated Space/Group YAML) | Runtime Entra provisioning or CDF ACL writes |
| Operator prefs (`discovery.local.config.yaml`) | Multi-user auth or hosted SaaS deployment |
| Local DuckDB queries over downloaded file content | CDF Transformations authoring/deploy |
| FastAPI + Vite operator UI | `functions/`, `workflows/`, generated Space/Group YAML |

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

---

## 4. Configuration

| File | Role |
| ---- | ---- |
| `discovery.config.template.yaml` | Committed template (`stars`, `workspace`, `saved_queries`) |
| `discovery.local.config.yaml` | Gitignored operator prefs (copy from template) |

Template file: `discovery.config.template.yaml`. Copy to gitignored `discovery.local.config.yaml` for operator prefs.

**Discovery tree roots:** `data` (Saved Queries at `data:sq`, RAW `raw`, Data Modeling `dm`, Classic `classic`), `integration` (Workflows `wf`, Pipelines `ep`, Functions `fn`, Transformations `tx`), `gov` (spaces, groups). Legacy starred ids (`sq`, `orch`, …) are migrated on load (`ui/server/tree_node_ids.py`).

---

## 5. CLI surface

| Command | Purpose |
| ------- | ------- |
| `module.py ui` | Start FastAPI + Vite dev server |
| `module.py build` | Generate Space/Group YAML under `governance/` (`--spaces-only` / `--groups-only`; compliance gates after write) |

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

Declared root: `CDF_DISCOVERY_GOVERNANCE_ROOT` or `governance.declared_root` in `discovery.local.config.yaml` (default: `governance/` under this module — config, templates, generated `spaces/` and `auth/`).

Vite proxies `/api` to the API via `VITE_API_PROXY` (set by `module.py ui`).

---

## 7. Security and NFRs

- **No API authentication** — intended for `127.0.0.1` on a trusted workstation only
- **Read-only** toward CDF (preview APIs; no module-driven writes)
- **Credentials** — repository-root `.env` (`COGNITE_*` / `CDF_*` / `IDP_*`), same as other accelerators
- SQL preview requires CDF ACLs (e.g. `transformationsAcl:READ` for run-query)

---

## 8. Dependencies (summary)

Python: see [requirements.txt](../requirements.txt) (`cognite-sdk`, `fastapi`, `uvicorn`, `duckdb`, `pyyaml`, …). Operator UI requires **Node.js 18+** and `npm` ([ui/package.json](../ui/package.json)). Full table: [README — Dependencies](../README.md#dependencies).
