# CDF Explorer — operator UI

Local read-only browser for CDF data and orchestration resources. Layout follows **SQL Server Management Studio / Enterprise Manager**: Object Explorer tree, document tabs, and a Properties panel.

## Prerequisites

- Python 3.11+ with [requirements.txt](../requirements.txt)
- Node.js 18+
- Repository root [`.env`](../../../../.env) with CDF credentials (same as cdf_discovery_aliasing)

## Start

```bash
export PYTHONPATH=.
pip install -r modules/accelerators/contextualization/cdf_explorer/requirements.txt
python modules/accelerators/contextualization/cdf_explorer/module.py ui
```

| Service | Default URL |
|---------|-------------|
| Vite UI | http://127.0.0.1:5193/ |
| FastAPI | http://127.0.0.1:8785/ |

Override ports: `python module.py ui --api-port 8785 --vite-port 5193`

Set `CDF_EXPLORER_ROOT` if the module path is non-standard.

## Navigation

### Object Explorer (left)

- **Connection** — project info; expand **Data** for **RAW**, **Data Modeling** (data models and views), **Classic**, and **Transformations**; **Orchestration** for **Workflows** and **Pipelines**; **Governance** for **Spaces** and **Groups**.
- **Classic** — Assets, Time Series, Files, Events, Sequences, Data Sets, Relationships, and Labels (**Open** / double-click → SQL query tab).
- **Data Modeling** — double-click a data model for a **diagram tab** (React Flow). Expand a model for its views; **Open** / double-click a view → SQL query tab seeded with `cdf_nodes(...)`.
- **RAW** — databases listed directly under the folder → tables (**Open** / double-click → SQL query tab for that table).
- **Transformations** — double-click opens a tab with the transformation SQL and definition.
- **Orchestration** — **Workflows** (double-click opens a **task DAG** diagram); **Pipelines** (browse-only); **Functions** (double-click opens definition tab).

The tree **lazy-loads**: expand a node to fetch its children from the API. Use the filter box to narrow labels. Right-click for **Favorite** / **Unfavorite**, **Open** / **Query** (where applicable), or **Refresh** (reloads that subtree).

### Favorites

Copy `default.config.yaml` to `explorer.local.config.yaml` in the module folder (gitignored) and add tree node ids under `stars.node_ids`, or use **Favorite** in the context menu (saved to the local file). Favorite nodes appear at the **top** of their sibling list (★ prefix); other siblings stay in A–Z order.

### Saved Queries

Under **Saved Queries** in the Object Explorer, named SQL queries are stored in `explorer.local.config.yaml` (`saved_queries.queries`). Use **Save** to update the query linked to the current tab, or **Save As** to name a new entry. Double-click a saved query to open it in a SQL tab. Right-click a saved query and choose **Delete** to remove it from the list (and close any open tab for that query).

### Workspace (open tabs)

Open document tabs and the active tab id are saved under `workspace` in `explorer.local.config.yaml` (debounced while you work). On launch, the UI restores tabs and refetches data model / workflow / transformation / function content. SQL tabs restore query text and run limits; result grids are not persisted.

### Document tabs (center)

- **SQL query tabs** — transformation SQL preview (**Run** / Ctrl+Enter), **Result limit** (unlimited source reads), and client-side **page size** / **Previous** / **Next** on results.
- **Data model tabs** — React Flow diagram with view search, zoom controls, and a **View properties** panel on the right when you select a node.
- **Workflow tabs** — React Flow diagram of tasks and `dependsOn` edges (latest version when none is specified), with task search and a **Task properties** side panel.
- **Transformation tabs** — double-click a transformation in the tree to load its SQL query (editable, **Run** preview) and an expandable **Transformation definition** section.
- **Function tabs** — double-click a function under **Orchestration → Functions** to load its definition JSON.

### Properties (bottom)

- Select a **tree node** for object metadata.
- Select a **tree node** or **SQL result row** for JSON detail (data model view details appear in the diagram’s right panel).

Drag the handle above Properties to resize; **Collapse** hides the panel.

### SQL Query (toolbar)

**SQL Query** opens a blank workspace tab. Explorer **Open** reuses or creates a tab per object (`sql:classic:assets`, `sql:raw:db:table`, etc.) with seeded `SELECT` text.

Preview uses the CDF [run query](https://docs.cognite.com/20230101-beta/query/run-query) API. Requires `transformationsAcl:READ`. Limits are API fields only—the editor SQL is not auto-modified.

## Operator API

The Vite UI talks to FastAPI on port **8785**. Endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/connection` | Project and auth mode |
| GET | `/api/cdf/explorer/config` | Operator config (`stars`, `workspace`) |
| PUT | `/api/cdf/explorer/config/stars` | Update starred node ids |
| PUT | `/api/cdf/explorer/config/workspace` | Update open tabs and active tab id |
| PUT | `/api/cdf/explorer/config/saved-queries` | Update saved SQL queries |
| GET | `/api/cdf/explorer/children?node_id=` | Lazy Object Explorer children |
| GET | `/api/cdf/data-modeling/data-model/graph` | Data model diagram (space, external_id, version) |
| GET | `/api/cdf/transformations/detail?id=` | Transformation query and definition |
| GET | `/api/cdf/functions/detail?id=` | Function definition (numeric id or external id) |
| GET | `/api/cdf/workflows/graph` | Workflow task DAG (`external_id`, optional `version`) |
| POST | `/api/cdf/sql/run` | SQL preview (`query`, `limit`, optional `source_limit`, …) |

## Security

No authentication. Bind to localhost only. Do not expose the API on untrusted networks.
