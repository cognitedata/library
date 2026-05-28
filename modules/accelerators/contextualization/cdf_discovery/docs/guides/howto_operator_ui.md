# CDF Discovery — operator UI

Local read-only browser for CDF data and integration resources. Layout follows **SQL Server Management Studio / Enterprise Manager**: Discovery object tree, document tabs, and a Properties panel.

**Install and launch:** see [README — Operator UI](../../README.md#operator-ui). **API reference:** [Module specification — Operator API](../MODULE_SPECIFICATION.md#6-operator-api).

## Prerequisites

- Python 3.11+ with dependencies installed (see README)
- Node.js 18+
- Repository root `.env` with CDF credentials

## Navigation

### Object tree (left)

- **Connection** — project info; expand **Data** for **Saved Queries**, **RAW**, **Data Modeling** (data models and views), and **Classic**; **Integration** for **Workflows**, **Pipelines**, **Functions**, and **Transformations**; **Governance** for **Spaces** and **Groups**.
- **Classic** — Assets, Time Series, Files, Events, Sequences, Data Sets, Relationships, and Labels (**Open** / double-click → SQL query tab).
- **Data Modeling** — double-click a data model for a **diagram tab** (React Flow). Expand a model for its views; **Open** / double-click a view → SQL query tab seeded with `cdf_nodes(...)`.
- **RAW** — databases listed directly under the folder → tables (**Open** / double-click → SQL query tab for that table).
- **Integration** — **Workflows** (double-click opens a **task DAG** diagram); **Pipelines** (browse-only); **Functions** (double-click opens definition tab); **Transformations** (double-click opens SQL and definition tab).

The tree **lazy-loads**: expand a node to fetch its children from the API. Use the filter box to narrow labels. Right-click for **Favorite** / **Unfavorite**, **Open** / **Query** (where applicable), or **Refresh** (reloads that subtree).

### Favorites

Copy `discovery.config.template.yaml` to `discovery.local.config.yaml` in the module folder (gitignored) and add tree node ids under `stars.node_ids`, or use **Favorite** in the context menu (saved to the local file). Favorite nodes appear at the **top** of their sibling list (★ prefix); other siblings stay in A–Z order.

### Saved Queries

Under **Data** → **Saved Queries** in the object tree, named SQL queries are stored in `discovery.local.config.yaml` (`saved_queries.queries`). Use **Save** to update the query linked to the current tab, or **Save As** to name a new entry. Double-click a saved query to open it in a SQL tab. Right-click a saved query and choose **Delete** to remove it from the list (and close any open tab for that query).

### Workspace (open tabs)

Open document tabs and the active tab id are saved under `workspace` in `discovery.local.config.yaml` (debounced while you work). On launch, the UI restores tabs and refetches data model / workflow / transformation / function content. SQL tabs restore query text and run limits; result grids are not persisted.

### Document tabs (center)

- **SQL query tabs** — transformation SQL preview (**Run** / Ctrl+Enter), **Result limit** (unlimited source reads), and client-side **page size** / **Previous** / **Next** on results. **Export** downloads the current result set as JSON, YAML, CSV, Excel, or Parquet.
- **File content query tabs** — from a **Files** or **CogniteFile** metadata result, select a parquet, CSV, or JSON/NDJSON row and choose **Query file** (toolbar or Properties panel). Opens a tab seeded with `SELECT * FROM data` that runs **locally via DuckDB** (SELECT-only; not CDF Transformations). Downloaded files are cached under `.cache/file_content/` in the module folder.
- **Data model tabs** — React Flow diagram with view search, zoom controls, and a **View properties** panel on the right when you select a node.
- **Workflow tabs** — React Flow diagram of tasks and `dependsOn` edges (latest version when none is specified), with task search and a **Task properties** side panel.
- **Transformation tabs** — double-click a transformation in the tree to load its SQL query (editable, **Run** preview) and an expandable **Transformation definition** section.
- **Function tabs** — double-click a function under **Integration → Functions** to load its definition JSON.
- **Pipeline canvas (Preview node)** — attach a **Preview** node to any upstream stage; on **local run** it snapshots cohort rows into stable RAW (`parameters.preview_raw_table_key`, default `etl_preview`). Double-click the preview node to open a SQL tab filtered by `RUN_ID` and `PREVIEW_NODE_ID`.

### Transform (Fusion) workflow canvas

Under **Fusion** → **Transform** in the object tree:

- **Workflows** — pipeline instances (`transform/workflow_definitions/instances/`). Double-click to open the canvas tab.
- **Templates** — reusable definitions (`transform/workflow_definitions/templates/`).

**Canvas toolbar**

- **Palette** (left) — drag query, transform, score, save, and orchestration nodes onto the graph; connect handles to wire the DAG.
- **Layout** — auto-layout, fit view, edge style, and handle orientation (left→right or top→bottom).
- **History** — undo and redo canvas edits.
- **Build** — compile the canvas to Toolkit YAML under `workflows/` (same as `python module.py transform build`).
- **Run locally** — execute the DAG with status in the toolbar (`module.py transform run`).
- **Optimize** — review suggested merges for sequential transform chains or parallel score/transform siblings; approve or decline, then apply.

**Node editing**

- Select a node to configure it in the inspector (handlers, fields, filters, save targets).
- Right-click or the node toolbar: **Copy** / **Paste**, **Merge** multi-step transforms or score nodes, **Explode** combined nodes back into separate steps.
- **Preview** nodes are canvas-only (not deployed); use them to snapshot upstream cohort rows for SQL inspection.

Authoring paths and CLI: [transform/docs/BUILD.md](../../transform/docs/BUILD.md), [transform/docs/LOCAL_RUN.md](../../transform/docs/LOCAL_RUN.md).

### Properties (bottom)

- Select a **tree node** for object metadata, or a **SQL result row** for column values.
- Read-only payloads use a **Structured** name/value inspector (expand nested objects and arrays). Toggle **JSON** for raw monospace output; the choice is remembered in the browser.
- **Fusion → Data Modeling → Containers** tree nodes load the full container schema from CDF on selection (`properties`, `indexes`, `constraints`).
- **All nodes** / **All edges** tree folders show a reference payload with a seeded query hint; run the query and select a row for full node or edge instance detail.
- **Node/edge SQL rows** from `cdf_nodes()`, `cdf_edges()`, or view-scoped node queries load full CDF instance detail when selected.
- Data model **view** details appear in the diagram’s right panel; workflow **task** details appear in the workflow sidebar (same structured viewer).
- When a selected row is an uploaded **parquet**, **CSV**, or **JSON/NDJSON** CDF File, use **Query file** to open a DuckDB query tab against the file content.

Drag the handle above Properties to resize; **Collapse** hides the panel.

### SQL Query (toolbar)

**SQL Query** opens a blank workspace tab. Tree **Open** reuses or creates a tab per object (`sql:classic:assets`, `sql:raw:db:table`, etc.) with seeded `SELECT` text.

Preview uses the CDF [run query](https://docs.cognite.com/20230101-beta/query/run-query) API. Requires `transformationsAcl:READ`. Limits are API fields only—the editor SQL is not auto-modified.

**Editor and execution**

- Syntax-highlighted SQL editor (CodeMirror). **Format SQL** uses Spark SQL for CDF tabs and standard SQL for file-content (DuckDB) tabs.
- **Run** (`Ctrl+Enter` / `Cmd+Enter`): runs the full query, or the non-empty editor selection if one is highlighted.
- **Run selection** (`Shift+Ctrl+Enter` or toolbar): same selection rules as Run.
- **Cancel** stops an in-flight request (CDF or DuckDB).
- CDF tabs only: optional **Source limit** and **Timeout (sec)** map to `source_limit` and `timeout` on `POST /api/cdf/sql/run` (persisted in workspace and saved queries).
- Status shows row count and elapsed milliseconds after a successful run.

**Results**

- Collapsible **Schema** panel (column names and types from the API).
- **Filter results** searches all columns client-side; pagination applies after sort and filter.
- **Copy row** / **Copy results** (TSV, including header for full results) for paste into Excel.
- Double-click a column header to copy the column name.
- Export (JSON, YAML, CSV, Excel, Parquet) uses sorted, filtered rows (not only the current page).

**File content**

- From a metadata query, select a parquet/CSV/JSON file row and use **Query file** for a DuckDB tab (`POST /api/cdf/file-content/sql/run`). Source limit and timeout do not apply there.

## Security

No authentication. Bind to localhost only. Do not expose the API on untrusted networks. See [Module specification — Security](../MODULE_SPECIFICATION.md#7-security-and-nfrs).
