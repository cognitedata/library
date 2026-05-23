# How to build configuration with the UI

The module ships a **local operator UI** (React + Vite) and a small **FastAPI** server under **`ui/server/`**. Together they load and save **`default.config.yaml`** on disk, validate configuration, and invoke **`module.py run`** with the same steps you would use on the CLI.

**This is not a hosted product:** there is **no authentication**. Run it only on a **trusted workstation** with localhost access. For file-only workflows, edit **`default.config.yaml`** directly or use **`python module.py validate`**.

**Prerequisites:** See [README — Dependencies](../../README.md#dependencies). **`.env`** at the repository root is required for **`module.py run`**.

The UI provides **light/dark theme**, **multi-language labels**, **form editors** for scope and patterns, and an **Advanced YAML** fullscreen editor with glob search.

---

## 1. Module root resolution

The API server resolves the module directory as:

1. Environment variable **`CDF_FILE_ASSET_SOURCE_ROOT`**, if set, or  
2. The **`cdf_file_asset_source`** package directory (parent of **`ui/server/`**).

Set **`CDF_FILE_ASSET_SOURCE_ROOT`** when your checkout path is unusual.

---

## 2. Start the API and the frontend

### 2.1 Recommended: `python module.py ui`

**Install and launch:** [README — Operator UI](../../README.md#operator-ui).

This **single command** starts FastAPI and Vite. The command runs **`npm install`** in **`ui/`** on first use if **`node_modules/`** is missing, then opens a **browser tab** (unless **`--no-browser`**). **Ctrl+C** stops both servers.

Useful flags (see **`python module.py ui --help`**):

| Flag | Purpose |
| ---- | ------- |
| **`--api-host`**, **`--api-port`** | API bind address and port (defaults `127.0.0.1`, `8770`) |
| **`--vite-port`** | Vite dev server port (default `5188`) |
| **`--no-browser`** | Do not open a browser tab |
| **`--no-reload`** | Disable uvicorn `--reload` on the API |

If port **8770** is taken:

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py ui --api-port 8771 --vite-port 5189
```

Vite receives **`VITE_API_PROXY`** and **`VITE_API_BASE`** automatically. Streamed run logs use **`VITE_API_BASE`** so the browser talks to FastAPI directly (avoids dev-proxy buffering).

### 2.2 Manual: API and Vite in separate terminals

**Terminal A — API:**

```bash
cd modules/accelerators/contextualization/cdf_file_asset_source
export PYTHONPATH=/path/to/repo/root:/path/to/module:/path/to/module/functions
python -m uvicorn ui.server.main:app --host 127.0.0.1 --port 8770
```

**Terminal B — Vite:**

```bash
cd modules/accelerators/contextualization/cdf_file_asset_source/ui
npm install
export VITE_API_PROXY=http://127.0.0.1:8770
npm run dev -- --host 127.0.0.1 --port 5188
```

Open **http://127.0.0.1:5188/**.

---

## 3. What the UI edits

The UI persists **real files** under the module root:

| Tab | Files | Purpose |
| --- | ----- | ------- |
| **Configure** | `default.config.yaml` (`file_asset_source.*`) | Form editors for scope and extract patterns; **Advanced YAML** for write parameters and full file |
| **Run** | — | Invokes **`module.py run`** for extract, create, write, or all |
| **Results** | `local_run_results/*.json` | JSON snapshots from local runs |

### Form editors (Configure tab)

| Step | Form fields | Sync |
| -------- | ----------- | ---- |
| **Scope** | Hierarchy tree, per-leaf file list, flat document catalog (add / bulk paste / move); other create parameters via Advanced YAML | `file_asset_source.create` |
| **Extract** | Pattern groups: category, resourceType/subType, standard, samples (one per line) | Merged into `file_asset_source.extract.data.patterns` |

**Save** writes the full **`default.config.yaml`**. Edit **`file_asset_source.write`** in **Advanced YAML** below the form editors.

**Industry templates** (`config.template.*.yaml`) are not edited in the UI; copy them manually, then merge scope and patterns into **`file_asset_source.create`** and **`file_asset_source.extract`**.

### Step roles

1. **Extract** — pattern matching on diagram files, RAW state  
2. **Create** — `hierarchy_levels`, scope tree, optional classifier path  
3. **Write** — push assets to CDF data modeling  

For CDF deploy, run **`python module.py build`** after config changes to sync the workflow trigger `input.configuration`. See **`workflows/create_asset_hierarchy_from_files.Workflow.yaml`**.

---

## 4. Validate configuration

- **UI:** **Validate all** on the Configure tab → **`POST /api/validate`**  
- **CLI:**

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py validate
python modules/accelerators/contextualization/cdf_file_asset_source/module.py validate --step create
python modules/accelerators/contextualization/cdf_file_asset_source/module.py build --check
```

Validation uses **`functions/shared/utils/config_validator.py`** (extract and create configs; write config gets basic checks).

---

## 5. Run pipeline from the UI or CLI

**UI buttons** call **`POST /api/run-stream`** for live NDJSON logs (task start/end + forwarded logger output), with fallback to **`POST /api/run`** on Windows.

**CLI:**

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py run --step extract
python modules/accelerators/contextualization/cdf_file_asset_source/module.py run --step all
```

**Credentials** are **not** stored in YAML: the subprocess inherits your **environment** (`.env`). If a run fails with auth errors, fix **`.env`** and retry.

Results are written under **`local_run_results/`** with a **`run_scope`** object, for example:

```json
{ "run_scope": { "target": "pipeline_extract" } }
```

The **Results** tab lists recent JSON files and previews their contents.

**Scripts** (same runner):

- `scripts/run_extract_assets_by_pattern.py`
- `scripts/run_create_asset_hierarchy.py`
- `scripts/run_write_asset_hierarchy.py`
- `scripts/run_asset_hierarchy_workflow.py`

---

## 6. Production build of the UI (optional)

```bash
cd ui && npm run build
```

Static assets land under **`ui/dist/`**. For development, prefer **`module.py ui`**.

---

## 7. Roadmap

Implemented: scope/patterns form editors, workflow DAG preview, theme, localization, Advanced YAML editor.

Planned:

- Industry template import into create/extract forms  
- In-UI browse of Cognite files / datasets (read-only)  
- Workflow trigger / deploy actions from the operator UI  

---

## Related links

| Topic | Document |
| ----- | -------- |
| Quick start (YAML) | [howto_quickstart.md](howto_quickstart.md) |
| Configuration fields | [config_schema.md](../specifications/config_schema.md) |
| Module spec | [MODULE_SPECIFICATION.md](../MODULE_SPECIFICATION.md) |
| Configuration fields | [config_schema.md](../specifications/config_schema.md) |
| Module overview | [README.md](../../README.md) |
