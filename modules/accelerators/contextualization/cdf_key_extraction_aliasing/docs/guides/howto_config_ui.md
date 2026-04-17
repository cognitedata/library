# How to build configuration with the UI

The module ships a **local operator UI** (React + Vite) and a small **FastAPI** server under **`ui/server/`**. Together they load and save YAML under the **module root**, run **`module.py build`**, and invoke **`module.py run`** with the same flags you would use on the CLI. The usual way to start both is **`python …/module.py ui`** (see §2.1 below).

**This is not a hosted product:** there is **no authentication**. Run it only on a **trusted workstation** with localhost access. For file-only workflows, see [How to build configuration with YAML](howto_config_yaml.md).

**Prerequisites:** Node.js 18+ (for `npm run dev`), Python 3.11+ with this module’s dependencies (FastAPI, uvicorn, PyYAML), and **`.env`** at the repository root for **`module.py run`** (see [Quickstart](howto_quickstart.md)).

---

## 1. Module root resolution

The API server resolves the module directory as:

1. Environment variable **`CDF_KEY_EXTRACTION_ALIASING_ROOT`**, if set, or  
2. The **`cdf_key_extraction_aliasing`** package directory (parent of **`ui/server/`**).

Set **`CDF_KEY_EXTRACTION_ALIASING_ROOT`** when your checkout path is unusual or you need to point at a specific clone.

---

## 2. Start the API and the frontend

### 2.1 Recommended: `python module.py ui`

From the **repository root** (the directory that contains `modules/`), with **`PYTHONPATH=.`** so imports resolve:

```bash
export PYTHONPATH=.
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py ui
```

This **single command** starts both processes:

- **FastAPI** operator API (uvicorn on **`ui.server.main:app`**, default **`127.0.0.1:8765`**)
- **Vite** dev server (default **`127.0.0.1:5173`**), with **`VITE_API_PROXY`** set automatically so **`/api`** calls reach the API

It runs **`npm install`** in **`ui/`** on first use if **`node_modules/`** is missing, then opens a **browser tab** to the Vite URL (unless you pass **`--no-browser`**). **Ctrl+C** stops both servers.

Useful flags (see **`python module.py ui --help`**):

| Flag | Purpose |
| ---- | ------- |
| **`--api-host`**, **`--api-port`** | API bind address and port (defaults `127.0.0.1`, `8765`) |
| **`--vite-port`** | Vite dev server port (default `5173`) |
| **`--no-browser`** | Do not open a browser tab |
| **`--no-reload`** | Disable uvicorn `--reload` on the API |

If another app already uses **8765**, run for example **`… module.py ui --api-port 8766 --vite-port 5173`** so the subprocess sets **`VITE_API_PROXY`** to match.

### 2.2 Manual: API and Vite in separate terminals

Use **two terminals** when you prefer to run **`uvicorn`** or **`npm run dev`** yourself (same defaults as above).

**Terminal A — API (default port 8765):**

```bash
cd modules/accelerators/contextualization/cdf_key_extraction_aliasing
export PYTHONPATH=/path/to/repo/root   # repository root containing modules/
python -m ui.server.main
```

Override port: **`PORT=8766`** (or another free port).

**Terminal B — Vite dev server (default port 5173):**

```bash
cd modules/accelerators/contextualization/cdf_key_extraction_aliasing/ui
npm install   # first time
npm run dev
```

Open **`http://127.0.0.1:5173`** (or the URL Vite prints). The Vite config **proxies `/api/*`** to **`http://127.0.0.1:8765`**. If the API uses another port, set:

```bash
export VITE_API_PROXY=http://127.0.0.1:8766
npm run dev
```

---

## 3. What the UI edits

The UI persists **real files** under the module root:

| Area | Typical files | Purpose |
| ---- | --------------- | ------- |
| **Scope** | **`default.config.yaml`** | Hierarchy (**`aliasing_scope_hierarchy`**), Toolkit-style module fields, schedules. |
| **Configure** | **`workflow.local.config.yaml`**, **`workflow_template/workflow.template.config.yaml`**, or a selected **`workflows/.../*.WorkflowTrigger.yaml`** | v1 **scope** (`source_views`, `key_extraction`, `aliasing`) for local/template/trigger-specific edits. |
| **Build** | Invokes **`module.py build`** | Creates missing workflow YAML; shows stdout/stderr. |
| **Run** | Invokes **`module.py run --config-path …`** | Targets: **workflow local**, **workflow template**, or a **WorkflowTrigger** (see below). |
| **Artifacts** | Browse/edit **`workflows/**/*.yaml`** | Review generated manifests; save writes **YAML only** (validated parse). |

**Forms + YAML:** Sub-panels expose structured editors (source views, key extraction parameters, aliasing, hierarchy) and **raw YAML** for the same document where applicable. Saving updates the file on disk.

---

## 4. Sync scope ↔ workflow template

The API exposes helpers to keep **`workflow.local.config.yaml`** and **`workflow_template/workflow.template.config.yaml`** aligned:

- **From scope → template:** copies the current scope document over the template file (for the next **`module.py build`** seed).
- **From template → scope:** overwrites the scope file with the template.

Use these after you finalize rules in one place so generated **WorkflowTrigger** **`input.configuration`** matches what you run locally.

---

## 5. Run pipeline from the UI

The **Run** action calls the backend **`POST /api/run`** with:

- **`target`:** `workflow_local` (default **`workflow.local.config.yaml`**), `workflow_template` (**`workflow_template/workflow.template.config.yaml`**), or `workflow_trigger` (a **`WorkflowTrigger`** path under **`workflows/`**).
- **`workflow_trigger_rel`:** required when **`target`** is **`workflow_trigger`** — extracts **`input.configuration`** into **`.operator_run_scope.yaml`** and runs against that snapshot.
- **`run_all`:** when true, appends **`--all`** to **`module.py run`** (full filtered scope under incremental settings—see [module README](../../README.md#incremental-cohort-processing-raw-cohort-cdm-state)).

**Credentials** are **not** stored in YAML: the subprocess inherits your **environment** (`.env` loaded per your shell / quickstart). If the run fails with auth errors, fix **`.env`** and retry.

Results of **`module.py run`** still appear as JSON under **`tests/results/`** on disk; the UI shows **stdout/stderr** in the run log panel.

---

## 6. Production build of the UI (optional)

```bash
cd ui && npm run build
```

Static assets land under **`ui/dist/`**. Serving them is team-specific; for development, prefer **`module.py ui`** or run **`npm run dev`** with the API separately (§2).

---

## Related links

| Topic | Document |
| ----- | -------- |
| YAML-first workflow | [How to build configuration with YAML](howto_config_yaml.md) |
| CLI flags and outputs | [Quickstart](howto_quickstart.md), [module README](../../README.md) |
| Multi-scope deploy | [Scoped deployment](howto_scoped_deployment.md) |
