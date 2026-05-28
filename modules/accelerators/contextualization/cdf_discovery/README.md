# CDF Discovery

**Toolkit utility module** — operator UI and local governance build output under `governance/` (`spaces/`, `auth/`). No CDF deploy from Discovery itself.

Local browser for CDF **Data** (RAW, Data Modeling, Classic, saved queries), **Integration** (Workflows, Pipelines, Functions, Transformations), and **Governance** (live CDF spaces/groups plus declared config, scoped build, and generated Space/Group YAML under `spaces/` and `auth/`). Document tabs cover SQL preview, data model diagrams, workflow DAGs, transformation detail, and governance workspaces. CDF API access is read-only except SQL preview. Declared governance config defaults to `governance/` (`CDF_DISCOVERY_GOVERNANCE_ROOT` or `governance.declared_root` in `discovery.local.config.yaml` can override). Uses repository-root `.env` credentials (`COGNITE_*` / `CDF_*` / `IDP_*`). When client-credentials are not set, toolkit-style interactive login (`LOGIN_FLOW=interactive`, `IDP_TENANT_ID`, `IDP_CLIENT_ID`, `CDF_PROJECT`, `CDF_CLUSTER`) opens a browser sign-in.

Config: `discovery.config.template.yaml` → copy to `discovery.local.config.yaml`.

**Full documentation:** [docs/README.md](docs/README.md) · [Module specification](docs/MODULE_SPECIFICATION.md)

## Install

From the **repository root** (the directory that contains `modules/`):

```bash
export PYTHONPATH=.
pip install -r modules/accelerators/contextualization/cdf_discovery/requirements.txt
```

## Dependencies

| Category | Packages / tools | In `requirements.txt`? |
|----------|------------------|------------------------|
| Python runtime | 3.11+; `cognite-sdk`, `python-dotenv`, `fastapi`, `uvicorn[standard]`, `pydantic`, `pyyaml`, `duckdb` | Yes |
| Operator UI | Node.js 18+, `npm`; React, Vite, CodeMirror, `@xyflow/react`, `xlsx`, `hyparquet` — see [ui/package.json](ui/package.json) | No |
| Credentials | Repo-root `.env` | No |
| Dev / CI | `pytest`; `npm run i18n:check` | No |

## `module.py` CLI

| Command | Purpose |
| ------- | ------- |
| `ui` | Start FastAPI + Vite operator UI |
| `build` | Generate Space/Group YAML via `governance_build` (`--spaces-only` / `--groups-only`; runs compliance gates after write) |
| `transform build` | Compile workflow canvas YAML → Toolkit artifacts under `workflows/` |
| `transform run` | Local DAG run (`transform/local_runner/`; `--dry-run`, `--instance`, `--predecessor-mode`) |
| `transform deploy-scope` | Deploy scoped workflow/functions to CDF (requires credentials) |

```bash
python modules/accelerators/contextualization/cdf_discovery/module.py build [--config governance/default.config.yaml] [--dry-run] [--force]
python modules/accelerators/contextualization/cdf_discovery/module.py build --clean [--yes]
python modules/accelerators/contextualization/cdf_discovery/module.py transform build --workflow discovery_etl_default
python modules/accelerators/contextualization/cdf_discovery/module.py transform run --instance discovery_etl_default --dry-run
```

Flags: `--api-host`, `--api-port`, `--vite-port`, `--no-browser`, `--no-reload`. Run `python module.py` with no args for help.

## ETL layout

| Path | Role |
| ---- | ---- |
| `default.config.yaml` | ETL scope (`workflow`, `dataset`, workflow_definitions paths) |
| `functions/` | Cognite Functions (`cdf_fn_common/`, `fn_etl_*`) |
| `transform/workflow_definitions/` | Authoring: instances, templates, `registry.yaml` |
| `workflows/` | Generated Workflow / WorkflowVersion / WorkflowTrigger / config YAML |
| `data_sets/ds_discovery_etl.DataSet.yaml` | Toolkit DataSet for ETL resources |
| `transform/docs/` | Build, local run, transform stage, scoring, DM query |

See [transform/docs/BUILD.md](transform/docs/BUILD.md) and [workflows/README.md](workflows/README.md).

## Operator UI

```bash
export PYTHONPATH=.
python modules/accelerators/contextualization/cdf_discovery/module.py ui
```

| Service | Default URL |
|---------|-------------|
| Vite UI | http://127.0.0.1:5193/ |
| FastAPI | http://127.0.0.1:8785/ |

Override ports: `python .../module.py ui --api-port 8785 --vite-port 5193`. Set `CDF_DISCOVERY_ROOT` if the module path is non-standard.

**Security:** no authentication on the operator API. Run only on a trusted workstation (`127.0.0.1`). See [docs/MODULE_SPECIFICATION.md](docs/MODULE_SPECIFICATION.md#7-security-and-nfrs).

**Navigation:** [docs/guides/howto_operator_ui.md](docs/guides/howto_operator_ui.md)

Other modules’ default ports: [Accelerators README](../../README.md#dev-port-matrix).

## Documentation

| Document | Contents |
| -------- | -------- |
| [docs/README.md](docs/README.md) | Documentation index |
| [docs/MODULE_SPECIFICATION.md](docs/MODULE_SPECIFICATION.md) | Canonical spec and API |
| [docs/guides/howto_operator_ui.md](docs/guides/howto_operator_ui.md) | Operator UI procedures |
| [transform/docs/BUILD.md](transform/docs/BUILD.md) | Workflow build and deploy layout |
| [transform/docs/LOCAL_RUN.md](transform/docs/LOCAL_RUN.md) | Local DAG runner |
| [OPERATOR_UI_STANDARD.md](../../OPERATOR_UI_STANDARD.md) | Shared UI conventions |
