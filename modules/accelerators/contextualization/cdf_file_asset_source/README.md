# Asset hierarchy from files (`cdf_file_asset_source`)

Toolkit module that extracts asset tags from CDF diagram files, builds a hierarchical structure from `scope_hierarchy`, and writes assets to CDF data modeling.

**Full documentation:** [docs/README.md](docs/README.md) · [Module specification](docs/MODULE_SPECIFICATION.md)

## Install

From the **repository root**:

```bash
export PYTHONPATH=.
pip install -r modules/accelerators/contextualization/cdf_file_asset_source/requirements.txt
```

## Dependencies

| Category | Packages / tools | In `requirements.txt`? |
|----------|------------------|------------------------|
| Python runtime | 3.11+; `cognite-sdk`, `python-dotenv`, `fastapi`, `uvicorn[standard]`, `pydantic`, `pyyaml` | Yes |
| Operator UI | Node.js 18+, `npm`; see [ui/package.json](ui/package.json) | No |
| Credentials | Repo-root `.env` | No |
| CDF Functions deploy | Per-function `functions/fn_dm_*/requirements.txt` (e.g. `requests`, `cognite-extractor-utils` on extract) | No |
| Toolkit deploy | Cognite Toolkit (`cdf build` / deploy) | No |
| Dev / CI | `pytest`, `pytest-mock`; `run_module_compliance_gates.py` on `validate` | No |

## `module.py` CLI

| Command | Purpose | Guide |
| ------- | ------- | ----- |
| `ui` | Operator UI (FastAPI + Vite) | [docs/guides/howto_config_ui.md](docs/guides/howto_config_ui.md) |
| `validate` | Validate `default.config.yaml` | [docs/guides/howto_quickstart.md](docs/guides/howto_quickstart.md) |
| `build` | Sync workflow trigger configuration | [workflows/README.md](workflows/README.md) |
| `run` | Local pipeline (`--step extract\|create\|write\|all`) | [docs/guides/howto_quickstart.md](docs/guides/howto_quickstart.md) |

```bash
python modules/accelerators/contextualization/cdf_file_asset_source/module.py validate
python modules/accelerators/contextualization/cdf_file_asset_source/module.py build
python modules/accelerators/contextualization/cdf_file_asset_source/module.py run --step all
```

## Operator UI

```bash
export PYTHONPATH=.
python modules/accelerators/contextualization/cdf_file_asset_source/module.py ui
```

| Service | Default URL |
|---------|-------------|
| FastAPI | http://127.0.0.1:8770/ |
| Vite UI | http://127.0.0.1:5188/ |

Flags: `--no-browser`, `--no-reload`, `--api-port`, `--vite-port`. Env: `CDF_FILE_ASSET_SOURCE_ROOT`.

**Security:** no API authentication — localhost only.

Other modules’ ports: [Accelerators README](../../README.md#dev-port-matrix).

## Pipeline

| Function | Role |
| -------- | ---- |
| `fn_dm_extract_assets_by_pattern` | Extract tags from files → RAW |
| `fn_dm_create_asset_hierarchy` | Build hierarchy from scope + extraction |
| `fn_dm_write_asset_hierarchy` | Write `CogniteAsset` to instance space |

Workflow: `create_asset_hierarchy_from_files`. Details: [docs/specifications/pipeline_api.md](docs/specifications/pipeline_api.md).

## Configuration

- **Production:** [default.config.yaml](default.config.yaml) — `file_asset_source.extract|create|write`, `scope_hierarchy`
- **Templates:** `config.simple.example.yaml`, `config.template.*.yaml`
- **Field reference:** [docs/specifications/config_schema.md](docs/specifications/config_schema.md)

After editing config for deploy: `python module.py build`, then Toolkit `cdf build`.

## Documentation

| Document | Contents |
| -------- | -------- |
| [docs/README.md](docs/README.md) | Index |
| [docs/MODULE_SPECIFICATION.md](docs/MODULE_SPECIFICATION.md) | Canonical spec |
| [docs/guides/howto_quickstart.md](docs/guides/howto_quickstart.md) | Quickstart |
| [docs/guides/howto_config_ui.md](docs/guides/howto_config_ui.md) | Operator UI |
| [workflows/README.md](workflows/README.md) | CDF workflow deploy |
## Tests

```bash
export PYTHONPATH=.
pytest modules/accelerators/contextualization/cdf_file_asset_source/tests/unit/ -q
```
