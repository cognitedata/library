# Quickstart — local run with `module.py run`

Run the key extraction and aliasing pipeline against your CDF data model from your laptop. This path uses the same engines as the deployed Cognite Functions; configuration is the v1 scope document at the module root ([`workflow.local.config.yaml`](../../workflow.local.config.yaml)) when you use `run --scope default`. With **`incremental_change_processing: true`**, local runs mirror workflow parity (state update → extraction → …) and use the same **Key Discovery** / **RAW fallback** parameters as deployed triggers — see [module README — Incremental cohort processing](../../README.md#incremental-cohort-processing-raw-cohort-cdm-state).

**CLI:** `python module.py` with no subcommand prints help. The pipeline is **`module.py run`** (with the flags below). Workflow YAML generation is **`module.py build`** (legacy: **`module.py --build`**).

**Documentation index:** [docs/README.md](../README.md)

## Prerequisites

- **Python** 3.11+
- **Dependencies:** Install packages required by this module (for example `cognite-sdk`, `pyyaml`, `python-dotenv`) in your environment.
- **Working directory:** Use the **repository root** — the directory that contains `modules/`.
- **`PYTHONPATH`:** Must include that root so imports resolve:

```bash
export PYTHONPATH=.
```

(On Windows PowerShell, use `$env:PYTHONPATH="."` for the session.)

## Credentials (`.env`)

Local runs call [`local_runner/client.py`](../../local_runner/client.py) via `module.py run`. Environment variables are loaded from **`.env`** if present: the loader prefers **`$REPO_ROOT/.env`** (repository root), then falls back to python-dotenv’s default search.

### API key

| Variable (any one name) | Purpose |
|-------------------------|---------|
| `COGNITE_API_KEY`, `API_KEY`, or `CDF_API_KEY` | CDF API key |

Also set project and base URL:

| Variable | Purpose |
|----------|---------|
| `COGNITE_PROJECT`, `PROJECT`, or `CDF_PROJECT` | CDF project name |
| `COGNITE_BASE_URL`, `BASE_URL`, `CDF_BASE_URL`, or `CDF_URL` | Cluster API base URL |

### OAuth (client credentials)

If no API key is set, the client expects OAuth. All of the following must be present (see error message in `create_cognite_client` if something is missing):

| Variable | Purpose |
|----------|---------|
| `COGNITE_TENANT_ID`, `TENANT_ID`, or `IDP_TENANT_ID` | IdP tenant |
| `COGNITE_CLIENT_ID`, `CLIENT_ID`, or `IDP_CLIENT_ID` | App registration client id |
| `COGNITE_CLIENT_SECRET`, `CLIENT_SECRET`, or `IDP_CLIENT_SECRET` | Client secret |
| `COGNITE_TOKEN_URL`, `TOKEN_URL`, or `IDP_TOKEN_URL` | OAuth token endpoint |
| `COGNITE_SCOPES`, `SCOPES`, or `IDP_SCOPES` | Space-separated scopes |

**Project and base URL:** Same as for API key. If `COGNITE_BASE_URL` (and aliases) are unset, you may set **`CDF_CLUSTER`** instead; the client builds `https://{CDF_CLUSTER}.cognitedata.com`.

## Run `module.py run`

From **repository root**, with `PYTHONPATH=.`:

```bash
# Safe first run: no alias write-back to CDF
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --dry-run

# Default scope document (module-root workflow.local.config.yaml)
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run --scope default

# Explicit path to the same file (equivalent when using the default scope file)
python modules/accelerators/contextualization/cdf_key_extraction_aliasing/module.py run \
  --config-path modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflow.local.config.yaml
```

Useful options:

- **`--limit N`** — cap instances per view (omit or `0` for no limit).
- **`--verbose`** — more logging.
- **`--instance-space <space>`** — restrict to views that match that data model instance space (see `module.py run --help`).

If your team uses Poetry or another runner, prefix the command as usual (for example `poetry run python ...`).

## Where results go

The pipeline writes **timestamped JSON** under the module’s **[`tests/results/`](../../tests/results/)** directory (relative to `cdf_key_extraction_aliasing/`):

- **`*_cdf_extraction.json`** — extraction output per run
- **`*_cdf_aliasing.json`** — aliasing output per run

Open the newest files to inspect keys, aliases, and metadata. The directory is often gitignored; create it if your clone does not have it yet.

**Optional report:** The module README describes regenerating [docs/key_extraction_aliasing_report.md](../key_extraction_aliasing_report.md) from result JSON via [`scripts/generate_report.py`](../../scripts/generate_report.py). Test layout and pytest commands: [`tests/README.md`](../../tests/README.md).

## Next steps

- **Tune rules and views:** [Configuration guide](configuration_guide.md), [config/README.md](../../config/README.md)
- **Multiple sites and Toolkit manifests:** [Scoped deployment how-to](howto_scoped_deployment.md)
- **Custom Python handlers:** [How to add a custom handler](howto_custom_handlers.md)
