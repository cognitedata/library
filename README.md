# Cognite Library (deployment packs)

This repository contains **Cognite deployment packs** — downloadable, extensible Toolkit modules that you configure and deploy with the [Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/).

> Templates guide users through deploying, customizing, and building Cognite solutions: data processing, contextualization pipelines, and application configuration.

![Cognite Toolkit Template Modules](templates.png)

## Repository layout

All deployable content lives under [`modules/`](modules/). The registry of packs is [`modules/packages.toml`](modules/packages.toml).

| Folder | Purpose |
|--------|---------|
| [`common/`](modules/common/) | Shared CDF platform modules (`cdf_common`, `cdf_ingestion`, `cdf_search`) |
| [`contextualization/`](modules/contextualization/) | File annotation, entity matching, P&ID, connection SQL |
| [`data_models/`](modules/data_models/) | Industry and extension data models (`rmdm`, ISA, CFIHOS, …) |
| [`solutions/`](modules/solutions/) | Product verticals (`cdm_maintain`, `cdf_infield`, `cdf_ai_extractor`) |
| [`sourcesystem/`](modules/sourcesystem/) | Source connectors (PI, SAP, SharePoint, OID sync) |
| [`dashboards/`](modules/dashboards/) | Streamlit dashboards and reporting |
| [`atlas_ai/`](modules/atlas_ai/) | Atlas AI agents |
| [`tools/`](modules/tools/) | Qualitizer, performance notebooks |
| [`custom/`](modules/custom/) | Empty module template |

### Naming conventions

| Prefix | Use for | Examples |
|--------|---------|----------|
| `cdf_` | Cognite-built platform capabilities | `cdf_common`, `cdf_pi`, `cdf_file_annotation` |
| `cdm_` / `cdf_` | Solutions on Cognite Data Model | `cdm_maintain`, `cdf_infield` |
| *(none)* | Industry models, dashboards, tools | `rmdm`, `context_quality`, `report_quality` |

Apply the prefix on **module folder names** where it helps discovery. Top-level folders (`common/`, `data_models/`, …) are not prefixed.

Contributors: see [ADDING_PACKAGES_AND_MODULES.md](ADDING_PACKAGES_AND_MODULES.md). Validation and module layout details are in [modules/README.md](modules/README.md).

## Usage

The Cognite Toolkit ships with `[library.cognite]` already pointing at this
repository's `latest` release, so a new project is wired up out of the box.

### 1. Enable alpha flag (Toolkit &lt; 0.7.0 only)

```toml
[alpha_flags]
external-libraries = true
```

### 2. Initialize or add modules

- New project: `cdf modules init`
- Existing project: `cdf modules add`

The Toolkit shows deployment packs defined in [`modules/packages.toml`](modules/packages.toml).

## CI/CD (Foundation Deployment Pack)

Foundation Deployment Pack (`dp:foundation`) modules are validated and deployed from this repo using Cognite Toolkit and GitHub Actions.

### Where it lives

| Location | Role |
|----------|------|
| [`.github/workflows/dry-run.yml`](.github/workflows/dry-run.yml) | Runs on every PR to `dev`, `test`, or `prod` |
| [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) | Runs on push (merge) to `dev`, `test`, or `prod` |
| [`foundation-deployment-pack/`](foundation-deployment-pack/) | Toolkit org folder (`config.*.yaml`, module symlink) |
| [`cdf.toml`](cdf.toml) | Toolkit project root (`default_organization_dir`) |

Module source files remain under [`modules/`](modules/); CI links them into `foundation-deployment-pack/modules/` before `cdf build`.

### Workflows

**`dry-run.yml`** (pull request → `dev` | `test` | `prod`):

1. Source-branch guardrail — `test` ← `dev` only; `prod` ← `test` or `hotfix/*` only  
2. Lint — pre-commit YAML/TOML checks, Ruff, Pyright  
3. Tests — `pytest`  
4. `cdf build` and `cdf deploy --dry-run` against the target environment’s CDF project  

**`deploy.yml`** (push to `dev` | `test` | `prod`):

1. `cdf build` then `cdf deploy` for the branch’s environment  

### Repository secrets and variables (required)

Configure under **Settings → Secrets and variables → Actions**. Use a **prefix per stage** (`DEV_`, `TEST_`, `PROD_`) so each branch uses the correct CDF project and service principal.

| Stage (branch) | Variables | Secret |
|----------------|-----------|--------|
| `dev` | `DEV_CDF_CLUSTER`, `DEV_CDF_PROJECT`, `DEV_LOGIN_FLOW`, `DEV_IDP_CLIENT_ID` | `DEV_IDP_CLIENT_SECRET` |
| `test` | `TEST_CDF_CLUSTER`, `TEST_CDF_PROJECT`, `TEST_LOGIN_FLOW`, `TEST_IDP_CLIENT_ID` | `TEST_IDP_CLIENT_SECRET` |
| `prod` | `PROD_CDF_CLUSTER`, `PROD_CDF_PROJECT`, `PROD_LOGIN_FLOW`, `PROD_IDP_CLIENT_ID` | `PROD_IDP_CLIENT_SECRET` |

Example for dev: `DEV_CDF_PROJECT=at-dev`, `DEV_LOGIN_FLOW=client_credentials`. `CDF_PROJECT` must match `config.dev.yaml` / your CDF project name.

Workflows select credentials from the PR **target** branch (`dry-run.yml`) or the branch **pushed** (`deploy.yml`). No GitHub Environments UI is required.

### Branch protection

Protect `dev`, `test`, and `prod` and require the dry-run workflow jobs to pass before merge.

### Triggering locally

See [`foundation-deployment-pack/README.md`](foundation-deployment-pack/README.md).

## Disclaimer

The open-source Github repository ("Repository") is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement. Usage of the Repository is voluntary and in no event shall Cognite be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the use of the Repository.
