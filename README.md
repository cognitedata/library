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
| [`solutions/`](modules/solutions/) | Product verticals (`cdm_maintain`, `cdf_ai_extractor`) |
| [`infield/`](modules/infield/) | Infield on CDM (`location`) |
| [`sourcesystem/`](modules/sourcesystem/) | Source connectors (PI, SAP, SharePoint, OID sync) |
| [`dashboards/`](modules/dashboards/) | Streamlit dashboards and reporting |
| [`atlas_ai/`](modules/atlas_ai/) | Atlas AI agents |
| [`tools/`](modules/tools/) | Qualitizer, performance notebooks |
| [`custom/`](modules/custom/) | Empty module template |

### Naming conventions

| Prefix | Use for | Examples |
|--------|---------|----------|
| `cdf_` | Cognite-built platform capabilities | `cdf_common`, `cdf_pi`, `cdf_file_annotation` |
| `cdm_` | Solutions on Cognite Data Model | `cdm_maintain` |
| *(none)* | Industry models, dashboards, tools | `rmdm`, `context_quality`, `report_quality` |

Contributors: see [ADDING_PACKAGES_AND_MODULES.md](ADDING_PACKAGES_AND_MODULES.md).

## Current release

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

## Usage

### 1. Add library reference to `cdf.toml`

```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
```

### 2. Enable alpha flag (Toolkit &lt; 0.7.0 only)

```toml
[alpha_flags]
external-libraries = true
```

### 3. Initialize or add modules

- New project: `cdf modules init`
- Existing project: `cdf modules add`

The Toolkit shows deployment packs defined in `modules/packages.toml`.

## Validation (contributors)

From the `library/` directory:

```bash
python validate_packages.py
python build_packages.py
```

## Disclaimer

The open-source Github repository ("Repository") is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement. Usage of the Repository is voluntary and in no event shall Cognite be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the use of the Repository.
