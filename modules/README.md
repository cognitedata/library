# Modules

Deployable [Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/) modules and **deployment packs** live here. Each pack is a curated set of module folders users can add with `cdf modules add`.

The canonical registry is [`packages.toml`](./packages.toml). Every listed path must contain a `module.toml` at its root.

## Folder layout

```
modules/
├── packages.toml              # Deployment pack registry
│
├── common/                      # Shared CDF platform building blocks
│   ├── cdf_common/
│   ├── cdf_ingestion/
│   └── cdf_search/
│
├── contextualization/         # Contextualization pipelines & transforms
│   ├── cdf_file_annotation/
│   ├── cdf_entity_matching/
│   ├── cdf_p_and_id_annotation/
│   ├── cdf_p_and_id_parser/
│   └── cdf_connection_sql/
│
├── data_models/                 # Industry & extension data models
│   ├── rmdm/
│   ├── isa_manufacturing_extension/
│   ├── cfihos_oil_and_gas_extension/
│   ├── cdf_process_industry_extension/
│   └── qs_enterprise_dm/
│
├── solutions/                   # Product verticals (CDM-backed)
│   ├── cdm_maintain/            # CDM Maintain (5 submodules)
│   └── cdf_ai_extractor/        # Atlas AI property extractor
│
├── infield/                     # Infield on CDM (per-location modules)
│   └── location/
│
├── sourcesystem/              # Source system connectors
│   ├── cdf_pi/
│   ├── cdf_sap_assets/
│   ├── cdf_sap_events/
│   ├── cdf_sharepoint/
│   └── cdf_oid_sync/
│
├── dashboards/                  # Streamlit apps & reporting
│   ├── context_quality/
│   ├── project_health/
│   └── report_quality/
│
├── atlas_ai/
│   └── ootb_agents/
│
├── tools/
│   ├── apps/qualitizer/
│   └── notebooks/               # cdf_performance_testing, transformation metrics
│
└── custom/
    └── my_module/               # Empty template (see custom/README.md)
```

## Deployment packs

These are the packs exposed in the Toolkit menu (from `packages.toml`):

| Package ID | Title | Folder roots |
|------------|-------|----------------|
| `dp:quickstart` | Quickstart Deployment Pack | `common/`, `contextualization/`, `sourcesystem/`, `dashboards/`, `data_models/qs_enterprise_dm` |
| `dp:contextualization` | Contextualization | `contextualization/` |
| `dp:common` | Common | `common/` |
| `dp:sourcesystem` | Source Systems | `sourcesystem/` |
| `dp:models` | Data models | `data_models/` |
| `dp:dashboards` | Dashboards | `dashboards/` |
| `dp:atlas_ai` | Atlas AI | `atlas_ai/`, `solutions/cdf_ai_extractor` |
| `dp:cdm_maintain` | CDM Maintain | `solutions/cdm_maintain/*` |
| `dp:infield` | Infield | `infield/location` |
| `tool` | Tools and Accelerators | `tools/` |
| `dp:emptymodule` | Empty Module | `custom/my_module` |

Some modules appear in more than one pack (for example `dashboards/report_quality` is in both `dp:dashboards` and `dp:quickstart`).

## Naming conventions

| Prefix | Use for | Examples |
|--------|---------|----------|
| `cdf_` | Cognite-built platform capabilities | `cdf_common`, `cdf_pi`, `cdf_file_annotation` |
| `cdm_` | Solutions built on Cognite Data Model | `cdm_maintain` |
| *(none)* | Industry-standard models, dashboards, tools | `rmdm`, `context_quality`, `report_quality` |

Apply the prefix on **module folder names** where it helps discovery. Top-level folders (`common/`, `data_models/`, …) are not prefixed.

## Module structure

Each deployable module is a directory with:

- **`module.toml`** — required (`title`, `id`, `package_id`)
- **`default.config.yaml`** — variables merged into the user’s `config.<env>.yaml` when pulling from the library
- **Assets** — YAML/JSON for CDF resources (transformations, functions, data models, etc.), following Toolkit conventions
- **`README.md`** — recommended: purpose, prerequisites, deploy steps

Optional **`[[extra_resources]]`** in `module.toml` references shared files under other module paths (paths relative to `modules/`).

## Validation

From the repository `library/` directory:

```bash
python validate_packages.py
python build_packages.py
```

`validate_packages.py` checks `packages.toml`, every module path, and `extra_resources` targets.

## Further reading

- [../README.md](../README.md) — consuming the library from `cdf.toml`
- [../ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md) — adding modules and packs
