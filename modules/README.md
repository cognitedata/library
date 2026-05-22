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
│   ├── cdf_infield/             # CDM Infield (location module)
│   └── cdf_ai_extractor/        # Atlas AI property extractor
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
| `dp:infield` | Infield | `solutions/cdf_infield/cdf_infield_location` |
| `tool` | Tools and Accelerators | `tools/` |
| `dp:emptymodule` | Empty Module | `custom/my_module` |

Some modules appear in more than one pack (for example `dashboards/report_quality` is in both `dp:dashboards` and `dp:quickstart`). See [ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md) for how `package_id` relates to multi-pack membership.

## Naming conventions

See [Naming conventions](../README.md#naming-conventions) in the root README.

## Module structure

Each deployable module is a directory with:

- **`module.toml`** — required (`title`, `id`, `package_id`; optional `is_selected_by_default`, `description`, `tags`)
- **`default.config.yaml`** — optional; variables merged into the user’s `config.<env>.yaml` when pulling from the library
- **Assets** — YAML/JSON for CDF resources (transformations, functions, data models, etc.), following Toolkit conventions
- **`README.md`** — recommended: purpose, prerequisites, deploy steps

Optional **`[[extra_resources]]`** in `module.toml` references shared files under other module paths (paths relative to `modules/`). `validate_packages.py` checks that each path exists.

## Validation

From the repository `library/` directory:

```bash
python validate_packages.py
python build_packages.py   # optional; do not commit packages.zip
```

`validate_packages.py` checks `packages.toml`, every module path, and `extra_resources` targets.

## Further reading

- [../README.md](../README.md) — consuming the library from `cdf.toml`
- [../ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md) — adding modules and packs
