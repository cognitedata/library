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

## Module IDs (canonical)

Every `module.toml` **`id`** must be `dp:<package_short>:<slug>` where `<package_short>` is the `package_id` without the `dp:` prefix (for example `package_id = "dp:common"` → `dp:common:cdf_common`). The **`title`** is the Toolkit display name and may differ from the slug.

| Module path | Module ID | Package (`package_id`) |
|-------------|-----------|-------------------------|
| `atlas_ai/ootb_agents` | `dp:atlas_ai:ootb_agents` | `dp:atlas_ai` |
| `common/cdf_common` | `dp:common:cdf_common` | `dp:common` |
| `common/cdf_ingestion` | `dp:common:cdf_ingestion` | `dp:common` |
| `common/cdf_search` | `dp:common:cdf_search` | `dp:common` |
| `contextualization/cdf_connection_sql` | `dp:contextualization:cdf_connection_sql` | `dp:contextualization` |
| `contextualization/cdf_entity_matching` | `dp:contextualization:cdf_entity_matching` | `dp:contextualization` |
| `contextualization/cdf_file_annotation` | `dp:contextualization:cdf_file_annotation` | `dp:contextualization` |
| `contextualization/cdf_p_and_id_annotation` | `dp:contextualization:cdf_p_and_id_annotation` | `dp:contextualization` |
| `contextualization/cdf_p_and_id_parser` | `dp:contextualization:cdf_p_and_id_parser` | `dp:contextualization` |
| `custom/my_module` | `dp:emptymodule:my_module` | `dp:emptymodule` |
| `dashboards/context_quality` | `dp:dashboards:context_quality` | `dp:dashboards` |
| `dashboards/project_health` | `dp:dashboards:project_health` | `dp:dashboards` |
| `dashboards/report_quality` | `dp:dashboards:report_quality` | `dp:dashboards` |
| `data_models/cdf_process_industry_extension` | `dp:models:cdf_process_industry_extension` | `dp:quickstart` |
| `data_models/cfihos_oil_and_gas_extension` | `dp:models:cfihos_oil_and_gas_extension` | `dp:models` |
| `data_models/cfihos_oil_and_gas_extension_search` | `dp:models:cfihos_oil_and_gas_extension_search` | `dp:models` |
| `data_models/isa_manufacturing_extension` | `dp:models:isa_manufacturing_extension` | `dp:models` |
| `data_models/qs_enterprise_dm` | `dp:models:qs_enterprise_dm` | `dp:quickstart` |
| `data_models/rmdm` | `dp:models:rmdm` | `dp:models` |
| `solutions/cdf_ai_extractor` | `dp:atlas_ai:ai_extractor` | `dp:atlas_ai` |
| `solutions/cdf_infield/cdf_infield_location` | `dp:infield:cdf_infield_location` | `dp:infield` |
| `solutions/cdm_maintain/cdf_maintain_config_base` | `dp:cdm_maintain:cdf_maintain_config_base` | `dp:cdm_maintain` |
| `solutions/cdm_maintain/cdf_maintain_location` | `dp:cdm_maintain:cdf_maintain_location` | `dp:cdm_maintain` |
| `solutions/cdm_maintain/cdf_maintain_solution_model` | `dp:cdm_maintain:cdf_maintain_solution_model` | `dp:cdm_maintain` |
| `solutions/cdm_maintain/cdf_maintain_source_data_model` | `dp:cdm_maintain:cdf_maintain_source_data_model` | `dp:cdm_maintain` |
| `solutions/cdm_maintain/cdf_sample_data` | `dp:cdm_maintain:cdf_sample_data` | `dp:cdm_maintain` |
| `sourcesystem/cdf_oid_sync` | `dp:sourcesystem:cdf_oid_sync` | `dp:sourcesystem` |
| `sourcesystem/cdf_pi` | `dp:sourcesystem:cdf_pi` | `dp:sourcesystem` |
| `sourcesystem/cdf_sap_assets` | `dp:sourcesystem:cdf_sap_assets` | `dp:sourcesystem` |
| `sourcesystem/cdf_sap_events` | `dp:sourcesystem:cdf_sap_events` | `dp:sourcesystem` |
| `sourcesystem/cdf_sharepoint` | `dp:sourcesystem:cdf_sharepoint` | `dp:sourcesystem` |
| `tools/apps/qualitizer` | `dp:tool:qualitizer` | `tool` |
| `tools/notebooks/cdf_performance_testing` | `dp:tool:cdf_performance_testing` | `tool` |
| `tools/notebooks/cdf_transformation_jobs_metric_explorer` | `dp:tool:cdf_transformation_jobs_metric_explorer` | `tool` |

`python validate_packages.py` checks unique ids and that each id uses a `dp:<pack>:` prefix allowed for that module (primary `package_id` or any pack in `packages.toml` that lists the module path).

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
