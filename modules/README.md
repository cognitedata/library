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
│   ├── cdf_dq_runtime/          # SHACL data-quality validation (dp:data_quality)
│   ├── cdf_ingestion/
│   ├── cdf_project_foundation/
│   └── cdf_search/
│
├── contextualization/         # Contextualization pipelines & transforms
│   ├── cdf_file_annotation/
│   ├── cdf_entity_matching/
│   └── cdf_p_and_id_annotation/
│
├── datamodels/                  # Industry & extension data models
│   ├── rmdm/
│   ├── isa_manufacturing_extension/
│   ├── isa_manufacturing_extension_search/
│   ├── cfihos_oil_and_gas_extension/
│   └── cfihos_oil_and_gas_extension_search/
│
├── solutions/                   # Product verticals (CDM-backed)
│   ├── cdm_maintain/            # CDM Maintain (5 submodules)
│   ├── cdf_infield/             # CDM Infield (location module)
│   └── cdf_ai_extractor/        # Atlas AI property extractor
│
├── sourcesystem/              # Source system connectors
│   ├── cdf_pi_extractor/        # Foundation: OSIsoft/AVEVA PI extraction pipeline config
│   ├── cdf_opcua_extractor/     # Foundation: OPC-UA extraction pipeline config
│   ├── cdf_sap_extractor/       # Foundation: SAP extraction pipeline config
│   ├── cdf_db_extractor/        # Foundation: generic database extraction pipeline config
│   ├── cdf_files_extractor/     # Foundation: file-source extraction pipeline config
│   ├── cdf_pi_data_dump/        # Demo: synthetic PI-shaped sample data
│   ├── cdf_sap_data_dump/       # Demo: synthetic SAP-shaped sample data
│   └── cdf_sharepoint_data_dump/ # Demo: synthetic SharePoint-shaped sample data
│
├── dashboards/                  # Streamlit apps & reporting
│   └── context_quality/
│
├── tools/
│   └── apps/qualitizer/
│
└── custom/
    └── my_module/               # Empty template (see custom/README.md)
```

## Deployment packs

These are the packs exposed in the Toolkit menu (from `packages.toml`):

| Package ID | Title | Folder roots |
|------------|-------|----------------|
| `dp:foundation` | Foundation Deployment Pack | `datamodels/*` (ISA, CFIHOS), `sourcesystem/*_extractor`, `common/cdf_project_foundation`, `contextualization/cdf_file_annotation`, `contextualization/cdf_entity_matching`, `tools/apps/qualitizer` |
| `dp:quickstart` | Foundation Deployment Pack Demo | `common/cdf_ingestion`, `common/cdf_project_foundation`, `contextualization/cdf_file_annotation`, `contextualization/cdf_entity_matching`, `sourcesystem/*_data_dump`, `datamodels/cfihos_oil_and_gas_extension*` |
| `dp:contextualization` | Contextualization | `contextualization/` |
| `dp:common` | Common | `common/` |
| `dp:sourcesystem` | Source Systems | `sourcesystem/*_extractor` (real connector configs), `sourcesystem/*_data_dump` (synthetic sample data) |
| `dp:models` | Data models | `datamodels/` |
| `dp:dashboards` | Dashboards | `dashboards/` |
| `dp:data_quality` | Data Quality Validation | `common/cdf_dq_runtime` |
| `dp:atlas_ai` | Atlas AI Deployment Pack | `solutions/cdf_ai_extractor` |
| `dp:cdm_maintain` | CDM Maintain | `solutions/cdm_maintain/*` |
| `dp:infield` | Infield | `solutions/cdf_infield/cdf_infield_location` |
| `tool` | Tools and Accelerators | `tools/` |
| `dp:emptymodule` | Empty Module | `custom/my_module` |

Some modules appear in more than one pack (for example `contextualization/cdf_file_annotation` is in `dp:foundation`, `dp:quickstart`, and `dp:contextualization`). See [CONTRIBUTING.md](../CONTRIBUTING.md) for how `package_id` relates to multi-pack membership.

## Module IDs

Every `module.toml` **`id`** must be `dp:<package_short>:<slug>` where `<package_short>` is the `package_id` without the `dp:` prefix (for example `package_id = "dp:common"` → `dp:common:cdf_common`). The **`title`** is the Toolkit display name and may differ from the slug. Each module's `module.toml` is the source of truth for its id; there is deliberately no copy of them here.

`python validate_packages.py` checks unique ids and that each id uses a `dp:<pack>:` prefix allowed for that module (primary `package_id` or any pack in `packages.toml` that lists the module path).

## Naming conventions

See [Naming conventions](../README.md#naming-conventions) in the root README.

## Module structure

Each deployable module is a directory with:

- **`module.toml`** — required (`title`, `id`, `package_id`; optional `is_selected_by_default`, `description`, `tags`)
- **`default.config.yaml`** — optional; variables merged into the user’s `config.<env>.yaml` when pulling from the library
- **Assets** — YAML/JSON for CDF resources (transformations, functions, data models, etc.), following Toolkit conventions
- **`README.md`** — required; follow the structure in [CONTRIBUTING.md](../CONTRIBUTING.md#every-module-needs-a-readme)

Optional **`[[extra_resources]]`** in `module.toml` references shared files under other module paths (paths relative to `modules/`). `validate_packages.py` checks that each path exists.

## Python dependencies (uv)

Modules that ship Python (CDF Functions, Streamlit apps, or helper packages) declare dependencies in a per-folder `pyproject.toml` under the [uv workspace](../pyproject.toml) at the repository root. The `requirements.txt` next to each handler lists **direct deploy dependencies** for CDF (packages on top of the Functions runtime) — edit `deploy_dependencies` in `scripts/generate_uv_member_projects.py`, then run `python scripts/export_deploy_requirements.py`. Use `pyproject.toml` / `uv lock` for local dev and tests.

From the repository root:

```bash
uv sync --group dev          # install repo + workspace dev tools
uv run pytest tests/ -q      # repo tooling tests
```

Each module README lists paths and test commands for its Python components. See [AGENTS.md](../AGENTS.md) and [scripts/README.md](../scripts/README.md) for the full uv / deploy-requirements workflow.

## Validation

From the repository `library/` directory:

```bash
python validate_packages.py
python build_packages.py   # optional; do not commit packages.zip
```

`validate_packages.py` checks `packages.toml`, every module path, and `extra_resources` targets.

## Further reading

- [../README.md](../README.md) — consuming the library from `cdf.toml`
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — adding modules and packs
