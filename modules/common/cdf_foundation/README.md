# CDF Foundation Module

Provides the **project-level foundation** for the `dp:foundation` deployment pack: three persona-based access groups and a project setup script, aligned with the project-setup SOP.

## Module Architecture

```
cdf_foundation/
├── auth/
│   ├── consumer.Group.yaml        # consumer persona (read-only)
│   ├── producer.Group.yaml        # producer persona (read/write)
│   └── admin.Group.yaml           # admin persona (full + groups:write)
├── scripts/
│   ├── _pack_config.py            # shared path / config helpers
│   └── setup_project.py           # project setup for dev / test / prod
├── default.config.yaml
└── module.toml
```

## Access groups

Three CDF groups are deployed, each bound to an Entra ID security group via its `sourceId` (the Entra ID group **Object ID**, recorded per SOP Step 3d). Naming follows **SOP**:

```
<persona>-[site]-<environment>
```

- `persona` (required): `consumer` | `producer` | `admin`
- `site` (optional): e.g. `oslo` — set via the `site` variable / `--site`
- `environment` (required): `dev` (covers **dev + test**) | `prod`

The group `name` is supplied per environment by `setup_project.py` as the computed variables `consumerGroupName` / `producerGroupName` / `adminGroupName`(e.g. `admin-dev`, `admin-prod`, `consumer-oslo-prod`).


| Group                 | Name (example)                   | Persona    | Capability scope (least privilege, SOP Step 3e)                                                                                                                      |
| --------------------- | -------------------------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `consumer.Group.yaml` | `consumer-dev` / `consumer-prod` | Read-only  | READ on data models / instances / timeseries / files / RAW / transformations, scoped to `{{ dataset }}` / `{{ instanceSpace }}` / `{{ schemaSpace }}` where possible |
| `producer.Group.yaml` | `producer-dev` / `producer-prod` | Read/write | Consumer rights plus WRITE to instances / timeseries / files / RAW, run transformations, workflow orchestration READ/WRITE, sessions CREATE                          |
| `admin.Group.yaml`    | `admin-dev` / `admin-prod`       | Admin      | Full capabilities including `groups:write`, projects, datasets, data models, transformations, workflows, extraction pipelines (`all` scope)                          |


> **Naming reconciliation**: The SOP's `<persona>-...` access-group pattern is authoritative for project-setup groups and intentionally **overrides** the `gp_cdf_...` pattern in `.cursor/rules/cdf-naming-conventions.mdc`. See `.cursor/rules/cdf-foundation-project-setup.mdc`.

> **Service-principal / per-extractor groups**: This module ships only the three core persona groups. Additional producer groups for service principals and extractors (e.g. `producer-ep-opcua-dev`, `producer-pp-prod`, and `cognite_toolkit_service_principal`) are added per the SOP Step 3c minimum-set table as concrete needs arise.

Populate `consumerSourceId` / `producerSourceId` / `adminSourceId` per environment with the Entra ID group Object IDs after the groups have been created. These are object IDs, **not secrets**.

## Project setup — `scripts/setup_project.py`

Creates and synchronises the Toolkit config files for the three mandatory environments (SOP Step 1) at the pack root, and keeps their data-model-driven variables and persona group names consistent with the data model installed under `modules/data_models/`.


| Env key | Maps to        | Config file        |
| ------- | -------------- | ------------------ |
| `dev`   | Development    | `config.dev.yaml`  |
| `test`  | Test / Staging | `config.test.yaml` |
| `prod`  | Production     | `config.prod.yaml` |


With `--site <name>` the files use the SOP's fuller convention `config.<env>.<site>.yaml` and the site segment is added to the group names.

For each environment it:

1. Ensures the config file exists (creates a minimal `environment:` block when missing).
2. Detects the data model variant (`isa_manufacturing_extension` or `cfihos_oil_and_gas_extension`), or uses `--variant`.
3. Merges variables into the config:

- `variables.modules.common.cdf_foundation`: `dataModelVariant`,
 `schemaSpace`, `instanceSpace`, `site`, and the three `*GroupName`
 values (resolved per environment).
- `variables.modules.data_models.<variant>`
- `variables.modules.contextualization.`* (entity matching, file annotation)
- `variables.modules.sourcesystem.<installed module>.instanceSpace`

The script is idempotent and writes a timestamped `.bak` before modifying an
existing config file. **No secrets are ever written** (SOP Step 3d) — credentials
are referenced via `${ENV_VAR}` / Key Vault only.

```bash
cd modules/common/cdf_foundation

python3 scripts/setup_project.py --help
python3 scripts/setup_project.py -y                          # apply (auto-detect variant)
python3 scripts/setup_project.py -y --variant isa_manufacturing_extension
python3 scripts/setup_project.py -y --site oslo              # config.<env>.oslo.yaml
python3 scripts/setup_project.py --check                     # CI drift check (exit 1 if out of sync)
```

> When more than one model directory is present under `modules/data_models/`
> (as in this catalog repo), auto-detection cannot pick one — pass `--variant`.
> A real deployment pack contains a single model directory.

## Configuration

```yaml
# default.config.yaml
site: ""                   # optional site segment for group names
dataset: "ds_ingestion"    # dataset used to scope group ACLs
schemaSpace: "sp_isa_manufacturing"
instanceSpace: "sp_isa_instance_space"
dataModelVariant: isa_manufacturing_extension

# Computed per env by setup_project.py (defaults shown):
consumerGroupName: "consumer-dev"
producerGroupName: "producer-dev"
adminGroupName: "admin-dev"

# Entra ID group object IDs (fill in per environment; not secrets):
consumerSourceId: ""
producerSourceId: ""
adminSourceId: ""
```

## Dependencies

**Package**: `dp:foundation`

Self-contained. The group ACLs reference `{{ dataset }}`, `{{ instanceSpace }}`, and
`{{ schemaSpace }}`, which must match the values used by the deployed
source-system and data-model modules.

## Deploy

```bash
python3 scripts/setup_project.py -y          # set up config.<env>.yaml
cdf build
cdf deploy
```

See `sop-cdf-project-setup.md` (repo root) for the authoritative project-setup
procedure, including environments, Entra ID integration, CI/CD, and sign-off.