# CDF Project Foundation Module

Provides the **project-level foundation** for the `dp:foundation` deployment pack: three persona-based access groups and a project setup script, aligned with the project-setup SOP.

## Module Architecture

```
cdf_project_foundation/
├── auth/
│   ├── consumer.Group.yaml        # consumer persona (read-only)
│   ├── producer.Group.yaml        # producer persona (read/write)
│   └── admin.Group.yaml           # admin persona (full + groups:write)
├── scripts/
│   ├── _pack_config.py            # shared path / config helpers (also used by generate_actions.py)
│   ├── _style.py                  # ANSI colours, section headers, ChangeRecord, changes table
│   ├── _prompts.py                # interactive prompts (text, yes/no, choice, .env variable)
│   ├── _env_io.py                 # .env file parse / upsert helpers
│   ├── _yaml_patch.py             # line-preserving YAML scalar patcher
│   ├── setup_project.py           # interactive wizard — creates / updates config.<env>.yaml
│   ├── generate_actions.py        # generates GitHub Actions CI/CD workflows
│   └── generate_env_configs.py    # generates config.{dev,test,prod}.yaml skeletons
├── templates/
│   └── github/                    # GitHub Actions workflow templates
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

Populate `consumerSourceId` / `producerSourceId` / `adminSourceId` per environment with the Entra ID group Object IDs after the groups have been created. The wizard stores them in `.env` as `CONSUMER_SOURCE_ID`, `PRODUCER_SOURCE_ID`, `ADMIN_SOURCE_ID` and the config files reference them via `${…}`. These are object IDs, **not secrets**.

## Project setup — `scripts/setup_project.py`

An interactive wizard that creates and synchronises the Toolkit config files for the selected environments at the pack root, and keeps their data-model-driven variables and persona group names consistent with the data model installed under `modules/data_models/`.

The wizard is split across four helper modules for readability:

| Module | Responsibility |
|--------|---------------|
| `_style.py` | ANSI colours, `_banner` / `_section` / `_ok` / `_warn` / `_hint`, `ChangeRecord`, changes table |
| `_prompts.py` | `prompt`, `prompt_yes_no`, `prompt_choice`, `prompt_env_var` |
| `_env_io.py` | `.env` file parse and upsert helpers |
| `_yaml_patch.py` | Line-preserving YAML scalar patcher (preserves comments and blank lines) |

### Wizard flow

When run, the wizard:

1. Prompts for which environments to set up (all three, dev only, dev+prod, or custom).
2. Asks for the CDF project name for each selected environment.
3. Asks for an optional site segment (inserted into access-group names).
4. Asks for an optional location name if `cdf_entity_matching` is installed.
5. Prompts for group source IDs (Entra ID object IDs) and writes them to `.env`.
6. Asks for the Streamlit ApplicationOwner email if `cdf_file_annotation` is installed.
7. Shows a pending-changes summary before writing anything.
8. Creates new config files from a skeleton or updates existing ones in-place (preserving comments).
9. Removes redundant auth files from contextualization modules covered by the foundation.
10. Optionally generates GitHub Actions CI/CD workflows (`generate_actions.py`).


| Env key | Maps to        | Config file        |
| ------- | -------------- | ------------------ |
| `dev`   | Development    | `config.dev.yaml`  |
| `test`  | Test / Staging | `config.test.yaml` |
| `prod`  | Production     | `config.prod.yaml` |


With `--site <name>` the files use the convention `config.<env>.<site>.yaml` and the site segment is added to the group names.

The script is idempotent and writes a timestamped `.bak` before modifying any existing file. **No secrets are ever written to config files** (SOP Step 3d) — credentials are referenced via `${ENV_VAR}` / Key Vault only.

```bash
cd modules/common/cdf_project_foundation

python3 scripts/setup_project.py              # interactive wizard
python3 scripts/setup_project.py -y           # skip confirmation prompt
python3 scripts/setup_project.py -y --variant isa_manufacturing_extension
python3 scripts/setup_project.py -y --site oslo   # config.<env>.oslo.yaml
python3 scripts/setup_project.py --check      # CI drift check (exit 1 if out of sync)
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

# Entra ID group object IDs — wizard writes values to .env; configs reference ${…}:
consumerSourceId: "${CONSUMER_SOURCE_ID}"
producerSourceId: "${PRODUCER_SOURCE_ID}"
adminSourceId: "${ADMIN_SOURCE_ID}"
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

## CI/CD setup (customer projects)

After `cdf modules add -d dp:foundation`, generate GitHub Actions from the **Toolkit project root**:

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

Set `enterprise` in `cdf.toml` under `[cdf]` (e.g. `enterprise = "acme"`) or pass `--enterprise <slug>` to override. The script reads `org-dir` and toolkit version from `cdf.toml` automatically.

This writes `.github/workflows/` (`dry-run.yml`, `deploy-dev.yml`, `deploy-test.yml`, `deploy-prod.yml`) and adds `docs/FOUNDATION_CICD.md` (GitHub Environments and secrets).

Branching model: PRs to `dev` / `main`; deploy **dev** on merge to `dev`, **test** on merge to `main`, **prod** on GitHub Release from `main` (see generated `docs/FOUNDATION_CICD.md`).