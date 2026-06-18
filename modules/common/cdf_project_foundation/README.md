# CDF Project Foundation Module

The **Foundation Deployment Pack** (`dp:foundation`) is the recommended starting point for any new Cognite Data Fusion project.

- **Built for new projects** — provides exactly what you need to start fresh
- **Quick to set up** — gets you up and running with minimal friction
- **Bloat-free** — no demo data or clutter to clean up later
- **Intuitive** — easy to understand and navigate from day one
- **Highly extensible** — simple to plug in your own data sources and processing logic
- **Reliable** — everything included works out of the box

This module provides the **project-level foundation** of the pack: three persona-based access groups and a project setup wizard, aligned with the [project-setup SOP](https://cogdocs-feat-cdf-project-setup-docs.mintlify.app/gvd/cdf-project-setup/cdf-foundation-setup).

---

## Deploying the Foundation Deployment Pack

### Step 0 — Prerequisites

> 📖 Before starting, read the [project-setup SOP](https://cogdocs-feat-cdf-project-setup-docs.mintlify.app/gvd/cdf-project-setup/cdf-foundation-setup) — it is required reading before any deployment step.

Ensure the following are in place:

- **Cognite Toolkit latest >= 0.8.102** installed. Follow the [setup instructions](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/setup).
- A `cdf.toml` exists in your project root. If missing, run `cdf init` and choose **Create toml file (required)**.
- Authentication configured and verified:
  ```bash
  cdf auth init
  cdf auth verify
  ```
  See the [Toolkit authentication docs](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth).

> **Important:** Keep all client IDs and secrets as environment variables — never hardcode them in config files.

---

### Step 1 — Initialise and download modules

From a clean Toolkit project directory, run the interactive module selector:

```bash
cdf modules init
```

Select **Foundation Deployment Pack** from the list.

> **Module selector controls:** use **Space** to select / deselect a module, **Enter** to confirm.

---

### Step 2 — Select modules

The module selector presents all available modules. Make selections carefully:

**Data model** — select **exactly one** core variant:

> ⚠️ **Select only one data model variant.** Selecting both will break auto-detection
> and require the `--variant` flag on every script run.

| Option | Description |
|--------|-------------|
| [`isa_manufacturing_extension`](../../datamodels/isa_manufacturing_extension/README.md) | ISA-95 enterprise data model for manufacturing assets (assets, equipment, functional locations, time series). |
| [`cfihos_oil_and_gas_extension`](../../datamodels/cfihos_oil_and_gas_extension/README.md) | CFIHOS enterprise data model for oil & gas assets. |

**Data model — solution extensions** — optional, select alongside the matching enterprise variant:

| Option | Description |
|--------|-------------|
| [`isa_manufacturing_extension_search`](../../datamodels/isa_manufacturing_extension_search/README.md) | Search-optimised solution views on top of the ISA manufacturing enterprise model. **Only select alongside `isa_manufacturing_extension`.** |
| [`cfihos_oil_and_gas_extension_search`](../../datamodels/cfihos_oil_and_gas_extension_search/README.md) | Search-optimised solution views on top of the CFIHOS enterprise model. **Only select alongside `cfihos_oil_and_gas_extension`.** |

**Source system modules** — select any combination:

| Module | Description |
|--------|-------------|
| [`cdf_pi_foundation`](../../sourcesystem/cdf_pi_foundation/README.md) | Sets up extraction pipeline configs for OSIsoft PI / AVEVA PI time series data. |
| [`cdf_sap_foundation`](../../sourcesystem/cdf_sap_foundation/README.md) | Sets up extraction pipeline configs for SAP assets, equipment, and functional locations via RAW staging. |
| [`cdf_opcua_foundation`](../../sourcesystem/cdf_opcua_foundation/README.md) | Sets up extraction pipeline configs for OPC-UA data via RAW staging. |
| [`cdf_db_foundation`](../../sourcesystem/cdf_db_foundation/README.md) | Sets up extraction pipeline configs for generic database sources (PostgreSQL, etc.) via RAW staging. |
| [`cdf_files_foundation`](../../sourcesystem/cdf_files_foundation/README.md) | Sets up extraction pipeline configs for file sources such as SharePoint. |

**Contextualization modules** — optional:

| Module | Description |
|--------|-------------|
| [`cdf_entity_matching`](../../contextualization/cdf_entity_matching/README.md) | Automated asset–time series matching using rule-based and ML-assisted methods. |
| [`cdf_file_annotation`](../../contextualization/cdf_file_annotation/README.md) | P&ID and document annotation with a Streamlit review app. |

**Common module** — always include:

- `cdf_project_foundation` ← this module (access groups, extractor groups, setup wizard)

**Project observability** — recommended:

| Module | Purpose |
|--------|---------|
| [`qualitizer`](../../tools/apps/qualitizer/README.md) | Real-time data quality monitoring and KPI dashboards. Not required for the pack to deploy, but strongly recommended — gives visibility into ingestion health and contextualization coverage from day one. |

---

### Step 3 — Run the setup wizard

From the Toolkit project root, run the interactive setup wizard. It prompts for CDF project names, site/location, Entra ID group source IDs, source system owner contacts, and ApplicationOwner (if file annotation is installed), then writes all `config.<env>.yaml` files and `.env` in one pass:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py
```

The wizard is **idempotent** — re-running it pre-fills every prompt with the current values so you can update individual fields without re-entering everything.

For non-interactive use (e.g. onboarding scripts), skip the confirmation prompt with `-y`:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py -y
```

Other options:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py -y --variant isa_manufacturing_extension

python modules/common/cdf_project_foundation/scripts/setup_project.py --check   # CI drift check
```

> When more than one data model directory exists under `modules/datamodels/` (as in
> this catalog repo), auto-detection cannot pick one — pass `--variant` explicitly.
> A real deployment pack contains exactly one model directory.

---

### Step 4 — Build and validate with a dry-run

Run a build and dry-run to catch config errors before touching any live CDF project:

```bash
# Toolkit < 0.8.0
cdf build --env dev

# Toolkit >= 0.8.0
cdf build -c config.dev.yaml

cdf deploy --dry-run
```

Repeat for `test` and `prod` as needed. Fix any reported issues and re-run.

---

### Step 5 — Deploy

Once the dry-run is clean, deploy to your project:

```bash
cdf deploy
```

---

### Step 6 — Set up CI/CD (optional)

Generate GitHub Actions workflows that automate build, dry-run, and deploy on PR / merge / release. This can also be triggered through the setup wizard (Step 3):

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

The script reads `org-dir` and toolkit version from `cdf.toml` automatically. It uses `environment.project` from each `config.<env>.yaml` as the CDF project name and validates that `environment.name` matches the expected environment.

This writes `.github/workflows/` (`dry-run.yml`, `deploy-dev.yml`, `deploy-prod.yml`, and `deploy-test.yml` when `config.test.yaml` exists) and `docs/FOUNDATION_CICD.md` (GitHub Environments and secrets). Configure `ADMIN_SOURCE_ID`, `CONSUMER_SOURCE_ID`, and `PRODUCER_SOURCE_ID` as GitHub Environment variables alongside the CDF auth variables.

Branching model: PRs to `dev`; PRs to `main` and deploy **test** on merge to `main` only when `config.test.yaml` exists; deploy **dev** on merge to `dev`, and **prod** on GitHub Release from `main`.

---

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

---

## Access Groups

Three CDF groups are deployed, each bound to an Entra ID security group via its `sourceId` (the Entra ID group **Object ID**, recorded per SOP Step 3d). Naming follows the **SOP**:

```
<persona>-[site]-<environment>
```

- `persona` (required): `consumer` | `producer` | `admin`
- `site` (optional): e.g. `oslo` — set via the site / location prompt in the wizard
- `environment` (required): `dev` (covers **dev + test**) | `prod`

| Group | Name (example) | Persona | Capability scope |
|-------|---------------|---------|-----------------|
| `consumer.Group.yaml` | `consumer-dev` / `consumer-prod` | Read-only | READ on data models / instances / timeseries / files / RAW / transformations, scoped to `{{ dataset }}` / `{{ instanceSpace }}` / `{{ schemaSpace }}` |
| `producer.Group.yaml` | `producer-dev` / `producer-prod` | Read/write | Consumer rights plus WRITE to instances / timeseries / files / RAW, run transformations, workflow orchestration, sessions CREATE |
| `admin.Group.yaml` | `admin-dev` / `admin-prod` | Admin | Full capabilities including `groups:write`, projects, datasets, data models, transformations, workflows, extraction pipelines |

The wizard stores group source IDs in `.env` as `CONSUMER_SOURCE_ID`, `PRODUCER_SOURCE_ID`, `ADMIN_SOURCE_ID` and the config files reference them via `${…}`. These are Entra ID object IDs, **not secrets**.

> **Service-principal / per-extractor groups**: This module ships only the three core persona groups. Additional producer groups for service principals and extractors are added per SOP Step 3c as concrete needs arise.

---

## Project Setup Wizard — Reference

The wizard (`scripts/setup_project.py`) is split across four helper modules:

| Module | Responsibility |
|--------|---------------|
| `_style.py` | ANSI colours, section headers, `_ok` / `_warn` / `_hint`, changes table |
| `_prompts.py` | `prompt`, `prompt_yes_no`, `prompt_choice`, `prompt_env_var` |
| `_env_io.py` | `.env` file parse and upsert helpers |
| `_yaml_patch.py` | Line-preserving YAML scalar patcher (preserves comments and blank lines) |

### Wizard flow

1. Prompts for which environments to set up (all three, dev only, dev+prod, or custom).
2. Asks for the CDF project name for each selected environment (pre-filled on re-run).
3. Asks for an required site / location name — used as access-group suffix, source system location, and entity-matching `location_name`.
4. Prompts for source system integration owner and data owner contacts (shared or per-module).
   - For the CFIHOS data model: prompts for **data model owner** name and email (renamed from "integration owner" to reflect its purpose).
5. Prompts for group source IDs (Entra ID object IDs) and writes them to `.env`.
6. Asks for the Streamlit ApplicationOwner email if `cdf_file_annotation` is installed.
7. Shows a review summary then confirms before writing anything.
8. Creates new config files or updates existing ones in-place (preserving comments).
9. Removes redundant auth files from contextualization and tools modules covered by the foundation.
10. Optionally generates GitHub Actions CI/CD workflows.

| Env key | Maps to | Config file |
|---------|---------|------------|
| `dev` | Development | `config.dev.yaml` |
| `test` | Test / Staging | `config.test.yaml` |
| `prod` | Production | `config.prod.yaml` |

---

## Configuration

```yaml
# default.config.yaml — key variables (populated by the wizard)
site: ""                                   # optional site segment for group names
dataset: []                                # auto-populated from installed source system modules
schemaSpace: "dm_dom_isa_manufacturing"    # ISA default; CFIHOS uses dm_dom_oil_and_gas
instanceSpace: "inst_isa_manufacturing"    # ISA default; CFIHOS uses inst_location
dataModelVariant: isa_manufacturing_extension

# Computed per env by setup_project.py:
consumerGroupName: "consumer-dev"
producerGroupName: "producer-dev"
adminGroupName: "admin-dev"

# Entra ID group object IDs — stored in .env, referenced here via ${…}:
consumerSourceId: "${CONSUMER_SOURCE_ID}"
producerSourceId: "${PRODUCER_SOURCE_ID}"
adminSourceId: "${ADMIN_SOURCE_ID}"
```

---

## Dependencies

**Package**: `dp:foundation`

Self-contained. The group ACLs reference `{{ dataset }}`, `{{ instanceSpace }}`, and `{{ schemaSpace }}`, which must match the values used by the deployed source-system and data-model modules.

See the [project-setup SOP](https://cogdocs-feat-cdf-project-setup-docs.mintlify.app/gvd/cdf-project-setup/cdf-foundation-setup) for the authoritative procedure covering environments, Entra ID integration, CI/CD, and sign-off.

---

## Running the Tests

The test suite lives in `tests/test_foundation_setup_wizard.py` at the repo root and covers `_yaml_patch`, `_env_io`, and the core logic of `setup_project.py`.

**Prerequisites** — a Python environment with `pytest` and `pyyaml`:

```bash
pip install pytest pyyaml
```

**Run all foundation wizard tests:**

```bash
# From the library repo root
pytest tests/test_foundation_setup_wizard.py -v
```

**Run alongside the CI/CD generator tests:**

```bash
pytest tests/ -v
```

**Run a specific class or test:**

```bash
pytest tests/test_foundation_setup_wizard.py::TestYamlPatchSetValue -v
pytest tests/test_foundation_setup_wizard.py::TestMigrateStagingToTest::test_renames_and_patches_file -v
```

> The tests use `tmp_path` fixtures for all file I/O — no project files are modified.
