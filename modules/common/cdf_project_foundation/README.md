# CDF Project Foundation Module

The **Foundation Deployment Pack** (`dp:foundation`) is the recommended starting point for any new Cognite Data Fusion project.

- **Built for new projects** — provides exactly what you need to start fresh
- **Quick to set up** — gets you up and running with minimal friction
- **Bloat-free** — no demo data or clutter to clean up later
- **Intuitive** — easy to understand and navigate from day one
- **Highly extensible** — simple to plug in your own data sources and processing logic
- **Reliable** — everything included works out of the box

This module provides the **project-level foundation** shared by two deployment packs: three persona-based access groups and a project setup wizard, aligned with the [project-setup SOP](https://cogdocs.mintlify.io/gvd) *(password-protected — request access via [#topic-deployment-packs](https://cognitedata.slack.com/archives/C098QJ09YKX) or contact [Valeriya Naumova](https://cognitedata.slack.com/team/U051XA95S0G)).*

## Foundation vs. Demo

| | `dp:foundation` (Foundation) | `dp:quickstart` (Foundation Deployment Pack Demo) |
|---|---|---|
| **Use for** | Real customer projects, no synthetic data | Exploring CDF / showcasing end-to-end ingestion + contextualization |
| **Source systems** | `*_extractor` modules (`cdf_pi_extractor`, `cdf_sap_extractor`, …) | `*_data_dump` modules (`cdf_pi_data_dump`, `cdf_sap_data_dump`, `cdf_sharepoint_data_dump`) |
| **Data model** | Choose CDM / ISA / CFIHOS | Always CFIHOS (`cfihos_oil_and_gas_extension`) |
| **Ingestion orchestration** | Each extractor module owns its own extraction pipeline config | `common/cdf_ingestion` (shared workflow driving all the CFIHOS transformations) |

Both packs include this module (`cdf_project_foundation`) for the persona access groups and the setup wizard. The wizard **auto-detects which pack you're running** from the sourcesystem modules you selected in Step 2 — you don't need to tell it which pack you're on. See [Which pack am I on?](#which-pack-am-i-on) below for how detection works.

---

## Deploying the Foundation Deployment Pack (or the Demo)

### Step 0 — Prerequisites

> 📖 Before starting, read the [project-setup SOP](https://cogdocs.mintlify.io/gvd) *(password-protected — request access via [#topic-deployment-packs](https://cognitedata.slack.com/archives/C098QJ09YKX) or contact [Valeriya Naumova](https://cognitedata.slack.com/team/U051XA95S0G))* — it is required reading before any deployment step.

Ensure the following are in place:

- **Cognite Toolkit latest >= 0.8.102** installed. Follow the [setup instructions](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/setup).
- A `cdf.toml` exists in your project root. If missing, run `cdf init` and choose **Create toml file (required)**.
- Authentication configured and verified:
  ```bash
  cdf auth init
  cdf auth verify
  ```
  See the [Toolkit authentication docs](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth).

> **Important:** Group source IDs (Entra group object IDs) are stored in `.env` by the setup wizard. Client IDs, client secrets, and other credentials should use interactive or device-code login as per the SOP — never hardcode secrets in config files or `.env`.

---

### Step 1 — Initialise and download modules

From a clean Toolkit project directory, run the interactive module selector:

```bash
cdf modules init
```

Select **Foundation Deployment Pack** for a real customer project, or **Foundation Deployment Pack Demo** to explore CDF with synthetic data.

> **Note:** The Toolkit selector shows display titles (e.g. "Foundation Deployment Pack"). The actual module IDs used in config files and referenced in the tables below are the directory names shown in each table (e.g. `cdf_project_foundation`, `isa_manufacturing_extension`).

> **Module selector controls:** use **Space** to select / deselect a module, **Enter** to confirm.

---

### Step 2 — Select modules

> **Demo pack users can skip this step.** `dp:quickstart` ships a fixed module list
> (`canCherryPick = false` in `packages.toml`) — `cdf modules init` installs everything
> below automatically: `cdf_ingestion`, `cdf_project_foundation`, `cdf_file_annotation`,
> `cdf_entity_matching`, the three `*_data_dump` source modules, and the CFIHOS data
> model + search extension. Go straight to [Step 3](#step-3--run-the-setup-wizard).

The Foundation pack's module selector presents all available modules individually. Make selections carefully:

**Data model** — optional, select **at most one** core variant:

> ⚠️ **Select only one data model variant.** Selecting both will break auto-detection
> and require the `--variant` flag on every script run. If you select **none**, the
> pack automatically configures itself against the base Cognite Data Model
> (CogniteCore, space `cdf_cdm`) — no extra module required, and the setup wizard
> will not error. Since there's no extension module to create the instance space,
> the setup wizard writes a `data_modeling/cdm_instance_space.Space.yaml` resource
> into `cdf_project_foundation` that creates `sp_{site}_instances` (the same
> `sp_<site>_<suffix>` convention used for the extractor instance spaces). This
> file is only ever written when no extension is selected — it isn't shipped by
> the module itself.

| Option | Description |
|--------|-------------|
| *(none selected)* | Falls back to the base Cognite Data Model (CogniteCore) — `CogniteAsset` / `CogniteTimeSeries` / `CogniteFile` in space `cdf_cdm`. |
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
| [`cdf_pi_extractor`](../../sourcesystem/cdf_pi_extractor/README.md) | Sets up extraction pipeline configs for OSIsoft PI / AVEVA PI time series data. |
| [`cdf_sap_extractor`](../../sourcesystem/cdf_sap_extractor/README.md) | Sets up extraction pipeline configs for SAP assets, equipment, and functional locations via RAW staging. |
| [`cdf_opcua_extractor`](../../sourcesystem/cdf_opcua_extractor/README.md) | Sets up extraction pipeline configs for OPC-UA data via RAW staging. |
| [`cdf_db_extractor`](../../sourcesystem/cdf_db_extractor/README.md) | Sets up extraction pipeline configs for generic database sources (PostgreSQL, etc.) via RAW staging. |
| [`cdf_files_extractor`](../../sourcesystem/cdf_files_extractor/README.md) | Sets up extraction pipeline configs for file sources such as SharePoint. |

**Contextualization modules** — optional:

| Module | Description |
|--------|-------------|
| [`cdf_entity_matching`](../../contextualization/cdf_entity_matching/README.md) | Automated asset–time series matching using rule-based and ML-assisted methods. |
| [`cdf_file_annotation`](../../contextualization/cdf_file_annotation/README.md) | P&ID and document annotation with a Streamlit review app. |

**Common module** — always include:

| Module | Description |
|--------|-------------|
| `cdf_project_foundation` | This module — persona access groups, per-extractor groups, and the interactive setup wizard. Also shipped by `dp:quickstart`, where it covers the same capabilities `cdf_ingestion` used to grant via its own auth files (now removed by the wizard as redundant — see [Access Groups](#access-groups)). |

**Project observability** — recommended:

| Module | Purpose |
|--------|---------|
| [`qualitizer`](../../tools/apps/qualitizer/README.md) | Real-time data quality monitoring and KPI dashboards. Not required for the pack to deploy, but strongly recommended — gives visibility into ingestion health and contextualization coverage from day one. |

---

### Step 3 — Run the setup wizard

From the Toolkit project root, run the interactive setup wizard. It prompts for CDF project names, site/location, Entra ID group source IDs, source system owner contacts, and ApplicationOwner (if file annotation is installed), then writes all `config.<env>.yaml` files and `.env` in one pass.

#### Which pack am I on?

The wizard header shows the resolved pack and data model variant on every run:

```
──────────────────────────────────────────────────────────
  Foundation Deployment Pack Demo — Project Setup
──────────────────────────────────────────────────────────
  ✓  Deployment pack    : demo
  ✓  Data model variant : cfihos_oil_and_gas_extension
```

Detection looks at which `modules/sourcesystem/` modules are installed:

| Installed sourcesystem modules | Resolved pack |
|---|---|
| Only `*_extractor` modules (`cdf_pi_extractor`, …) | `foundation` |
| Only `*_data_dump` modules (`cdf_pi_data_dump`, …) | `demo` |
| Both kinds, or neither | **Ambiguous** — the wizard prompts you to choose |

You never need to pass a flag for this — it's fully automatic for both packs shipped via `cdf modules init`. The ambiguous case only comes up if you hand-cherry-picked source system modules from both packs into the same project.

> **Environment selection — first prompt:** Toolkit creates `dev`, `prod`, and `staging` (= test) config files during `cdf modules init`. The wizard detects these and asks:
> *"You selected dev and prod while installing the DP. Continue with current selection (dev, prod)?"*
> Answer **Y** to proceed with the detected environments, or **N** to modify the selection. Config files for deselected environments are removed after you confirm all changes.

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

python modules/common/cdf_project_foundation/scripts/setup_project.py -y --variant cdm

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

Generate CI/CD that automates build, dry-run, and deploy on PR / merge / release, for either GitHub Actions (default) or Azure DevOps. This can also be triggered through the setup wizard (Step 3):

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force --provider ado
```

The script reads `org-dir` and toolkit version from `cdf.toml` automatically. It uses `environment.project` from each `config.<env>.yaml` as the CDF project name and validates that `environment.name` matches the expected environment.

**GitHub (default):** writes `.github/workflows/` (`dry-run.yml`, `deploy-dev.yml`, `deploy-prod.yml`, and `deploy-test.yml` when `config.test.yaml` exists) and `docs/FOUNDATION_CICD.md` (GitHub Environments and secrets). Configure `ADMIN_SOURCE_ID`, `CONSUMER_SOURCE_ID`, and `PRODUCER_SOURCE_ID` as GitHub Environment variables alongside the CDF auth variables.

**Azure DevOps (`--provider ado`):** writes `.devops/` (`dry-run-pipeline.yml` and one `deploy-<env>-pipeline.yml` per configured environment) and `docs/FOUNDATION_CICD.md` (variable groups, the per-environment deploy pipeline registrations, and the Build Validation branch policy). See `docs/FOUNDATION_CICD.md` for the exact setup steps. Unlike GitHub's single per-environment Environment, each environment gets two variable groups: a non-secret `<env>-toolkit-config` group used by both `toolkit-pr-validate` and the deploy pipeline, and an `<env>-toolkit-credentials` group used by the deploy pipeline only — PR validation never loads a secret, since Build Validation runs compile the pipeline YAML from the PR's own merge ref and can't be trusted with one.

Branching model: PRs to `dev`; PRs to `main` and deploy **test** on merge to `main` only when `config.test.yaml` exists; deploy **dev** on merge to `dev`, and **prod** on a GitHub Release (or, for Azure DevOps, a `vX.Y.Z` tag) from `main`.

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
│   ├── generate_actions.py        # generates GitHub Actions or Azure DevOps CI/CD (--provider)
│   └── generate_env_configs.py    # generates config.{dev,test,prod}.yaml skeletons
├── templates/
│   ├── github/                    # GitHub Actions workflow templates
│   └── ado/                       # Azure DevOps pipeline templates
├── default.config.yaml
└── module.toml
```

---

## Access Groups

Three CDF groups are deployed, each bound to an Entra ID security group via its `sourceId` (the Entra ID group **Object ID**, recorded per SOP Step 3d). Naming follows the **SOP**:

```
<persona>_[{site}_]all_<environment>
```

- `persona` (required): `consumer` | `producer` | `admin`
- `site` (optional): e.g. `oslo` — set via the site / location prompt in the wizard
- `type` (required): `all` for broad persona groups; `ep_<source>` for per-extractor groups (e.g. `ep_sap`, `ep_pi`)
- `environment` (required): `dev` (covers **dev + test**) | `prod`

| Group | Name (example) | Persona | Capability scope |
|-------|---------------|---------|-----------------|
| `consumer.Group.yaml` | `consumer_all_dev` / `consumer_oslo_all_prod` | Read-only | READ on data models / instances / timeseries / files / transformations, scoped to `{{ dataset }}` / `{{ instanceSpaces }}` / `{{ schemaSpace }}` — `instanceSpaces` includes the project-level DM space plus one per installed extractor |
| `producer.Group.yaml` | `producer_all_dev` / `producer_oslo_all_prod` | Read/write | Consumer rights plus WRITE to instances / timeseries / files / RAW, run transformations, workflow orchestration, sessions CREATE, plus `functionsAcl` and `entitymatchingAcl` and read access to `{{ additionalSchemaSpaces }}` (base CDM/IDM + the installed variant's search-solution space) |
| `admin.Group.yaml` | `admin_all_dev` / `admin_all_prod` | Admin | Full capabilities including `groups:write`, projects, datasets, data models, transformations, workflows, extraction pipelines |

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

1. Resolves which pack (`foundation` or `demo`) this project is set up for — see [Which pack am I on?](#which-pack-am-i-on). Shown in the header; only prompts if genuinely ambiguous.
2. Prompts for which environments to set up (all three, dev only, dev+prod, or custom).
3. Asks for the CDF project name for each selected environment (pre-filled on re-run).
4. Asks for an required site / location name — used as access-group suffix, source system location, and entity-matching `location_name`.
5. Prompts for source system integration owner and data owner contacts (shared or per-module).
   - For the CFIHOS data model: prompts for **data model owner** name and email (renamed from "integration owner" to reflect its purpose).
6. Prompts for group source IDs (Entra ID object IDs) and writes them to `.env`.
7. Asks for the Streamlit ApplicationOwner email if `cdf_file_annotation` is installed.
8. Shows a review summary then confirms before writing anything.
9. Creates new config files or updates existing ones in-place (preserving comments).
10. Removes redundant auth files from contextualization, tools, and (Demo pack) `cdf_ingestion` modules covered by the foundation.
11. Removes the synthetic diagram-annotation pipeline if both `cdf_sharepoint_data_dump` and `cdf_file_annotation` are installed — see [Demo pack: synthetic diagram-annotation cleanup](#demo-pack-synthetic-diagram-annotation-cleanup).
12. Optionally generates GitHub Actions CI/CD workflows (this now includes a `setup_project.py --check` step before every `cdf build`, so a project with stale config fails with an actionable message instead of a raw Toolkit build error).

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
# instanceSpace is site-derived once a site is entered: inst_{site}_isa_manufacturing
# (CFIHOS: inst_{site}_cfihos_oil_and_gas). Falls back to the domain-only default
# (shown here) before a site has been set, same convention as the CDM sp_{site}_instances fallback.
instanceSpace: "inst_isa_manufacturing"    # ISA default; CFIHOS uses inst_cfihos_oil_and_gas
instanceSpaces: ["inst_isa_manufacturing"] # project-level space + per-extractor spaces (computed by wizard)
dataModelVariant: isa_manufacturing_extension

# Read-only producer.Group.yaml scope, in addition to schemaSpace. Base CDM/IDM spaces
# are always included; the installed variant's own search-solution space is appended
# (cfihos_oil_and_gas_extension -> dm_sol_oil_and_gas_search,
#  isa_manufacturing_extension -> dm_sol_isa_manufacturing_search, cdm -> none extra).
# Computed per env by setup_project.py; static default here is CDM-only.
additionalSchemaSpaces: ["cdf_cdm", "cdf_idm", "cdf_cdm_units"]

# Computed per env by setup_project.py:
consumerGroupName: "consumer_all_dev"
producerGroupName: "producer_all_dev"
adminGroupName: "admin_all_dev"

# Entra ID group object IDs — stored in .env, referenced here via ${…}:
consumerSourceId: "${CONSUMER_SOURCE_ID}"
producerSourceId: "${PRODUCER_SOURCE_ID}"
adminSourceId: "${ADMIN_SOURCE_ID}"
```

---

## Dependencies

**Package**: `dp:foundation` (primary), also shipped by `dp:quickstart` (Demo).

Self-contained. The group ACLs reference `{{ dataset }}`, `{{ instanceSpaces }}`, `{{ schemaSpace }}`, and `{{ additionalSchemaSpaces }}`, which must match the values used by the deployed source-system and data-model modules. `instanceSpaces` is computed by the setup wizard as the project-level DM space plus one per installed extractor module.

On `dp:quickstart`, this module's persona groups replace `common/cdf_ingestion`'s own auth files (removed by the wizard as redundant) — `cdf_ingestion` itself (workflows, datasets) stays installed and required.

See the [project-setup SOP](https://cogdocs.mintlify.io/gvd) *(password-protected — request access via [#topic-deployment-packs](https://cognitedata.slack.com/archives/C098QJ09YKX) or contact [Valeriya Naumova](https://cognitedata.slack.com/team/U051XA95S0G))* for the authoritative procedure covering environments, Entra ID integration, CI/CD, and sign-off.

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
