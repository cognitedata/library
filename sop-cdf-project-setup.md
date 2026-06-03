# SOP: CDF Project Setup

Author:       Philippe Betler, Valeriya Naumova   
Owner:        Accelerator Team   
Approver:     Accelerator Team Lead   
Valid From:   June 2026   
Version:      1.0   
Status:       Draft

---

## TABLE OF CONTENTS

1. Purpose  
2. Scope  
3. Roles and Responsibilities  
4. Prerequisites  
5. Steps  
6. Agentic Skills & Artifacts  
7. Governance  
8. Continuous Improvement  
9. Related Best Practices

---

## 1\. PURPOSE

This SOP defines the standard procedure for setting up the technical foundation of a new customer CDF project within Global Value Delivery engagements. It covers environment provisioning, naming conventions, Microsoft Entra ID integration, persona-based access control, and CI/CD pipeline.

Every project **must** conform to the standards here. Deviations require Regional Head approval before work begins.

Motivated by:

- **Quality Assurance**: Consistent, auditable foundation that supports delivery at scale.  
- **Continuous Learning**: Front-line learnings are incorporated each review cycle.  
- **Compliance Enforcement**: CI/CD and agentic skills enforce naming and access policies continuously.

AI-assisted tooling is a standard part of Value Delivery engagements. Engineers use company-approved AI tools (Claude, Cursor, Gemini) to accelerate configuration generation, code authoring, and application development. Quality and security are enforced at the point of generation through skills: structured best-practice guardrails embedded in the AI tooling, published via the private Claude and Cursor marketplaces and maintained in the plugin marketplace repo[^1].  All AI-assisted outputs pass through the same CI checks and peer review gates defined in this SOP. Approved tools and mandatory setup steps are maintained on Confluence by the Cognite IT team.  
---

## 2\. SCOPE

**In Scope**

- CDF environment provisioning (three environments mandatory)  
- Repository template on Github or Azure DevOps  
- Naming conventions   
- Access control   
- CI/CD   
- Foundation validation and sign-off

**Out of Scope**

- Data ingestion  
- Data modeling and transformations  
- Contextualisation  
- Application development and deployment

---

## 3\. ROLES AND RESPONSIBILITIES

| Company | Role | Responsibility |
| :---- | :---- | :---- |
| Cognite | Solution Architect | Leads foundation setup; owns environment and access design; coordinates with customer IT; performs sign-off. |
| Cognite | Data Engineer | Executes setup steps; configures Toolkit, CI/CD, CDF groups, service principals. |
| Cognite | Project Manager | Confirms prerequisites; tracks milestones; coordinates with customer stakeholders. |
| Cognite | Regional Head (EMEA / MENA / AMER) | Escalation point for deviations; approves exceptions to this SOP; ensures regional compliance. |
| Cognite | Accelerator Team Lead | Owns and maintains this SOP; approves structural changes to the standard. |
| Customer | Entra ID / IdP admin | Creates security groups, app registrations, client secrets; rotates secrets; manages guest invites |
| Customer | Infrastructure / network admin | Provisions VMs for extractors; opens firewall paths; issues VM credentials |
| Customer | Data steward (data set owner lead) | Routes per-data-set ownership questions; lines up data set owners for M4 |
| Customer | Project sponsor / business lead | Scope and timeline escalation |

---

## 4\. PREREQUISITES

Before this SOP begins, four prerequisite meetings must have been completed. These are owned by Field Engineering or the Solution Architect when FE is not available. Confirm all meetings are done before starting Step 1\.

CDF projects have already been requested using the [Playbook page](https://cognitedata.atlassian.net/wiki/spaces/AUTH/pages/4419485836/Playbook+for+creating+customer+CDF+orgs+and+projects) and provisioned on the confirmed cluster and region; delivery team has working admin access to all three environments  
---

**Meeting 1 (M1) — M1 Identity and Access Management** *(FE / SA \+ named Customer Entra ID admin)*

- [ ] Customer Entra ID tenant active; Tenant ID confirmed; Cognite SA added as guest user with CDF admin group membership  
- [ ] CDF organisation updated to point to customer Entra ID; consent granted to Cognite Enterprise applications (Fusion UI \+ API)  
- [ ] **Service-principal inventory completed**: one app registration per source system per environment plus one deploy CLI app per environment. Each entry has a planned client ID, secret name, Key Vault location, and rotation owner captured in Section 10  
- [ ] **IdP credential pair** captured per environment: Client ID and Client Secret  
- [ ] **SDK app registration creation** Follow the guide [https://docs.cognite.com/dev/sdks/python/register\_app\_jupyter\_sdk](https://docs.cognite.com/dev/sdks/python/register_app_jupyter_sdk) in order to create an application registration that people can use to authenticate to cdf without secrets, but using their own credentials (“interactive” method [https://cognite-sdk-python.readthedocs-hosted.com/en/latest/credential\_providers.html](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/credential_providers.html) )  
- [ ] **Bootstrap admin path** agreed: an interim CDF admin group exists in each project, the deploy SP is a member of it for first deploy only, and removal of that membership after Toolkit-managed groups are live is on the project plan (see Step 3e)  
- [ ] Project team members added as guest users in Entra ID (where known)

**Meeting 2 — M2 Infrastructure Setup** *(Field Engineering / SA \+ Customer Infra, Network, Data Source Admins)*

- [ ] Repository (GitHub or Azure DevOps) created (private); delivery team has access  
- [ ] CDF integration architecture diagram approved by customer  
- [ ] Virtual machines for extractors ordered and availability date confirmed  
- [ ] Data source endpoints (SAP PM and MM, OSI PI or OPC-UA, file management system) identified and network/firewall access agreed  
- [ ] Data sets within source systems identified (input to data set review)  
- [ ] Live vs. extract data approach confirmed per data source

**Meeting 3 — M3 Data Set Review** *(SA \+ Customer Data Set Owners)*

- [ ] Asset processing unit in scope defined  
- [ ] Data set list agreed and named data owners assigned per data set  
- [ ] Interface / landing zone for data sets agreed (RAW or direct DM)  
- [ ] Sample data exports received for quality and usability validation  
- [ ] KPIs for data quality agreed

**Meeting 4  — M4 Project Kickoff** *(SA \+ Customer Project Sponsor \+ Customer IT Lead; FE and AE/M if available)*

Teams agree on ways of working, understand roles and responsibilities and agree on key requirements.   
Confirm a **Customer / Cognite Roles & Responsibility (R\&R) matrix (see paragraph3 above)**. No role may be left as TBD — if a role is unstaffed, scope is reduced or the dependent meeting (M2 / M3 / M4) is blocked.

**Outputs:**

- [ ] R\&R matrix above recorded in Section 10 with name \+ email per role  
- [ ] **User onboarding mode of operation** agreed for the foundation phase: request channel (Jira / ADO work item / shared inbox / Slack), approver per environment, target SLA, guest-user invitation flow for Cognite engineers and third parties. This is the foundation-phase process only; post-handover is owned by Customer IT  
- [ ] **Naming convention source** decided: Cognite default (per Step 2\) | customer-incumbent | hybrid. If non-default, customer's incumbent docs collected and attached to Section 10  
- [ ] **Access-control model** decided: persona-based: Cognite-default (per Step 3a) | system/function-based | hybrid. If non-persona, Regional Head deviation logged per Section 7  
- [ ] **Secret store** confirmed: **Azure Key Vault** (default) or GitHub Secrets / ADO variable groups  
- [ ] **Three-environment intent** confirmed: Development, Test and Production  
- [ ] **CI/CD platform** decided: GitHub Actions (default) | Azure DevOps. This unblocks M3 (org / project provisioning) and the repository bootstrap step  
- [ ] **Communication channels** stood up: Teams, Slack channel, Jira / ADO board, project distribution lists, Sharepoint — recorded in Section 10  
- [ ] **Cognite team entitlements** to approved AI tools (Claude, Cursor, Gemini) verified per Confluence IT Ops setup guide

**Rule**: Regional Head deviations identified in this meeting are raised within 48 hours of the kickoff. M2 cannot start until any blocking deviation is approved.

---

**Repository and Tooling** *(Delivery Team — confirm before starting Step 1\)*

- [ ] Access control requirements gathered: personas, site/domain restrictions, service principal inventory  
- [ ] Python ≥ 3.12 available on all developer machines  
- [ ] `uv` installed (`pip install uv` once — then pip is no longer needed)  
- [ ] **Cog-vd-security, cog-vd-best-practices** Cursor/Claude plugin installed on every developer's machine  
- [ ] Approved AI tools configured per Confluence IT Ops setup guide (Claude, Cursor) — mandatory security configuration steps completed before first use  
- [ ] Three CDF project names confirmed following the naming pattern: `{enterprise}-dev`, `{enterprise}-test`, `{enterprise}-prod`

---

## 5\. STEPS

Legend: 👤 \= Manual step 🤖 \= Automated / agentic step

---

### Step 1 — Environment Verification

**Owner**: Solution Architect | Duration: \~0.5 day

CDF project provisioning and IdP/Entra ID integration are completed during the prerequisite meetings. This step confirms everything is in order before the delivery team proceeds, against the standard below.

**Single-site vs. multi-site.** Default to single-site. Adopt multi-site only when there is more than one physical site with independent data ownership. The choice is made at M4 and applied consistently across project name, config files (Step 4), access groups (Step 3b), and CI/CD environments.

**Naming.** All foundation resources use the ordered dimensions `[{segment}-][{region}-][{country}-]{site}`. `{site}` is mandatory for multi-site (the unit of independent data ownership); `{segment}`, `{region}`, `{country}` are optional prefixes, added only to disambiguate sites that would otherwise collide. Single-site projects omit `{site}` and use `{enterprise}` alone.

| Mode | CDF project name | Example |
| :---- | :---- | :---- |
| Single-site | `{enterprise}-{env}` | `acme-dev` |
| Multi-site | `{enterprise}-[{segment}-][{region}-][{country}-]{site}-{env}` | `acme-emea-no-oslo-dev` |

**Environments**. Every project has at least three: `dev` (active development, unstable by design), `test` (integration testing and UAT), `prod` (live; CI/CD-only deployments). `test` mirrors `prod` in configuration structure, access-group layout, and module set — differing only in data volume, scope, and secret values (per site in multi-site projects). Parity is what makes a passing `test` deploy a reliable predictor of a `prod` release.

**Verify:**

* 👤 All CDF projects accessible with the delivery team's admin credentials. If any is missing or inaccessible, raise it immediately with the SA who ran the prerequisite meetings.  
* 👤 Cognite API consent active in Entra ID and the Fusion application registered. If not, the Customer Entra ID admin completes this before proceeding.

---

### Step 2 — Naming Conventions

**Owner**: Solution Architect \+ Data Engineer | Duration: \~0.5 day (initial); ongoing

Follow the best practices on the naming convention [https://docs.cognite.com/cdf/deploy/reference/cdf\_resource\_naming\_convetnions](https://docs.cognite.com/cdf/deploy/reference/cdf_resource_naming_conventions)  and [https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734](https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734) and [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/references/naming\_conventions](https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/naming_conventions) If the customer has existing naming convention rules that deviates from the one described in the best practices, adopt theirs but be aware that it will affect the development timelines. 

### Step 3 — Access Control Design and Group Setup

**Owner**: Solution Architect \+ Data Engineer \+ Customer IT | Duration: \~1–2 days

#### 3a — Persona Framework

Every project **must** use a persona-based access control model:

| Persona | Access Level | Typical Members |
| :---- | :---- | :---- |
| **Consumer** | Read-only to relevant features and data | Business analysts, field workers, executives |
| **Producer** | Read/write to needed features and data | Data engineers and data scientists, solution architects, service principal |
| **Admin** | Full access including group management | Project leads, security admins, platform owners |

#### 3b — Access Group Naming

```
<persona>_[{segment}_][{region}_][{country}_][{site}_][{type}_]<environment>
```

| Component | Required | Examples |
| :---- | :---- | :---- |
| `persona` | Yes | `consumer`, `producer`, `admin` |
| `site` (+ optional `segment`/`region`/`country` prefixes) | Required for multi-site; omitted for single-site | `oslo`, `hou`, `tokyo` |
| `type` | Optional; required for service principal   | `ep_pi, ep_opcua or pp` |
| `environment` | Yes | `prod` (production), `dev` (dev \+ test) |

#### 3c — Minimum Required Groups

Every project **must** deploy these groups before going to production:

| Group | Persona | Env | Purpose |
| :---- | :---- | :---- | :---- |
| `admin_prod` | Admin | prod | Full platform access in production |
| `admin_dev` | Admin | dev and test | Full platform access in dev \+ test |
| `producer_dev` | Producer | dev and test | Data engineers, data scientists in dev/test |
| `producer_ep_opcua_dev` | Producer | dev and test | Create one access group per extractor |
| `producer_ep_opcua_prod` | Producer | prod | Create one access group per extractor in prod |
| `producer_pp_dev` | Producer | dev and test | Service principal for process pipelines in dev/test |
| `producer_pp_prod` | Producer | prod | Service principal for process pipelines in prod |
| `consumer_prod` | Consumer | prod | Business users, Data Engineers and Data Scientists, read-only |
| `cognite_toolkit_service_principal` | — | All | Service principal for Toolkit  |

**Rule**: Start with this minimum set. Add groups only when a concrete, documented need arises.

#### 3d — Entra ID Groups and App Registrations

👤 Customer IT creates all Entra ID security groups following the naming convention and share **Object ID**. Group membership is owned by Customer IT throughout the project.

👤 Secrets are stored in **Azure Key Vault** (default). Secret names, Key Vault references, and expiry dates are recorded in Section 10\.

**Rule**: No secrets in the repository, `.env` files, or hardcoded config. All secrets are referenced via Key Vault or CI/CD secret stores only.

#### **Rule**: Service-principal secrets are rotated on a fixed cadence (default 90 days; align to customer policy if stricter). The rotation owner per secret is captured at M1.

#### 🤖 A scheduled check (`secret-expiry-monito`r) warns the delivery team 14 days before any recorded secret expiry, so CI/CD credentials are rotated before a pipeline break — not after. Until automated, the SA reviews recorded expiry dates at each bi-weekly checkpoint.

#### 3e — CDF Groups and Capabilities

For every Entra ID group created in Step 3d, a corresponding CDF group must exist. 

👤 For each group in the minimum required set (Step 3c), create a `<name>.Group.yaml` file in `all-envs/auth-governance/auth/` following the Toolkit `auth` module convention. When `cdf deploy` runs, the Toolkit creates or updates the CDF groups from these files — groups are never created manually via the Fusion UI.

Each `<name>.Group.yaml` specifies:

- `name` — CDF group name, matching the Entra ID group name per the convention in Step 3b  
- `sourceId` — the Object ID of the corresponding Entra ID security group (recorded in Step 3d)  
- `capabilities` — list of capability types with their actions and scopes (datasets, spaces, or all)

Capabilities follow **least privilege**:

| Persona | Capabilities |
| :---- | :---- |
| Consumer | `READ` on relevant resource types, scoped to permitted datasets/spaces |
| Producer | `READ` \+ `WRITE` on needed resource types, scoped appropriately |
| Admin | Full capabilities including `groups:write` |

---

### Step 4 — Repository and Toolkit Setup

**Owner**: Data Engineer | Duration: \~0.5 day

All project configuration is managed as code from day one. Follow the official Toolkit guides for installation and initialisation steps — this section defines what must be in place, not how to run the commands.

* **Setup guide**: [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/guides/setup](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/setup)

**Key requirements**

| Requirement | Standard |
| :---- | :---- |
| Toolkit version | Always latest when setting up and pin that version for the usage — check [https://github.com/cognitedata/toolkit/releases](https://github.com/cognitedata/toolkit/releases) before starting |
| Package management | `uv` only — `uv tool install cognite-toolkit`, `uv tool upgrade cognite-toolkit`.  |
| Code quality | `ruff` (linting \+ formatting), `pyright` (type checking, unused variables) and relevant AI skills as listed in Section 10 |
| Module structure | `all-envs/auth-governance/`, `all-envs/data-models/`, `all-envs/data-sources/` |
| Config files | One `config.<env>.<country>.<site>.yaml` per environment (`dev`, `test`, `prod`) in the organisation directory |
| Secrets | Never in the repository — all secrets via Key Vault or CI/CD secret store |

#### Branch Protection and CODEOWNERS

The following must be in place before the first PR is opened.

**Branch protection** — enable on `main`:

- Require pull request before merging  
- Require at least 1 reviewer  
- Require status checks to pass (dry-run pipeline)  
- No direct pushes  
- Require CODEOWNERS review

**CODEOWNERS** — create `.github/CODEOWNERS`. Use team handles, never individual usernames:

```
*                               @cognitedata/<sa-team>
all-envs/auth-governance/       @cognitedata/<sa-team>
all-envs/data-models/           @cognitedata/<data-engineering-team>
all-envs/data-sources/          @cognitedata/<data-engineering-team>
```

**Rule**: Never use individual GitHub usernames in CODEOWNERS. If a person leaves the project, their pending reviews become a permanent blocker.

#### Gemini Code Assist (GitHub / cognitedata org only)

Gemini automatically reviews every PR for security vulnerabilities, hard-coded credentials, logic gaps, and clean code adherence. This applies only to repositories hosted under the `cognitedata` GitHub organisation — not available for ADO or external GitHub orgs.

👤 Enable Gemini for the repo by opening a PR on the [Terraform repo](https://github.com/cognitedata/terraform/tree/master/github/gemini-enabled-repos).

👤 Create the `.gemini/` folder by copying the reference config from [https://github.com/cognitedata/terraform/tree/master/.gemini](https://github.com/cognitedata/terraform/tree/master/.gemini) and adapting for the project.

### Step 5 — CI/CD Pipeline Setup

**Owner**: Data Engineer | Duration: \~1 day

#### Branching Model

Two long-lived protected branches plus release tags. Promotion is always a PR or a release — deployment is fully automated after merge or tag creation.

```
feat/<description>  ──┐
fix/<description>   ──┤  PR   dev ──► {enterprise}-dev
                      │        │
hotfix/<description>──┘        │  PR (from dev only)
        │                       ▼
        └──────────────────►  main ──► {enterprise}-test
                                 │
                                 │  GitHub Release (tag vX.Y.Z)
                                 ▼
                           {enterprise}-prod


```

| Trigger | CDF Project | Allowed Source | Auto-deploys |
| ----- | ----- | ----- | ----- |
| Push to `dev` | `{enterprise}-dev` | `feat/*`, `fix/*` via PR | Yes, on merge |
| Push to `main` | `{enterprise}-test` | `dev` or `hotfix/*` via PR | Yes, on merge |
| GitHub Release (tag) | `{enterprise}-prod` | Must be tagged from `main` only | Yes, on release publish |

**Rule**: No direct pushes to `dev` or `main`. All changes flow through a reviewed PR with passing checks.

**Rule**: Production releases are triggered by publishing a GitHub Release from `main`. Tag format: `vX.Y.Z`. Tagging from any branch other than `main` is rejected by the pipeline source-branch check.

**Rule**: If a production deploy fails or introduces a regression, roll back by publishing a GitHub Release from the **last known-good tag** (`vX.Y.Z`). No hotfix or direct prod edit is required for rollback — re-releasing the previous tag redeploys the prior config. The failed tag is then investigated via the normal `hotfix/*` flow.

#### Branch Naming Convention

| Pattern | Example |
| :---- | :---- |
| `feat/<description>` | `feat/add-opi-extractor` |
| `fix/<description>` | `fix/auth-group-capabilities` |
| `hotfix/<description>` | `hotfix/fix-prod-access-group` |

#### Hotfix Process

Use `hotfix/*` only when production is broken and cannot wait for the normal `dev → test → prod` cycle.

**Flow:**

```
hotfix/<description>
        │
        ├──► PR to main ──► auto-deploy to {enterprise}-test
        │
        ├──► GitHub Release from main ──► auto-deploy to {enterprise}-prod
        │
        └──► back-merge PR to dev   (immediately after)
```

	  
**Most likely CDF hotfix scenarios:**

- Access group misconfiguration blocking business users in production  
- CI/CD service principal secret expired, causing pipeline failures  
- Transformation or extraction pipeline config error in production

---

#### PR Validation Checks (All Platforms)[^2]

Every PR pipeline **must** include:

| Check | Platforms | Purpose |
| :---- | :---- | :---- |
| `cdf build` | All | Toolkit config compiles without errors |
| `cdf deploy --dry-run` | All | Deployment intent validated without applying changes |
| YAML / config lint | All | Syntax and schema correctness on all config files |
| `ruff check` 🤖 | All | Python linting — enforces code style, catches common errors |
| `pyright` 🤖 | All | Static type checking for Python code |
| `Pre-commit naming convention` | All | Manual, automation planned Validates resource and group names against the Step 2 / Step 3b patterns before commit  |
| cog-vd-security 🤖 | Cursor | Catches secrets, credential leaks, public repo exposure |
| Gemini Code Assist 🤖 | GitHub / cognitedata org only | See section 4 |
| Source-branch guardrail 🤖 | All | PRs to `main` from `dev` or `hotfix/*` only; prod is release-from-`main` only. |
| Access group structure check 👤 | All | Manual step, automation planned |

#### Option A — GitHub Actions (Default)

👤 Configure workflows for the three-branch model:

| Workflow | Trigger | Action |
| :---- | :---- | :---- |
| `dry-run.yml` | PR to `dev` or `main` | `cdf build` \+ `cdf deploy --dry-run` \+ `ruff check` \+ `pyright` \+ source-branch check on `main` |
| `deploy-dev.yml` | Merge to `dev` | Deploy to `{enterprise}-dev` |
| `deploy-test.yml` | Merge to `main` | Deploy to `{enterprise}-test`  |
| `deploy-prod.yml` | GitHub Release published from `main` | Deploy to `{enterprise}`\-`prod`  |

---

#### Option B — Azure DevOps

👤 Create **variable groups** under Pipelines \> Library — scoped to specific pipelines only:

| Variable Group | Linked To | Points To |
| :---- | :---- | :---- |
| `dev-toolkit-credentials` | dev pipelines only | `{enterprise}-dev` |
| `test-toolkit-credentials` | test pipelines only | `{enterprise}-test` |
| `prod-toolkit-credentials` | prod pipelines only | `{enterprise}-prod` |

Each group contains: `CDF_CLUSTER` (plain), `CDF_PROJECT` (plain), `LOGIN_FLOW` \= `client_credentials` (plain), `IDP_CLIENT_ID` (plain), `IDP_CLIENT_SECRET` (**secret** — mark with padlock), plus one secret per service principal referenced in `config.<env>.yaml` as `${...}` variables.

👤 Register the following pipelines in ADO pointing to `.devops/`:

| Pipeline | YAML | Trigger | Action |
| :---- | :---- | :---- | :---- |
| `Toolkit-PR-Validate` | `dry-run-pipeline.yml` | PR to `dev` or `main` | `cdf build` \+ `cdf deploy --dry-run` \+ `ruff check` \+ `pyright`; used as Build Validation policy |
| `Toolkit-Deploy-Dev` | `deploy-pipeline.yml` | Push to `dev` | Deploys to `{enterprise}-dev` |
| `Toolkit-Deploy-Test` | `deploy-pipeline.yml` | Push to `main` | Deploys to `{enterprise}-test` |
| `Toolkit-Deploy-Prod` | `deploy-pipeline.yml` | Git tag `vX.Y.Z` from `main` | Deploy to `{enterprise}`\-`prod`  |

**ADO-only additional pipelines** (Value Delivery standard; no GitHub equivalent yet):

| Pipeline | Trigger | Purpose |
| :---- | :---- | :---- |
| `toolkit-access-control-validate` | PR touching `auth-governance/**` | Scoped dry-run for access control changes only |
| `toolkit-data-modelers-validate` | PR touching `data-models/**` | Scoped dry-run for data model changes only |
| `toolkit-ingestion-validate` | PR touching `data-sources/**` | Scoped dry-run for ingestion changes only |
| `auth-governance-sync` | Scheduled / manual | Detects drift between group definitions and deployed CDF config |

---

### Step 6 — Validation & Sign-off

**Owner**: Solution Architect | Duration: \~0.5 day

👤 The Solution Architect validates the full checklist below. Naming convention and access group checks are manual until dedicated validation skills are available.

**Environments:**

- [ ] At least three CDF projects provisioned and accessible: `{enterprise}-dev`, `{enterprise}-test`, `{enterprise}-prod`

**Naming Conventions:**

- [ ] All resources created during foundation follow the patterns in Step 2  
- [ ] No two identifiers differ only by capitalisation  
- [ ] All abbreviations documented

**Access Control:**

- [ ] All group names follow `<persona>_[site]_[type]_<environment>` (single-site omits `site`; multi-site includes it per Step 3b)  
- [ ] All minimum required groups (Step 3c) deployed across all environments  
- [ ] CDF groups linked to Entra ID groups via correct `sourceId`  
- [ ] Capabilities scoped per persona (Consumer \= read-only, Producer \= read/write, Admin \= full)  
- [ ] CI/CD principals scoped to Toolkit deployment capabilities only  
- [ ] No secrets in the repository; all in Key Vault / CI/CD secret store

**Repository and CI/CD:**

- [ ] Module structure matches Step 4c  
- [ ] Branch protection active on `main`: no direct pushes, 1 reviewer, validator must pass  
- [ ] Source-branch guardrails confirmed: `main` only accepts from `dev` or `hotfix/*`

👤 Sign-off recorded either in GitHub PR / ADO work item or another document per project choice.

---

## 6\. AGENTIC SKILLS & ARTIFACTS

Value Delivery uses a "left of code" quality model: skills enforce best-practice patterns at the point of AI generation, so that quality is built in before code reaches the repo. Skills are published via the Claude and Cursor marketplaces for easy installation across VD, and are maintained in the company-wide plugin marketplace repo where all engineers can contribute.

Deployment Packs provide pre-validated, production-ready config and code templates for the most common CDF delivery patterns. They are added to a project via \`cdf modules add\` from the Cognite library (github.com/cognitedata/library) and cover most code intensive tasks like P\&ID  contextualisation, entity matching, and related patterns. AI is used to adapt and extend these templates within the constraints they define.

| Skill | Automates | Triggered By | Output |
| :---- | :---- | :---- | :---- |
| Toolkit project Deployment pack | Sets up Toolkit folder structure, CI config, and repo skeleton following this SOP | AI prompts | Correct folder structure, base CI pipeline files |
| **cog-vd-security** (Cursor plugin) | 6-layer security architecture active throughout the Cursor session: session start (enforces .cursorignore), file access (blocks .env/.pem/.key, .ssh/.aws/.kube), shell commands (blocks public repo creation, secret reads, unapproved network auth, git tampering), prompt submission (scrubs API keys/tokens/JWTs before reaching LLM), MCP execution (approved servers only, fails closed), git pre-push (blocks pushes to public repos). Non-blocking observational logging for bulk CDF data exposure. | Cursor IDE — file save / edit; also enforced in CI on PRs | In-editor warnings \+ CI pass/fail gate |
| **cog-vd-best-practices**  | Enforces VD best practices for data models, transformations, workflows, and functions — constrains AI generation to follow CDF patterns | Claude and Cursor marketplaces; installed by all VD teams | DMS-compliant configs, idiomatic transformation logic, correctly structured workflow and function definitions |
| **Deployment packs cognitedata/library**  | Pre-validated config and code templates for contextualisation pipelines and entity matching — AI adapts and extends within defined constraints | cdf modules add from github.com/cognitedata/library | Production-ready YAML configs and Python scripts for common CDF delivery patterns |

---

## 7\. GOVERNANCE

**Approval Gates:**

| Gate | Condition | Approver |
| :---- | :---- | :---- |
| Pre-execution | All prerequisites confirmed; naming patterns and access design agreed with Customer IT before any resource is created | Solution Architect |
| Sign-off | All Step 6 checklist items pass; CI pipeline validated end-to-end | Solution Architect \+ Regional Head |
| Expert approval | Deviation from: three-environment structure, persona-based access, Toolkit CI/CD, or Entra ID as IdP | Regional Head |

**Compliance — Deviations requiring Regional Head approval before proceeding:**

- Fewer than three CDF environments  
- Not using the persona-based access model  
- Manual access group management without CI/CD  
- No branch protection on `main`  
- Secrets outside Key Vault or CI/CD secret store  
- IdP other than Microsoft Entra ID

Access control ownership transfers to Customer IT at end of foundation phase. Post-handover changes follow the customer's IT process.

---

## 8\. CONTINUOUS IMPROVEMENT

Reviewed every 6 months by the Accelerator Team. The template version is tracked in the project repository. Front-line contributors are encouraged to flag unclear steps or recurring deviations — contact the Accelerator Team Lead.

---

## 9\. RELATED BEST PRACTICES

Links marked \[TBD\] will be published as standalone best practice documents. This SOP does not depend on them.

- **Naming Conventions Best Practice**: [https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734](https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734)   
  [https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734](https://hub.cognite.com/best-practices-internal-461/best-practices-naming-conventions-5734) and  
   [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/references/naming\_conventions](https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/naming_conventions)   
- **Toolkit Authentication and Authorization**: [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/guides/auth](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/auth)  
- **Cognite Toolkit — Setup**: [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/guides/setup](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/setup)  
- **Cognite Toolkit — Configure, Build, Deploy**: [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/guides/usage](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/usage)  
- **CI/CD**: [https://docs.cognite.com/cdf/deploy/cdf\_toolkit/guides/cicd](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/cicd)  
- **Identity and Access Management**: [https://docs.cognite.com/cdf/access/](https://docs.cognite.com/cdf/access/)  
- **CDF Allowlist URLs**: [https://docs.cognite.com/cdf/admin/allowlist](https://docs.cognite.com/cdf/admin/allowlist)

---

*Last updated: May 2026 — Version 1.0*  


[^1]:  Company-wide plugin marketplace for AI coding tools [https://github.com/cognitedata/ai-tooling-marketplace](https://github.com/cognitedata/ai-tooling-marketplace) 

[^2]:  All PR validation checks apply to every change regardless of how the code was produced. Code generated or substantially modified using AI tools passes through the same build, lint, type-check, security scan, and peer review gates as human-written code. No exemptions apply.