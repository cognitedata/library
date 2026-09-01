# Foundation Deployment Pack — CI/CD setup

Generated from committed Toolkit environment configs.

This follows the [CDF Foundation Setup guide](https://cogdocs.mintlify.io/gvd) *(password-protected — request access via [#topic-deployment-packs](https://cognitedata.slack.com/archives/C098QJ09YKX) or contact [Valeriya Naumova](https://cognitedata.slack.com/team/U051XA95S0G))* — Step 5, Option B (Azure DevOps).

## Branching model

| Git branch / event | CDF project | Trigger |
|--------------------|-------------|---------|
{{BRANCHING_ROWS}}

If a pre-production environment is present, PRs to `main` must come from `dev` or `hotfix/*` only.

## Branch policies

Protect the branches used above under **Repos → Branches → Branch policies**. This is required, not optional:

| Branch | Required reviewers | Required status checks |
|--------|---------------------|--------------------------|
{{BRANCH_PROTECTION_ROWS}}

{{BRANCH_PROTECTION_NOTE}}

Register `dry-run-pipeline.yml` (the `toolkit-pr-validate` pipeline, see below) as a **Build Validation** policy on every branch listed above, so it runs automatically as a required check on each PR.

## Variable groups

Create one variable group per environment under **Pipelines → Library**:

| Variable group | Used by | `CDF_PROJECT` example |
|----------------|---------|-------------------------|
{{ENVIRONMENT_ROWS}}

Scope each group via **Pipeline permissions** to only the pipeline(s) that need it — the `dev-toolkit-credentials` group should grant access to `toolkit-pr-validate` and `toolkit-deploy-dev` only, and so on per environment.

**Pipeline permissions alone are not sufficient for `toolkit-pr-validate`.** It runs as a Build Validation policy, which executes the pipeline YAML as modified by the pull request itself — a PR author who edits `.devops/dry-run-pipeline.yml` could otherwise use that access to exfiltrate the loaded secrets, including `IDP_CLIENT_SECRET`. On every variable group, also add an **Approvals and checks → Branch control** check (and/or a required approval) under **Pipelines → Library**, so secrets are only released to runs building from a trusted target branch and pipeline definition, not to arbitrary PR-modified YAML. Do this before using any of these variable groups against a real customer project.

Each group needs these **variables**:

- `CDF_CLUSTER`
- `CDF_PROJECT` (must match `config.<env>.yaml`)
- `LOGIN_FLOW` (typically `client_credentials`)
- `IDP_TENANT_ID`
- `IDP_TOKEN_URL` — token URL for non-Entra identity providers; for Entra ID, use the standard Entra configuration and `IDP_TENANT_ID`.
- `IDP_CLIENT_ID`
- `ADMIN_SOURCE_ID`
- `CONSUMER_SOURCE_ID`
- `PRODUCER_SOURCE_ID`

And this **secret** (mark it secret in the Library UI):

- `IDP_CLIENT_SECRET`

## Pipelines to register

Import the generated YAML as Azure DevOps pipelines under **Pipelines → New pipeline → Azure Repos Git**, selecting **Existing Azure Pipelines YAML file**:

| Pipeline name | YAML file | Trigger |
|---------------|-----------|---------|
| `toolkit-pr-validate` | `.devops/dry-run-pipeline.yml` | PR to `dev` or `main` (via Build Validation policy above) |
{{DEPLOY_PIPELINE_ROWS}}

Each deploy pipeline has its own YAML file, scoped to only that environment's variable group — Azure authorizes every variable group referenced anywhere in a pipeline's YAML, not just the one an eventual runtime condition ends up using, so a shared file would force authorizing all three groups on all three registrations. With one file per environment, `dev-toolkit-credentials` only ever needs to be authorized for `toolkit-deploy-dev`, and so on. Both `dry-run-pipeline.yml` and every `deploy-*-pipeline.yml` set `trigger: none`; for each pipeline above, open **Edit → Triggers → Continuous integration** and override it to that pipeline's branch or tag pattern from the table, so it actually runs when its branch/tag is pushed.

## Toolkit configs

This generator only writes Azure DevOps pipeline YAML and this guide. It does not
create or refresh {{ENV_CONFIG_LIST}}.

Before opening a PR, run the project setup wizard and commit the resulting config
files together with the pipelines:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py
cdf build {{EXAMPLE_BUILD_ARGS}}
```

CI validates the committed configs as-is; it does not regenerate them.
If the repository does not have a root `.pre-commit-config.yaml`, the generated
PR pipeline skips the pre-commit config lint step.

If any CDF Function under a `functions/` folder has Python source, the PR pipeline
also runs `ruff check` and `pyright` against it, installing each function's
`requirements.txt` first so imports resolve. Projects with no `functions/` Python
code skip this step.

## Regenerate pipelines

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --provider ado --force
```

## Toolkit version

Pipelines install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
