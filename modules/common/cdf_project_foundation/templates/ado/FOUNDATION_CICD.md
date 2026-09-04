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

| Branch | Minimum number of reviewers | Required status checks |
|--------|------------------------------|--------------------------|
{{BRANCH_PROTECTION_ROWS}}

{{BRANCH_PROTECTION_NOTE}}

{{DRY_RUN_REGISTRATION_NOTES}}

## Variable groups

{{VARIABLE_GROUPS_INTRO}}

| Variable group | Used by | `CDF_PROJECT` example |
|----------------|---------|-------------------------|
{{ENVIRONMENT_ROWS}}

Scope each group via **Pipeline permissions** to only the pipeline(s) that need it — {{VARIABLE_GROUP_SCOPING_EXAMPLE}}, and so on per environment.

{{TRUST_BOUNDARY_NOTE}}

Each `-toolkit-config` group needs these **variables**:

- `CDF_CLUSTER`
- `CDF_PROJECT` (must match `config.<env>.yaml`)
- `LOGIN_FLOW` (typically `client_credentials`)
- `IDP_TENANT_ID`
- `IDP_TOKEN_URL` — token URL for non-Entra identity providers; for Entra ID, use the standard Entra configuration and `IDP_TENANT_ID`.
- `IDP_CLIENT_ID`
- `ADMIN_SOURCE_ID`
- `CONSUMER_SOURCE_ID`
- `PRODUCER_SOURCE_ID`

Each `-toolkit-credentials` group needs only this **secret** (mark it secret in the Library UI):

- `IDP_CLIENT_SECRET`

## Pipelines to register

Import the generated YAML as Azure DevOps pipelines under **Pipelines → New pipeline → Azure Repos Git**, selecting **Existing Azure Pipelines YAML file**:

| Pipeline name | YAML file | Trigger |
|---------------|-----------|---------|
{{PIPELINE_ROWS}}

Each deploy pipeline has its own YAML file, scoped to only that environment's variable groups (`-toolkit-config` and `-toolkit-credentials`) — Azure authorizes every variable group referenced anywhere in a pipeline's YAML, not just the one an eventual runtime condition ends up using, so a shared file would force authorizing every environment's groups on all three registrations. With one file per environment, {{DEPLOY_AUTHORIZATION_EXAMPLE}}, and so on. Each `deploy-*-pipeline.yml` declares its own trigger directly in the YAML (the branch or tag pattern from the table above), so importing it is enough — no manual **Edit → Triggers** step, which wouldn't be visible in git history and would be lost if the pipeline is ever re-registered.{{DRY_RUN_TRIGGER_NOTE}}

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
