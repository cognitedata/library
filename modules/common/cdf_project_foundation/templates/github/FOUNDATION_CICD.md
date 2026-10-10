# Foundation Deployment Pack — CI/CD setup

Generated from committed Toolkit environment configs.

This follows the Cognite Documentation on [Setting up CI/CD pipelines](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/cicd/index).

## Before you start

Generating the workflows needs no special access; configuring the repository needs admin:

| Step | Where | Access you need |
|------|-------|-----------------|
| 1. Generate the workflows (`generate_actions.py`) | Your machine | None. It only writes files locally. |
| 2. Commit and push `.github/workflows/` | Git | Write access to the repository, and a token with the `workflow` scope |
| 3. Create environments, variables, and the secret | **Settings → Environments** | Repository admin |
| 4. Protect the branches | **Settings → Branches** | Repository admin |

- **Step 2 fails with "refusing to allow an OAuth App to create or update workflow"?** That is a
  token scope problem, not missing admin rights. Run `gh auth refresh -s workflow` (or add the
  `workflow` scope to your personal access token) and push again.
- **No Settings tab, or "You don't have access to repository options"?** You are not a repository
  admin. Push the workflows (steps 1–2), then send this file to a repository admin or your
  organization's GitHub administrators and ask them to do steps 3–4. Deploys fail until step 3 is
  done.

### Already set up?

If `.github/workflows/` already contains the `Toolkit Deploy …` and `Toolkit PR Validate` workflows
and the `*-toolkit-credentials` environments exist, CI/CD is already set up. You don't need to
repeat steps 1–4: open pull requests following the branching model below, and the workflows
validate and deploy them.

Re-run the generator only when you add or remove a `config.<env>.yaml`, or upgrade the Toolkit
version (see [Regenerate workflows](#regenerate-workflows)).

## Branching model

| Git branch / event | CDF project | Trigger |
|--------------------|-------------|---------|
{{BRANCHING_ROWS}}

If a pre-production environment is present, PRs to `main` must come from `dev` or `hotfix/*` only.

## Branch protection

Protect the branches used above under **Settings → Branches**. This is required, not optional:

| Branch | Required reviewers | Required status checks |
|--------|---------------------|--------------------------|
{{BRANCH_PROTECTION_ROWS}}

{{BRANCH_PROTECTION_NOTE}}

## GitHub Environments

Create the generated environments under **Settings → Environments**. Deploy workflows
use them; PR validation does not.

| Environment | Used by | `CDF_PROJECT` example |
|-------------|---------|-------------------------|
{{ENVIRONMENT_ROWS}}

Each environment needs these **variables**:

- `CDF_CLUSTER`
- `CDF_PROJECT` (must match `config.<env>.yaml`)
- `LOGIN_FLOW` (typically `client_credentials`)
- `PROVIDER` (only for a non-Entra identity provider — see below)
- `IDP_TENANT_ID`
- `IDP_CLIENT_ID`
- `ADMIN_SOURCE_ID`
- `CONSUMER_SOURCE_ID`
- `PRODUCER_SOURCE_ID`

And this **secret**:

- `IDP_CLIENT_SECRET`

{{TRUST_BOUNDARY_NOTE}}

### Identity provider

`PROVIDER` tells the Toolkit which identity provider to authenticate against. The generated
deploy workflows default it to `entra_id`, so Entra ID projects can leave the variable unset.

| Identity provider | `PROVIDER` | `IDP_TENANT_ID` | `IDP_TOKEN_URL` |
|-------------------|------------|-----------------|-----------------|
| Microsoft Entra ID | `entra_id` (or unset) | required | not used |
| Cognite IdP (CogIdP) | `cdf` | not used | not used — the Toolkit authenticates against `https://auth.cognite.com/oauth2/token` |
| Other OIDC provider | `other` | not used | required, together with `IDP_AUDIENCE` |

The generated workflows do not pass `IDP_TOKEN_URL` or `IDP_AUDIENCE`, so the third row
needs a change to the workflow templates.

## Toolkit configs

This generator only writes GitHub Actions workflows and this guide. It does not
create or refresh {{ENV_CONFIG_LIST}}.

Before opening a PR, run the project setup wizard and commit the resulting config
files together with the workflows:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py
cdf build {{EXAMPLE_BUILD_ARGS}}
```

CI validates the committed configs as-is; it does not regenerate them.
If the repository does not have a root `.pre-commit-config.yaml`, the generated
PR workflow skips the pre-commit config lint step.

If a team-authored module has Python source under a `functions/` folder, the PR workflow
also runs `ruff check` and `pyright` against it, installing that function's
`requirements.txt` first so imports resolve. Modules installed by a deployment pack are
excluded — their code is not the team's to fix — so a project whose only functions come
from packs skips this step.

## Regenerate workflows

Regenerate when you add or remove a `config.<env>.yaml` or upgrade the Toolkit version:

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

`--force` overwrites the generated workflows, including any edits you made to them. Without it,
the generator asks before overwriting each file. Review `git diff .github/workflows` before you
commit. Workflows you added yourself (other file names) are not touched.

Regenerating only changes files. It does not need admin, and existing environments and branch
protection keep working as long as the job names (`cdf build`, `Source branch guardrail`) stay the
same.

## Toolkit version

Workflows install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
