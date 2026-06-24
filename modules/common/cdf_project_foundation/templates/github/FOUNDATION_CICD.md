# Foundation Deployment Pack — CI/CD setup

Generated from committed Toolkit environment configs.

This follows the [CDF Foundation Setup guide](https://cogdocs-feat-cdf-project-setup-docs.mintlify.app/gvd/cdf-project-setup/cdf-foundation-setup) Step 5.

## Branching model

| Git branch / event | CDF project | Trigger |
|--------------------|-------------|---------|
| PR → `dev` | `{{DEV_PROJECT}}` | Dry-run (`cdf build`, `cdf deploy --dry-run`) |
| Push to `dev` | `{{DEV_PROJECT}}` | Deploy |
{{TEST_BRANCHING_ROWS}}
| GitHub Release (tag `vX.Y.Z` from `main`) | `{{PROD_PROJECT}}` | Deploy |

If the test environment is present, PRs to `main` must come from `dev` or `hotfix/*` only.

## GitHub Environments

Create three environments under **Settings → Environments**:

| Environment | Used by | `CDF_PROJECT` example |
|-------------|---------|-------------------------|
| `dev-toolkit-credentials` | PR → dev, push `dev` | `{{DEV_PROJECT}}` |
{{TEST_ENVIRONMENT_ROW}}
| `prod-toolkit-credentials` | Release published | `{{PROD_PROJECT}}` |

Each environment needs these **variables**:

- `CDF_CLUSTER`
- `CDF_PROJECT` (must match `config.<env>.yaml`)
- `LOGIN_FLOW` (typically `client_credentials`)
- `IDP_TENANT_ID`
- `IDP_CLIENT_ID`
- `ADMIN_SOURCE_ID`
- `CONSUMER_SOURCE_ID`
- `PRODUCER_SOURCE_ID`

And this **secret**:

- `IDP_CLIENT_SECRET`

## Toolkit configs

This generator only writes GitHub Actions workflows and this guide. It does not
create or refresh {{ENV_CONFIG_LIST}}.

Before opening a PR, run the project setup wizard and commit the resulting config
files together with the workflows:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py
cdf build {{DEV_BUILD_ARGS}}
```

CI validates the committed configs as-is; it does not regenerate them.
If the repository does not have a root `.pre-commit-config.yaml`, the generated
PR workflow skips the pre-commit config lint step.

## Regenerate workflows

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

## Toolkit version

Workflows install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
