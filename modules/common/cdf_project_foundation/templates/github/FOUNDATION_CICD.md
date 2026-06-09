# Foundation Deployment Pack — CI/CD setup

Generated for enterprise **`{{ENTERPRISE}}`**.

This follows [sop-cdf-project-setup.md](https://github.com/cognitedata/library/blob/main/sop-cdf-project-setup.md) Step 5.

## Branching model

| Git branch / event | CDF project | Trigger |
|--------------------|-------------|---------|
| PR → `dev` | `{{ENTERPRISE}}-dev` | Dry-run (`cdf build`, `cdf deploy --dry-run`) |
| PR → `main` | `{{ENTERPRISE}}-test` | Dry-run |
| Push to `dev` | `{{ENTERPRISE}}-dev` | Deploy |
| Push to `main` | `{{ENTERPRISE}}-test` | Deploy |
| GitHub Release (tag `vX.Y.Z` from `main`) | `{{ENTERPRISE}}-prod` | Deploy |

PRs to `main` must come from `dev` or `hotfix/*` only.

## GitHub Environments

Create three environments under **Settings → Environments**:

| Environment | Used by | `CDF_PROJECT` example |
|-------------|---------|-------------------------|
| `dev-toolkit-credentials` | PR → dev, push `dev` | `{{ENTERPRISE}}-dev` |
| `test-toolkit-credentials` | PR → main, push `main` | `{{ENTERPRISE}}-test` |
| `prod-toolkit-credentials` | Release published | `{{ENTERPRISE}}-prod` |

Each environment needs these **variables**:

- `CDF_CLUSTER`
- `CDF_PROJECT` (must match `config.<env>.yaml`)
- `LOGIN_FLOW` (typically `client_credentials`)
- `IDP_TENANT_ID`
- `IDP_CLIENT_ID`

And this **secret**:

- `IDP_CLIENT_SECRET`

## Toolkit configs

This generator only writes GitHub Actions workflows and this guide. It does not
create or refresh `config.dev.yaml`, `config.test.yaml`, or `config.prod.yaml`.

Before opening a PR, run the project setup wizard and commit the resulting config
files together with the workflows:

```bash
python modules/common/cdf_project_foundation/scripts/setup_project.py
cdf build --env dev
```

CI validates the committed configs as-is; it does not regenerate them.

## Regenerate workflows

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

## Toolkit version

Workflows install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
