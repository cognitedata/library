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

## Regenerate workflows

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

## Toolkit version

Workflows install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
