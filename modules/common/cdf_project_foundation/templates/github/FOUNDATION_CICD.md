# Foundation Deployment Pack — CI/CD setup

Generated for enterprise **`{{ENTERPRISE}}`** (organization directory: **`{{ORG_DIR}}`**).

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

`generate_actions.py` writes GitHub Actions workflows and, by default, regenerates
`config.dev.yaml`, `config.test.yaml`, and `config.prod.yaml` under `{{ORG_DIR}}/`.
Pass `--skip-configs` to refresh workflows only.

CI runs `prepare-toolkit-project.sh` before build/deploy to refresh configs from
the committed modules. Commit the generated configs together with the workflows.

## Regenerate workflows or configs

```bash
python modules/common/cdf_project_foundation/scripts/generate_actions.py --force
```

Refresh only environment YAML:

```bash
python {{ORG_DIR}}/scripts/generate_env_configs.py \
  --enterprise {{ENTERPRISE}} \
  --org-dir {{ORG_DIR}} \
  --repo-root .
```

## Toolkit version

Workflows install `cognite-toolkit=={{TOOLKIT_VERSION}}`. Keep in sync with `[modules].version` in `cdf.toml`.
