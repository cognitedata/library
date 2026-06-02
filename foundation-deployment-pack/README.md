# Foundation Deployment Pack — CI toolkit project

This directory is the **Cognite Toolkit organization folder** used by GitHub Actions to validate and deploy the Foundation Deployment Pack (`dp:foundation`) modules that live in the repository root [`modules/`](../modules/).

## Layout

| Path | Purpose |
|------|---------|
| `config.dev.yaml` / `config.test.yaml` / `config.prod.yaml` | Environment-specific module selection and variables (generated; see below) |
| `modules/` | Symlink to `../modules/` (created locally and in CI by `.github/scripts/prepare-fdp-project.sh`) |
| `scripts/generate_env_configs.py` | Regenerates the three config files from module `default.config.yaml` files |

The Toolkit project root is the **repository root**, where [`cdf.toml`](../cdf.toml) sets `default_organization_dir = "foundation-deployment-pack"`.

## Regenerate configs

After changing FDP module lists or defaults:

```bash
bash .github/scripts/prepare-fdp-project.sh
```

## Local dry-run

```bash
export CDF_CLUSTER=your-cluster
export CDF_PROJECT=your-dev-project
export LOGIN_FLOW=client_credentials
export IDP_CLIENT_ID=...
export IDP_CLIENT_SECRET=...

bash .github/scripts/prepare-fdp-project.sh
pip install "cognite-toolkit==0.7.220"
cdf build --env dev
cdf deploy --dry-run --env dev
```

## CI/CD

See the [CI/CD section in the repository README](../README.md#cicd-foundation-deployment-pack).
