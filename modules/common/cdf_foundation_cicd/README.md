# Foundation Deployment Pack — CI/CD generator

Toolkit module shipped with **`dp:foundation`**. Generates GitHub Actions workflows and environment configs for **customer Toolkit projects**, per `sop-cdf-project-setup.md` Step 5.

This module is **not** deployed to CDF (`cdf build` excludes it from `config.*.yaml` selected modules).

## Prerequisites

- Cognite Toolkit project with `cdf.toml` and `[alpha_flags] deployment-pack = true`
- Foundation pack modules installed (`cdf modules add -d dp:foundation`)
- `default_organization_dir` set in `cdf.toml`

## Generate CI/CD

From the **project repository root**:

```bash
python modules/common/cdf_foundation_cicd/scripts/generate_actions.py \
  --enterprise acme \
  --force
```

Replace `acme` with your enterprise slug (`acme-dev`, `acme-test`, `acme-prod` CDF projects).

### Options

| Flag | Description |
|------|-------------|
| `--enterprise` | Required. Enterprise slug for `{enterprise}-{env}` project names |
| `--org-dir` | Organization directory (default: `cdf.toml` → `default_organization_dir`) |
| `--toolkit-version` | Override Toolkit pip version (default: `cdf.toml` `[modules].version`) |
| `--force` | Overwrite existing generated files |
| `--skip-configs` | Only generate workflows, not `config.{dev,test,prod}.yaml` |

## Generated artifacts

| Path | Purpose |
|------|---------|
| `.github/workflows/dry-run.yml` | PR validation to `dev` or `main` |
| `.github/workflows/deploy-dev.yml` | Deploy on push to `dev` |
| `.github/workflows/deploy-test.yml` | Deploy on push to `main` |
| `.github/workflows/deploy-prod.yml` | Deploy on GitHub Release from `main` |
| `.github/scripts/prepare-toolkit-project.sh` | Refresh configs before `cdf build` |
| `{org_dir}/scripts/generate_env_configs.py` | Regenerate `config.*.yaml` |
| `{org_dir}/config.{dev,test,prod}.yaml` | Toolkit environment configs |
| `docs/FOUNDATION_CICD.md` | Setup checklist (GitHub Environments, secrets) |

## GitHub Environments

Configure **`dev-toolkit-credentials`**, **`test-toolkit-credentials`**, and **`prod-toolkit-credentials`** with `CDF_*` and `IDP_*` variables/secrets. See generated `docs/FOUNDATION_CICD.md`.
