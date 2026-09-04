# Data Quality Validation

## Overview

Publishes SHACL **RuleSets** and **DataProducts** with Cognite Toolkit (`cdf deploy`), then runs the same Python deploy path as [data-quality-validation-deploy](https://github.com/cognitedata/data-quality-validation-deploy) (`scripts/deploy_infrastructure.py`).

Bundled sample: cog-ai **YourOrg** (`config/environments/cog-ai`).

| Layer | Toolkit (`cdf deploy`) | Module script (`deploy_infrastructure.py`) |
| --- | --- | --- |
| RuleSets / DataProducts | Yes | Consumes via `external_dataproducts` |
| Function + containers | — | Yes |
| Instance workflows | — | Yes (external DataProduct + `views/` overrides) |
| Time-series workflows | — | Yes (`timeseries/` configs) |
| `data_product_sync` + historic queue | — | Yes (`data_product_sync_cron`) |

Only `dq_pypi_version` is in `default.config.yaml`.

## Operator flow

### Local

```bash
cdf modules add dp:data_quality
cdf build && cdf deploy                    # RuleSets + DataProducts
pip install cognite-data-quality==0.4.9
python modules/common/cdf_dq_runtime/scripts/deploy_infrastructure.py \
  --toolkit-config config.dev.yaml
```

Credentials come from environment variables (same as [data-quality-validation-deploy](https://github.com/cognitedata/data-quality-validation-deploy)): `COGNITE_PROJECT`, `COGNITE_CLIENT_ID`, `COGNITE_CLIENT_SECRET`, and `AZURE_TENANT_ID` or `COGNITE_TOKEN_URL`. In Toolkit projects, `CDF_*` / `IDP_*` work too. Optionally pass `--config-toml` for a local gitignored TOML file (notebooks).

Optional historic enqueue (same as deploy repo `--enqueue-historic`):

```bash
python modules/common/cdf_dq_runtime/scripts/deploy_infrastructure.py \
  --toolkit-config config.dev.yaml --enqueue-historic
```

Per-view historic pipeline:

```bash
python modules/common/cdf_dq_runtime/scripts/deploy_pipeline.py \
  --view-external-id YourOrgAsset --historic-mode enqueue --toolkit-config config.dev.yaml
```

### CI/CD

Run `deploy_infrastructure.py` in the same GitHub Actions job as `cdf deploy`, reusing the Toolkit environment block (`IDP_*`, `CDF_*`). Do not generate a credentials file. See [Toolkit pack CI/CD](https://github.com/cognitedata/data-quality-validation/blob/main/docs/usage/toolkit_pack.md#cicd).

## Contents (cog-ai sample)

**Views:** `YourOrgAsset`, `YourOrgEquipment`, `YourOrgMaintenanceOrder`, `YourOrgNotification`, `YourOrgOperation`, `YourOrgTimeSeries`

**Time-series config:** `yourorg_timeseries_quality` only (no demo data generator / `demo_timeseries_quality`).

**RuleSets:** matching `*_shacl_rules` / `timeseries_quality_rules` from deploy repo (inline in `rulesets/`)

**Settings:** `settings.yaml` mirrors cog-ai workflow prefixes, records stream, `data_product_sync_cron`, and `timeseries.config_dir`.

## Support

- [data-quality-validation](https://github.com/cognitedata/data-quality-validation)
- [data-quality-validation-deploy (cog-ai)](https://github.com/cognitedata/data-quality-validation-deploy/tree/main/config/environments/cog-ai)
