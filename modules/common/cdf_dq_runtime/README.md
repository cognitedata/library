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

## Prerequisites

RuleSets and DataProducts require the Toolkit [`data-products` alpha flag](https://docs.cognite.com/cdf/deploy/cdf_toolkit/api/alpha_flags#alpha-flag-data-products) (Toolkit `0.7.207+`). Enable it in `cdf.toml` before `cdf build`:

```toml
[alpha_flags]
data-products = true
```

The `[alpha_flags]` header must be uncommented. If only `data-products = true` is set (for example under `[plugins]`), Toolkit ignores it: `cdf build` emits only the space under `data_modeling/` and skips `rulesets/` / `data_products/`. Then `deploy_infrastructure.py` fails with `Data product version 'enterprise-process-industry:1.1.10' not found` (404).

After enabling the flag, run `cdf build && cdf deploy` so version `1.1.10` exists in CDF before the Python script.

### Access control

The identity used by `cdf deploy` and `deploy_infrastructure.py` (Toolkit `.env` / `IDP_*` service principal or user) needs these capabilities on the CDF project. Actions are `CREATE`, `READ`, `UPDATE`, and `DELETE` (there is no `WRITE`):

| Capability | Needed for |
| --- | --- |
| `dataProductsAcl:CREATE` / `UPDATE` / `DELETE` | Create/update/delete DataProducts via `cdf deploy` |
| `dataProductsAcl:READ` | `deploy_infrastructure.py` loading external DataProducts (for example `enterprise-process-industry`) when deploying workflows |
| `ruleSetsAcl:CREATE` / `READ` / `UPDATE` / `DELETE` | Create/update/delete RuleSets and RuleSetVersions via `cdf deploy` |

Grant these (typically all actions, scope `all`) in Fusion → Access management on the group linked to that identity — for Toolkit this is often `cognite_toolkit_service_principal`.

Common errors:

```text
CogniteAPIError: Missing capability DataProducts:READ | code: 403
CogniteAPIError: Missing capability RuleSets:CREATE | code: 403
Data product version 'enterprise-process-industry:1.1.10' not found | code: 404
```

The 404 means Toolkit has not deployed that DataProduct version yet (alpha flag off, or `cdf deploy` not run / failed).

Spaces used by the sample are defined under `data_modeling/` and created by `cdf deploy`: `sp_enterprise_process_industry` (schema), `springfield_instances` and `test_data_pumps` (instances), and `dataQuality` (runtime / `settings.yaml` `config_space`). Change the DataProductVersion YAML if your project uses different instance spaces.

## Operator flow

### Local

```bash
cdf modules add dp:data_quality
# Ensure data-products = true under [alpha_flags] in cdf.toml (see Prerequisites)
cdf build && cdf deploy                    # RuleSets + DataProducts
pip install cognite-data-quality==0.4.9
python modules/common/cdf_dq_runtime/scripts/deploy_infrastructure.py \
  --toolkit-config config.dev.yaml
```

### Build notes

**`IgnoredFileWarning` for `timeseries/yourorg_timeseries_quality.yaml`**

Toolkit treats `timeseries/` as a CDF TimeSeries resource directory, so it warns that this file has no kind suffix (for example `.TimeSeries.yaml`) and ignores it. That is expected: the YAML is config for `deploy_infrastructure.py`, not a Toolkit TimeSeries resource. The warning is harmless; build and deploy can continue.

Credentials come from the Toolkit project ``.env`` (next to ``cdf.toml``) when present,
then environment variables (same as [data-quality-validation-deploy](https://github.com/cognitedata/data-quality-validation-deploy)):
``COGNITE_PROJECT``, ``COGNITE_CLIENT_ID``, ``COGNITE_CLIENT_SECRET``, and ``AZURE_TENANT_ID`` or ``COGNITE_TOKEN_URL``.
In Toolkit projects, ``CDF_*`` / ``IDP_*`` work too. Optionally pass ``--config-toml`` for a local gitignored TOML file (notebooks).

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
