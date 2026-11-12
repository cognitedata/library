# CDF PI Module (Industry) — PI time series and alarm synthetic data

## Overview

This module provides synthetic PI historian data for the industry-neutral enterprise model
`Industry_DOM` (`datamodels/industry_domain_extension`). It is the Industry counterpart of
`cdf_pi_data_dump`: the sample data is copied from the CFIHOS PI sample so the two setups can be deployed side by
side, but everything is renamed (`raw_sample_all_pi`, `tr_pi_all_to_industry_*`) and written to the Industry model.

It covers two kinds of PI data:

- **Time series and operating limits** are loaded with Transformations from RAW into `TimeSeriesData` and
  `OperatingLimit`.
- **Alarm events** are Records in a stream. The Records service cannot be populated through RAW + Transformations, so a
  small CDF Function stands in for the extractor (a PI / OPC UA extractor or hosted extractor writes to the stream
  directly in production).

## Module Components

- `raw/` — database `raw_database_pi` with tables `timeseries` and `alarm_event`.
- `upload_data/` — manifests and CSV rows: 375 PI tags (copied from `cdf_pi_data_dump`) and 30 synthetic alarm
  events (Raised → Acknowledged → Cleared for two limit crossings on each of the five instruments that have set
  points in the SAP sample).
- `transformations/`
  - `tr_pi_all_to_industry_time_series` — `timeseries` → `TimeSeriesData`; links each time series to its `Tag` and,
    through a lookup on the SAP `equipment` table, to its `Equipment`.
  - `tr_pi_all_to_industry_operating_limit` — SAP `instrument_equipment` ⋈ `timeseries` → `OperatingLimit`
    (`<instrument key>_limit`), for instruments with `setPointHigh` / `setPointLow`.
- `data_modeling/function.Space.yaml` — the code space `function_space` for the Function (code only, never
  instances).
- `functions/` — `ingest_alarm_event.Function.yaml` and the `fn_pi_all_ingest_alarm_event` handler, which reads
  `alarm_event` and ingests the records that are not yet in the stream `alarm_stream_external_id`.

Transformations are full loads with `upsert`; this is a sample-data choice. For real sources use incremental loads
(see the `cdf-transformation` skill).

### Alarm Function contract

| `data` key | Required | Meaning |
|------------|----------|---------|
| `stream_external_id` | yes | Stream to ingest into (`alarm_stream_external_id`) |
| `instance_space` | yes | Space of the records and of the directly related nodes |
| `enterprise_space` | yes | Space of the `AlarmEvent` container |
| `raw_database` | yes | `raw_database_pi` |
| `raw_table` | yes | `alarm_event` |
| `last_updated_time_gt` | no | ISO-8601 lower bound for the duplicate check; defaults to the last 7 days |

The Function returns `read`, `invalid`, `skipped` (already in the stream) and `ingested`. It carries no secrets; the
injected client holds the credentials. Rows are mapped to the `AlarmEvent` record container; `asset`, `timeSeries`
and `operatingLimit` are direct relations to nodes in `instance_space`.

The deployed Function needs `cognite-sdk` 8.24 or later (Records API); `requirements.txt` pins it.

## Deployment

### Prerequisites

- Cognite Toolkit with Records / streams support.
- The `datamodels/industry_domain_extension` module deployed, so the model, the data set `dataset_external_id`
  and the alarm stream exist.
- `sourcesystem/cdf_sap_industry_data_dump` deployed first: the transformations read its `equipment` and
  `instrument_equipment` tables from `raw_database_sap`.
- Load order: `Tag` nodes (SAP module) before the alarm Function, since records do not validate their direct
  relations.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_pi_industry_data_dump` to your module list and set the variables under
`variables.modules.cdf_pi_industry_data_dump` in `config.<env>.yaml`. The variables are shared with the other
`*_industry_*` modules and must have the same values everywhere. `ingestion_client_id` and
`ingestion_client_secret` come from `${IDP_CLIENT_ID}` and `${IDP_CLIENT_SECRET}`.

### Starting from scratch

```bash
cdf build --env <your-env>
cdf deploy --env <your-env>
```

Run the transformations and the Function through the `common/cdf_industry_ingestion` workflow.

### Local tests

```bash
uv run pytest modules/sourcesystem/cdf_pi_industry_data_dump/functions/fn_pi_all_ingest_alarm_event -q
```

The unit tests cover the row → record mapping and the duplicate filter and need no network access.

## Module Structure

```
cdf_pi_industry_data_dump/
├── module.toml
├── default.config.yaml
├── README.md
├── raw/
│   ├── pi.Database.yaml
│   ├── timeseries.Table.yaml
│   └── alarm_event.Table.yaml
├── upload_data/
│   ├── timeseries.Manifest.yaml
│   ├── timeseries.RawRows.csv
│   ├── alarm_event.Manifest.yaml
│   └── alarm_event.RawRows.csv
├── transformations/
│   ├── tr_pi_all_to_industry_time_series.Transformation.{yaml,sql}
│   └── tr_pi_all_to_industry_operating_limit.Transformation.{yaml,sql}
├── data_modeling/
│   └── function.Space.yaml
└── functions/
    ├── ingest_alarm_event.Function.yaml
    └── fn_pi_all_ingest_alarm_event/
        ├── handler.py
        ├── test_handler.py
        ├── pyproject.toml
        └── requirements.txt
```

## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite Hub.
