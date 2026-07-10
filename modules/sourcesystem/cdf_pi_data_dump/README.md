# CDF PI Module (CFIHOS) — PI time series synthetic data

## Overview

This module provides CFIHOS-shaped synthetic PI time series data and the transformations that
populate the `TimeSeriesData` view of the CFIHOS Oil & Gas domain model
(`cfihos_oil_and_gas_extension`). It is the CFIHOS counterpart of `cdf_pi`: rather than the
Springfield/AVEVA-style sample data, it ships time series records shaped for the CFIHOS 2.0
specification and links them to CFIHOS `Equipment` nodes.

Use this module when deploying `dp:quickstart` against the `cfihos_oil_and_gas_extension` data
model. It is not a replacement for `cdf_pi` in other deployment packs — `cdf_pi` remains the
correct choice for `dp:foundation`.

## Module Components

- `raw/timeseries.Table.yaml` — RAW table definition for `cfihos_oil_and_gas.timeseries`.
- `upload_data/RAW/timeseries.Manifest.yaml` + `timeseries.RawRows.csv` — synthetic PI tag rows.
- `transformations/tr_timeseries_all_to_timeseries_data.Transformation.{yaml,sql}` — populates
  `TimeSeriesData` nodes.
- `transformations/tr_timeseries_all_to_timeseries_equipment_connection.Transformation.{yaml,sql}`
  — links each time series to its owning `Equipment` node.

## Deployment

### Prerequisites

- Cognite Toolkit (minimum version as required by `dp:quickstart`).
- The `cfihos_oil_and_gas_extension` data model module deployed to the same project/space.
- `cdf_sap_assets_new` deployed first (or in the same run) so the `Equipment` nodes referenced by
  the equipment-connection transformation exist.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_pi_new` to your package's module list and configure the variables under
`variables.modules.cdf_pi_new` in your `config.<env>.yaml`.

### Starting from scratch

```bash
cdf-tk build --env <your-env>
cdf-tk deploy --env <your-env>
```

## Module Structure

```
cdf_pi_new/
├── module.toml
├── default.config.yaml
├── raw/
│   └── timeseries.Table.yaml
├── upload_data/
│   └── RAW/
│       ├── timeseries.Manifest.yaml
│       └── timeseries.RawRows.csv
└── transformations/
    ├── tr_timeseries_all_to_timeseries_data.Transformation.yaml
    ├── tr_timeseries_all_to_timeseries_data.Transformation.sql
    ├── tr_timeseries_all_to_timeseries_equipment_connection.Transformation.yaml
    └── tr_timeseries_all_to_timeseries_equipment_connection.Transformation.sql
```

## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite
Hub.
