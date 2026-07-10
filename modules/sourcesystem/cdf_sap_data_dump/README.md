# CDF SAP Module (CFIHOS) — Asset, equipment, and work order synthetic data

## Overview

This module provides CFIHOS-shaped synthetic SAP data — the tag/equipment hierarchy and the
maintenance work order pipeline — and the transformations that populate the corresponding views
of the CFIHOS Oil & Gas domain model (`cfihos_oil_and_gas_extension`). It combines what were
previously two separate modules (`cdf_sap_assets_data_dump` and `cdf_sap_events_new`) into a
single SAP source-system module, since both ultimately model the same source system (SAP) and
share the same RAW database and Toolkit variables.

It is the CFIHOS counterpart of `cdf_sap_assets` and `cdf_sap_events`: it ships the full CFIHOS
tag hierarchy, all 19 CFIHOS equipment-class overlays, and the work order / operation /
notification / failure-mode pipeline, instead of the Workmate/SAP-style sample data those modules
ship.

This module owns the `cfihos_oil_and_gas` RAW database definition
(`raw/cfihos_oil_and_gas.Database.yaml`) — the single physical database shared with its
`cdf_pi_data_dump` and `cdf_sharepoint_data_dump` siblings.

Use this module when deploying `dp:quickstart` against the `cfihos_oil_and_gas_extension` data
model. It is not a replacement for `cdf_sap_assets`/`cdf_sap_events` in other deployment packs —
those remain the correct choice for `dp:foundation`.

## Module Components

- `raw/cfihos_oil_and_gas.Database.yaml` — the shared CFIHOS RAW database definition.
- `raw/*.Table.yaml` — RAW table definitions for `tag`, `functional_location`, `equipment`, the 19
  equipment-class tables, and `work_order`, `work_order_operation`, `notification`,
  `failure_mode`.
- `upload_data/` — matching manifests and synthetic CSV rows for each table above.
- `transformations/` — 37 transformations:
  - Asset/tag domain: population of `Tag`, `FunctionalLocation`, `Equipment`, and each equipment
    class, plus the `Tag`-to-equipment/functional-location connection transformations.
  - Work order domain: population of `WorkOrder`, `WorkOrderOperation`, `Notification`,
    `FailureMode`, and their connections, including the assets-backfill and time-series
    connection transformations that used to live in `cdf_connection_sql`.

## Deployment

### Prerequisites

- Cognite Toolkit (minimum version as required by `dp:quickstart`).
- The `cfihos_oil_and_gas_extension` data model module deployed to the same project/space.

### Adding to an existing Toolkit project

Add `sourcesystem/cdf_sap_data_dump` to your package's module list and configure the variables
under `variables.modules.cdf_sap_data_dump` in your `config.<env>.yaml`.

### Starting from scratch

```bash
cdf-tk build --env <your-env>
cdf-tk deploy --env <your-env>
```

## Module Structure

```
cdf_sap_data_dump/
├── module.toml
├── default.config.yaml
├── raw/
│   ├── cfihos_oil_and_gas.Database.yaml
│   ├── tag.Table.yaml
│   ├── functional_location.Table.yaml
│   ├── equipment.Table.yaml
│   ├── <19 equipment-class Table.yaml files>
│   ├── work_order.Table.yaml
│   ├── work_order_operation.Table.yaml
│   ├── notification.Table.yaml
│   └── failure_mode.Table.yaml
├── upload_data/
│   └── <Manifest.yaml + RawRows.csv pairs for each table above>
└── transformations/
    └── <37 tr_*.Transformation.{yaml,sql} pairs>
```

## Support

For questions or issues, open an issue in the
[Cognite library repository](https://github.com/cognitedata/library) or reach out via Cognite
Hub.
