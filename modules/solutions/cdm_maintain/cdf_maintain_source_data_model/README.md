# Maintain Source Data Model (source system data for Maintain)

Library path: `modules/solutions/cdm_maintain/cdf_maintain_source_data_model/` · Module ID: `dp:cdm_maintain:cdf_maintain_source_data_model` · Deployment pack: `dp:cdm_maintain`

## Overview

This module deploys the `MaintainSource` data model: the source system data that Maintain reads, such as maintenance
orders, operations, assets, files, and time series. Your ingestion writes to these views. It extends the CDM/IDM
maintenance order with the Maintain-specific properties in `SourceMaintenanceOrder`.

## Module Components

`MaintainSource` data model (`v1`) in `{{schemaSpace}}`, with these views:

| View | Space | Purpose |
|---|---|---|
| `SourceMaintenanceOrder` | `{{schemaSpace}}` | Maintain properties on top of `CogniteMaintenanceOrder` (status flags, dates, hours, grid reference, files) |
| `CogniteMaintenanceOrder` | `cdf_idm` | Maintenance orders (activities in Maintain) |
| `CogniteOperation` | `cdf_idm` | Operations |
| `CogniteAsset` | `cdf_cdm` | Assets, including the location's root asset |
| `CogniteFile` | `cdf_cdm` | Files |
| `CogniteTimeSeries` | `cdf_cdm` | Time series |

The `mainAsset` of each maintenance order must point to a `CogniteAsset` in the source data space. Otherwise the Asset
tab in Maintain stays empty. See [Troubleshooting](../README.md#troubleshooting).

## Variables

| Variable | Default | Description |
|---|---|---|
| `schemaSpace` | `maintain_source_data` | Space for the container, view, and data model |

## Deployment

Deployed as part of the pack. See the [pack README](../README.md#deployment) for prerequisites and steps.

## Module Structure

```
cdf_maintain_source_data_model/
├── module.toml
├── default.config.yaml
└── data_modeling/
    ├── maintain_source_data.Space.yaml
    ├── MaintainSource.DataModel.yaml
    ├── containers/SourceMaintenanceOrder.Container.yaml
    └── views/SourceMaintenanceOrder.View.yaml
```

## Support

See [Support](../README.md#support) in the pack README.
