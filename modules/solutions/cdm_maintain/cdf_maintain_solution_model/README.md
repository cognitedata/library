# Maintain Solution Model (Maintain app data model)

Library path: `modules/solutions/cdm_maintain/cdf_maintain_solution_model/` · Module ID: `dp:cdm_maintain:cdf_maintain_solution_model` · Deployment pack: `dp:cdm_maintain`

## Overview

This module deploys the `Maintain` data model: the data the Maintain application creates and stores itself, such as
plans, layouts, canvases, comments, and 3D mappings. Source system data lives in the separate
[`cdf_maintain_source_data_model`](../cdf_maintain_source_data_model/README.md).

## Module Components

`Maintain` data model (`v1`) in `{{schemaSpace}}`, with these views and their containers:

| View | Purpose |
|---|---|
| `Plan` | Maintenance plans and the work orders linked to them |
| `MaintainLayout` | Saved layouts: filters, grouping, and sorting of activities |
| `MaintainCanvas` | Canvas state |
| `MaintainWorkOrder` | Maintain-specific work order properties |
| `ManualMappings3D` | Manual mappings between assets and 3D objects |
| `FileGridZoneAnnotation` | Grid zones on files, used to place activities on drawings |
| `APM_Comment` | Comments |
| `APM_Optimisation` | Optimisation data |

The global node `plans.MaintenanceOrdersLink` is the edge type that links plans to maintenance orders.

## Variables

| Variable | Default | Description |
|---|---|---|
| `schemaSpace` | `maintain_solution_model` | Space for the containers, views, data model, and global nodes |
| `sourceDataSpace` | `maintain_source_data` | Must match `schemaSpace` in `cdf_maintain_source_data_model` |

## Deployment

Deployed as part of the pack. See the [pack README](../README.md#deployment) for prerequisites and steps.

## Module Structure

```
cdf_maintain_solution_model/
├── module.toml
├── default.config.yaml
└── data_modeling/
    ├── maintain_solution_model.Space.yaml
    ├── Maintain.DataModel.yaml
    ├── containers/                    # One container per view, see above
    ├── views/                         # Plan, MaintainLayout, MaintainCanvas, MaintainWorkOrder, ...
    └── nodes/globalNodes.Node.yaml
```

## Support

See [Support](../README.md#support) in the pack README.
