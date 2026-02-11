# Quick Start Enterprise Data Model

This module provides a comprehensive enterprise data model for Cognite Data Fusion (CDF), extending the Cognite Data Model (CDM) with enterprise-specific views and containers for process industry use cases. It serves as the foundational data layer for the Quick Start deployment pack.

## Overview

The Quick Start Enterprise Data Model (`qs_enterprise_dm`) delivers a production-ready data modeling layer that:

- Extends standard **CDM** (`cdf_cdm`) types with enterprise-scoped views
- Defines **39 enterprise views** backed by **46 containers** covering assets, equipment, maintenance, 3D, and more
- Ships two data models: a **full enterprise model** and a **search-optimized model**
- Organizes data across three CDF spaces: schema, enterprise instance, and site instance
- Supports templatized configuration via CDF Toolkit variables for reuse across environments

## Module Identity

| Field | Value |
|-------|-------|
| **Title** | Quick Start Enterprise Data Model |
| **Module ID** | `dp:models:qs_enterprise_dm` |
| **Package** | `dp:quickstart` |
| **Selected by default** | No |

## Module Architecture

```
qs_enterprise_dm/
├── data_modeling/
│   ├── qs-dm-spaces/
│   │   ├── schema.space.yaml                          # Schema space (enterpriseSchemaSpace)
│   │   ├── enterprise.instance.space.yaml             # Enterprise instance space
│   │   └── site.instance.space.yaml                   # Site instance space
│   ├── containers/                                    # 46 container definitions
│   │   ├── Asset.Container.yaml
│   │   ├── Equipment.Container.yaml
│   │   ├── WorkOrder.Container.yaml
│   │   ├── WorkOrderOperation.Container.yaml
│   │   ├── WorkOrderOperationConfirmation.Container.yaml
│   │   ├── MaintenanceOrder.Container.yaml
│   │   ├── Notification.Container.yaml
│   │   ├── Operation.Container.yaml
│   │   ├── FileRevision.Container.yaml
│   │   ├── Reportable.Container.yaml
│   │   ├── ... (36 more)
│   │   └── FunctionalLocation.Container.yaml
│   ├── views/                                         # 39 view definitions
│   │   ├── Asset.view.yaml
│   │   ├── Equipment.view.yaml
│   │   ├── WorkOrder.view.yaml
│   │   ├── WorkOrderOperation.view.yaml
│   │   ├── WorkOrderOperationConfirmation.view.yaml
│   │   ├── MaintenanceOrder.view.yaml
│   │   ├── Notification.view.yaml
│   │   ├── Operation.view.yaml
│   │   ├── FileRevision.view.yaml
│   │   ├── Reportable.view.yaml
│   │   ├── ... (29 more)
│   │   └── FunctionalLocation.view.yaml
│   ├── qs-enterprise.datamodel.yaml                   # Full enterprise data model
│   └── qs-enterprise-search.datamodel.yaml            # Search-optimized data model
├── default.config.yaml                                # Default configuration variables
└── module.toml                                        # Module metadata
```

## CDF Spaces

The module provisions three spaces to separate schema definitions from instance data:

| Space Variable | Default Value | Purpose |
|----------------|---------------|---------|
| `enterpriseSchemaSpace` | `sp_enterprise_process_industry` | Holds all view and container definitions (the data model schema) |
| `enterpriseInstanceSpace` | `sp_enterprise_instance` | Stores enterprise-wide data instances shared across sites |
| `siteInstanceSpace` | `sp_site_instance` | Stores site-specific data instances |

## Data Models

### 1. Enterprise Data Model (`DataModel`)

The primary data model that combines **all 39 enterprise views** together with **31 CDM base views** into a single queryable model. It includes:

- All CDM interface and base types from `cdf_cdm` (CogniteAsset, CogniteEquipment, CogniteTimeSeries, etc.)
- All custom enterprise views from the schema space (Asset, Equipment, WorkOrder, etc.)

### 2. Enterprise Search Data Model (`DataModelSearch`)

A lightweight subset of the enterprise model designed for search use cases. It includes the most commonly queried CDM types and their corresponding enterprise views:

- CDM types: CogniteAsset, CogniteEquipment, CogniteTimeSeries, CogniteFile, CogniteAssetClass, CogniteAssetType, CogniteEquipmentType, CogniteFileCategory
- Enterprise views: Asset, Equipment, TimeSeries, FileRevision, Notification, WorkOrder, AssetClass, AssetType, EquipmentType, FileCategory

## Enterprise Views

### Core Asset & Equipment Views

| View | Implements | Description |
|------|-----------|-------------|
| **Asset** | `CogniteAsset` | Physical assets with hierarchical parent/root/path structure, linked to equipment, files, activities, and time series |
| **FunctionalLocation** | `CogniteAsset` | Hierarchical structures representing specific positions where assets are installed or functions are performed |
| **Equipment** | `CogniteEquipment` | Physical devices or supplies linked to an asset, with equipment type, files, activities, and time series |
| **AssetClass** | `CogniteAssetClass` | Classification categories for assets |
| **AssetType** | `CogniteAssetType` | Type definitions for assets |
| **EquipmentType** | `CogniteEquipmentType` | Type definitions for equipment |

### Maintenance & Work Management Views

| View | Implements | Description |
|------|-----------|-------------|
| **MaintenanceOrder** | `CogniteMaintenanceOrder` | Formal requests to perform maintenance tasks such as repair, inspection, or servicing. Links to operations, assets, equipment, and time series |
| **Operation** | `CogniteOperation` | A specific part of the work included in a maintenance order (a work order item). Linked to a maintenance order and assets |
| **Notification** | `CogniteNotification` | Formal records to report maintenance issues, defects, or requests. Links to a maintenance order and asset |
| **WorkOrder** | `CogniteMaintenanceOrder`, `CogniteActivity`, `CogniteSchedulable`, `CogniteSourceable`, `CogniteDescribable` | Work orders with operations, linked to assets and equipment. Includes `objectNumber` and `mainEquipment` custom properties |
| **WorkOrderOperation** | `CogniteOperation`, `CogniteActivity`, `CogniteSchedulable`, `CogniteSourceable`, `CogniteDescribable` | Individual operations on a work order with confirmations. Includes `objectNumber` custom property |
| **WorkOrderOperationConfirmation** | `CogniteDescribable`, `CogniteSchedulable` | Tracks actual vs. planned work on operations with fields for actual work, forecast work, costs, and timing |
| **Activity** | `CogniteActivity` | Activities happening over a time period, linked to assets, equipment, and time series |

### Data & File Views

| View | Implements | Description |
|------|-----------|-------------|
| **TimeSeries** | `CogniteTimeSeries` | Series of data points in time order, linked to assets, equipment, and a unit |
| **FileRevision** | `CogniteFile` | Documents and files with custom properties for facility, unit, line, and file revision |
| **FileCategory** | `CogniteFileCategory` | Category classification for files |
| **Unit** | `CogniteUnit` | Units of measurement for time series |

### 3D & Visualization Views

| View | Implements | Description |
|------|-----------|-------------|
| **3DModel** | `Cognite3DModel` | Top-level 3D model resources |
| **3DObject** | `Cognite3DObject` | Individual 3D objects |
| **3DRevision** | `Cognite3DRevision` | Revisions of 3D models |
| **3DTransformation** | `Cognite3DTransformation` | 3D transformation matrices |
| **CADModel** | `CogniteCADModel` | CAD-specific 3D models |
| **CADRevision** | `CogniteCADRevision` | CAD model revisions |
| **CADNode** | `CogniteCADNode` | Individual nodes in a CAD model |
| **PointCloudModel** | `CognitePointCloudModel` | Point cloud 3D models |
| **PointCloudRevision** | `CognitePointCloudRevision` | Point cloud model revisions |
| **PointCloudVolume** | `CognitePointCloudVolume` | Volumes within point clouds |
| **CubeMap** | `CogniteCubeMap` | Cube map textures for 3D environments |

### 360-Degree Image Views

| View | Implements | Description |
|------|-----------|-------------|
| **360Image** | `Cognite360Image` | Individual 360-degree images |
| **360ImageAnnotation** | `Cognite360ImageAnnotation` | Annotations on 360 images |
| **360ImageCollection** | `Cognite360ImageCollection` | Collections of 360 images |
| **360ImageModel** | `Cognite360ImageModel` | 360 image model resources |
| **360ImageStation** | `Cognite360ImageStation` | Stations from which 360 images are captured |

### Cross-Cutting Views

| View | Implements | Description |
|------|-----------|-------------|
| **SourceSystem** | `CogniteSourceSystem` | Standardized representation of source systems (e.g., SAP, PI) |
| **Annotation** | `CogniteAnnotation` | General annotations on resources |
| **DiagramAnnotation** | `CogniteDiagramAnnotation` | Annotations specific to diagrams |
| **Reportable** | _(standalone)_ | Cross-cutting view providing `sysSite`, `sysUnit`, `sysTagsFound`, and `sysTagsLinked` properties used for data quality reporting |

## Key Relationships

```
                                +-----------+
                                |   Asset   |
                                +-----+-----+
                               /  |   |   \   \
                    parent/   files| equipment| activities\  timeSeries
                    children      |     |     |            \
                              +---+  +--+--+ +--+------+ +--------+
                              |File| |Equip| |Activity | |TimeSeries|
                              |Rev.| |ment | |         | |          |
                              +----+ +-----+ +---------+ +----------+

  +---------------+       +-------------+       +-------------------+
  | Notification  |------>| Maintenance |<------| Operation         |
  |               |       | Order       |       |                   |
  +---------------+       +------+------+       +-------------------+
                                 |
                          assets, equipment,
                          timeSeries

  +-------------+       +-------------------+       +-----------------------------+
  | WorkOrder   |<------| WorkOrderOperation|<------| WorkOrderOperationConfirmation|
  |             |       |                   |       | (actual/forecast work, costs) |
  +-------------+       +-------------------+       +-----------------------------+
```

- **Asset** is the central entity, connected to children (hierarchy), equipment, files, activities, time series, and 3D objects
- **FunctionalLocation** mirrors Asset hierarchy but represents functional positions in a facility
- **Notification** triggers **MaintenanceOrder**, which contains **Operations**
- **WorkOrder** contains **WorkOrderOperations**, each tracked by **WorkOrderOperationConfirmations**
- **Reportable** provides cross-cutting `sys*` properties (site, unit, tags found/linked) shared by Activity, TimeSeries, MaintenanceOrder, and Operation
- All major entities carry a `source` relation to **SourceSystem** and a `UUID` custom property

## Configuration

### Default Variables (`default.config.yaml`)

```yaml
# Enterprise spaces
enterpriseSchemaSpace: sp_enterprise_process_industry
enterpriseInstanceSpace: sp_enterprise_instance
siteInstanceSpace: sp_site_instance

# Data model configuration
organizationName: Enterprise
enterpriseDataModelId: DataModel
enterpriseDataModelVersion: v1
enterpriseSearchDataModelVersion: v1

# CDM version
cdmDataModelVersion: v1

# Reserved word prefix (for views starting with numbers or reserved words)
reservedWordPrefix: Enterprise_

```

### Key Configuration Points

| Variable | Purpose | Example Override |
|----------|---------|-----------------|
| `enterpriseSchemaSpace` | Space for all schema definitions | `sp_myorg_process_industry` |
| `enterpriseInstanceSpace` | Space for enterprise instance data | `sp_myorg_instance` |
| `siteInstanceSpace` | Space for site-specific instance data | `sp_mysite_instance` |
| `organizationName` | Prefix used in data model names | `MyOrg` |
| `enterpriseDataModelVersion` | Version tag for enterprise views | `v2` |
| `reservedWordPrefix` | Prefix for views whose names start with numbers (e.g., `3DModel` becomes `Enterprise_3DModel`) | `MyOrg_` |
| `sourceName` | Name of the primary source system | `Houston AVEVA PI` |

### Environment Override

Override defaults in your environment config file (`config.<env>.yaml`):

```yaml
variables:
  modules:
    qs_enterprise_dm:
      enterpriseSchemaSpace: sp_myorg_process_industry
      enterpriseInstanceSpace: sp_myorg_instance
      siteInstanceSpace: sp_mysite_instance
      organizationName: MyOrg
      reservedWordPrefix: MyOrg_
      sourceName: Houston AVEVA PI
```

## Getting Started

### Prerequisites

- CDF project with data modeling enabled
- CDF Toolkit (`cdf`) CLI installed and configured
- Admin permissions to create spaces, containers, views, and data models

### Deployment Steps

1. **Review and customize** the configuration variables in `default.config.yaml` or your environment override file.

2. **Deploy the module** using CDF Toolkit:

```bash
cdf deploy --env <your-environment>
```

3. **Verify** the deployment:

```bash
# List data models in the schema space
cdf data-models list --space sp_enterprise_process_industry
```

4. **Populate data** using transformations or ingestion pipelines that write to the enterprise and site instance spaces.

### Customizing Containers

Add custom properties to any container to extend the model for your organization:

```yaml
# Example: Adding a custom property to the Asset container
properties:
  myCustomField:
    type:
      type: primitive
      primitive: string
    description: Organization-specific custom field
```

## Dependencies

This module should be deployed **before** any modules that:

- Ingest data into the enterprise views (e.g., transformation modules for SAP, PI)
- Build applications or dashboards that query the enterprise data model
- Define site-level extensions that reference these enterprise views

## Notes

- Views whose external IDs start with a number or reserved word use the `reservedWordPrefix` variable (e.g., `3DModel` is deployed as `Enterprise_3DModel`)
- The **Reportable** container provides shared system-level properties (`sysSite`, `sysUnit`, `sysTagsFound`, `sysTagsLinked`) that are reused across Activity, TimeSeries, MaintenanceOrder, and Operation views
- Each major entity includes a `UUID` property from its own container for cross-system identification
- The search data model is a curated subset, version it independently via `enterpriseSearchDataModelVersion` when changing which views are included
