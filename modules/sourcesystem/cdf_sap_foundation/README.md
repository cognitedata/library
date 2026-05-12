# SAP Foundation Module

This module ingests SAP functional locations, equipment, maintenance orders, and operations into CDF RAW via a **single SAP OData extraction pipeline** with multiple entity queries, then transforms each entity type into the ISA Manufacturing Extension data model. Transformations are split by entity type and must be run in the correct order to ensure referential integrity.

The extractor configuration template is sourced from `gss-knowledge-base` and covers all standard PM/AM entity types from SAP S/4HANA.

> **SAP OData service names and entity keys vary across SAP versions and NW Gateway configurations.** Verify all service names and field names in the extractor config and transformation SQL against your SAP landscape before deploying.

## Module Architecture

```
cdf_sap_foundation/
├── extraction_pipelines/
│   ├── ep_sap.ExtractionPipeline.yaml          # Single pipeline, all entity types
│   └── ep_sap.ExtractionPipeline.Config.yaml   # Full SAP OData extractor config template
├── raw/
│   └── db_sap.Database.yaml                    # db_{{location}}_sap
├── transformations/
│   ├── tr_sap_assets.Transformation.yaml/.sql             # Functional locations → ISAAsset
│   ├── tr_sap_equipment.Transformation.yaml/.sql          # Equipment → Equipment
│   ├── tr_sap_equipment_to_asset.Transformation.yaml/.sql # Equipment.asset relation
│   ├── tr_sap_maintenance_orders.Transformation.yaml/.sql # Work orders → WorkOrder
│   ├── tr_sap_operations.Transformation.yaml/.sql         # Work tasks → Operation
│   └── tr_sap_operation_to_order.Transformation.yaml/.sql # Operation.workOrder relation
├── default.config.yaml
└── module.toml
```

## Data Flow

```
SAP NW Gateway
      │
      ▼
SAP OData Extractor (single pipeline, 6 entity queries)
      │
      ├── FunclocListSet  ──► RAW: functional_location
      ├── EquipmentListSet ──► RAW: equipment
      ├── ExHeaderSet     ──► RAW: workorder
      ├── ExOlistSet      ──► RAW: workpackage
      ├── ExOperationsSet ──► RAW: worktask
      └── ExNotifheader   ──► RAW: workitem
                                    │
                          ┌─────────┴──────────┐
                          ▼  (6 transformations, run in order)
               ISA Manufacturing Extension DM
               ├── ISAAsset instances
               ├── Equipment instances
               ├── Equipment.asset relation
               ├── WorkOrder instances
               ├── Operation instances
               └── Operation.workOrder relation
```

## Transformation Run Order

| Order | Transformation | Depends on |
|---|---|---|
| 1 | `tr_sap_assets` | — |
| 2 | `tr_sap_equipment` | `tr_sap_assets` |
| 3 | `tr_sap_equipment_to_asset` | `tr_sap_assets`, `tr_sap_equipment` |
| 4 | `tr_sap_maintenance_orders` | `tr_sap_assets` |
| 5 | `tr_sap_operations` | `tr_sap_maintenance_orders` |
| 6 | `tr_sap_operation_to_order` | `tr_sap_maintenance_orders`, `tr_sap_operations` |

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_sap` | Single pipeline with all SAP entity queries |
| RAW Database | `db_{{location}}_sap` | SAP data landing zone |
| Transformation | `tr_{{location}}_sap_assets` | Functional locations → ISAAsset |
| Transformation | `tr_{{location}}_sap_equipment` | Equipment master → Equipment |
| Transformation | `tr_{{location}}_sap_equipment_to_asset` | Equipment.asset relation |
| Transformation | `tr_{{location}}_sap_maintenance_orders` | Work orders → WorkOrder |
| Transformation | `tr_{{location}}_sap_operations` | Work tasks → Operation |
| Transformation | `tr_{{location}}_sap_operation_to_order` | Operation.workOrder relation |

## Configuration

```yaml
variables:
  modules:
    cdf_sap_foundation:
      location: "site1"
      instanceSpace: "sp_isa_instance_space"
      schemaSpace: "sp_isa_manufacturing"
      dataModelVersion: "v1"
      dataset: "ds_sap"
      sapSystem: s4hana
      sapPlant: "1000"
      sapPlants: []
      sapDisableSsl: false
      populateSysTagsFound: true
      integration_owner_name: "Integration Owner"
      integration_owner_email: "integration.owner@example.com"
      data_owner_name: "Data Owner"
      data_owner_email: "data.owner@example.com"
```

## Environment Variables

Set these on the host running the SAP OData Extractor:

| Variable | Description |
|---|---|
| `SAP_GATEWAY_URL` | SAP NW Gateway base URL, e.g. `https://sap-host:8000` |
| `SAP_SOURCE_NAME` | Unique label for this SAP source, e.g. `s4hana_oslo` |
| `SAP_CLIENT` | SAP client number, e.g. `100` |
| `SAP_USERNAME` | SAP service account username |
| `SAP_PASSWORD` | SAP service account password |
| `CDF_PROJECT` | CDF project name |
| `CDF_URL` | CDF base URL |
| `IDP_TENANT_ID` | IdP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

SAP OData service names (`sap_service`), entity keys (`sap_key`), and **filter property casing** vary across landscapes. The shipped template uses `MaintPlant`, `Maintplant`, and `Mainplant` in different filters — confirm exact spelling against your service `$metadata` before deploying.

## Transformation SQL — Important Note

All six SQL files are **generalized scaffolds**. Preview each transformation against your actual RAW data and verify column names match your extractor output. See `.cursor/rules/cdf-transformations.mdc` for AI-assisted adaptation guidance.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed
- SAP OData Extractor installed with network access to SAP NW Gateway
- Extractor service account with read/write to `db_{{location}}_sap` and the `{{dataset}}` data set
- SAP service account with READ access to PM/AM entities

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_sap_foundation --env your-environment
```

### Configure and run the extractor

Set environment variables on the extractor host and start the extractor — it pulls config from `ep_{{location}}_sap` automatically.

### Run transformations in order

```bash
cdf transformations run tr_{{location}}_sap_assets --env your-environment
cdf transformations run tr_{{location}}_sap_equipment --env your-environment
cdf transformations run tr_{{location}}_sap_equipment_to_asset --env your-environment
cdf transformations run tr_{{location}}_sap_maintenance_orders --env your-environment
cdf transformations run tr_{{location}}_sap_operations --env your-environment
cdf transformations run tr_{{location}}_sap_operation_to_order --env your-environment
```

In production, the ingestion workflow (`cdf_ingestion_foundation`) handles ordering automatically.

## Dependencies

**Depends on**: `models/isa_manufacturing_extension`

**Used by**: `foundation/cdf_ingestion_foundation` (references all 6 transformations with `dependsOn` ordering in the ingestion workflow)
