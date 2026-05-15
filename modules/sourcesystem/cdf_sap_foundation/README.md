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
│   ├── db_sap.Database.yaml                    # db_{{location}}_sap
│   ├── functional_location.Table.yaml          # SAP FunclocListSet  (master, weekly)
│   ├── equipment.Table.yaml                    # SAP EquipmentListSet (master, weekly)
│   ├── workorder.Table.yaml                    # SAP ExHeaderSet      (orders, daily)
│   ├── workpackage.Table.yaml                  # SAP ExOlistSet       (order list, daily)
│   ├── worktask.Table.yaml                     # SAP ExOperationsSet  (operations, daily)
│   ├── workitem.Table.yaml                     # SAP ExNotifheader    (notifications, daily)
│   └── state_store.Table.yaml                  # Extractor state (managed by extractor)
├── default.config.yaml
└── module.toml
```

> **Note:** This foundation module ingests SAP data into RAW only. Transformations
> from RAW into the ISA Manufacturing Extension data model are not yet shipped
> with this module — see `.cursor/rules/cdf-transformations.mdc` for guidance on
> authoring them, and the table below for the intended target instance types.

## Data Flow

```
SAP NW Gateway
      │
      ▼
SAP OData Extractor (single pipeline, 6 entity queries)
      │
      ├── FunclocListSet   ──► RAW: functional_location   ──► ISAAsset
      ├── EquipmentListSet ──► RAW: equipment             ──► Equipment + Equipment.asset relation
      ├── ExHeaderSet      ──► RAW: workorder             ──► WorkOrder
      ├── ExOlistSet       ──► RAW: workpackage           ──► (target TBD — order line items)
      ├── ExOperationsSet  ──► RAW: worktask              ──► Operation + Operation.workOrder relation
      └── ExNotifheader    ──► RAW: workitem              ──► (target TBD — notifications)

      State checkpoints  ───► RAW: state_store            (delta tracking, extractor-managed)
```



## Configuration

All variables are declared locally in `default.config.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_sap_foundation:
      location: "site1"             # Site code, used in externalIds (ep_<location>_sap, db_<location>_sap)
      instanceSpace: "sp_instances" # DM space for ISA Manufacturing Extension instances
      dataset: "ds_sap"             # dataSetExternalId for the pipeline and RAW database
      sapPlant: "1000"              # SAP plant code, used in OData filter expressions (MaintPlant eq '<sapPlant>')
      sapDisableSsl: false          # Set true only if SAP server uses an untrusted self-signed certificate
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
| `CDF_URL` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `IDP_TENANT_ID` | IdP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

SAP OData service names (`sap_service`), entity keys (`sap_key`), and **filter
property casing** vary across SAP versions and customer NW Gateway
configurations. The current `ep_sap.ExtractionPipeline.Config.yaml` ships with
plausible defaults but **must be verified against your SAP landscape**:

- `sap_service` values (`ZGW_FUNCLOC_SRV`, `ZGW_GETEQIP_SRV`,
  `ZPM_ORDER_DATA_EXPORT_SRV`, `ZPM_NOTI_EXTRACT_DATA_SRV`) are customer-specific
  Z-services.
- `sap_key` field names (e.g. `Functlocation`, `Equipment`, `OrderId`,
  `Activity`, `NotifNo`) follow each gateway's naming.
- **Filter property casing is inconsistent in the shipped template** —
  `MaintPlant` (Funcloc), `Maintplant` (Equipment), `Mainplant` (Notifications).
  The notifications filter in particular (`Mainplant`) is suspected to be a typo
  for `Maintplant` or `MaintPlant`. Confirm the exact spelling against your
  service `$metadata` document before deploying.

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted guidance when
authoring the downstream transformations into ISA Manufacturing Extension.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed (downstream target)
- SAP OData Extractor installed with network access to SAP NW Gateway
- Extractor service account with read/write to the `db_{{location}}_sap` RAW
  database and read access to the `{{dataset}}` data set
- SAP service account with READ access to PM/AM entities

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_sap_foundation --env your-environment
```