# SAP Extractor Module

This module ingests SAP functional locations, equipment, maintenance orders, and operations into CDF RAW via a **single SAP OData extraction pipeline** with multiple entity queries. Downstream transformations from RAW into the ISA Manufacturing Extension data model are not shipped with this module — author them as needed (one per entity type, run in dependency order to preserve referential integrity).

## Module Architecture

```
cdf_sap_extractor/
├── auth/
│   └── producer.ep.sap.Group.yaml              # Scoped extractor service-principal group
├── data_modeling/
│   └── sp_sap_extractor.Space.yaml             # Per-extractor DM instance space
├── extraction_pipelines/
│   ├── ep_sap.ExtractionPipeline.yaml          # Single pipeline, all entity types
│   └── ep_sap.ExtractionPipeline.Config.yaml   # Full SAP OData extractor config template
├── raw/
│   ├── db_sap.Database.yaml                    # raw_maintenance_{{location}}_{{sourceSystem}}
│   ├── functional_location.Table.yaml          # SAP FunclocListSet  (master, weekly)
│   ├── equipment.Table.yaml                    # SAP EquipmentListSet (master, weekly)
│   ├── workorder.Table.yaml                    # SAP ExHeaderSet      (orders, daily)
│   ├── workpackage.Table.yaml                  # SAP ExOlistSet       (order list, daily)
│   ├── worktask.Table.yaml                     # SAP ExOperationsSet  (operations, daily)
│   ├── workitem.Table.yaml                     # SAP ExNotifheader    (notifications, daily)
│   └── state_store.Table.yaml                  # Extractor state (managed by extractor)
└── module.toml
```

## Data Flow

```
SAP NW Gateway
      │
      ▼
SAP OData Extractor (single pipeline, 6 entity queries)
      │
      ├── FunclocListSet   ──► RAW: functional_location   ──► ISAAsset (downstream — not shipped)
      ├── EquipmentListSet ──► RAW: equipment             ──► Equipment + Equipment.asset relation (downstream)
      ├── ExHeaderSet      ──► RAW: workorder             ──► WorkOrder (downstream)
      ├── ExOlistSet       ──► RAW: workpackage           ──► (target TBD — order line items)
      ├── ExOperationsSet  ──► RAW: worktask              ──► Operation + Operation.workOrder relation (downstream)
      └── ExNotifheader    ──► RAW: workitem              ──► (target TBD — notifications)

      State checkpoints  ───► RAW: state_store            (delta tracking, extractor-managed)
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_maintenance_{{location}}_{{sourceSystem}}` | Pipeline health tracking and config delivery |
| RAW Database | `raw_maintenance_{{location}}_{{sourceSystem}}` | SAP entity landing zone |
| RAW Tables | `functional_location`, `equipment`, `workorder`, `workpackage`, `worktask`, `workitem`, `state_store` | One per OData query plus an extractor-managed state-store table |
| DM Space | `{{instanceSpace}}` | Per-extractor instance space for DM instances |
| Access Group | `producer_{{location}}_ep_sap_{{sourceSystem}}_{{environment}}` | Scoped service-principal group — one per extractor type × source system |

Names follow the [CDF resource naming conventions](https://docs.cognite.com/cdf/deploy/reference/cdf_resource_naming_conventions):
pipelines use `ep_{data_type}_{location}_{source}` and access groups use the persona-led
pattern `producer_[{site}_]ep_{extractortype}_{sourcesystem}_{environment}`.

## Configuration

All variables are declared locally in `config.<env>.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_sap_extractor:
      location: "oslo"                                        # Site code, used in externalIds (ep_maintenance_<location>_<sourceSystem>, raw_maintenance_<location>_<sourceSystem>)
      sourceSystem: "sap"                                     # Source system token, used in the pipeline ID and group name
      instanceSpace: "sp_oslo_sap"                           # Per-extractor DM instance space — computed by setup_project.py
      dataset: "ds_sap_oslo"                                  # ds_<data_type>_<location> — computed by setup_project.py
      sapPlant: "1000"                                        # SAP plant code, used in OData filter expressions (MaintPlant eq '<sapPlant>')
      sapDisableSsl: false                                    # Set true only if SAP server uses an untrusted self-signed certificate

      integration_owner_name: "Integration Owner"             # Technical contact for the pipeline
      integration_owner_email: "integration.owner@example.com"

      data_owner_name: "Data Owner"                           # Business contact for the data
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
- Extractor service account with read/write to the `raw_maintenance_{{location}}_{{sourceSystem}}` RAW
  database and read access to the `{{dataset}}` data set
- SAP service account with READ access to PM/AM entities

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_sap_extractor --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_maintenance_{{location}}_{{sourceSystem}}` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Migrating from earlier versions

The pipeline external ID changed from `ep_{{location}}_sap` to
`ep_maintenance_{{location}}_{{sourceSystem}}`, and the access group from
`producer_{{location}}_ep_sap_{{environment}}` to
`producer_{{location}}_ep_sap_{{sourceSystem}}_{{environment}}`. To upgrade an existing
deployment:

1. Add `sourceSystem` to `cdf_sap_extractor` in each `config.<env>.yaml`.
2. Run `cdf deploy`. This creates the new pipeline and group alongside the old ones.
3. Update `extraction-pipeline.external-id` in the extractor host's local `config.yaml`,
   then restart the extractor.
4. Once the extractor reports runs on the new pipeline, delete the old pipeline and group
   in Fusion. Toolkit does not remove them for you, and the new group keeps the same
   `sourceId`, so no IdP change is needed.

The RAW database also changed from `db_{{location}}_sap` to
`raw_maintenance_{{location}}_{{sourceSystem}}`. The `state_store` table lives in that
database, so the extractor starts without delta checkpoints and runs a full extraction of
every endpoint. Point downstream transformations at the new name, then delete the old
database.

### Verify

Check that all seven RAW tables under `raw_maintenance_{{location}}_{{sourceSystem}}` are populated in CDF Data Explorer (the master tables — `functional_location`, `equipment` — populate weekly; the order/notification tables populate daily).
