# OPC-UA Foundation Module

This module ingests OPC-UA node metadata from an OPC-UA server into CDF RAW, then transforms it into `ISATimeSeries` data model instances in the ISA Manufacturing Extension. OPC-UA timeseries values (measurements) are written directly to CDF by the OPC-UA Extractor — RAW is used for node metadata (name, description, data type) to ensure consistency with PI and SAP modules and to provide an audit trail.

The extractor configuration template is sourced from `gss-knowledge-base` and includes all required parameters with documentation.

> **Node filters must be configured before production use.** Without filters the extractor browses the full OPC-UA server tree, which may be very large and slow. See the extractor config for commented-out examples.

## Module Architecture

```
cdf_opcua_foundation/
├── extraction_pipelines/
│   ├── ep_opcua.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_opcua.ExtractionPipeline.Config.yaml   # Full OPC-UA Extractor config template
├── raw/
│   └── db_opcua.Database.yaml                    # db_{{location}}_opcua
├── transformations/
│   ├── tr_opcua_timeseries.Transformation.yaml   # Targets ISATimeSeries view
│   └── tr_opcua_timeseries.Transformation.sql    # Scaffold SQL — adapt before production use
├── default.config.yaml
└── module.toml
```

## Data Flow

```
OPC-UA Server
      │
      ▼
OPC-UA Extractor
      │
      ├── Timeseries values ──────────────────► CDF Timeseries (direct write)
      │
      └── Node metadata (name, desc, type) ──► RAW: db_{{location}}_opcua.nodes
                                                        │
                                                        ▼
                                               Transformation: tr_{{location}}_opcua_timeseries
                                                        │
                                                        ▼
                                               ISATimeSeries DM instances in {{instanceSpace}}
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_opcua` | Pipeline health tracking and config delivery |
| RAW Database | `db_{{location}}_opcua` | OPC-UA node metadata landing zone |
| Transformation | `tr_{{location}}_opcua_timeseries` | RAW metadata → ISATimeSeries DM instances |

## Configuration

```yaml
variables:
  modules:
    cdf_opcua_foundation:
      location: "site1"
      instanceSpace: "sp_isa_instance_space"
      schemaSpace: "sp_isa_manufacturing"
      dataModelVersion: "v1"
      dataset: "ds_opcua"
      opcuaIdPrefix: "opcua:"
      opcuaPublishingInterval: 5000
      opcuaSamplingInterval: 5000
      populateSysTagsFound: true
      integration_owner_name: "Integration Owner"
      integration_owner_email: "integration.owner@example.com"
      data_owner_name: "Data Owner"
      data_owner_email: "data.owner@example.com"
```

## Environment Variables

Set these on the host running the OPC-UA Extractor:

| Variable | Description |
|---|---|
| `OPCUA_ENDPOINT_URL` | OPC-UA server URL, e.g. `opc.tcp://192.168.1.10:4840` |
| `OPCUA_USER` | OPC-UA server username |
| `OPCUA_PASSWORD` | OPC-UA server password |
| `CDF_PROJECT` | CDF project name |
| `CDF_URL` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `IDP_TENANT_ID` | IdP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

1. **Node filters** — configure `extraction.transformations` in `ep_opcua.ExtractionPipeline.Config.yaml` before production.
2. **Endpoint URL** — set `OPCUA_ENDPOINT_URL` to your server's address.
3. **Certificates** — for production, set `auto-accept: false` and configure certificate storage per OPC-UA extractor docs.

## Transformation SQL — Important Note

`tr_opcua_timeseries.Transformation.sql` is a **generalized scaffold**. Column names (`Id`, `DisplayName`, `Description`, `DataType`, `NodeClass`) reflect the default OPC-UA Extractor RAW schema when `store-raw-metadata: true` is set. Preview against your actual RAW data before production use. See `.cursor/rules/cdf-transformations.mdc` for AI-assisted adaptation guidance.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed
- OPC-UA Extractor installed and network-accessible to the OPC-UA server
- Extractor service account with read/write to `db_{{location}}_opcua` and the `{{dataset}}` data set
- Node filters configured in the extractor config

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_opcua_foundation --env your-environment
```

### Configure and run the extractor

Set environment variables on the extractor host and start the extractor — it pulls config from `ep_{{location}}_opcua` automatically.

### Run the transformation

```bash
cdf transformations run tr_{{location}}_opcua_timeseries --env your-environment
```

### Verify

Check that `ISATimeSeries` instances appear in `{{instanceSpace}}` in CDF Data Explorer.

## Dependencies

**Depends on**: `models/isa_manufacturing_extension`

**Used by**: `foundation/cdf_ingestion_foundation` (references `tr_{{location}}_opcua_timeseries` in the ingestion workflow)
