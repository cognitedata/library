# OPC-UA Foundation Module

This module ingests OPC-UA node metadata from an OPC-UA server into CDF RAW, then transforms it into `ISATimeSeries` data model instances in the ISA Manufacturing Extension. OPC-UA timeseries values (measurements) are written directly to CDF by the OPC-UA Extractor — RAW is used for node metadata (name, description, data type) to ensure consistency with PI and SAP modules and to provide an audit trail.


> **Node filters must be configured before production use.** Without filters the extractor browses the full OPC-UA server tree, which may be very large and slow. See the extractor config for commented-out examples.

## Module Architecture

```
cdf_opcua_foundation/
├── extraction_pipelines/
│   ├── ep_opcua.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_opcua.ExtractionPipeline.Config.yaml   # Full OPC-UA Extractor config template
├── raw/
│   └── db_opcua.Database.yaml                    # db_{{location}}_opcua
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

## Configuration

All variables are declared locally in `default.config.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_opcua_foundation:
      location: "site1"                                       # Site code, used in externalIds (ep_<location>_opcua, db_<location>_opcua)
      instanceSpace: "sp_instances"                           # DM space for ISATimeSeries / CogniteTimeSeries instances
      dataset: "ds_opcua"                                     # dataSetExternalId for the pipeline and RAW database

      integration_owner_name: "Integration Owner"             # Technical contact for the pipeline
      integration_owner_email: "integration.owner@example.com"

      data_owner_name: "Data Owner"                           # Business contact for the data
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
| `IDP_TENANT_ID` | IDP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Node Filters — Required Before Production Use

OPC-UA node structure is highly site-specific. The extractor config ships with node filter examples commented out. You must configure these before production use:

1. Obtain the OPC-UA namespace and node IDs from the site OPC-UA administrator or browse the server with a tool like UA Expert
2. Uncomment and adapt the `extraction.transformations` block in `ep_opcua.ExtractionPipeline.Config.yaml`
3. Start with `Include` rules for the Object nodes that contain your data, then `Include` the Variable nodes you need

Without filters, the extractor will browse the entire server tree, which can be very large on industrial OPC-UA servers and may result in slow or incomplete extractions.

## Transformation SQL — Important Note

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted adaptation guidance.

## Getting Started

### Prerequisites

- `foundation/cdf_foundation` deployed
- `models/isa_manufacturing_extension` deployed
- OPC-UA Extractor installed and network-accessible to the OPC-UA server
- Extractor service account added to `grp_{{location}}_extractors` group
- Node filters configured in the extractor config

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_opcua_foundation --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_opcua` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.


### Verify

Check that `ISATimeSeries` instances appear in `{{instanceSpace}}` in CDF Data Explorer.

