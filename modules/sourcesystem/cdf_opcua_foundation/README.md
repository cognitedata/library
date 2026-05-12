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
      opcuaIdPrefix: "opcua:"           # External ID prefix for all OPC-UA node timeseries
      opcuaPublishingInterval: 5000     # Subscription publishing interval in ms
      opcuaSamplingInterval: 5000       # Subscription sampling interval in ms
      populateSysTagsFound: true        # Set false to omit sysTagsFound (opt-out)
```

Variables inherited from `cdf_foundation` (no need to set again):
`location`, `instanceSpace`, `dataset`, `rawSourceDatabase`, `schemaSpace`, `dataModelVersion`

## Environment Variables

Set these on the host running the OPC-UA Extractor:

| Variable | Description |
|---|---|
| `OPCUA_ENDPOINT_URL` | OPC-UA server URL, e.g. `opc.tcp://192.168.1.10:4840` |
| `OPCUA_USER` | OPC-UA server username |
| `OPCUA_PASSWORD` | OPC-UA server password |
| `COGNITE_PROJECT` | CDF project name |
| `COGNITE_HOST` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `COGNITE_TENANT_ID` | IDP tenant ID |
| `COGNITE_CLIENT_ID` | Service account client ID |
| `COGNITE_CLIENT_SECRET` | Service account client secret |

## Node Filters — Required Before Production Use

OPC-UA node structure is highly site-specific. The extractor config ships with node filter examples commented out. You must configure these before production use:

1. Obtain the OPC-UA namespace and node IDs from the site OPC-UA administrator or browse the server with a tool like UA Expert
2. Uncomment and adapt the `extraction.transformations` block in `ep_opcua.ExtractionPipeline.Config.yaml`
3. Start with `Include` rules for the Object nodes that contain your data, then `Include` the Variable nodes you need

Without filters, the extractor will browse the entire server tree, which can be very large on industrial OPC-UA servers and may result in slow or incomplete extractions.

## Transformation SQL — Important Note

`tr_opcua_timeseries.Transformation.sql` is a **generalized scaffold**. The column names (`Id`, `DisplayName`, `Description`, `DataType`, `NodeClass`) reflect the default OPC-UA Extractor RAW schema when `store-raw-metadata: true` is set, but may vary by extractor version.

Before running in production:
1. Preview the transformation against your actual RAW data in CDF
2. Verify column names match your extractor output
3. Adapt the `sysTagsFound` regex to your site's OPC-UA node naming convention

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

### Run the transformation

```bash
cdf transformations run tr_{{location}}_opcua_timeseries --env your-environment
```

### Verify

Check that `ISATimeSeries` instances appear in `{{instanceSpace}}` in CDF Data Explorer.

## Dependencies

**Depends on**: `foundation/cdf_foundation`

**Used by**: `foundation/cdf_ingestion_foundation` (references `tr_{{location}}_opcua_timeseries` in the ingestion workflow)
