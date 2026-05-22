# OPC-UA Foundation Module

This module ingests OPC-UA node metadata, references, and subscription state into CDF RAW. OPC-UA timeseries values (measurements) are written directly to CDF by the OPC-UA Extractor — RAW is used for node metadata (name, description, data type), reference structure, and extractor browse/state caches to provide an audit trail and enable downstream contextualization. Transformations from RAW into the ISA Manufacturing Extension data model are not shipped here; author them downstream as needed.


> **Node filters must be configured before production use.** Without filters the extractor browses the full OPC-UA server tree, which may be very large and slow. See the extractor config for commented-out examples.

## Module Architecture

```
cdf_opcua_foundation/
├── extraction_pipelines/
│   ├── ep_opcua.ExtractionPipeline.yaml          # Pipeline definition with RAW table references
│   └── ep_opcua.ExtractionPipeline.Config.yaml   # Full OPC-UA Extractor config template
├── raw/
│   └── db_opcua.Database.yaml                    # db_{{location}}_opcua
├── default.config.yaml
└── module.toml
```

> **Note:** This foundation module ingests OPC-UA data into RAW (and Asset/TS
> directly). Transformations from RAW into the ISA Manufacturing Extension data
> model are not yet shipped with this module — see
> `.cursor/rules/cdf-transformations.mdc` for guidance on authoring them.

## Data Flow

```
OPC-UA Server
      │
      ▼
OPC-UA Extractor
      │
      ├── Variable values  ──────────► CDF Timeseries (direct write)
      ├── Object nodes     ──────────► RAW: assets
      ├── Variable nodes   ──────────► RAW: timeseries
      ├── References       ──────────► RAW: relationships
      ├── Browse cache     ──────────► RAW: known_objects, known_references, known_variables
      └── Subscription state ────────► RAW: state-store-variables
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_opcua` | Pipeline health tracking and config delivery |
| RAW Database | `db_{{location}}_opcua` | OPC-UA node metadata + state landing zone |

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

## Verify Before Deploy

OPC-UA node structure is highly site-specific. The extractor config ships with
sensible defaults but several aspects must be configured before production use:

1. **Node filters** — without filters the extractor browses the entire server
   tree, which can be very large on industrial OPC-UA servers. Obtain the
   namespace and node IDs from the site OPC-UA administrator (or browse with a
   tool like UA Expert), then uncomment and adapt the
   `extraction.transformations` block in
   `ep_opcua.ExtractionPipeline.Config.yaml`. Start with `Include` rules for
   the Object nodes that contain your data, then `Include` the Variable nodes
   you need.
2. **Endpoint URLs** — the documentation block in
   `ep_opcua.ExtractionPipeline.yaml` lists primary/secondary endpoints as
   examples; replace with the actual server addresses for your site.
3. **Certificate storage** — by default the OPC-UA extractor stores
   certificates in the OS user store. For service-account deployments, edit
   `config/opc.ua.net.extractor.Config.xml` to point at a known directory (see
   the install instructions in `ep_opcua.ExtractionPipeline.yaml`).

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted guidance when
authoring the downstream transformations into ISA Manufacturing Extension.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed (downstream target)
- OPC-UA Extractor installed and network-accessible to the OPC-UA server
- Extractor service account with read/write to the `db_{{location}}_opcua` RAW
  database and read access to the `{{dataset}}` data set
- Node filters configured in the extractor config

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_opcua_foundation --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_opcua` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.



