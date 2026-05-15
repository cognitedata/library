# PI Foundation Module

This module ingests PI tag metadata from an OSIsoft/AVEVA PI Data Archive into CDF RAW. PI timeseries values (measurements) are written directly to CDF by the PI .NET Extractor — RAW is used only for tag metadata (name, description, unit, engineering range), staged for downstream transformation into the ISA Manufacturing Extension `ISATimeSeries` view. Transformations are not shipped with this module; author them downstream as needed.

The extractor configuration template is sourced from `gss-knowledge-base` and includes all required parameters with documentation.

## Module Architecture

```
cdf_pi_foundation/
├── extraction_pipelines/
│   ├── ep_pi.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_pi.ExtractionPipeline.Config.yaml   # Full PI .NET Extractor config template
├── default.config.yaml
└── module.toml
```

> **Note:** This foundation module ingests PI data into RAW (and Timeseries
> directly). Transformations from RAW into the ISA Manufacturing Extension data
> model are not yet shipped with this module — see
> `.cursor/rules/cdf-transformations.mdc` for guidance on authoring them.

## Data Flow

```
PI Data Archive
      │
      ▼
PI .NET Extractor
      │
      ├── Timeseries values ──────────────────► CDF Timeseries (direct write)
      │
      └── Tag metadata (name, unit, desc) ───► RAW: db_{{location}}_pi.timeseries_metadata
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_pi` | Pipeline health tracking and config delivery |
| RAW Database | `db_{{location}}_pi` | PI tag metadata landing zone (declared by the pipeline's `rawTables`) |

## Configuration

All variables are declared locally in `default.config.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_pi_foundation:
      location: "site"              # Site code, used in externalIds (ep_<location>_pi, db_<location>_pi)
      instanceSpace: "sp_instances" # DM space for ISATimeSeries / CogniteTimeSeries instances
      dataset: "ds_pi"              # dataSetExternalId for the pipeline and RAW database
      piIdPrefix: "pi:"             # External ID prefix for all PI tag timeseries
```

## Environment Variables

Set these on the host running the PI .NET Extractor:

| Variable | Description |
|---|---|
| `PI_HOST` | PI Data Archive hostname or IP |
| `PI_USER` | PI server username |
| `PI_PASSWORD` | PI server password |
| `CDF_PROJECT` | CDF project name |
| `CDF_URL` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `IDP_TENANT_ID` | IDP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

The PI .NET Extractor RAW schema (column names in `timeseries_metadata`) varies
across extractor versions. When you author the downstream transformation into
`ISATimeSeries` (not shipped with this module), be prepared to:

1. Preview the transformation against your actual RAW data in CDF.
2. Verify column names match your extractor output (defaults are `name`,
   `description`, `unit`).
3. Adapt any `sysTagsFound`-style regex patterns to your site's PI tag naming
   convention.

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted authoring guidance.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed (downstream target)
- PI .NET Extractor installed on a Windows host with network access to the PI
  Data Archive and to CDF
- Extractor service account with read/write to the `db_{{location}}_pi` RAW
  database and read access to the `{{dataset}}` data set

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_pi_foundation --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_pi` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Verify

Check that the `db_{{location}}_pi.timeseries_metadata` RAW table is populated and that classic timeseries with the `{{piIdPrefix}}` prefix appear in CDF Data Explorer.

## Dependencies

**Depends on**: `models/isa_manufacturing_extension` (target data model for the downstream transformation you author on top of this module)
