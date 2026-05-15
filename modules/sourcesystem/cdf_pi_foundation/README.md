# PI Foundation Module

This module ingests PI tag metadata from an OSIsoft/AVEVA PI Data Archive into CDF RAW, then transforms it into `ISATimeSeries` data model instances in the ISA Manufacturing Extension. PI timeseries values (measurements) are written directly to CDF by the PI .NET Extractor — RAW is used only for tag metadata (name, description, unit, engineering range) which is then transformed into DM view properties.

The extractor configuration template is sourced from `gss-knowledge-base` and includes all required parameters with documentation.

## Module Architecture

```
cdf_pi_foundation/
├── extraction_pipelines/
│   ├── ep_pi.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_pi.ExtractionPipeline.Config.yaml   # Full PI .NET Extractor config template
├── raw/
│   └── db_pi.Database.yaml                    # db_{{location}}_pi
├── transformations/
│   ├── tr_pi_timeseries.Transformation.yaml   # Targets ISATimeSeries view
│   └── tr_pi_timeseries.Transformation.sql    # Scaffold SQL — adapt before production use
├── default.config.yaml
└── module.toml
```

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
                                                        │
                                                        ▼
                                               Transformation: tr_{{location}}_pi_timeseries
                                                        │
                                                        ▼
                                               ISATimeSeries DM instances in {{instanceSpace}}
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_pi` | Pipeline health tracking and config delivery |
| RAW Database | `db_{{location}}_pi` | PI tag metadata landing zone |
| Transformation | `tr_{{location}}_pi_timeseries` | RAW metadata → ISATimeSeries DM instances |

## Configuration

```yaml
variables:
  modules:
    cdf_pi_foundation:
      piIdPrefix: "pi:"               # External ID prefix for all PI tag timeseries
      populateSysTagsFound: true      # Set false to omit sysTagsFound (opt-out)
```

Variables inherited from `cdf_foundation` (no need to set again):
`location`, `instanceSpace`, `dataset`, `rawSourceDatabase`, `schemaSpace`, `dataModelVersion`

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

## Transformation SQL — Important Note

`tr_pi_timeseries.Transformation.sql` is a **generalized scaffold**. The column names (`name`, `description`, `unit`) reflect the default PI .NET Extractor RAW schema but may vary depending on extractor version and configuration.

Before running in production:
1. Preview the transformation against your actual RAW data in CDF
2. Verify column names match your extractor output
3. Adapt the `sysTagsFound` regex pattern to your site's PI tag naming convention

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted adaptation guidance.

## Getting Started

### Prerequisites

- `foundation/cdf_foundation` deployed
- `models/isa_manufacturing_extension` deployed
- PI .NET Extractor installed and accessible to the PI Data Archive
- Extractor service account added to `grp_{{location}}_extractors` group

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_pi_foundation --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_pi` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Run the transformation

```bash
cdf transformations run tr_{{location}}_pi_timeseries --env your-environment
```

### Verify

Check that `ISATimeSeries` instances appear in `{{instanceSpace}}` in CDF Data Explorer.

## Dependencies

**Depends on**: `foundation/cdf_foundation`

**Used by**: `foundation/cdf_ingestion_foundation` (references `tr_{{location}}_pi_timeseries` in the ingestion workflow)
