# PI Foundation Module

This module ingests PI tag metadata from an OSIsoft/AVEVA PI Data Archive into CDF RAW, then transforms it into `ISATimeSeries` data model instances in the ISA Manufacturing Extension. PI timeseries values (measurements) are written directly to CDF by the PI .NET Extractor — RAW is used only for tag metadata (name, description, unit, engineering range).

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
      location: "site1"
      instanceSpace: "sp_isa_instance_space"
      schemaSpace: "sp_isa_manufacturing"
      dataModelVersion: "v1"
      dataset: "ds_pi"
      piIdPrefix: "pi:"
      populateSysTagsFound: true
      integration_owner_name: "Integration Owner"
      integration_owner_email: "integration.owner@example.com"
      data_owner_name: "Data Owner"
      data_owner_email: "data.owner@example.com"
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
| `IDP_TENANT_ID` | IdP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

1. **`{{instanceSpace}}` and ISA data model** — deploy `models/isa_manufacturing_extension` first.
2. **`piIdPrefix` is unique** — use different prefixes if multiple PI extractors share one project.
3. **PI Point selection** — add tag filters in `ep_pi.ExtractionPipeline.Config.yaml` on large PI servers.

## Transformation SQL — Important Note

`tr_pi_timeseries.Transformation.sql` is a **generalized scaffold**. Preview against your actual RAW data and adapt column names and `sysTagsFound` regex to your site's PI tag naming convention. See `.cursor/rules/cdf-transformations.mdc` for AI-assisted adaptation guidance.

## Getting Started

### Prerequisites

- `models/isa_manufacturing_extension` deployed
- PI .NET Extractor installed on Windows with network access to the PI Data Archive and CDF
- Extractor service account with read/write to `db_{{location}}_pi` and the `{{dataset}}` data set

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_pi_foundation --env your-environment
```

### Configure and run the extractor

Set environment variables on the extractor host and start the extractor — it pulls config from `ep_{{location}}_pi` automatically.

### Run the transformation

```bash
cdf transformations run tr_{{location}}_pi_timeseries --env your-environment
```

### Verify

Check that `ISATimeSeries` instances appear in `{{instanceSpace}}` in CDF Data Explorer.

## Dependencies

**Depends on**: `models/isa_manufacturing_extension`

**Used by**: `foundation/cdf_ingestion_foundation` (references `tr_{{location}}_pi_timeseries` in the ingestion workflow)
