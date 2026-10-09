# PI Extractor Module

This module ingests PI tags and tag values from an OSIsoft/AVEVA PI Data Archive directly into the Core Data Model `ExtractorTimeSeries` view in `{{instanceSpace}}`, via the Cognite **PI .NET Extractor** (Windows-only). The extractor's `time-series.space-id` setting (in `ep_pi.ExtractionPipeline.Config.yaml`) enables the CDM destination — there is **no RAW staging** and no separate transformation step. Tag metadata (`name`, `description`, `unit`) lands as fields on the `ExtractorTimeSeries` instances.

## Module Architecture

```
cdf_pi_extractor/
├── auth/
│   └── producer.ep.pi.Group.yaml              # Scoped extractor service-principal group
├── data_modeling/
│   └── sp_pi_extractor.Space.yaml             # Per-extractor DM instance space
├── extraction_pipelines/
│   ├── ep_pi.ExtractionPipeline.yaml          # Pipeline definition (contacts, schedule, source)
│   └── ep_pi.ExtractionPipeline.Config.yaml   # Full PI .NET Extractor config template (CDM destination)
└── module.toml
```

## Data Flow

```
PI Data Archive
      │
      ▼  (PI SDK / PI Web API)
PI .NET Extractor   (time-series.space-id: {{instanceSpace}})
      │
      └── Tags + values ──────────► CDM ExtractorTimeSeries instances in {{instanceSpace}}
                                    (externalId = "{{piIdPrefix}}<PI Point name>")
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_timeseries_{{location}}_{{sourceSystem}}` | Pipeline health tracking and config delivery |
| DM Space | `{{instanceSpace}}` | Per-extractor instance space for DM instances |
| Access Group | `producer_{{location}}_ep_pi_{{sourceSystem}}_{{environment}}` | Scoped service-principal group — one per extractor type × source system |

Names follow the [CDF resource naming conventions](https://docs.cognite.com/cdf/deploy/reference/cdf_resource_naming_conventions):
pipelines use `ep_{data_type}_{location}_{source}` and access groups use the persona-led
pattern `producer_[{site}_]ep_{extractortype}_{sourcesystem}_{environment}`.

The producer group follows the least-privilege scoping in the GVD data onboarding SOP:
`extractionConfigs:READ`, `extractionRuns:WRITE`, and `extractionPipelines:READ` on the
pipeline's data set, with no `extractionPipelines:WRITE`. It also has `projects:LIST`
(all) and `groups:LIST` (current user), the minimum capabilities for any CDF application.

## Configuration

All variables are declared locally in `config.<env>.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_pi_extractor:
      location: "oslo"                                       # Site code, used in externalIds (ep_timeseries_<location>_<sourceSystem>)
      sourceSystem: "pi"                                     # Source system token, used in the pipeline ID and group name
      instanceSpace: "inst_oslo_pi"                           # Per-extractor DM instance space — computed by setup_project.py
      dataset: "ds_pi_oslo"                                 # ds_<data_type>_<location> — computed by setup_project.py
      piIdPrefix: "pi:"                                     # External ID prefix for all PI tag timeseries

      integration_owner_name: "Integration Owner"            # Technical contact for the pipeline
      integration_owner_email: "integration.owner@example.com"

      data_owner_name: "Data Owner"                          # Business contact for the data
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
| `IDP_TENANT_ID` | IDP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

## Verify Before Deploy

Confirm the following before running the extractor in production:

1. **DM space exists** — `{{instanceSpace}}` must already be deployed (the
   extractor will not create it). This module ships a `data_modeling/` space
   definition, so running `cdf deploy` will create it automatically.
2. **`ExtractorTimeSeries` view available** — this is part of the Cognite Core
   Data Model (CDM v1) `ExtractorTimeSeries` extension. Confirm CDM is enabled
   in your project.
3. **`piIdPrefix` is unique** — if you run multiple PI extractors against the
   same CDF project, give each one a different prefix so external IDs don't
   collide.
4. **PI Point selection** — the extractor browses the PI Data Archive and
   subscribes to all PI Points by default. Add tag filters in
   `ep_pi.ExtractionPipeline.Config.yaml` if you need to restrict the scope
   (large PI servers can have hundreds of thousands of tags).

## Getting Started

### Prerequisites

- DM space `{{instanceSpace}}` deployed and writable (Core Data Model with the
  `ExtractorTimeSeries` extension) — deployed automatically by this module
- PI .NET Extractor installed on a Windows host with network access to the PI
  Data Archive and to CDF
- CDF service account with write access to the `{{dataset}}` data set and write
  access to `{{instanceSpace}}` for `ExtractorTimeSeries` instances

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_pi_extractor --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_timeseries_{{location}}_{{sourceSystem}}` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Migrating from earlier versions

The pipeline external ID changed from `ep_{{location}}_pi` to
`ep_timeseries_{{location}}_{{sourceSystem}}`, and the access group from
`producer_{{location}}_ep_pi_{{environment}}` to
`producer_{{location}}_ep_pi_{{sourceSystem}}_{{environment}}`. To upgrade an existing
deployment:

1. Add `sourceSystem` to `cdf_pi_extractor` in each `config.<env>.yaml`.
2. Run `cdf deploy`. This creates the new pipeline and group alongside the old ones.
3. Update `extraction-pipeline.external-id` in the extractor host's local `config.yaml`,
   then restart the extractor.
4. Once the extractor reports runs on the new pipeline, delete the old pipeline and group
   in Fusion. Toolkit does not remove them for you, and the new group keeps the same
   `sourceId`, so no IdP change is needed.

The instance space also changed from `sp_{{location}}_pi` to `inst_{{location}}_pi`
(`setup_project.py` writes the new name on its next run). Instances already in the old
space are not moved: the extractor creates new time series in the new space on its next
run and backfills datapoint history into them from the PI Data Archive. Verify the new
time series, then delete the old space. To keep the old space instead, set
`instanceSpace` back to its old value after running the wizard;
`setup_project.py --check` then reports it as drift.

The producer group's capabilities were also narrowed (see [Resources Created](#resources-created)).
If anything else uses this group's service principal, check that it does not rely on the
removed capabilities.

### Verify

The PI extractor writes timeseries directly to the **Core Data Model `ExtractorTimeSeries`** view in `{{instanceSpace}}` (driven by `time-series.space-id` in `ep_pi.ExtractionPipeline.Config.yaml`). Open the `ExtractorTimeSeries` view in Fusion → Data Models, filter on space `{{instanceSpace}}`, and confirm instances exist with `externalId` prefixed by `{{piIdPrefix}}` (default `pi:`) and metadata fields (`name`, `description`, `unit`) populated from the PI tags.

## Dependencies

**Depends on**: DM space (`{{instanceSpace}}`) with the Core Data Model `ExtractorTimeSeries` view available — created by this module's `data_modeling/` resources.
