# Files Extractor Module

This module ingests files from a file source (SharePoint Online, file shares, S3, ...) into CDF via the Cognite **Files Extractor**. The shipped example pipeline targets **SharePoint Online** in CDM mode — files are uploaded to CDF and registered as `CogniteFile` instances in a DM space. Adapt the file-provider block to target other sources (file system, S3, …) as needed. Downstream contextualization (P&ID parsing, document classification, etc.) is not shipped here; configure it separately.

## Module Architecture

```
cdf_files_extractor/
├── auth/
│   └── producer.ep.files.Group.yaml                         # Scoped extractor service-principal group
├── data_modeling/
│   └── sp_files_extractor.Space.yaml                        # Per-extractor DM instance space
├── data_sets/
│   └── ds_files.DataSet.yaml                                # Data set for uploaded files and pipeline
├── extraction_pipelines/
│   ├── ep_files_sharepoint.ExtractionPipeline.yaml         # Pipeline definition (contacts, schedule, source)
│   └── ep_files_sharepoint.ExtractionPipeline.Config.yaml  # Files Extractor runtime config (file provider, paths, filters)
├── raw/
│   └── db_files.Database.yaml                               # db_{{location}}_files (optional RAW-backed state store)
└── module.toml
```

> **Note:** File bytes and `CogniteFile` metadata land in CDF Files and
> `{{instanceSpace}}` (the shipped example uses `destination-mode: cdm`). The
> module does **not** stage file content to RAW. The `raw/` folder deploys
> `db_{{location}}_files` so the producer group can use RAW for an optional
> extractor state store (see `state-store` in `ExtractionPipeline.Config.yaml`).

## Data Flow

```
SharePoint Online (sites / document libraries / files)
      │
      ▼  (Microsoft Graph API + Azure AD auth)
Files Extractor   (destination-mode: cdm)
      │
      ├── File bytes ─────────────────► CDF Files
      └── File metadata ──────────────► CogniteFile instances in {{instanceSpace}}
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_files_sharepoint` | Pipeline health tracking and config delivery |
| Data set | `{{dataset}}` | Groups uploaded files and pipeline resources |
| RAW Database | `db_{{location}}_files` | Optional RAW-backed extractor state store (not used for file ingestion) |
| DM Space | `{{instanceSpace}}` | Per-extractor instance space for DM instances |
| Access Group | `producer_{{location}}_ep_files_{{environment}}` | Scoped service-principal group for the Files extractor |

## Configuration

All variables are declared locally in `config.<env>.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_files_extractor:
      location: "oslo"                                       # Site code, used in externalIds (ep_<location>_files_sharepoint)
      dataset: "ds_files"                                    # dataSetExternalId for the pipeline (Files land in this data set)
      instanceSpace: "sp_oslo_files"                        # Per-extractor DM instance space — computed by setup_project.py

      integration_owner_name: "Integration Owner"            # Technical contact for the pipeline
      integration_owner_email: "integration.owner@example.com"

      data_owner_name: "Data Owner"                          # Business contact for the data
      data_owner_email: "data.owner@example.com"
```

## Environment Variables

Set these on the host running the Files Extractor:

| Variable | Description |
|---|---|
| `CDF_PROJECT` | CDF project name |
| `CDF_URL` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `IDP_TENANT_ID` | IdP tenant ID (for the CDF service account) |
| `IDP_CLIENT_ID` | CDF service account client ID |
| `IDP_CLIENT_SECRET` | CDF service account client secret |
| `SP_AZURE_TENANT_ID` | Azure AD tenant ID hosting the SharePoint site |
| `SP_CLIENT_ID` | Azure AD app registration client ID with SharePoint permissions |
| `SP_CLIENT_SECRET` | Azure AD app registration client secret |
| `SP_EXTRACT_URL` | URL of the SharePoint site, document library, folder, or file to extract |

Either `SP_CLIENT_SECRET` **or** a certificate (set `SP_CERTIFICATE_PATH` or `SP_CERTIFICATE_DATA` and uncomment the matching line in `Config.yaml`) is required. Certificate auth is required if you want to use SharePoint group/user restrictions.

## Verify Before Deploy

The shipped `ep_files_sharepoint.ExtractionPipeline.Config.yaml` is a working
SharePoint Online example with a single extraction path. Before production use:

1. **Set `SP_EXTRACT_URL`** to the SharePoint site / document library / folder
   you actually want to extract from. The same URL is also used as the
   `source.name` and `source.external_id` for `CogniteFile` provenance.
2. **Adjust `files.extensions`** (default: `.pdf`, `.txt`, `.csv`) to match the
   document types you want to ingest. Remove the list to ingest every file.
3. **Tune `max-file-size`** (default: `64Mb`) — files larger than this are
   skipped. The extractor itself supports up to 100 GiB.
4. **Review `delete-behavior`** — currently set to `hard` (deletions in
   SharePoint cause hard deletes in CDF). Switch to `soft` if you prefer to
   keep deleted files visible with a `deleted` metadata flag.
5. **Pick a state-store path** in `Config.yaml` that the extractor service
   account can write to (default: `/path/to/state-store.json` is a placeholder
   — change before running). For cluster deployments, uncomment the RAW
   `state-store` block and point `database` at `db_{{location}}_files` (deployed
   from `raw/db_files.Database.yaml`). Add a `*.Table.yaml` under `raw/` if you
   want Toolkit to create the state table at deploy time.
6. **Confirm `data_model.space`** matches your `instanceSpace` — `CogniteFile`
   instances are written there. The CDM destination requires that the space
   already exists (this module deploys it automatically).
7. **Configure SharePoint app permissions** — the Azure AD app registration
   used by `SP_CLIENT_ID` needs the `Sites.Read.All` (or narrower)
   application permission with admin consent granted. Follow the SharePoint
   setup guide linked below.

## Getting Started

### Prerequisites

- Files Extractor installed on a host with network access to SharePoint and CDF
- Azure AD app registration with the appropriate Microsoft Graph / SharePoint permissions and admin consent
- CDF service account with write access to CDF Files and the `{{dataset}}` data set, and write access to `{{instanceSpace}}` for `CogniteFile` instances

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_files_extractor --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_files_sharepoint` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Verify

Check that uploaded documents appear under the `{{dataset}}` data set in CDF Data Explorer (Files tab) and that the corresponding `CogniteFile` instances are present in `{{instanceSpace}}`.

## References

* Files Extractor docs: <https://docs.cognite.com/cdf/integration/guides/extraction/file>
* Files Extractor configuration reference: <https://docs.cognite.com/cdf/integration/guides/extraction/configuration/file-extractor>
* SharePoint setup guide: <https://docs.cognite.com/cdf/integration/guides/extraction/file/file_sharepoint_setup>
