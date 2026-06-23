# DB Foundation Module

This module ingests rows from a relational database (PostgreSQL, MSSQL, Oracle, MySQL, …) into CDF RAW via the Cognite **DB Extractor** (ODBC-based). The shipped example is configured for PostgreSQL — update the connection string, database type, and SQL queries to target other databases. Transformations from RAW into a downstream data model are not shipped here; author them as needed.

## Module Architecture

```
cdf_db_foundation/
├── extraction_pipelines/
│   ├── ep_db_postgres.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_db_postgres.ExtractionPipeline.Config.yaml   # DB Extractor runtime config (queries, ODBC)
├── raw/
│   └── db_postgres.Database.yaml                       # db_{{location}}_db_postgres
└── module.toml
```

## Data Flow

```
Relational Database (PostgreSQL / MSSQL / Oracle / MySQL / …)
      │
      ▼  (ODBC + SQL queries)
DB Extractor
      │
      └── Query result rows ──────────► RAW: db_{{location}}_db_postgres.<query.destination.table>
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_{{location}}_db_postgres` | Pipeline health tracking and config delivery |
| RAW Database | `db_{{location}}_db_postgres` | Landing zone for query result rows |

## Configuration

All variables are declared locally in `config.<env>.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_db_foundation:
      location: "site1"                                       # Site code, used in externalIds (ep_<location>_db_postgres, db_<location>_db_postgres)
      dataset: "ds_db_postgres"                               # dataSetExternalId for the pipeline and RAW database

      integration_owner_name: "Integration Owner"             # Technical contact for the pipeline
      integration_owner_email: "integration.owner@example.com"

      data_owner_name: "Data Owner"                           # Business contact for the data
      data_owner_email: "data.owner@example.com"
```

## Environment Variables

Set these on the host running the DB Extractor:

| Variable | Description |
|---|---|
| `DB_CONNECTION_STRING` | ODBC connection string for the source database (see examples below) |
| `CDF_PROJECT` | CDF project name |
| `CDF_URL` | CDF base URL (e.g. `https://api.cognitedata.com`) |
| `IDP_TENANT_ID` | IdP tenant ID |
| `IDP_CLIENT_ID` | Service account client ID |
| `IDP_CLIENT_SECRET` | Service account client secret |

Example `DB_CONNECTION_STRING` values:

- **PostgreSQL:** `Driver={PostgreSQL Unicode};SERVER=hostname;DATABASE=dbname;PORT=5432;UID=username;PWD=password`
- **MSSQL:** `Driver={ODBC Driver 17 for SQL Server};SERVER=hostname;DATABASE=dbname;UID=username;PWD=password`
- **Oracle:** `Driver={Oracle in OraClient19Home1};DBQ=hostname:1521/service;UID=username;PWD=password`

## Verify Before Deploy

The shipped `ep_db_postgres.ExtractionPipeline.Config.yaml` is a minimal
example with a single query against `mytable`. Before production use:

1. **Update `databases.type`** from `odbc` (default) if you need a native driver,
   and ensure the matching ODBC driver is installed on the extractor host.
2. **Replace the example query** in the `queries:` block with your real SQL,
   set the correct `incremental-field` (used for delta extractions), and pick
   a sensible `initial-start` value.
3. **Adjust `destination.table`** so each query writes to a meaningfully-named
   RAW table — and add a corresponding `*.Table.yaml` under `raw/` if you want
   the toolkit to provision the table at deploy time.
4. **Verify the RAW database name** in `destination.database` matches
   `db_{{location}}_db_postgres` so rows land in the database declared by this
   module.
5. **If targeting a different DB engine**, rename this pipeline (and `dataset`)
   accordingly, e.g. `ep_db_mssql` / `ds_db_mssql`.

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted guidance when
authoring the downstream transformation from RAW into a data model.

## Getting Started

### Prerequisites

- Source database reachable from the extractor host with appropriate ODBC driver installed
- DB Extractor service account with read access to the source database
- Cognite service account with read/write to the `db_{{location}}_db_postgres` RAW database and read access to the `{{dataset}}` data set (default: `ds_db_postgres`)

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_db_foundation --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_{{location}}_db_postgres` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Verify

Check that the configured RAW table(s) under `db_{{location}}_db_postgres` are populated in CDF Data Explorer.
