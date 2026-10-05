# DB Extractor Module

This module ingests rows from a relational database (PostgreSQL, MSSQL, Oracle, MySQL, …) into CDF RAW via the Cognite **DB Extractor** (ODBC-based). The shipped example is configured for PostgreSQL — update the connection string, database type, and SQL queries to target other databases. Transformations from RAW into a downstream data model are not shipped here; author them as needed.

## Module Architecture

```
cdf_db_extractor/
├── auth/
│   └── producer.ep.db.Group.yaml                        # Scoped extractor service-principal group
├── data_modeling/
│   └── sp_db_extractor.Space.yaml                       # Per-extractor DM instance space
├── extraction_pipelines/
│   ├── ep_db_postgres.ExtractionPipeline.yaml          # Pipeline definition with RAW table reference
│   └── ep_db_postgres.ExtractionPipeline.Config.yaml   # DB Extractor runtime config (queries, ODBC)
├── raw/
│   └── db_postgres.Database.yaml                       # raw_table_{{location}}_{{sourceSystem}}
└── module.toml
```

## Data Flow

```
Relational Database (PostgreSQL / MSSQL / Oracle / MySQL / …)
      │
      ▼  (ODBC + SQL queries)
DB Extractor
      │
      └── Query result rows ──────────► RAW: raw_table_{{location}}_{{sourceSystem}}.<query.destination.table>
```

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| ExtractionPipeline | `ep_table_{{location}}_{{sourceSystem}}` | Pipeline health tracking and config delivery |
| RAW Database | `raw_table_{{location}}_{{sourceSystem}}` | Landing zone for query result rows |
| DM Space | `{{instanceSpace}}` | Per-extractor instance space for DM instances |
| Access Group | `producer_{{location}}_ep_db_{{sourceSystem}}_{{environment}}` | Scoped service-principal group — one per extractor type × source system |

Names follow the [CDF resource naming conventions](https://docs.cognite.com/cdf/deploy/reference/cdf_resource_naming_conventions):
pipelines use `ep_{data_type}_{location}_{source}` and access groups use the persona-led
pattern `producer_[{site}_]ep_{extractortype}_{sourcesystem}_{environment}`.

## Configuration

All variables are declared locally in `config.<env>.yaml` (no inheritance):

```yaml
variables:
  modules:
    cdf_db_extractor:
      location: "oslo"                                        # Site code, used in externalIds (ep_table_<location>_<sourceSystem>, raw_table_<location>_<sourceSystem>)
      sourceSystem: "postgres"                                # Source system token, used in the pipeline ID and group name
      instanceSpace: "sp_oslo_db"                            # Per-extractor DM instance space — computed by setup_project.py
      dataset: "ds_db_oslo"                                   # ds_<data_type>_<location> — computed by setup_project.py

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
   `raw_table_{{location}}_{{sourceSystem}}` so rows land in the database declared by this
   module.
5. **If targeting a different DB engine**, set `sourceSystem` (e.g. `mssql`) so the
   pipeline becomes `ep_table_{{location}}_mssql` and the group
   `producer_{{location}}_ep_db_mssql_{{environment}}`.

See `.cursor/rules/cdf-transformations.mdc` for AI-assisted guidance when
authoring the downstream transformation from RAW into a data model.

## Getting Started

### Prerequisites

- Source database reachable from the extractor host with appropriate ODBC driver installed
- DB Extractor service account with read access to the source database
- Cognite service account with read/write to the `raw_table_{{location}}_{{sourceSystem}}` RAW database and read access to the `{{dataset}}` data set (`ds_db_{{location}}`)

### Deploy

```bash
cdf deploy modules/sourcesystem/cdf_db_extractor --env your-environment
```

### Configure and run the extractor

The extractor config is delivered via the `ep_table_{{location}}_{{sourceSystem}}` extraction pipeline in CDF. Set the environment variables on the extractor host and start the extractor — it will pull its config from CDF automatically.

### Migrating from earlier versions

The pipeline external ID changed from `ep_{{location}}_db_postgres` to
`ep_table_{{location}}_{{sourceSystem}}`, and the access group from
`producer_{{location}}_ep_db_{{environment}}` to
`producer_{{location}}_ep_db_{{sourceSystem}}_{{environment}}`. To upgrade an existing
deployment:

1. Add `sourceSystem` to `cdf_db_extractor` in each `config.<env>.yaml`.
2. Run `cdf deploy`. This creates the new pipeline and group alongside the old ones.
3. Update `extraction-pipeline.pipeline-id` in the extractor host's local `config.yaml`,
   then restart the extractor.
4. Once the extractor reports runs on the new pipeline, delete the old pipeline and group
   in Fusion. Toolkit does not remove them for you, and the new group keeps the same
   `sourceId`, so no IdP change is needed.

The RAW database also changed from `db_{{location}}_db_postgres` to
`raw_table_{{location}}_{{sourceSystem}}`. Rows already in the old database are not
moved: either let the extractor re-extract from `initial-start`, or copy the tables across
before deleting the old database. Point any downstream transformations at the new name.

The data set changed from `ds_db_postgres_{{location}}` to `ds_db_{{location}}` so it no
longer hard-codes the database engine (`sourceSystem` now carries it).
`setup_project.py` writes the new ID on its next run but keeps the old one in
`cdf_project_foundation`'s `dataset` list; remove it there once nothing references it.

### Verify

Check that the configured RAW table(s) under `raw_table_{{location}}_{{sourceSystem}}` are populated in CDF Data Explorer.
