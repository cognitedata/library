# Records and streams (Discovery ETL)

CDF **Streams** and **Records** use dedicated REST APIs (`/api/v1/projects/{project}/streams/...`), not Transformations SQL (`POST /api/cdf/sql/run`).

## Canvas node kinds

| Kind | Function | Purpose |
|------|----------|---------|
| `query_records` | `fn_etl_records_query` | Paginated sync/filter read → cohort RAW |
| `save_records` | `fn_etl_records_save` | Ingest / upsert / delete from cohort rows |
| `save_stream` | `fn_etl_stream_save` | Create stream (`POST /streams`) |

## `query_records` config

- `stream_external_id` (required)
- `read_mode`: `sync` (default) or `filter`
- `filter`, `sources`, `batch_size` (page, max 1000), `read_limit`, `cursor`, `include_tombstones`
- Cohort rows use `RECORD_KIND=record` with `STREAM_EXTERNAL_ID`, `RECORD_SPACE`, optional `RECORD_SOURCES_JSON`

## `save_records` config

- `stream_external_id`, `write_mode`: `ingest` | `upsert` | `delete`
- `batch_size`, `save_fan_in_mode`, `save_field_policies` (mutable streams)
- Upsert/delete require a **mutable** stream

## Workflow trigger (`recordStream`)

Start node `trigger_type: recordStream` with `stream_external_id`, `batch_size` (1–1000), `batch_timeout` (10–86400), optional `filter` and `sources`. Build emits `triggerRule.streamExternalId`, etc.

## ACLs

Operator credentials need `streams:READ/WRITE` and `streamRecords:READ/WRITE` on target spaces.

## Local preview

- `POST /api/transform/records-query/preview` — read sample rows
- `POST /api/transform/records-save/preview` — validate save config (dry-run)

Discovery browse: `GET/POST /api/cdf/streams/...` (see `ui/server/records_api.py`).
