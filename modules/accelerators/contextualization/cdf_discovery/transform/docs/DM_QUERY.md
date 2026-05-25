# DM view query

ETL view query uses `query_all_view_instances` in `cdf_fn_common/etl_dm_query.py`.

- API: `client.data_modeling.instances(chunk_size=..., limit=None)` cursor pagination
- Filters: `source_view_filter_build.build_source_view_query_filter`
- Page size: `query_enumeration.resolve_page_size` (`batch_size` / `limit` aliases)

See `fn_etl_view_query/handler.py` for the Cognite Function entry point.
