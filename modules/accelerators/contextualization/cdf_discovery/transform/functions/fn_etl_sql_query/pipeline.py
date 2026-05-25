from fn_etl_sql_query.handler import etl_handle_query_sql


def query_sql(fn_external_id, data, client, log):
    return etl_handle_query_sql(fn_external_id, data, client, log)
