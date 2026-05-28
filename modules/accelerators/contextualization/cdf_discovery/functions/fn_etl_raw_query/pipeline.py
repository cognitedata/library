from fn_etl_raw_query.handler import etl_handle_query_raw


def query_raw(fn_external_id, data, client, log):
    return etl_handle_query_raw(fn_external_id, data, client, log)
