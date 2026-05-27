from fn_etl_records_query.handler import etl_handle_query_records


def query_records(fn_external_id, data, client, log):
    return etl_handle_query_records(fn_external_id, data, client, log)
