from fn_etl_classic_query.handler import etl_handle_query_classic


def query_classic(fn_external_id, data, client, log):
    return etl_handle_query_classic(fn_external_id, data, client, log)
