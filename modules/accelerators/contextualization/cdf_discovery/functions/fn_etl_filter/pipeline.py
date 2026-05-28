from fn_etl_filter.handler import etl_handle_filter


def filter(fn_external_id, data, client, log):
    return etl_handle_filter(fn_external_id, data, client, log)
