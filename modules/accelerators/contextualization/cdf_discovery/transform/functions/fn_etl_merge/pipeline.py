from fn_etl_merge.handler import etl_handle_merge


def merge(fn_external_id, data, client, log):
    return etl_handle_merge(fn_external_id, data, client, log)
