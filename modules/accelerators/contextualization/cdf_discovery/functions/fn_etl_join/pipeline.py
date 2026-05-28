from fn_etl_join.handler import etl_handle_join


def join(fn_external_id, data, client, log):
    return etl_handle_join(fn_external_id, data, client, log)
