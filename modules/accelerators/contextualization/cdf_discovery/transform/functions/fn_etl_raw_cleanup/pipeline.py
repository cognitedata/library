from fn_etl_raw_cleanup.handler import etl_handle_raw_cleanup


def raw_cleanup(fn_external_id, data, client, log):
    return etl_handle_raw_cleanup(fn_external_id, data, client, log)
