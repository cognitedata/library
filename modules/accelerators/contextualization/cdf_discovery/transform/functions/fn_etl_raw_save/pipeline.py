from fn_etl_raw_save.handler import etl_handle_save_raw


def save_raw(fn_external_id, data, client, log):
    return etl_handle_save_raw(fn_external_id, data, client, log)
