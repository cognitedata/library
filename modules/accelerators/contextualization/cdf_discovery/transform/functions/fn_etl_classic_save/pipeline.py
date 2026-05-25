from fn_etl_classic_save.handler import etl_handle_save_classic


def save_classic(fn_external_id, data, client, log):
    return etl_handle_save_classic(fn_external_id, data, client, log)
