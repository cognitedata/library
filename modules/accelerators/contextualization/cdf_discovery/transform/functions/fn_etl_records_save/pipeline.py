from fn_etl_records_save.handler import etl_handle_save_records


def save_records(fn_external_id, data, client, log):
    return etl_handle_save_records(fn_external_id, data, client, log)
