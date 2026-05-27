from fn_etl_stream_save.handler import etl_handle_save_stream


def save_stream(fn_external_id, data, client, log):
    return etl_handle_save_stream(fn_external_id, data, client, log)
