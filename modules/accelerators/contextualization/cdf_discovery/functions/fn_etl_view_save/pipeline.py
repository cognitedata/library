from fn_etl_view_save.handler import etl_handle_save_view


def save_view(fn_external_id, data, client, log):
    return etl_handle_save_view(fn_external_id, data, client, log)
