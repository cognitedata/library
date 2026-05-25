from fn_etl_field_map.handler import etl_handle_field_map


def field_map(fn_external_id, data, client, log):
    return etl_handle_field_map(fn_external_id, data, client, log)
