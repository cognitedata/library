from fn_etl_build_index.handler import etl_handle_build_index


def build_index(fn_external_id, data, client, log):
    return etl_handle_build_index(fn_external_id, data, client, log)
