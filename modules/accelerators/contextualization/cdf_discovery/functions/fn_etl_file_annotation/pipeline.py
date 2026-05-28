from fn_etl_file_annotation.handler import etl_handle_file_annotation


def file_annotation(fn_external_id, data, client, log):
    return etl_handle_file_annotation(fn_external_id, data, client, log)
