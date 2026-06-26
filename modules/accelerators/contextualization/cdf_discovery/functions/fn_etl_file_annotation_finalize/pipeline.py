from fn_etl_file_annotation_finalize.handler import etl_handle_file_annotation_finalize


def file_annotation_finalize(fn_external_id, data, client, log):
    return etl_handle_file_annotation_finalize(fn_external_id, data, client, log)
