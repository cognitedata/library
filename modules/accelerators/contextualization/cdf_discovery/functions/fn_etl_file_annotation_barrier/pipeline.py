from fn_etl_file_annotation_barrier.handler import etl_handle_file_annotation_barrier


def file_annotation_barrier(fn_external_id, data, client, log):
    return etl_handle_file_annotation_barrier(fn_external_id, data, client, log)
