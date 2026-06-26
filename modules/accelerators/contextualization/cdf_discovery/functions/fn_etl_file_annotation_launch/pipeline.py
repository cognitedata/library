from fn_etl_file_annotation_launch.handler import etl_handle_file_annotation_launch


def file_annotation_launch(fn_external_id, data, client, log):
    return etl_handle_file_annotation_launch(fn_external_id, data, client, log)
