from fn_etl_transform.handler import etl_handle_transform


def transform(fn_external_id, data, client, log):
    return etl_handle_transform(fn_external_id, data, client, log)
