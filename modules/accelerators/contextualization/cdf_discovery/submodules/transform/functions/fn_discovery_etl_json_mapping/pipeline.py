from fn_discovery_etl_json_mapping.handler import etl_handle_json_mapping


def json_mapping(fn_external_id, data, client, log):
    return etl_handle_json_mapping(fn_external_id, data, client, log)
