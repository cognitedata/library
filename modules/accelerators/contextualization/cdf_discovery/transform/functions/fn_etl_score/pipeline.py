from fn_etl_score.handler import etl_handle_score


def score(fn_external_id, data, client, log):
    return etl_handle_score(fn_external_id, data, client, log)
