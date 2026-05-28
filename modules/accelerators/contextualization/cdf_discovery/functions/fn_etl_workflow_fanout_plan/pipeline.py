from fn_etl_workflow_fanout_plan.handler import etl_handle_workflow_fanout_plan


def workflow_fanout_plan(fn_external_id, data, client, log):
    return etl_handle_workflow_fanout_plan(fn_external_id, data, client, log)
