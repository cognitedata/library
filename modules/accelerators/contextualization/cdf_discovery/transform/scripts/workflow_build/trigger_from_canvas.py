"""Read workflow trigger settings from the canvas ``start`` node."""

from __future__ import annotations

from typing import Any, Dict, Mapping, MutableMapping


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "false", "0", "no", "off"}
    return bool(value)


def _start_node_config(canvas: Mapping[str, Any]) -> Dict[str, Any]:
    for n in canvas.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        if str(n.get("kind") or "").strip() != "start":
            continue
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        return dict(cfg)
    return {}


def read_start_trigger_config(
    canvas: Mapping[str, Any],
    *,
    default_cron: str = "0 2 * * *",
) -> Dict[str, Any]:
    cfg = _start_node_config(canvas)
    trigger_type = str(cfg.get("trigger_type") or "schedule").strip() or "schedule"
    cron = str(cfg.get("cron_expression") or default_cron).strip()
    rule: Dict[str, Any] = {"triggerType": trigger_type}
    if trigger_type == "schedule":
        rule["cronExpression"] = cron or default_cron
    elif trigger_type == "dataModeling":
        batch_size = cfg.get("batch_size")
        if batch_size is not None:
            try:
                rule["batchSize"] = max(1, min(int(batch_size), 1000))
            except (TypeError, ValueError):
                pass
        batch_timeout = cfg.get("batch_timeout")
        if batch_timeout is not None:
            try:
                rule["batchTimeout"] = max(10, min(int(batch_timeout), 86400))
            except (TypeError, ValueError):
                pass
        dm_query = cfg.get("data_modeling_query")
        if isinstance(dm_query, dict):
            rule["dataModelingQuery"] = dict(dm_query)
    elif trigger_type == "recordStream":
        stream_ext = str(cfg.get("stream_external_id") or "").strip()
        if stream_ext:
            rule["streamExternalId"] = stream_ext
        batch_size = cfg.get("batch_size")
        if batch_size is not None:
            try:
                rule["batchSize"] = max(1, min(int(batch_size), 1000))
            except (TypeError, ValueError):
                pass
        batch_timeout = cfg.get("batch_timeout")
        if batch_timeout is not None:
            try:
                rule["batchTimeout"] = max(10, min(int(batch_timeout), 86400))
            except (TypeError, ValueError):
                pass
        if isinstance(cfg.get("filter"), dict):
            rule["filter"] = cfg.get("filter")
        if isinstance(cfg.get("sources"), list) and cfg.get("sources"):
            rule["sources"] = cfg.get("sources")
    extra = cfg.get("trigger_rule")
    if isinstance(extra, dict):
        for k, v in extra.items():
            if k == "triggerType":
                continue
            rule[k] = v
    return {
        "workflow_version": str(cfg.get("workflow_version") or "1"),
        "trigger_rule": rule,
        "incremental_change_processing": _as_bool(cfg.get("incremental_change_processing", True)),
    }


def apply_start_trigger_to_workflow_trigger(
    trigger_doc: MutableMapping[str, Any],
    *,
    workflow_external_id: str,
    trigger_cfg: Mapping[str, Any],
) -> None:
    from workflow_build.ids import workflow_trigger_external_id

    wf_ext = str(workflow_external_id).strip()
    trigger_doc["externalId"] = workflow_trigger_external_id(wf_ext)
    trigger_doc["workflowExternalId"] = wf_ext
    trigger_doc["workflowVersion"] = str(trigger_cfg.get("workflow_version") or "1")
    rule = trigger_cfg.get("trigger_rule")
    if isinstance(rule, dict) and rule:
        trigger_doc["triggerRule"] = dict(rule)
    inp = trigger_doc.get("input")
    if isinstance(inp, dict):
        if "incremental_change_processing" not in inp:
            inp["incremental_change_processing"] = _as_bool(
                trigger_cfg.get("incremental_change_processing", True)
            )
