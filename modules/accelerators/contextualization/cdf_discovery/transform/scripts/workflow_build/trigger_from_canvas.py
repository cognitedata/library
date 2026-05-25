"""Read workflow trigger settings from the canvas ``start`` node."""

from __future__ import annotations

from typing import Any, Dict, Mapping, MutableMapping


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
    trigger_type = (
        str(cfg.get("trigger_type") or cfg.get("triggerType") or "schedule").strip() or "schedule"
    )
    cron = str(cfg.get("cron_expression") or cfg.get("cronExpression") or default_cron).strip()
    rule: Dict[str, Any] = {"triggerType": trigger_type}
    if trigger_type == "schedule":
        rule["cronExpression"] = cron or default_cron
    extra = cfg.get("trigger_rule")
    if isinstance(extra, dict):
        for k, v in extra.items():
            rule[k] = v
    return {
        "workflow_version": str(cfg.get("workflow_version") or cfg.get("workflowVersion") or "1"),
        "trigger_rule": rule,
        "incremental_change_processing": bool(cfg.get("incremental_change_processing", True)),
        "run_id": str(cfg.get("run_id") or ""),
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
        inp["incremental_change_processing"] = bool(
            trigger_cfg.get("incremental_change_processing", True)
        )
        inp["run_id"] = str(trigger_cfg.get("run_id") or "")
