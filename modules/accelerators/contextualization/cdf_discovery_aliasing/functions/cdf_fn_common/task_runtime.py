"""Merge ``compiled_workflow`` task slices into Cognite function handler ``data`` (v5 workflows)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, MutableMapping, Optional


def _tasks_list(compiled_workflow: Any) -> Optional[List[Any]]:
    if not isinstance(compiled_workflow, dict):
        return None
    raw = compiled_workflow.get("tasks")
    return raw if isinstance(raw, list) else None


def find_compiled_task(
    compiled_workflow: Any, *, task_id: str
) -> Optional[Dict[str, Any]]:
    """Return the task dict for ``task_id``, or None."""
    for t in _tasks_list(compiled_workflow) or []:
        if isinstance(t, dict) and str(t.get("id") or "") == str(task_id):
            return t
    return None


def _merge_slot_empty(value: Any) -> bool:
    """True when *value* should be replaced by compiled-task ``persistence`` / ``payload``."""
    if value is None or value == "":
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    return False


def merge_compiled_task_into_data(data: MutableMapping[str, Any]) -> None:
    """
    If ``task_id`` and ``compiled_workflow`` are present, merge ``persistence`` / ``payload`` / ``pipeline_node_id``.

    Mutates *data* in place. Safe to call when keys are absent (no-op).
    """
    tid = data.get("task_id")
    cw = data.get("compiled_workflow")
    if not tid or cw is None:
        return
    task = find_compiled_task(cw, task_id=str(tid))
    if not isinstance(task, dict):
        return

    pers = task.get("persistence")
    if isinstance(pers, dict):
        for k, v in pers.items():
            if k not in data or _merge_slot_empty(data.get(k)):
                data[k] = copy.deepcopy(v) if isinstance(v, (dict, list)) else v

    payload = task.get("payload")
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k not in data or _merge_slot_empty(data.get(k)):
                data[k] = copy.deepcopy(v) if isinstance(v, (dict, list)) else v

    pnode = task.get("pipeline_node_id")
    if pnode is not None and str(pnode).strip():
        data["pipeline_node_id"] = str(pnode).strip()
