from __future__ import annotations

from typing import Any, Mapping, Tuple

from cdf_fn_common.etl_common import _first_nonempty
from cdf_fn_common.etl_task_runtime import find_compiled_task


def resolve_two_task_ids(
    data: Mapping[str, Any],
    *,
    left_key: str,
    right_key: str,
    error_context: str,
) -> Tuple[str, str]:
    left = _first_nonempty(data.get(left_key))
    right = _first_nonempty(data.get(right_key))
    if left and right:
        return left, right
    cw = data.get("compiled_workflow")
    tid = _first_nonempty(data.get("task_id"))
    task = find_compiled_task(cw, task_id=str(tid)) if cw and tid else None
    if isinstance(task, dict):
        payload = task.get("payload")
        if isinstance(payload, dict):
            left = left or _first_nonempty(payload.get(left_key))
            right = right or _first_nonempty(payload.get(right_key))
    if not left or not right:
        raise ValueError(f"{error_context} requires {left_key} and {right_key} in payload")
    return left, right
