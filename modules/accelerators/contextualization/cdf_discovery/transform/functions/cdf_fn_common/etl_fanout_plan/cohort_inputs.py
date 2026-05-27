"""Resolve dual cohort inputs for fan-out planner profiles."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


def input_a_task_id(data: Mapping[str, Any]) -> str:
    return task_id_from_data(data, "input_a_task_id") or task_id_from_data(
        data, "entities_input_task_id"
    )


def input_b_task_id(data: Mapping[str, Any]) -> str:
    return task_id_from_data(data, "input_b_task_id") or task_id_from_data(
        data, "files_input_task_id"
    )


def load_input_a_rows(client: Any, data: Mapping[str, Any]) -> List[Dict[str, Any]]:
    tid = input_a_task_id(data)
    if not tid:
        raise ValueError(
            "workflow_fanout_plan: wire in__input_a (search context cohort) or set input_a_task_id"
        )
    return predecessor_cohort_rows(client, data, tid)


def load_input_b_rows(client: Any, data: Mapping[str, Any]) -> List[Dict[str, Any]]:
    tid = input_b_task_id(data)
    if not tid:
        return []
    return predecessor_cohort_rows(client, data, tid)
