"""Resolve dual cohort inputs for fan-out planner profiles."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


def input_a_task_id(data: Mapping[str, Any]) -> str:
    tids = input_a_task_ids(data)
    return tids[0] if tids else ""


def input_a_task_ids(data: Mapping[str, Any]) -> List[str]:
    tids: List[str] = []
    seen: set[str] = set()

    def _add(raw: Any) -> None:
        s = str(raw or "").strip()
        if not s or s in seen:
            return
        seen.add(s)
        tids.append(s)

    def _add_list(raw: Any) -> None:
        if raw is None:
            return
        if isinstance(raw, str):
            for part in raw.replace(";", ",").split(","):
                _add(part.strip())
            return
        if isinstance(raw, list):
            for item in raw:
                _add(item)

    _add(task_id_from_data(data, "input_a_task_id"))
    _add(task_id_from_data(data, "entities_input_task_id"))
    _add_list(data.get("input_a_task_ids"))
    _add_list(data.get("entities_input_task_ids"))
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    if isinstance(cfg, Mapping):
        _add_list(cfg.get("input_a_task_ids"))
        _add_list(cfg.get("entities_input_task_ids"))
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    if isinstance(payload, Mapping):
        _add_list(payload.get("input_a_task_ids"))
        _add_list(payload.get("entities_input_task_ids"))
    return tids


def input_b_task_id(data: Mapping[str, Any]) -> str:
    return task_id_from_data(data, "input_b_task_id") or task_id_from_data(
        data, "files_input_task_id"
    )


def load_input_a_rows(client: Any, data: Mapping[str, Any]) -> List[Dict[str, Any]]:
    tids = input_a_task_ids(data)
    if not tids:
        raise ValueError(
            "workflow_fanout_plan: wire in__input_a (search context cohort) or set input_a_task_id"
        )
    rows: List[Dict[str, Any]] = []
    for tid in tids:
        rows.extend(predecessor_cohort_rows(client, data, tid))
    return rows


def load_input_b_rows(client: Any, data: Mapping[str, Any]) -> List[Dict[str, Any]]:
    tid = input_b_task_id(data)
    if not tid:
        return []
    return predecessor_cohort_rows(client, data, tid)
