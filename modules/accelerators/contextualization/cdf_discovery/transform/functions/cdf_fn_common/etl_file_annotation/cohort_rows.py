"""Load predecessor cohort rows for file annotation / fan-out inputs."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_common import iter_predecessor_rows_for_task
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors


def predecessor_cohort_rows(
    client: Any,
    data: Mapping[str, Any],
    dep_task_id: str,
) -> List[Dict[str, Any]]:
    """Cohort rows from a wired predecessor task (any instance type)."""
    tid = str(dep_task_id or "").strip()
    if not tid:
        return []
    rows: List[Dict[str, Any]] = []
    if client is not None and not use_in_memory_predecessors(data):
        from cdf_fn_common.etl_discovery_cohort import iter_predecessor_instance_props

        for cols, props in iter_predecessor_instance_props(client, data, tid):
            rows.append({"columns": dict(cols), "properties": dict(props)})
        return rows
    for cols, props in iter_predecessor_rows_for_task(data, tid):
        rows.append({"columns": dict(cols), "properties": dict(props)})
    return rows


def task_id_from_data(data: Mapping[str, Any], key: str) -> str:
    raw = data.get(key)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    if isinstance(payload, dict):
        nested = payload.get(key)
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    if isinstance(cfg, dict):
        alt = cfg.get(key.replace("_task_id", "")) or cfg.get(key)
        if isinstance(alt, str) and alt.strip():
            return alt.strip()
    return ""
