"""Deterministic hash of downstream rule-bearing task configs."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from typing import Any, Dict, List, Mapping, Sequence, Tuple

RULE_INCLUSION_EXECUTABLE_KINDS = frozenset(
    {
        "transform",
        "score",
        "merge",
        "build_index",
    }
)


def _task_list(compiled_workflow: Any) -> List[Dict[str, Any]]:
    if not isinstance(compiled_workflow, Mapping):
        return []
    raw = compiled_workflow.get("tasks")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, Mapping):
            out.append(dict(item))
    return out


def _as_dep_list(value: Any) -> Sequence[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        dep = str(item or "").strip()
        if dep:
            out.append(dep)
    return out


def _downstream_task_ids(tasks: Sequence[Mapping[str, Any]], task_id: str) -> List[str]:
    """Task ids reachable from *task_id* via reverse depends_on adjacency."""
    start = str(task_id or "").strip()
    if not start:
        return []
    children_by_parent: Dict[str, List[str]] = {}
    all_task_ids: set[str] = set()
    for task in tasks:
        tid = str(task.get("id") or "").strip()
        if not tid:
            continue
        all_task_ids.add(tid)
        for dep in _as_dep_list(task.get("depends_on")):
            children_by_parent.setdefault(dep, []).append(tid)
    if start not in all_task_ids:
        return []
    out: List[str] = []
    seen: set[str] = {start}
    queue: deque[str] = deque(children_by_parent.get(start, []))
    while queue:
        tid = queue.popleft()
        if tid in seen:
            continue
        seen.add(tid)
        out.append(tid)
        for child in children_by_parent.get(tid, []):
            if child not in seen:
                queue.append(child)
    return out


def compute_rule_inclusion_hash(
    compiled_workflow: Any,
    *,
    task_id: str,
) -> Tuple[str, List[str]]:
    """
    Hash downstream rule-bearing configs for one query task.

    Returns ``(rule_inclusion_hash, included_task_ids)`` where task ids are sorted.
    """
    tasks = _task_list(compiled_workflow)
    if not tasks:
        return "", []

    by_id = {str(t.get("id") or "").strip(): t for t in tasks if str(t.get("id") or "").strip()}
    downstream_ids = _downstream_task_ids(tasks, task_id)
    if not downstream_ids:
        return "", []

    included: List[Dict[str, Any]] = []
    included_task_ids: List[str] = []
    for tid in sorted(downstream_ids):
        task = by_id.get(tid)
        if not task:
            continue
        executable_kind = str(task.get("executable_kind") or "").strip()
        if executable_kind not in RULE_INCLUSION_EXECUTABLE_KINDS:
            continue
        payload = task.get("payload")
        cfg = payload.get("config") if isinstance(payload, Mapping) else None
        included_task_ids.append(tid)
        included.append(
            {
                "id": tid,
                "executable_kind": executable_kind,
                "config": cfg,
            }
        )
    if not included:
        return "", []

    canonical = {
        "hash_version": 1,
        "query_task_id": str(task_id or "").strip(),
        "tasks": included,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()
    return digest, included_task_ids

