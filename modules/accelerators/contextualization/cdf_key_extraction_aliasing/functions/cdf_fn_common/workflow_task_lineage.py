"""Resolve workflow DAG predecessor lineage for per-task data scoping."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set

FN_DM_KEY_EXTRACTION = "fn_dm_key_extraction"
RULES_USED_JSON_COLUMN = "RULES_USED_JSON"


def _tasks_by_id(compiled_workflow: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = compiled_workflow.get("tasks")
    if not isinstance(raw, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for t in raw:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "").strip()
        if tid:
            out[tid] = t
    return out


def transitive_predecessor_task_ids(
    compiled_workflow: Mapping[str, Any], task_id: str
) -> Set[str]:
    """All task ids reachable via ``depends_on`` edges from *task_id* (not including *task_id*)."""
    tasks = _tasks_by_id(compiled_workflow)
    cur = str(task_id).strip()
    if not cur or cur not in tasks:
        return set()
    seen: Set[str] = set()
    stack: List[str] = list(tasks[cur].get("depends_on") or [])
    while stack:
        p = str(stack.pop()).strip()
        if not p or p in seen:
            continue
        seen.add(p)
        pt = tasks.get(p)
        if not isinstance(pt, dict):
            continue
        for q in pt.get("depends_on") or []:
            qq = str(q).strip()
            if qq and qq not in seen:
                stack.append(qq)
    return seen


def allowed_extraction_rule_names_for_task(
    compiled_workflow: Mapping[str, Any], task_id: str
) -> Optional[Set[str]]:
    """
    Union of ``payload.extraction_rule_names`` from every transitive predecessor
    that runs ``fn_dm_key_extraction``.

    Returns:
        ``None`` when there is no restriction (legacy / missing IR / no KE predecessor).
        A non-empty set when at least one predecessor KE task contributed rule names.
        ``None`` when predecessors include KE tasks with no named rules in payload.
    """
    preds = transitive_predecessor_task_ids(compiled_workflow, task_id)
    if not preds:
        return None
    tasks = _tasks_by_id(compiled_workflow)
    names: Set[str] = set()
    saw_ke = False
    for pid in preds:
        t = tasks.get(pid)
        if not isinstance(t, dict):
            continue
        if str(t.get("function_external_id") or "").strip() != FN_DM_KEY_EXTRACTION:
            continue
        saw_ke = True
        payload = t.get("payload")
        if not isinstance(payload, dict):
            continue
        raw = payload.get("extraction_rule_names")
        if not isinstance(raw, list) or not raw:
            continue
        for x in raw:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                names.add(s)
    if not saw_ke:
        return None
    if not names:
        return None
    return names


def parse_rules_used_json(cols: Mapping[str, Any]) -> Set[str]:
    raw = cols.get(RULES_USED_JSON_COLUMN)
    if not isinstance(raw, str) or not raw.strip():
        return set()
    try:
        data = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(data, list):
        return set()
    out: Set[str] = set()
    for x in data:
        if x is None:
            continue
        s = str(x).strip()
        if s:
            out.add(s)
    return out


def _rule_ids_from_ref_json(items: Any) -> Set[str]:
    if not isinstance(items, list):
        return set()
    out: Set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        rid = item.get("rule_id")
        if rid is not None:
            s = str(rid).strip()
            if s:
                out.add(s)
    return out


def extraction_rule_attribution_from_raw_row(
    cols: Mapping[str, Any],
    fk_list: Optional[List[Any]] = None,
    doc_list: Optional[List[Any]] = None,
) -> Set[str]:
    """Rule names/ids attributed on a key-extraction RAW entity row (candidate + FK + doc)."""
    acc = set(parse_rules_used_json(cols))
    if fk_list is not None:
        acc |= _rule_ids_from_ref_json(fk_list)
    if doc_list is not None:
        acc |= _rule_ids_from_ref_json(doc_list)
    return acc


def raw_row_allowed_for_predecessor_extraction_rules(
    cols: Mapping[str, Any],
    *,
    fk_list: Optional[List[Any]] = None,
    doc_list: Optional[List[Any]] = None,
    allowed: Optional[Set[str]],
) -> bool:
    """If *allowed* is ``None`` or empty, always allow. Otherwise require a non-empty intersection."""
    if not allowed:
        return True
    attr = extraction_rule_attribution_from_raw_row(cols, fk_list=fk_list, doc_list=doc_list)
    if not attr:
        return False
    return bool(attr & allowed)


def filter_entities_keys_extracted_by_rules(
    entities_keys_extracted: Mapping[str, Any], allowed: Set[str]
) -> Dict[str, Any]:
    """Drop keys / FK / doc entries whose rule is not in *allowed*; omit entities left empty."""
    out: Dict[str, Any] = {}
    for eid, meta in entities_keys_extracted.items():
        if not isinstance(meta, dict):
            continue
        keys_in = meta.get("keys")
        keys_out: Dict[str, Any] = {}
        if isinstance(keys_in, dict):
            for field_name, key_values in keys_in.items():
                if not isinstance(key_values, dict):
                    continue
                kept: Dict[str, Any] = {}
                for key_value, key_info in key_values.items():
                    if not isinstance(key_info, dict):
                        continue
                    rn = key_info.get("rule_name") or key_info.get("rule_id")
                    if rn is not None and str(rn).strip() in allowed:
                        kept[key_value] = key_info
                if kept:
                    keys_out[str(field_name)] = kept

        def _filter_ref_list(items: Any) -> List[Any]:
            if not isinstance(items, list):
                return []
            acc: List[Any] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                rid = item.get("rule_id")
                if rid is not None and str(rid).strip() in allowed:
                    acc.append(item)
            return acc

        fk_out = _filter_ref_list(meta.get("foreign_key_references"))
        doc_out = _filter_ref_list(meta.get("document_references"))

        if keys_out or fk_out or doc_out:
            next_meta = dict(meta)
            next_meta["keys"] = keys_out
            next_meta["foreign_key_references"] = fk_out
            next_meta["document_references"] = doc_out
            out[str(eid)] = next_meta
    return out


def apply_predecessor_extraction_allowlist_to_task_data(data: MutableMapping[str, Any]) -> None:
    """
    When ``task_id`` + ``compiled_workflow`` resolve a non-empty predecessor extraction
    allowlist, filter ``entities_keys_extracted`` in place (when present).
    """
    cw = data.get("compiled_workflow")
    tid = data.get("task_id")
    if cw is None or not tid:
        return
    allowed = allowed_extraction_rule_names_for_task(cw, str(tid))
    if not allowed:
        return
    eke = data.get("entities_keys_extracted")
    if not isinstance(eke, dict) or not eke:
        return
    data["entities_keys_extracted"] = filter_entities_keys_extracted_by_rules(eke, allowed)
