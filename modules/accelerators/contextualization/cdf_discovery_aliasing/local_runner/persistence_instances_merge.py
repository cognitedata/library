"""Merge cohort rows from all persistence task snapshots into one instances map."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cdf_fn_common.property_merge import (
    FieldPolicy,
    MergeListOptions,
    STRATEGY_MERGE_LIST,
    merge_property_dicts,
)

from .persistence_cohort_snapshot import PERSISTENCE_SNAPSHOT_FUNCTIONS

PERSISTENCE_INSTANCES_MERGE_SCHEMA_VERSION = 1

_LIST_MERGE_PROPERTIES = frozenset({"aliases", "indexKey"})


def _default_list_merge_policies() -> Dict[str, FieldPolicy]:
    out: Dict[str, FieldPolicy] = {}
    for prop in _LIST_MERGE_PROPERTIES:
        out[prop] = FieldPolicy(
            property_name=prop,
            strategy=STRATEGY_MERGE_LIST,
            merge_list=MergeListOptions(unique=True, branch_order="by_dependency"),
        )
    return out


def _props_from_columns(cols: Mapping[str, Any]) -> Dict[str, Any]:
    raw = cols.get("PROPERTIES_JSON")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _instance_key_from_columns(cols: Mapping[str, Any], *, row_key: str = "") -> str:
    nid = str(cols.get("NODE_INSTANCE_ID") or "").strip()
    if nid:
        return nid
    scope = str(cols.get("SCOPE_KEY") or "").strip()
    ext = str(cols.get("EXTERNAL_ID") or "").strip()
    if scope and ext:
        return f"{scope}:{ext}"
    if ext:
        return ext
    return str(row_key or "").strip() or "unknown"


def _contribution(
    *,
    task_id: str,
    function_external_id: str,
    source: str,
    row_key: str,
    columns: Mapping[str, Any],
    properties: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "task_id": task_id,
        "function_external_id": function_external_id,
        "source": source,
        "row_key": row_key,
        "properties": dict(properties),
        "columns": dict(columns),
    }


def _ingest_cohort_rows(
    acc: MutableMapping[str, Dict[str, Any]],
    rows: Any,
    *,
    task_id: str,
    function_external_id: str,
    source: str,
) -> int:
    if not isinstance(rows, list):
        return 0
    n = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        row_key = str(item.get("key") or "").strip()
        cols = item.get("columns") if isinstance(item.get("columns"), dict) else {}
        ikey = _instance_key_from_columns(cols, row_key=row_key)
        if not ikey or ikey == "unknown":
            continue
        props = _props_from_columns(cols)
        entry = acc.setdefault(
            ikey,
            {
                "instance_key": ikey,
                "scope_key": str(cols.get("SCOPE_KEY") or "").strip() or None,
                "node_instance_id": str(cols.get("NODE_INSTANCE_ID") or "").strip() or None,
                "external_id": str(cols.get("EXTERNAL_ID") or "").strip() or None,
                "properties_list": [],
                "contributions": [],
            },
        )
        entry["properties_list"].append(dict(props))
        entry["contributions"].append(
            _contribution(
                task_id=task_id,
                function_external_id=function_external_id,
                source=source,
                row_key=row_key,
                columns=cols,
                properties=props,
            )
        )
        n += 1
    return n


def build_merged_persistence_instances(
    handler_data_snapshots: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Union entity cohort rows from every save / inverted-index persistence snapshot.

    Instances are keyed by ``NODE_INSTANCE_ID`` (or ``scope:externalId``). Properties
    from multiple tasks are merged with ``merge_list`` for ``aliases`` and ``indexKey``.
    Inverted-index **sink** rows (lookup-key postings) are listed separately — they are
    not instance-keyed.
    """
    policies = _default_list_merge_policies()
    by_instance: Dict[str, Dict[str, Any]] = {}
    inverted_sink_rows: List[Dict[str, Any]] = []
    persistence_tasks: List[Dict[str, Any]] = []
    cohort_rows_ingested = 0

    if not isinstance(handler_data_snapshots, Mapping):
        handler_data_snapshots = {}

    for snap_key, entry in handler_data_snapshots.items():
        if not isinstance(entry, dict):
            continue
        fn_ext = str(entry.get("function_external_id") or "").strip()
        if fn_ext not in PERSISTENCE_SNAPSHOT_FUNCTIONS:
            continue
        task_id = str(entry.get("task_id") or snap_key).strip()
        summary = entry.get("handler_summary") if isinstance(entry.get("handler_summary"), dict) else {}
        persistence_tasks.append(
            {
                "task_id": task_id,
                "function_external_id": fn_ext,
                "handler_summary": dict(summary),
            }
        )
        cohort = entry.get("cohort_snapshot") if isinstance(entry.get("cohort_snapshot"), dict) else {}
        pred = cohort.get("predecessor_cohort") if isinstance(cohort.get("predecessor_cohort"), dict) else {}
        cohort_rows_ingested += _ingest_cohort_rows(
            by_instance,
            pred.get("cohort_rows"),
            task_id=task_id,
            function_external_id=fn_ext,
            source="predecessor_cohort",
        )
        inv = (
            cohort.get("inverted_index_persistence")
            if isinstance(cohort.get("inverted_index_persistence"), dict)
            else {}
        )
        for item in inv.get("index_rows") or []:
            if not isinstance(item, dict):
                continue
            inverted_sink_rows.append(
                {
                    "task_id": task_id,
                    "function_external_id": fn_ext,
                    "raw_db": inv.get("raw_db"),
                    "raw_table": inv.get("raw_table"),
                    "key": item.get("key"),
                    "columns": dict(item.get("columns") or {})
                    if isinstance(item.get("columns"), dict)
                    else {},
                }
            )

    instances_out: List[Dict[str, Any]] = []
    for ikey in sorted(by_instance.keys()):
        entry = by_instance[ikey]
        props_list = entry.pop("properties_list", [])
        merged_props = merge_property_dicts(props_list, policies) if props_list else {}
        instances_out.append(
            {
                "instance_key": entry["instance_key"],
                "scope_key": entry.get("scope_key"),
                "node_instance_id": entry.get("node_instance_id"),
                "external_id": entry.get("external_id"),
                "properties": merged_props,
                "contributions": entry.get("contributions") or [],
                "contribution_count": len(entry.get("contributions") or []),
            }
        )

    persistence_tasks.sort(key=lambda x: x["task_id"])

    return {
        "schema_version": PERSISTENCE_INSTANCES_MERGE_SCHEMA_VERSION,
        "persistence_tasks": persistence_tasks,
        "instance_count": len(instances_out),
        "cohort_rows_ingested": cohort_rows_ingested,
        "inverted_index_sink_row_count": len(inverted_sink_rows),
        "instances": instances_out,
        "inverted_index_sink_rows": inverted_sink_rows,
    }


__all__ = [
    "PERSISTENCE_INSTANCES_MERGE_SCHEMA_VERSION",
    "build_merged_persistence_instances",
]
