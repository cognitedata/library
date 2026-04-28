"""Build ``compiled_workflow`` IR from a v1-style scope document (root-level source_views, key_extraction, aliasing)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, MutableMapping, Optional, Tuple

from ..reference_index_naming import reference_index_raw_table_from_key_extraction_table

COMPILED_WORKFLOW_SCHEMA_VERSION = 1

# Stable task ids (must match WorkflowVersion task externalIds and codegen).
TASK_INCREMENTAL = "kea__incremental_state"
TASK_KEY_EXTRACTION = "kea__key_extraction"
TASK_REFERENCE_INDEX = "kea__reference_index"
TASK_ALIASING = "kea__aliasing"
TASK_ALIAS_PERSISTENCE = "kea__alias_persistence"


def _ke_config(doc: Dict[str, Any]) -> Dict[str, Any]:
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        return {}
    cfg = ke.get("config")
    return cfg if isinstance(cfg, dict) else {}


def _ke_parameters(doc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _ke_config(doc)
    p = cfg.get("parameters")
    return p if isinstance(p, dict) else {}


def _aliasing_config(doc: Dict[str, Any]) -> Dict[str, Any]:
    al = doc.get("aliasing")
    if not isinstance(al, dict):
        return {}
    cfg = al.get("config")
    return cfg if isinstance(cfg, dict) else {}


def _aliasing_parameters(doc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _aliasing_config(doc)
    p = cfg.get("parameters")
    return p if isinstance(p, dict) else {}


def _first_source_view(doc: Dict[str, Any]) -> Dict[str, Any]:
    svs = doc.get("source_views")
    if isinstance(svs, list) and svs and isinstance(svs[0], dict):
        return svs[0]
    return {}


def _default_alias_persistence_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Flattened keys for fn_dm_alias_persistence / merge into handler ``data``."""
    kep = _ke_parameters(doc)
    alp = _aliasing_parameters(doc)
    v0 = _first_source_view(doc)
    write_fk = bool(alp.get("write_foreign_key_references", False))
    fk_prop = str(alp.get("foreign_key_writeback_property") or "references_found")
    return {
        "raw_db": str(alp.get("raw_db") or "db_tag_aliasing"),
        "raw_table_aliases": str(alp.get("raw_table_aliases") or ""),
        "raw_read_limit": int(alp.get("raw_read_limit") or 10000),
        "source_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "source_raw_table_key": str(kep.get("raw_table_key") or ""),
        "source_raw_read_limit": int(kep.get("raw_read_limit") or 10000),
        "incremental_auto_run_id": bool(alp.get("incremental_auto_run_id", True)),
        "incremental_transition": bool(alp.get("incremental_transition", True)),
        "source_view_space": str(v0.get("view_space") or "cdf_cdm"),
        "source_view_external_id": str(v0.get("view_external_id") or "CogniteFile"),
        "source_view_version": str(v0.get("view_version") or "v1"),
        "write_foreign_key_references": write_fk,
        "foreign_key_writeback_property": fk_prop,
    }


def _default_reference_index_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    kep = _ke_parameters(doc)
    v0 = _first_source_view(doc)
    rtk = str(kep.get("raw_table_key") or "")
    ref_table = reference_index_raw_table_from_key_extraction_table(rtk) if rtk else ""
    return {
        "source_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "source_raw_table_key": rtk,
        "source_raw_read_limit": int(kep.get("raw_read_limit") or 10000),
        "incremental_auto_run_id": True,
        "reference_index_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "reference_index_raw_table": ref_table,
        "source_view_space": str(v0.get("view_space") or "cdf_cdm"),
        "source_view_external_id": str(v0.get("view_external_id") or "CogniteFile"),
        "source_view_version": str(v0.get("view_version") or "v1"),
        "reference_index_fk_entity_type": str(kep.get("reference_index_fk_entity_type") or "asset"),
        "reference_index_document_entity_type": str(
            kep.get("reference_index_document_entity_type") or "file"
        ),
        "enable_reference_index": bool(kep.get("enable_reference_index", False)),
    }


def _default_aliasing_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    kep = _ke_parameters(doc)
    alp = _aliasing_parameters(doc)
    v0 = _first_source_view(doc)
    return {
        "source_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "source_raw_read_limit": int(kep.get("raw_read_limit") or 10000),
        "incremental_auto_run_id": bool(alp.get("incremental_auto_run_id", True)),
        "incremental_transition": bool(alp.get("incremental_transition", True)),
        "source_view_space": str(v0.get("view_space") or "cdf_cdm"),
        "source_view_external_id": str(v0.get("view_external_id") or "CogniteFile"),
        "source_view_version": str(v0.get("view_version") or "v1"),
        "source_entity_type": str(alp.get("source_entity_type") or "file"),
    }


def compile_legacy_configuration(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a JSON-serializable ``compiled_workflow`` dict for workflow.input.

    Tasks mirror the historical macro DAG; each task carries ``persistence`` / ``payload``
    slices merged into function ``data`` at runtime (see ``cdf_fn_common.task_runtime``).

    The incremental task is **always** present so WorkflowVersion / execution graphs stay stable;
    ``fn_dm_incremental_state_update`` no-ops when ``incremental_change_processing`` is false.
    """
    if not isinstance(doc, dict):
        raise ValueError("compile_legacy_configuration expects a mapping")

    tasks: List[Dict[str, Any]] = []
    channels: List[Dict[str, Any]] = []

    def add_channel(frm: str, to: str, name: str) -> None:
        channels.append({"from": frm, "to": to, "channel": name})

    tasks.append(
        {
            "id": TASK_INCREMENTAL,
            "function_external_id": "fn_dm_incremental_state_update",
            "executor_kind": "incremental_state",
            "depends_on": [],
            "pipeline_node_id": TASK_INCREMENTAL,
            "payload": {},
        }
    )
    tasks.append(
        {
            "id": TASK_KEY_EXTRACTION,
            "function_external_id": "fn_dm_key_extraction",
            "executor_kind": "key_extraction",
            "depends_on": [TASK_INCREMENTAL],
            "pipeline_node_id": TASK_KEY_EXTRACTION,
            "payload": {},
        }
    )
    add_channel(TASK_INCREMENTAL, TASK_KEY_EXTRACTION, "cohort_raw_and_run_id")

    ref_payload = _default_reference_index_payload(doc)
    tasks.append(
        {
            "id": TASK_REFERENCE_INDEX,
            "function_external_id": "fn_dm_reference_index",
            "executor_kind": "reference_index",
            "depends_on": [TASK_KEY_EXTRACTION],
            "pipeline_node_id": TASK_REFERENCE_INDEX,
            "persistence": ref_payload,
            "payload": {},
        }
    )
    add_channel(TASK_KEY_EXTRACTION, TASK_REFERENCE_INDEX, "key_extraction_raw_fk_doc_json")

    alias_payload = _default_aliasing_payload(doc)
    tasks.append(
        {
            "id": TASK_ALIASING,
            "function_external_id": "fn_dm_aliasing",
            "executor_kind": "aliasing",
            "depends_on": [TASK_KEY_EXTRACTION],
            "pipeline_node_id": TASK_ALIASING,
            "persistence": alias_payload,
            "payload": {},
        }
    )
    add_channel(TASK_KEY_EXTRACTION, TASK_ALIASING, "key_extraction_raw_candidate_keys")

    ap_payload = _default_alias_persistence_payload(doc)
    tasks.append(
        {
            "id": TASK_ALIAS_PERSISTENCE,
            "function_external_id": "fn_dm_alias_persistence",
            "executor_kind": "alias_persistence",
            "depends_on": [TASK_ALIASING],
            "pipeline_node_id": TASK_ALIAS_PERSISTENCE,
            "persistence": ap_payload,
            "payload": {},
        }
    )
    add_channel(TASK_ALIASING, TASK_ALIAS_PERSISTENCE, "tag_aliasing_raw")

    out = {
        "schemaVersion": COMPILED_WORKFLOW_SCHEMA_VERSION,
        "tasks": tasks,
        "channels": channels,
        "dag_source": "legacy",
    }
    _overlay_canvas_persistence_into_tasks(doc, out)
    return out


def _overlay_canvas_persistence_into_tasks(doc: Dict[str, Any], cw: MutableMapping[str, Any]) -> None:
    """Merge ``node.data.persistence_config`` from an optional root ``canvas`` onto IR tasks (by node kind)."""
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        return
    nodes = canvas.get("nodes")
    if not isinstance(nodes, list):
        return
    tasks = cw.get("tasks")
    if not isinstance(tasks, list):
        return

    def task_by_id(tid: str) -> Optional[MutableMapping[str, Any]]:
        for t in tasks:
            if isinstance(t, dict) and str(t.get("id") or "") == tid:
                return t
        return None

    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = str(n.get("kind") or "").strip()
        data = n.get("data")
        if not isinstance(data, dict):
            continue
        pc = data.get("persistence_config")
        if not isinstance(pc, dict):
            continue
        if kind == "alias_persistence" and str(pc.get("kind") or "") == "alias_persistence":
            t = task_by_id(TASK_ALIAS_PERSISTENCE)
            if not t:
                continue
            base = dict(t.get("persistence") or {}) if isinstance(t.get("persistence"), dict) else {}
            for k, v in pc.items():
                if k in ("kind", "profile"):
                    continue
                if v is not None and v != "":
                    base[k] = v
            t["persistence"] = base
        if kind == "reference_index" and str(pc.get("kind") or "") == "reference_index":
            t = task_by_id(TASK_REFERENCE_INDEX)
            if not t:
                continue
            base = dict(t.get("persistence") or {}) if isinstance(t.get("persistence"), dict) else {}
            for k, v in pc.items():
                if k in ("kind", "profile"):
                    continue
                if v is not None and v != "":
                    base[k] = v
            t["persistence"] = base


def compiled_workflow_signature(cw: Dict[str, Any]) -> Tuple[str, ...]:
    """Ordered structural signature for verifying leaves share the same compiled graph shape."""
    tasks = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks, list):
        return ()
    sig: List[str] = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("id") or "")
        fn = str(t.get("function_external_id") or "")
        deps = t.get("depends_on")
        ds = tuple(sorted(str(x) for x in deps)) if isinstance(deps, list) else ()
        sig.append(f"{tid}|{fn}|{ds}")
    return tuple(sig)


