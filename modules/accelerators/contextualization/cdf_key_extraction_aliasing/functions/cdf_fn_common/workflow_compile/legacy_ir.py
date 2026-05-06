"""Shared constants and default persistence payloads for canvas DAG compilation."""

from __future__ import annotations

from typing import Any, Dict

from ..inverted_index_naming import inverted_index_raw_table_from_key_extraction_table

COMPILED_WORKFLOW_SCHEMA_VERSION = 1

# Stable task ids (must match WorkflowVersion task externalIds and codegen).
TASK_INCREMENTAL = "kea__incremental_state"
TASK_KEY_EXTRACTION = "kea__key_extraction"
TASK_INVERTED_INDEX = "kea__inverted_index"
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


def _default_inverted_index_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    kep = _ke_parameters(doc)
    v0 = _first_source_view(doc)
    rtk = str(kep.get("raw_table_key") or "")
    ref_table = inverted_index_raw_table_from_key_extraction_table(rtk) if rtk else ""
    return {
        "source_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "source_raw_table_key": rtk,
        "source_raw_read_limit": int(kep.get("raw_read_limit") or 10000),
        "incremental_auto_run_id": True,
        "inverted_index_raw_db": str(kep.get("raw_db") or "db_key_extraction"),
        "inverted_index_raw_table": ref_table,
        "source_view_space": str(v0.get("view_space") or "cdf_cdm"),
        "source_view_external_id": str(v0.get("view_external_id") or "CogniteFile"),
        "source_view_version": str(v0.get("view_version") or "v1"),
        "inverted_index_fk_entity_type": str(kep.get("inverted_index_fk_entity_type") or "asset"),
        "inverted_index_document_entity_type": str(
            kep.get("inverted_index_document_entity_type") or "file"
        ),
        "enable_inverted_index": bool(kep.get("enable_inverted_index", False)),
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


