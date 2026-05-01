"""Resolve v1 scope mapping from workflow payload (``configuration`` on task data; legacy ``scope_document``)."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set

from .aliasing_rule_refs import resolve_aliasing_pipeline_refs_in_scope_document
from .confidence_match_rule_refs import resolve_confidence_match_rule_refs_in_scope_document

# Task ``data`` keys that may carry the v1 scope mapping (workflow.input.configuration vs legacy).
_TASK_DATA_SCOPE_KEYS: tuple[str, str] = ("configuration", "scope_document")


def materialize_scope_confidence_refs_on_task_data(data: MutableMapping[str, Any]) -> None:
    """Expand confidence-match and extraction aliasing pipeline refs on task ``configuration`` / ``scope_document``.

    Mutates *data* in place so workflow and function handlers share one resolved v1 document
    (string refs, ``sequence:`` entries, and ``confidence_match_rule_targets`` become concrete
    rule dicts; ``aliasing_rule_definitions`` / ``aliasing_rule_sequences`` expand
    ``extraction_rules[].aliasing_pipeline``). Safe to call more than once on the same mapping.
    """
    for key in _TASK_DATA_SCOPE_KEYS:
        raw = data.get(key)
        if isinstance(raw, dict) and raw:
            doc = copy.deepcopy(raw)
            resolve_confidence_match_rule_refs_in_scope_document(doc)
            resolve_aliasing_pipeline_refs_in_scope_document(doc)
            data[key] = doc


from .reference_index_naming import reference_index_raw_table_from_key_extraction_table


def _workflow_v1_from_task_data(data: Mapping[str, Any]) -> Dict[str, Any]:
    """Read v1 scope mapping from task ``data`` (``configuration`` preferred; ``scope_document`` legacy).

    Callers must run :func:`materialize_scope_confidence_refs_on_task_data` on *data* first when
    the task may carry ``validation_rule_definitions`` / ``sequence`` indirections.
    """
    for key in _TASK_DATA_SCOPE_KEYS:
        raw = data.get(key)
        if isinstance(raw, dict) and raw:
            return copy.deepcopy(raw)
    raise ValueError(
        "Missing non-empty 'configuration' in function data (v4 workflow input); "
        "CDF functions expect the v1 scope mapping from workflow.input.configuration "
        "(legacy key 'scope_document' is still accepted)."
    )


def resolve_scope_document_source_views(doc: Dict[str, Any]) -> List[Any]:
    """Return a deep copy of the document root ``source_views`` list (required, non-empty)."""
    raw = doc.get("source_views")
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "scope document must define a non-empty top-level source_views list"
        )
    return copy.deepcopy(raw)


def _space_from_filter_values(vals: Any) -> Optional[str]:
    if vals is None:
        return None
    if isinstance(vals, list):
        if len(vals) != 1 or vals[0] is None:
            return None
        s = str(vals[0]).strip()
        return s or None
    s = str(vals).strip()
    return s or None


def resolve_instance_space_from_scope_document(doc: Dict[str, Any]) -> str:
    """Infer DM instance space from top-level ``source_views`` (field or node space filter)."""
    views = resolve_scope_document_source_views(doc)
    for v in views:
        if not isinstance(v, dict):
            continue
        ins = v.get("instance_space")
        if isinstance(ins, str) and ins.strip():
            return ins.strip()
        for f in v.get("filters") or []:
            if str(f.get("property_scope", "view")).lower() != "node":
                continue
            if f.get("target_property") != "space":
                continue
            op = str(f.get("operator", "")).upper()
            vals = f.get("values")
            if op == "EQUALS":
                s = _space_from_filter_values(vals)
                if s:
                    return s
            if op == "IN" and isinstance(vals, list) and len(vals) == 1:
                s = _space_from_filter_values(vals)
                if s:
                    return s
    raise ValueError(
        "Cannot derive instance_space from configuration: set "
        "source_views[].instance_space or add a node "
        "space filter (EQUALS with one value, or IN with one value)"
    )


def ensure_instance_space_from_scope_document(
    data: MutableMapping[str, Any],
    doc: Optional[Dict[str, Any]] = None,
) -> str:
    """Use ``data['instance_space']`` if set; otherwise resolve from v1 configuration and set on ``data``."""
    raw = data.get("instance_space")
    if raw is not None and str(raw).strip():
        space = str(raw).strip()
        data["instance_space"] = space
        return space
    if doc is None:
        if isinstance(data, MutableMapping):
            materialize_scope_confidence_refs_on_task_data(data)
        doc = _workflow_v1_from_task_data(data)
    space = resolve_instance_space_from_scope_document(doc)
    data["instance_space"] = space
    return space


def _view_has_node_space_filter(view: Mapping[str, Any]) -> bool:
    for f in view.get("filters") or []:
        if str(f.get("property_scope", "view")).lower() != "node":
            continue
        if f.get("target_property") != "space":
            continue
        op = str(f.get("operator", "")).upper()
        vals = f.get("values")
        if op == "EQUALS" and _space_from_filter_values(vals):
            return True
        if op == "IN" and isinstance(vals, list) and _space_from_filter_values(vals):
            return True
    return False


def _merge_instance_space_into_source_views(inner: MutableMapping[str, Any], instance_space: str) -> None:
    data = inner.get("data")
    if not isinstance(data, dict):
        return
    views = data.get("source_views")
    if not isinstance(views, list):
        return
    for v in views:
        if not isinstance(v, dict):
            continue
        # Preserve per-view restrictions when explicitly configured.
        current = v.get("instance_space")
        if isinstance(current, str) and current.strip():
            continue
        if _view_has_node_space_filter(v):
            continue
        v["instance_space"] = instance_space


def build_key_extraction_workflow_config(
    doc: Dict[str, Any],
    *,
    instance_space: str,
    incremental_change_processing: bool,
    run_all: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return ``{externalId, config}`` for key-extraction / incremental handlers.

    ``raw_table_key`` and default ``run_all`` come from ``key_extraction.config.parameters``
    in the scope document. When ``run_all`` is not ``None``, it overrides the document value.
    """
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        raise ValueError("scope document missing key_extraction")
    inner = copy.deepcopy(ke.get("config"))
    if not isinstance(inner, dict):
        raise ValueError("scope document missing key_extraction.config")
    params = inner.setdefault("parameters", {})
    if not isinstance(params, dict):
        inner["parameters"] = {}
        params = inner["parameters"]
    raw_key = str(params.get("raw_table_key") or "").strip()
    if not raw_key:
        raise ValueError(
            "scope document must set key_extraction.config.parameters.raw_table_key "
            "(used as key-extraction RAW table key)"
        )
    if run_all is not None:
        params["run_all"] = bool(run_all)
    params["incremental_change_processing"] = incremental_change_processing
    data = inner.setdefault("data", {})
    if not isinstance(data, dict):
        inner["data"] = {}
        data = inner["data"]
    data["source_views"] = resolve_scope_document_source_views(doc)
    raw_assoc = doc.get("associations")
    if isinstance(raw_assoc, list):
        data["associations"] = copy.deepcopy(raw_assoc)
    _merge_instance_space_into_source_views(inner, instance_space)
    ext = ke.get("externalId")
    return {"externalId": ext, "config": inner}


def build_aliasing_workflow_config(
    doc: Dict[str, Any],
    *,
    instance_space: str,
) -> Dict[str, Any]:
    """Return ``{externalId, config}`` for fn_dm_aliasing.

    ``raw_table_aliases`` and ``raw_table_state`` come from ``aliasing.config.parameters``.
    """
    al = doc.get("aliasing")
    if not isinstance(al, dict):
        raise ValueError("scope document missing aliasing")
    inner = copy.deepcopy(al.get("config"))
    if not isinstance(inner, dict):
        raise ValueError("scope document missing aliasing.config")
    params = inner.setdefault("parameters", {})
    if not isinstance(params, dict):
        inner["parameters"] = {}
        params = inner["parameters"]
    aliases_key = str(params.get("raw_table_aliases") or "").strip()
    state_key = str(params.get("raw_table_state") or "").strip()
    if not aliases_key or not state_key:
        raise ValueError(
            "scope document must set aliasing.config.parameters.raw_table_aliases "
            "and raw_table_state"
        )
    params.setdefault("raw_db", "db_tag_aliasing")
    _merge_instance_space_into_source_views(inner, instance_space)
    ext = al.get("externalId")
    return {"externalId": ext, "config": inner}


def reference_index_raw_table_key_from_scope(ke_params: Mapping[str, Any], raw_table_key: str) -> str:
    """Resolve reference-index RAW table key from scope parameters or naming convention."""
    explicit = ke_params.get("reference_index_raw_table_key")
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    return reference_index_raw_table_from_key_extraction_table(str(raw_table_key))


def read_enable_reference_index(doc: Dict[str, Any]) -> bool:
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        return False
    cfg = ke.get("config")
    if not isinstance(cfg, dict):
        return False
    params = cfg.get("parameters")
    if not isinstance(params, dict):
        return False
    return bool(params.get("enable_reference_index", False))


def build_reference_index_config_block(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Shape expected under ``data['config']`` for fn_dm_reference_index."""
    al = doc.get("aliasing")
    if not isinstance(al, dict):
        raise ValueError("scope document missing aliasing for reference index config")
    inner = copy.deepcopy(al.get("config"))
    if not isinstance(inner, dict):
        raise ValueError("scope document missing aliasing.config")
    return {"config": inner}


def incremental_change_processing_in_task_configuration(
    data: MutableMapping[str, Any],
) -> bool:
    """Return ``key_extraction.config.parameters.incremental_change_processing`` from v1 scope on ``data``."""
    materialize_scope_confidence_refs_on_task_data(data)
    doc = _workflow_v1_from_task_data(data)
    ke = doc.get("key_extraction")
    if not isinstance(ke, dict):
        return False
    kcfg = ke.get("config")
    if not isinstance(kcfg, dict):
        return False
    params = kcfg.get("parameters")
    if not isinstance(params, dict):
        return False
    return bool(params.get("incremental_change_processing"))


def _filter_extraction_rules_on_data(data: MutableMapping[str, Any], cfg: Dict[str, Any]) -> None:
    """When ``data`` carries ``extraction_rule_names`` (inlined WorkflowVersion task), drop other rules."""
    raw = data.get("extraction_rule_names")
    if not isinstance(raw, list) or not raw:
        return
    wanted = {str(x).strip() for x in raw if x is not None and str(x).strip()}
    if not wanted:
        return
    inner = cfg.get("config")
    if not isinstance(inner, dict):
        return
    sec = inner.get("data")
    if not isinstance(sec, dict):
        return
    rules = sec.get("extraction_rules")
    if not isinstance(rules, list):
        return
    kept: List[Any] = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        rid = str(r.get("rule_id") or r.get("name") or "").strip()
        if rid and rid in wanted:
            kept.append(r)
    sec["extraction_rules"] = kept


def ensure_key_extraction_config_from_scope_dm(
    data: MutableMapping[str, Any],
    client: Any,
    *,
    incremental_change_processing: bool,
) -> None:
    """Mutate ``data`` with ``config`` from v1 ``configuration`` when ``config`` not already set."""
    del client  # unused; kept for handler signature compatibility
    materialize_scope_confidence_refs_on_task_data(data)
    existing = data.get("config")
    if isinstance(existing, dict) and existing:
        return
    doc = _workflow_v1_from_task_data(data)
    space = ensure_instance_space_from_scope_document(data, doc)
    fr_override: Optional[bool] = bool(data["run_all"]) if "run_all" in data else None
    cfg = build_key_extraction_workflow_config(
        doc,
        run_all=fr_override,
        instance_space=str(space),
        incremental_change_processing=incremental_change_processing,
    )
    _filter_extraction_rules_on_data(data, cfg)
    data["config"] = cfg
    ke = doc.get("key_extraction")
    ke_cfg = ke.get("config") if isinstance(ke, dict) else None
    ke_params = ke_cfg.get("parameters") if isinstance(ke_cfg, dict) else None
    raw_key = str(ke_params.get("raw_table_key") or "").strip() if isinstance(ke_params, dict) else ""
    if raw_key:
        data["key_extraction_raw_table_key"] = raw_key


def narrow_aliasing_engine_config_for_inline_rule_names(
    engine_cfg: Dict[str, Any],
    aliasing_rule_names: Any,
) -> Dict[str, Any]:
    """Deep-copy *engine_cfg* and keep only rules named in *aliasing_rule_names* (flat + pathways).

    Used by the local Kahn runner, which builds :class:`AliasingEngine` from pre-merged engine
    config while per-canvas task ``aliasing_rule_names`` live on task ``data`` (same as CDF
    WorkflowVersion inlining).
    """
    if not isinstance(engine_cfg, dict):
        return {}
    out = copy.deepcopy(engine_cfg)
    raw = aliasing_rule_names
    if not isinstance(raw, list) or not raw:
        return out
    wanted = {str(x).strip() for x in raw if x is not None and str(x).strip()}
    if not wanted:
        return out
    rules = out.get("rules")
    if isinstance(rules, list):
        out["rules"] = [
            r
            for r in rules
            if isinstance(r, dict) and str(r.get("name") or "").strip() in wanted
        ]
    restrict_aliasing_pathways_to_wanted_names(out, wanted)
    return out


def restrict_aliasing_pathways_to_wanted_names(
    data_section: MutableMapping[str, Any], wanted: Set[str]
) -> None:
    """Keep only pathway rule rows whose ``name`` is in *wanted*; drop empty steps / ``pathways``."""
    if not wanted:
        return
    pw = data_section.get("pathways")
    if not isinstance(pw, dict):
        return
    steps_in = pw.get("steps")
    if not isinstance(steps_in, list):
        return
    new_steps: List[Dict[str, Any]] = []
    for step in steps_in:
        if not isinstance(step, dict):
            continue
        mode = str(step.get("mode") or "sequential").strip().lower()
        if mode == "sequential":
            rules_raw = step.get("rules")
            if not isinstance(rules_raw, list):
                continue
            kept = [
                r
                for r in rules_raw
                if isinstance(r, dict) and str(r.get("name") or "").strip() in wanted
            ]
            if kept:
                out_step = dict(step)
                out_step["rules"] = kept
                new_steps.append(out_step)
        elif mode == "parallel":
            branches_in = step.get("branches")
            if not isinstance(branches_in, list):
                continue
            new_branches: List[Any] = []
            for br in branches_in:
                rules_raw: List[Any] = []
                if isinstance(br, dict):
                    rules_raw = br.get("rules") or []
                elif isinstance(br, list):
                    rules_raw = br
                if not isinstance(rules_raw, list):
                    continue
                kept_br = [
                    r
                    for r in rules_raw
                    if isinstance(r, dict) and str(r.get("name") or "").strip() in wanted
                ]
                if not kept_br:
                    continue
                if isinstance(br, dict):
                    nb = dict(br)
                    nb["rules"] = kept_br
                    new_branches.append(nb)
                else:
                    new_branches.append(kept_br)
            if new_branches:
                out_step = dict(step)
                out_step["branches"] = new_branches
                new_steps.append(out_step)
    if new_steps:
        data_section["pathways"] = {**pw, "steps": new_steps}
    else:
        data_section.pop("pathways", None)


def _filter_aliasing_rules_on_data(data: MutableMapping[str, Any], cfg: Dict[str, Any]) -> None:
    """When ``data`` carries ``aliasing_rule_names``, keep only those rules in ``config.config.data``.

    Applies to flat ``aliasing_rules`` and to ``pathways.steps`` (per canvas aliasing node /
    WorkflowVersion inlined payload), so pathway-only scopes match codegen intent.
    """
    raw = data.get("aliasing_rule_names")
    if not isinstance(raw, list) or not raw:
        return
    wanted = {str(x).strip() for x in raw if x is not None and str(x).strip()}
    if not wanted:
        return
    inner = cfg.get("config")
    if not isinstance(inner, dict):
        return
    sec = inner.get("data")
    if not isinstance(sec, dict):
        return
    rules = sec.get("aliasing_rules")
    if isinstance(rules, list):
        kept: List[Any] = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            nm = str(r.get("name") or "").strip()
            if nm and nm in wanted:
                kept.append(r)
        sec["aliasing_rules"] = kept
    restrict_aliasing_pathways_to_wanted_names(sec, wanted)


def ensure_aliasing_config_from_scope_dm(data: MutableMapping[str, Any], client: Any) -> None:
    del client
    materialize_scope_confidence_refs_on_task_data(data)
    existing = data.get("config")
    if isinstance(existing, dict) and existing:
        return
    doc = _workflow_v1_from_task_data(data)
    space = ensure_instance_space_from_scope_document(data, doc)
    cfg = build_aliasing_workflow_config(doc, instance_space=str(space))
    _filter_aliasing_rules_on_data(data, cfg)
    data["config"] = cfg
    ke = doc.get("key_extraction")
    ke_cfg = ke.get("config") if isinstance(ke, dict) else None
    ke_params = ke_cfg.get("parameters") if isinstance(ke_cfg, dict) else None
    src_raw = str(ke_params.get("raw_table_key") or "").strip() if isinstance(ke_params, dict) else ""
    if src_raw:
        data.setdefault("source_raw_table_key", src_raw)
        data.setdefault(
            "source_raw_db",
            str(ke_params.get("raw_db") or "db_key_extraction") if isinstance(ke_params, dict) else "db_key_extraction",
        )
    data.setdefault("source_instance_space", str(space))


def apply_reference_index_scope_document(
    data: MutableMapping[str, Any],
    client: Any,
) -> None:
    """Load reference-index settings from v1 ``configuration`` when present."""
    del client
    if data.get("enable_reference_index") is False:
        return
    try:
        materialize_scope_confidence_refs_on_task_data(data)
        doc = _workflow_v1_from_task_data(data)
    except ValueError:
        return
    if "enable_reference_index" not in data:
        data["enable_reference_index"] = read_enable_reference_index(doc)
    ke = doc.get("key_extraction")
    ke_cfg = ke.get("config") if isinstance(ke, dict) else None
    ke_params = ke_cfg.get("parameters") if isinstance(ke_cfg, dict) else None
    if not isinstance(ke_params, dict):
        ke_params = {}
    space = ensure_instance_space_from_scope_document(data, doc)
    raw_key = str(ke_params.get("raw_table_key") or "").strip()
    if raw_key:
        data.setdefault("source_raw_table_key", raw_key)
        data.setdefault("source_raw_db", str(ke_params.get("raw_db") or "db_key_extraction"))
        data.setdefault("source_instance_space", str(space))
        data.setdefault(
            "reference_index_raw_table",
            reference_index_raw_table_key_from_scope(ke_params, raw_key),
        )
        data.setdefault(
            "reference_index_raw_db",
            str(data.get("source_raw_db") or "db_key_extraction"),
        )
    if not data.get("enable_reference_index"):
        return
    if isinstance(data.get("config"), dict) and data["config"]:
        return
    data["config"] = build_reference_index_config_block(doc)


def ensure_alias_persistence_from_scope_dm(data: MutableMapping[str, Any], client: Any) -> None:
    """Set RAW / source table keys on ``data`` from v1 ``configuration`` when not already provided."""
    del client
    if (
        data.get("raw_table_aliases") or data.get("raw_table")
    ) and data.get("source_raw_table_key"):
        return
    try:
        materialize_scope_confidence_refs_on_task_data(data)
        doc = _workflow_v1_from_task_data(data)
    except ValueError:
        return
    space = ensure_instance_space_from_scope_document(data, doc)
    ke = doc.get("key_extraction")
    ke_cfg = ke.get("config") if isinstance(ke, dict) else None
    ke_params = ke_cfg.get("parameters") if isinstance(ke_cfg, dict) else None
    al = doc.get("aliasing")
    al_cfg = al.get("config") if isinstance(al, dict) else None
    al_params = al_cfg.get("parameters") if isinstance(al_cfg, dict) else None
    if not isinstance(ke_params, dict) or not isinstance(al_params, dict):
        raise ValueError("scope document missing key_extraction or aliasing parameters")
    src_raw = str(ke_params.get("raw_table_key") or "").strip()
    aliases = str(al_params.get("raw_table_aliases") or "").strip()
    if not src_raw or not aliases:
        raise ValueError(
            "scope document must set key_extraction.config.parameters.raw_table_key "
            "and aliasing.config.parameters.raw_table_aliases"
        )
    data.setdefault("source_raw_table_key", src_raw)
    data.setdefault("source_raw_db", str(ke_params.get("raw_db") or "db_key_extraction"))
    data.setdefault("raw_db", str(al_params.get("raw_db") or "db_tag_aliasing"))
    data.setdefault("raw_table_aliases", aliases)
    data.setdefault("raw_table", aliases)
    data.setdefault("source_instance_space", str(space))
