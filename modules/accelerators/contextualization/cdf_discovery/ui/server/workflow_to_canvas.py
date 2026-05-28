"""Map a deployed CDF workflow task graph to a transform pipeline canvas document."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

# Mirrors cdf_fn_common.workflow_compile.canvas_dag._KIND_SPEC (function external id → canvas kind).
_FN_TO_KIND: Dict[str, str] = {
    "fn_etl_view_query": "query_view",
    "fn_etl_raw_query": "query_raw",
    "fn_etl_classic_query": "query_classic",
    "fn_etl_sql_query": "query_sql",
    "fn_etl_records_query": "query_records",
    "fn_etl_transform": "transform",
    "fn_etl_filter": "filter",
    "fn_etl_score": "score",
    "fn_etl_join": "join",
    "fn_etl_merge": "merge",
    "fn_etl_build_index": "build_index",
    "fn_etl_file_annotation": "file_annotation",
    "fn_etl_workflow_fanout_plan": "workflow_fanout_plan",
    "fn_etl_view_save": "save_view",
    "fn_etl_raw_save": "save_raw",
    "fn_etl_classic_save": "save_classic",
    "fn_etl_records_save": "save_records",
    "fn_etl_stream_save": "save_stream",
    "fn_etl_raw_cleanup": "raw_cleanup",
}

_NODE_W = 280
_NODE_H = 100
_GAP_X = 80
_GAP_Y = 56
_START_X = 80
_BASE_Y = 200

_PIPELINE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


def suggest_pipeline_id(workflow_external_id: str) -> str:
    """Derive a registry-safe pipeline id from a CDF workflow external id."""
    raw = str(workflow_external_id or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    if slug.startswith("wf_"):
        slug = slug[3:]
    if not slug:
        slug = "workflow"
    if not slug[0].isalpha():
        slug = f"wf_{slug}"
    return slug[:128]


def _function_external_id(parameters: Mapping[str, Any]) -> str:
    fn = parameters.get("function")
    if not isinstance(fn, dict):
        return ""
    ext = fn.get("externalId") if fn.get("externalId") is not None else fn.get("external_id")
    return str(ext or "").strip()


def _canvas_node_id(task_external_id: str, taken: Set[str]) -> str:
    raw = str(task_external_id or "").strip()
    nid = re.sub(r"[^a-zA-Z0-9_-]", "_", raw).strip("_")
    if not nid or nid in ("start", "end"):
        nid = f"task_{nid or 'node'}"
    base = nid
    n = 2
    while nid in taken:
        nid = f"{base}_{n}"
        n += 1
    taken.add(nid)
    return nid


def _task_kind_and_config(task: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    task_type = str(task.get("type") or "function").strip()
    params = task.get("parameters") if isinstance(task.get("parameters"), dict) else {}
    name = str(task.get("name") or task.get("label") or task.get("external_id") or "").strip()
    description = str(task.get("description") or "").strip()
    base_label = name or str(task.get("external_id") or "")

    if task_type == "jsonMapping":
        jm = params.get("jsonMapping") if isinstance(params.get("jsonMapping"), dict) else {}
        return "json_mapping", {
            "description": description,
            "input": dict(jm.get("input") or {}) if isinstance(jm.get("input"), dict) else {},
            "expression": str(jm.get("expression") or ""),
        }

    if task_type == "transformation":
        tr = params.get("transformation") if isinstance(params.get("transformation"), dict) else {}
        ext = str(tr.get("externalId") or tr.get("external_id") or "").strip()
        return "transformation_ref", {
            "description": description,
            "transformation_external_id": ext,
        }

    if task_type == "subworkflow":
        sw = params.get("subworkflow") if isinstance(params.get("subworkflow"), dict) else {}
        return "subworkflow", {
            "description": description,
            "workflow_external_id": str(sw.get("externalId") or sw.get("external_id") or "").strip(),
            "workflow_version": str(sw.get("version") or "1").strip() or "1",
        }

    if task_type == "dynamic":
        dyn = params.get("dynamic") if isinstance(params.get("dynamic"), dict) else {}
        gen = ""
        tasks_val = dyn.get("tasks")
        if isinstance(tasks_val, str) and "." in tasks_val:
            gen = tasks_val.split(".", 1)[0].removeprefix("${").removesuffix("}")
        return "dynamic_fanout", {
            "description": description,
            "generator_task_id": gen,
        }

    if task_type == "simulation":
        sim = params.get("simulation") if isinstance(params.get("simulation"), dict) else {}
        return "simulation", {
            "description": description,
            "simulation_external_id": str(sim.get("externalId") or sim.get("external_id") or "").strip(),
        }

    if task_type == "cdf":
        cdf = params.get("cdf") if isinstance(params.get("cdf"), dict) else {}
        return "cdf_task", {"description": description, "cdf": dict(cdf)}

    fn_ext = _function_external_id(params)
    if fn_ext and fn_ext in _FN_TO_KIND:
        kind = _FN_TO_KIND[fn_ext]
        cfg: Dict[str, Any] = {"description": description}
        fn = params.get("function")
        if isinstance(fn, dict):
            data = fn.get("data")
            if isinstance(data, dict):
                nested = data.get("config")
                if isinstance(nested, dict):
                    for k, v in nested.items():
                        if k not in cfg and v is not None:
                            cfg[k] = v
        return kind, cfg

    if fn_ext:
        return "function_ref", {
            "description": description,
            "function_external_id": fn_ext,
        }

    return "cdf_task", {
        "description": description or f"Imported CDF task ({task_type or 'task'})",
        "cdf_task_type": task_type,
        "parameters": dict(params),
    }


def _layout_positions(
    node_ids: List[str],
    edges: List[Tuple[str, str]],
    *,
    start_id: str = "start",
    end_id: str = "end",
) -> Dict[str, Dict[str, float]]:
    task_ids = [n for n in node_ids if n not in (start_id, end_id)]
    preds: Dict[str, List[str]] = {n: [] for n in task_ids}
    succs: Dict[str, List[str]] = {n: [] for n in task_ids}
    for src, tgt in edges:
        if src == start_id or tgt == end_id:
            continue
        if src in preds and tgt in preds:
            preds[tgt].append(src)
            succs[src].append(tgt)

    rank: Dict[str, int] = {n: 0 for n in task_ids}
    for _ in range(len(task_ids) + 1):
        changed = False
        for n in task_ids:
            if preds[n]:
                next_rank = max(rank[p] + 1 for p in preds[n])
                if next_rank > rank[n]:
                    rank[n] = next_rank
                    changed = True
        if not changed:
            break

    by_rank: Dict[int, List[str]] = {}
    for n in task_ids:
        by_rank.setdefault(rank[n], []).append(n)
    for nodes in by_rank.values():
        nodes.sort()

    max_rank = max(rank.values(), default=0)
    positions: Dict[str, Dict[str, float]] = {
        start_id: {"x": float(_START_X), "y": float(_BASE_Y)},
        end_id: {
            "x": float(_START_X + (max_rank + 2) * (_NODE_W + _GAP_X)),
            "y": float(_BASE_Y),
        },
    }
    for r, nodes in sorted(by_rank.items()):
        for i, nid in enumerate(nodes):
            positions[nid] = {
                "x": float(_START_X + (r + 1) * (_NODE_W + _GAP_X)),
                "y": float(_BASE_Y + i * (_NODE_H + _GAP_Y)),
            }
    return positions


def _rule_get(rule: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in rule and rule[key] is not None:
            return rule[key]
    return None


def _normalize_trigger_type(raw: str) -> str:
    tt = str(raw or "").strip()
    if not tt or tt == "scheduled":
        return "schedule"
    if tt == "data_modeling":
        return "dataModeling"
    return tt


def cdf_trigger_to_start_config(
    trigger: Mapping[str, Any] | None,
    *,
    workflow_external_id: str,
    workflow_version: str,
    description: str = "",
) -> Dict[str, Any]:
    """Map a serialized CDF WorkflowTrigger to transform canvas start-node config."""
    desc = str(description or "").strip() or f"Imported from CDF workflow {workflow_external_id}"
    cfg: Dict[str, Any] = {
        "description": desc,
        "trigger_type": "schedule",
        "cron_expression": "0 2 * * *",
        "workflow_version": workflow_version or "1",
        "workflow_base": "",
        "workflow_external_id": workflow_external_id,
        "trigger_external_id": "",
        "incremental_change_processing": True,
        "run_id": "",
    }
    if not trigger:
        return cfg

    trg_ext = str(trigger.get("external_id") or "").strip()
    if trg_ext:
        cfg["trigger_external_id"] = trg_ext

    wf_ver = str(trigger.get("workflow_version") or "").strip()
    if wf_ver:
        cfg["workflow_version"] = wf_ver

    inp = trigger.get("input")
    if isinstance(inp, dict):
        if "incremental_change_processing" in inp:
            cfg["incremental_change_processing"] = bool(inp["incremental_change_processing"])
        run_id = inp.get("run_id")
        if run_id is not None:
            cfg["run_id"] = str(run_id)

    rule_raw = trigger.get("trigger_rule")
    if not isinstance(rule_raw, dict):
        return cfg

    rule = dict(rule_raw)
    tt = _normalize_trigger_type(
        str(_rule_get(rule, "trigger_type", "triggerType") or "schedule")
    )
    cfg["trigger_type"] = tt

    if tt == "schedule":
        cron = str(_rule_get(rule, "cron_expression", "cronExpression") or "").strip()
        if cron:
            cfg["cron_expression"] = cron
        cfg["trigger_rule"] = rule
    elif tt == "recordStream":
        stream = str(
            _rule_get(rule, "stream_external_id", "streamExternalId") or ""
        ).strip()
        if stream:
            cfg["stream_external_id"] = stream
        batch_size = _rule_get(rule, "batch_size", "batchSize")
        if batch_size is not None:
            cfg["batch_size"] = batch_size
        batch_timeout = _rule_get(rule, "batch_timeout", "batchTimeout")
        if batch_timeout is not None:
            cfg["batch_timeout"] = batch_timeout
        filt = rule.get("filter")
        if isinstance(filt, dict):
            cfg["filter"] = dict(filt)
        sources = rule.get("sources")
        if isinstance(sources, list):
            cfg["sources"] = list(sources)
        cfg["trigger_rule"] = rule
    elif tt == "dataModeling":
        batch_size = _rule_get(rule, "batch_size", "batchSize")
        if batch_size is not None:
            cfg["batch_size"] = batch_size
        batch_timeout = _rule_get(rule, "batch_timeout", "batchTimeout")
        if batch_timeout is not None:
            cfg["batch_timeout"] = batch_timeout
        cfg["trigger_rule"] = rule
    else:
        cfg["trigger_rule"] = rule

    return cfg


def workflow_graph_to_canvas(
    graph: Mapping[str, Any],
    *,
    workflow_external_id: str,
    workflow_trigger: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a transform canvas from a CDF workflow graph payload (cdf_browse.workflow_graph)."""
    wf_meta = graph.get("workflow") if isinstance(graph.get("workflow"), dict) else {}
    version = str(wf_meta.get("version") or "1").strip() or "1"
    tasks_in = graph.get("tasks") if isinstance(graph.get("tasks"), list) else []
    edges_in = graph.get("edges") if isinstance(graph.get("edges"), list) else []

    taken_ids: Set[str] = {"start", "end"}
    task_id_map: Dict[str, str] = {}
    canvas_nodes: List[Dict[str, Any]] = []

    start_config = cdf_trigger_to_start_config(
        workflow_trigger,
        workflow_external_id=workflow_external_id,
        workflow_version=version,
        description=str(wf_meta.get("description") or "").strip(),
    )
    start_data = {
        "label": "Workflow trigger",
        "config": start_config,
    }
    canvas_nodes.append(
        {
            "id": "start",
            "kind": "start",
            "position": {"x": _START_X, "y": _BASE_Y},
            "data": start_data,
        }
    )

    for task in tasks_in:
        if not isinstance(task, dict):
            continue
        ext = str(task.get("external_id") or task.get("id") or "").strip()
        if not ext:
            continue
        canvas_id = _canvas_node_id(ext, taken_ids)
        task_id_map[ext] = canvas_id
        kind, cfg = _task_kind_and_config(task)
        label = str(task.get("name") or task.get("label") or ext).strip() or ext
        canvas_nodes.append(
            {
                "id": canvas_id,
                "kind": kind,
                "position": {"x": 0, "y": 0},
                "data": {"label": label, "config": cfg},
            }
        )

    canvas_nodes.append(
        {
            "id": "end",
            "kind": "end",
            "position": {"x": 0, "y": 0},
            "data": {"label": "End", "config": {"description": ""}},
        }
    )

    canvas_edges: List[Dict[str, Any]] = []
    edge_pairs: List[Tuple[str, str]] = []
    task_ext_ids = set(task_id_map.keys())

    preds: Dict[str, Set[str]] = {ext: set() for ext in task_ext_ids}
    succs: Dict[str, Set[str]] = {ext: set() for ext in task_ext_ids}

    for e in edges_in:
        if not isinstance(e, dict):
            continue
        src_ext = str(e.get("from") or "").strip()
        tgt_ext = str(e.get("to") or "").strip()
        if src_ext not in task_id_map or tgt_ext not in task_id_map:
            continue
        preds[tgt_ext].add(src_ext)
        succs[src_ext].add(tgt_ext)
        src_c = task_id_map[src_ext]
        tgt_c = task_id_map[tgt_ext]
        eid = f"e_{src_c}_{tgt_c}"
        canvas_edges.append(
            {"id": eid, "source": src_c, "target": tgt_c, "kind": "data"}
        )
        edge_pairs.append((src_c, tgt_c))

    roots = [ext for ext in task_ext_ids if not preds[ext]]
    leaves = [ext for ext in task_ext_ids if not succs[ext]]

    for root in roots:
        tgt_c = task_id_map[root]
        eid = f"e_start_{tgt_c}"
        if not any(e["id"] == eid for e in canvas_edges):
            canvas_edges.append(
                {"id": eid, "source": "start", "target": tgt_c, "kind": "data"}
            )
            edge_pairs.append(("start", tgt_c))

    for leaf in leaves:
        src_c = task_id_map[leaf]
        eid = f"e_{src_c}_end"
        if not any(e["id"] == eid for e in canvas_edges):
            canvas_edges.append(
                {"id": eid, "source": src_c, "target": "end", "kind": "data"}
            )
            edge_pairs.append((src_c, "end"))

    if not task_ext_ids:
        canvas_edges.append(
            {"id": "e_start_end", "source": "start", "target": "end", "kind": "data"}
        )
        edge_pairs.append(("start", "end"))

    positions = _layout_positions(
        [n["id"] for n in canvas_nodes],
        edge_pairs,
    )
    for node in canvas_nodes:
        pos = positions.get(node["id"])
        if pos:
            node["position"] = pos

    return {
        "schemaVersion": 1,
        "handle_orientation": "lr",
        "layout_method": "layered",
        "edge_path_style": "smoothstep",
        "nodes": canvas_nodes,
        "edges": canvas_edges,
    }


def resolve_unique_pipeline_id(
    workflow_external_id: str,
    pipeline_exists: Any,
) -> str:
    """Pick a free pipeline id based on the workflow external id."""
    base = suggest_pipeline_id(workflow_external_id)
    if not _PIPELINE_ID_RE.match(base):
        base = "wf_import"
    if not pipeline_exists(base):
        return base
    for i in range(2, 1000):
        candidate = f"{base}_{i}"[:128]
        if _PIPELINE_ID_RE.match(candidate) and not pipeline_exists(candidate):
            return candidate
    raise ValueError(f"Could not allocate pipeline id for workflow {workflow_external_id!r}")
