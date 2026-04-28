"""Local workflow execution: topological runner over ``compiled_workflow`` tasks (canvas or embedded)."""

from __future__ import annotations

import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence, Set

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.task_runtime import (
    merge_compiled_task_into_data,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.workflow_compile.legacy_ir import (
    TASK_ALIAS_PERSISTENCE,
    TASK_ALIASING,
    TASK_INCREMENTAL,
    TASK_KEY_EXTRACTION,
    TASK_REFERENCE_INDEX,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.workflow_execution_graph import (
    default_execution_graph_path,
    load_execution_graph,
    validate_execution_graph,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (
    persist_aliases_to_entities,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.pipeline import (
    tag_aliasing,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_incremental_state_update.pipeline import (
    incremental_state_update,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
    key_extraction,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline import (
    persist_reference_index,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.reference_index_naming import (
    reference_index_raw_table_from_key_extraction_table,
)

from .kahn_run_context import KahnRunContext
from .ui_progress import emit_ui_progress

LEGACY_MACRO_TASK_IDS: Set[str] = {
    TASK_INCREMENTAL,
    TASK_KEY_EXTRACTION,
    TASK_REFERENCE_INDEX,
    TASK_ALIASING,
    TASK_ALIAS_PERSISTENCE,
}


def should_validate_macro_execution_graph(compiled_workflow: Optional[Dict[str, Any]]) -> bool:
    """True only when the compiled DAG matches the fixed five-task macro (workflow.execution.graph.yaml)."""
    if not isinstance(compiled_workflow, dict):
        return False
    tasks = compiled_workflow.get("tasks")
    if not isinstance(tasks, list):
        return False
    ids = {str(t.get("id")) for t in tasks if isinstance(t, dict) and t.get("id")}
    return ids == LEGACY_MACRO_TASK_IDS


def validate_execution_graph_at_startup(
    module_root: Path,
    logger: logging.Logger,
    compiled_workflow: Optional[Dict[str, Any]] = None,
) -> None:
    if compiled_workflow is not None and not should_validate_macro_execution_graph(compiled_workflow):
        logger.info(
            "Skipping workflow.execution.graph.yaml check (compiled_workflow is not the legacy macro DAG)."
        )
        return
    gpath = default_execution_graph_path(module_root)
    graph = load_execution_graph(gpath)
    errs = validate_execution_graph(graph)
    if errs:
        raise RuntimeError(f"Invalid execution graph {gpath}: {'; '.join(errs)}")
    logger.info(
        "Kahn macro graph: %s (%d nodes, %d edges)",
        gpath.name,
        len(graph.nodes),
        len(graph.edges),
    )


def step_incremental_state_update(ctx: KahnRunContext, task_id: str = TASK_INCREMENTAL) -> None:
    ctx.state_data = {
        "logLevel": "INFO",
        "run_all": bool(getattr(ctx.args, "run_all", False)),
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
    }
    merge_compiled_task_into_data(ctx.state_data)
    incremental_state_update(ctx.client, ctx.pipe_logger, ctx.state_data, ctx.cdf_config)
    ctx.run_id = str(ctx.state_data.get("run_id") or "")
    if not ctx.run_id:
        raise RuntimeError("incremental_state_update did not set run_id")

    if str(ctx.state_data.get("status") or "") == "success":
        try:
            msg_raw = ctx.state_data.get("message")
            if isinstance(msg_raw, str) and msg_raw.strip():
                st_msg = json.loads(msg_raw)
                if isinstance(st_msg, dict):
                    ctx.cohort_rows = int(st_msg.get("cohort_rows_written", 0) or 0)
                    ctx.cohort_skipped_hash = int(
                        st_msg.get("cohort_rows_skipped_unchanged_hash", 0) or 0
                    )
        except Exception:
            pass

    if ctx.cohort_rows is not None:
        ctx.logger.info(
            "✓ State update: run_id=%s, cohort_rows=%s%s",
            ctx.run_id,
            ctx.cohort_rows,
            (
                f", skipped_unchanged_hash={ctx.cohort_skipped_hash}"
                if ctx.cohort_skipped_hash
                else ""
            ),
        )
    else:
        ctx.logger.info("✓ State update: run_id=%s", ctx.run_id)


def step_key_extraction(
    ctx: KahnRunContext,
    task_id: str = TASK_KEY_EXTRACTION,
    merge_lock: Optional[Lock] = None,
) -> None:
    ke_data: Dict[str, Any] = {
        "logLevel": "INFO",
        "run_id": ctx.run_id,
        "run_all": bool(getattr(ctx.args, "run_all", False)),
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
    }
    merge_compiled_task_into_data(ke_data)
    engine = KeyExtractionEngine(ctx.engine_config)
    key_extraction(ctx.client, ctx.pipe_logger, ke_data, engine, ctx.cdf_config)
    new_ek = ke_data.get("entities_keys_extracted") or {}
    new_keys = int(ke_data.get("keys_extracted") or 0)

    def _merge() -> None:
        ctx.ke_data = ke_data
        if not isinstance(ctx.entities_keys_extracted, dict):
            ctx.entities_keys_extracted = {}
        for k, v in new_ek.items():
            ctx.entities_keys_extracted[k] = v
        ctx.keys_extracted = int(ctx.keys_extracted or 0) + new_keys
        ctx.raw_db = str(getattr(ctx.cdf_config.parameters, "raw_db", "") or "")
        ctx.raw_table_key = str(getattr(ctx.cdf_config.parameters, "raw_table_key", "") or "")
        ctx.v0 = ctx.source_views[0] if ctx.source_views else {}
        ctx.fallback_instance_space = next(
            (str(v.get("instance_space")) for v in ctx.source_views if v.get("instance_space")),
            "all_spaces",
        )

    if merge_lock is not None:
        with merge_lock:
            _merge()
    else:
        _merge()


def _reference_index_branch(ctx: KahnRunContext, task_id: str = TASK_REFERENCE_INDEX) -> Dict[str, Any]:
    args = ctx.args
    logger = ctx.logger
    cdf_config = ctx.cdf_config
    ref_from_scope = bool(getattr(cdf_config.parameters, "enable_reference_index", False))
    if getattr(args, "skip_reference_index", False):
        logger.info("Skipping reference index (--skip-reference-index).")
        return {"status": "skipped", "reason": "skip_reference_index"}
    if not ref_from_scope:
        logger.info(
            "Skipping reference index: enable_reference_index is false in scope "
            "(set key_extraction.config.parameters.enable_reference_index: true)."
        )
        return {"status": "skipped", "reason": "enable_reference_index_false"}
    if args.dry_run:
        logger.info(
            "Dry-run: skipping reference index RAW writes (same as alias persistence)."
        )
        return {"status": "skipped", "reason": "dry_run"}
    if not ctx.raw_db or not ctx.raw_table_key:
        logger.warning(
            "Reference index skipped: raw_db or raw_table_key missing in key_extraction parameters."
        )
        return {"status": "skipped", "reason": "missing_raw_db_or_table"}

    ref_data: Dict[str, Any] = {
        "logLevel": "INFO",
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "progress_every": ctx.progress_every,
        "source_run_id": ctx.run_id,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
        "source_raw_db": ctx.raw_db,
        "source_raw_table_key": ctx.raw_table_key,
        "source_raw_read_limit": 10000,
        "incremental_auto_run_id": True,
        "reference_index_raw_db": ctx.raw_db,
        "reference_index_raw_table": reference_index_raw_table_from_key_extraction_table(
            ctx.raw_table_key
        ),
        "source_instance_space": str(
            ctx.v0.get("instance_space") or ctx.fallback_instance_space
        ),
        "source_view_space": ctx.v0.get("view_space", "cdf_cdm"),
        "source_view_external_id": ctx.v0.get("view_external_id", "CogniteAsset"),
        "source_view_version": ctx.v0.get("view_version", "v1"),
        "reference_index_fk_entity_type": "asset",
        "reference_index_document_entity_type": "file",
        "config": {
            "config": {
                "parameters": {"debug": True},
                "data": {
                    "aliasing_rules": ctx.aliasing_config.get("rules") or [],
                    "validation": ctx.aliasing_config.get("validation") or {},
                },
            },
        },
    }
    merge_compiled_task_into_data(ref_data)
    persist_reference_index(ctx.client, ctx.pipe_logger, ref_data)
    logger.info(
        "✓ Reference index: %s entities processed, %s inverted writes, %s postings "
        "(%s foreign_key, %s document)",
        ref_data.get("reference_index_entities_processed", 0),
        ref_data.get("reference_index_inverted_writes", 0),
        ref_data.get("reference_index_posting_events", 0),
        ref_data.get("reference_index_fk_posting_events", 0),
        ref_data.get("reference_index_document_posting_events", 0),
    )
    return {
        "status": "ok",
        "entities": int(ref_data.get("reference_index_entities_processed", 0) or 0),
        "inverted_writes": int(ref_data.get("reference_index_inverted_writes", 0) or 0),
        "postings": int(ref_data.get("reference_index_posting_events", 0) or 0),
        "fk_postings": int(ref_data.get("reference_index_fk_posting_events", 0) or 0),
        "doc_postings": int(ref_data.get("reference_index_document_posting_events", 0) or 0),
    }


def _aliasing_branch(ctx: KahnRunContext, task_id: str = TASK_ALIASING) -> None:
    src_run = ctx.ke_data.get("run_id") if isinstance(ctx.ke_data, dict) else None
    if not src_run:
        src_run = ctx.run_id
    ctx.alias_data = {
        "logLevel": "INFO",
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "progress_every": ctx.progress_every,
        "entities_keys_extracted": ctx.entities_keys_extracted,
        "source_run_id": src_run,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
        "source_raw_db": ctx.raw_db,
        "source_raw_table_key": ctx.raw_table_key,
        "source_raw_read_limit": 10000,
        "incremental_auto_run_id": True,
        "incremental_transition": True,
        "source_instance_space": str(
            ctx.v0.get("instance_space") or ctx.fallback_instance_space
        ),
        "source_view_space": ctx.v0.get("view_space", "cdf_cdm"),
        "source_view_external_id": ctx.v0.get("view_external_id", "CogniteAsset"),
        "source_view_version": ctx.v0.get("view_version", "v1"),
        "source_entity_type": str(ctx.v0.get("entity_type", "asset")),
    }
    merge_compiled_task_into_data(ctx.alias_data)
    engine = AliasingEngine(ctx.aliasing_config, client=ctx.client)
    tag_aliasing(ctx.client, ctx.pipe_logger, ctx.alias_data, engine)
    ar = ctx.alias_data.get("aliasing_results") or []
    if isinstance(ar, list):
        ctx.accumulated_aliasing_results.extend(ar)
    ctx.logger.info(
        "✓ Aliasing: tags=%s, aliases_generated=%s, raw_workflow_rows_updated=%s",
        ctx.alias_data.get("total_tags_processed", 0),
        ctx.alias_data.get("total_aliases_generated", 0),
        ctx.alias_data.get("key_extraction_workflow_rows_updated", "n/a"),
    )


def _alias_persistence_branch(ctx: KahnRunContext, task_id: str) -> Dict[str, Any]:
    logger = ctx.logger
    args = ctx.args
    if args.dry_run:
        logger.info("Dry-run: skipping alias persistence (task_id=%s).", task_id)
        ctx.persistence_summary = {"status": "skipped", "reason": "dry_run"}
        return ctx.persistence_summary
    aliasing_results = (
        list(ctx.accumulated_aliasing_results)
        if ctx.accumulated_aliasing_results
        else (ctx.alias_data.get("aliasing_results") or [])
    )
    persistence_data: Dict[str, Any] = {
        "aliasing_results": aliasing_results,
        "entities_keys_extracted": ctx.entities_keys_extracted,
        "logLevel": "INFO",
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "compiled_workflow": ctx.compiled_workflow,
        "task_id": task_id,
    }
    merge_compiled_task_into_data(persistence_data)
    if ctx.alias_writeback_property:
        persistence_data["alias_writeback_property"] = ctx.alias_writeback_property
    wfk = ctx.write_foreign_key_references or getattr(args, "write_foreign_keys", False)
    if wfk:
        persistence_data["write_foreign_key_references"] = True
        fk_prop = ctx.foreign_key_writeback_property
        if getattr(args, "foreign_key_writeback_property", None):
            fk_prop = str(args.foreign_key_writeback_property).strip() or fk_prop
        if fk_prop:
            persistence_data["foreign_key_writeback_property"] = fk_prop
    persist_aliases_to_entities(
        client=ctx.client,
        logger=logger,
        data=persistence_data,
    )
    fk_written = int(persistence_data.get("foreign_keys_persisted", 0) or 0)
    logger.info(
        "✓ Persisted to data model: %s entities updated, %s alias value(s) written, "
        "%s foreign key value(s) written",
        persistence_data.get("entities_updated", 0),
        persistence_data.get("aliases_persisted", 0),
        fk_written,
    )
    ctx.persistence_summary = {
        "status": "ok",
        "entities_updated": int(persistence_data.get("entities_updated", 0) or 0),
        "aliases_persisted": int(persistence_data.get("aliases_persisted", 0) or 0),
        "fk_persisted": fk_written,
        "raw": persistence_data,
    }
    return ctx.persistence_summary


def _alias_persistence_topological_layers(
    cw: Dict[str, Any],
) -> List[List[str]]:
    """Topological layers for ``fn_dm_alias_persistence`` tasks only (edges between those ids)."""
    tasks_raw = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks_raw, list):
        return []
    persist_ids = [
        str(t["id"])
        for t in tasks_raw
        if isinstance(t, dict)
        and str(t.get("function_external_id") or "") == "fn_dm_alias_persistence"
        and t.get("id")
    ]
    if not persist_ids:
        return []
    pset = set(persist_ids)
    idx = _task_index(cw)
    pred_map: Dict[str, Set[str]] = {}
    for tid in persist_ids:
        t = idx.get(tid) or {}
        deps = t.get("depends_on") if isinstance(t.get("depends_on"), list) else []
        pred_map[tid] = {str(d) for d in deps if d and str(d) in pset}
    return _topological_layers(persist_ids, pred_map)


def _topological_layers(task_ids: Sequence[str], pred_map: Dict[str, Set[str]]) -> List[List[str]]:
    remaining = set(task_ids)
    completed: Set[str] = set()
    layers: List[List[str]] = []
    while remaining:
        ready = sorted(t for t in remaining if pred_map.get(t, set()) <= completed)
        if not ready:
            raise RuntimeError("compiled_workflow task graph has a cycle or invalid dependencies")
        layers.append(ready)
        completed.update(ready)
        remaining -= set(ready)
    return layers


def _task_index(compiled: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for t in compiled.get("tasks") or []:
        if isinstance(t, dict) and t.get("id"):
            out[str(t["id"])] = t
    return out


def _compiled_workflow_without_alias_persistence(cw: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of *cw* with ``fn_dm_alias_persistence`` tasks removed (edges unchanged; unused deps ignored)."""
    out = copy.deepcopy(cw) if isinstance(cw, dict) else {}
    tasks = out.get("tasks")
    if not isinstance(tasks, list):
        return out
    out["tasks"] = [
        t
        for t in tasks
        if isinstance(t, dict) and str(t.get("function_external_id") or "") != "fn_dm_alias_persistence"
    ]
    return out


def _ui_task_progress_fields(ctx: KahnRunContext, task_id: str) -> Dict[str, Any]:
    """Metadata for operator UI (graph node id + function id)."""
    task = _task_index(ctx.compiled_workflow).get(task_id) or {}
    fn_ext = str(task.get("function_external_id") or "").strip()
    cnv = str(task.get("canvas_node_id") or "").strip()
    pnode = str(task.get("pipeline_node_id") or "").strip()
    out: Dict[str, Any] = {"task_id": task_id}
    if fn_ext:
        out["function_external_id"] = fn_ext
    if cnv:
        out["canvas_node_id"] = cnv
    if pnode and pnode != task_id:
        out["pipeline_node_id"] = pnode
    return out


def _dispatch_task_tracked(ctx: KahnRunContext, task_id: str, merge_lock: Lock) -> None:
    emit_ui_progress("task_start", **_ui_task_progress_fields(ctx, task_id))
    try:
        _dispatch_task(ctx, task_id, merge_lock)
    finally:
        emit_ui_progress("task_end", **_ui_task_progress_fields(ctx, task_id))


def _dispatch_task(ctx: KahnRunContext, task_id: str, merge_lock: Lock) -> None:
    idx = _task_index(ctx.compiled_workflow)
    task = idx.get(task_id)
    if not task:
        raise RuntimeError(f"Unknown task_id in compiled_workflow: {task_id!r}")
    fn_ext = str(task.get("function_external_id") or "")
    if fn_ext == "fn_dm_incremental_state_update":
        step_incremental_state_update(ctx, task_id)
    elif fn_ext == "fn_dm_key_extraction":
        step_key_extraction(ctx, task_id, merge_lock)
    elif fn_ext == "fn_dm_reference_index":
        ctx.ref_summary = _reference_index_branch(ctx, task_id)
    elif fn_ext == "fn_dm_aliasing":
        _aliasing_branch(ctx, task_id)
    elif fn_ext == "fn_dm_alias_persistence":
        _alias_persistence_branch(ctx, task_id)
    else:
        raise RuntimeError(f"Unsupported function_external_id for local runner: {fn_ext!r} (task {task_id!r})")


def run_compiled_workflow_dag(
    ctx: KahnRunContext,
    *,
    defer_alias_persistence: bool = False,
) -> None:
    """
    Execute every task in ``ctx.compiled_workflow`` in topological order; parallelize independent tasks.

    When *defer_alias_persistence* is True, ``fn_dm_alias_persistence`` tasks are omitted; call
    ``run_deferred_alias_persistence_tasks`` afterward (after writing local JSON mirrors).
    """
    cw = (
        _compiled_workflow_without_alias_persistence(ctx.compiled_workflow)
        if defer_alias_persistence
        else ctx.compiled_workflow
    )
    tasks_raw = cw.get("tasks") if isinstance(cw, dict) else None
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise RuntimeError("compiled_workflow.tasks is missing or empty")
    task_ids: List[str] = []
    pred_map: Dict[str, Set[str]] = {}
    for t in tasks_raw:
        if not isinstance(t, dict) or not t.get("id"):
            continue
        tid = str(t["id"])
        task_ids.append(tid)
        deps = t.get("depends_on") if isinstance(t.get("depends_on"), list) else []
        pred_map[tid] = {str(d) for d in deps if d}
    layers = _topological_layers(task_ids, pred_map)
    merge_lock = Lock()
    for layer in layers:
        if len(layer) == 1:
            _dispatch_task_tracked(ctx, layer[0], merge_lock)
        else:
            with ThreadPoolExecutor(max_workers=len(layer)) as ex:
                futs = [
                    ex.submit(_dispatch_task_tracked, ctx, tid, merge_lock) for tid in layer
                ]
                for fut in as_completed(futs):
                    fut.result()


def run_deferred_alias_persistence_tasks(ctx: KahnRunContext) -> None:
    """Run all ``fn_dm_alias_persistence`` tasks in DAG order (``depends_on`` between persistence tasks)."""
    merge_lock = Lock()
    cw = ctx.compiled_workflow
    if not isinstance(cw, dict):
        return
    layers = _alias_persistence_topological_layers(cw)
    for layer in layers:
        if len(layer) == 1:
            _dispatch_task_tracked(ctx, layer[0], merge_lock)
        else:
            with ThreadPoolExecutor(max_workers=len(layer)) as ex:
                futs = [
                    ex.submit(_dispatch_task_tracked, ctx, tid, merge_lock) for tid in layer
                ]
                for fut in as_completed(futs):
                    fut.result()


def run_post_extraction_parallel(ctx: KahnRunContext) -> None:
    """
    Run fn_dm_reference_index and fn_dm_aliasing concurrently (matches CDF DAG after key extraction).

    On failure in either branch, raises (aligns with onFailure: abortWorkflow per task).
    """
    ref_box: Dict[str, Any] = {}

    def run_ref() -> None:
        ref_box["summary"] = _reference_index_branch(ctx)

    def run_alias() -> None:
        _aliasing_branch(ctx)

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_ref = ex.submit(run_ref)
        f_alias = ex.submit(run_alias)
        for fut in as_completed([f_ref, f_alias]):
            fut.result()

    ctx.ref_summary = ref_box.get("summary")
