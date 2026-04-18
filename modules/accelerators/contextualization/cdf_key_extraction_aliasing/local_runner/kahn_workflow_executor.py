"""Kahn-style macro workflow steps for local CLI (aligns with workflow.execution.graph.yaml)."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.workflow_execution_graph import (
    default_execution_graph_path,
    load_execution_graph,
    validate_execution_graph,
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


def validate_execution_graph_at_startup(module_root: Path, logger: logging.Logger) -> None:
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


def step_incremental_state_update(ctx: KahnRunContext) -> None:
    ctx.state_data = {
        "logLevel": "INFO",
        "run_all": bool(getattr(ctx.args, "run_all", False)),
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
    }
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


def step_key_extraction(ctx: KahnRunContext) -> None:
    ctx.ke_data = {
        "logLevel": "INFO",
        "run_id": ctx.run_id,
        "run_all": bool(getattr(ctx.args, "run_all", False)),
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
    }
    engine = KeyExtractionEngine(ctx.engine_config)
    key_extraction(ctx.client, ctx.pipe_logger, ctx.ke_data, engine, ctx.cdf_config)
    ctx.entities_keys_extracted = ctx.ke_data.get("entities_keys_extracted") or {}
    ctx.keys_extracted = int(ctx.ke_data.get("keys_extracted") or 0)

    ctx.raw_db = str(getattr(ctx.cdf_config.parameters, "raw_db", "") or "")
    ctx.raw_table_key = str(getattr(ctx.cdf_config.parameters, "raw_table_key", "") or "")
    ctx.v0 = ctx.source_views[0] if ctx.source_views else {}
    ctx.fallback_instance_space = next(
        (str(v.get("instance_space")) for v in ctx.source_views if v.get("instance_space")),
        "all_spaces",
    )


def _reference_index_branch(ctx: KahnRunContext) -> Dict[str, Any]:
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


def _aliasing_branch(ctx: KahnRunContext) -> None:
    ctx.alias_data = {
        "logLevel": "INFO",
        "configuration": ctx.scope_document,
        "instance_space": ctx.wf_instance_space,
        "progress_every": ctx.progress_every,
        "entities_keys_extracted": ctx.entities_keys_extracted,
        "source_run_id": ctx.ke_data.get("run_id"),
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
    engine = AliasingEngine(ctx.aliasing_config, client=ctx.client)
    tag_aliasing(ctx.client, ctx.pipe_logger, ctx.alias_data, engine)
    ctx.logger.info(
        "✓ Aliasing: tags=%s, aliases_generated=%s, raw_workflow_rows_updated=%s",
        ctx.alias_data.get("total_tags_processed", 0),
        ctx.alias_data.get("total_aliases_generated", 0),
        ctx.alias_data.get("key_extraction_workflow_rows_updated", "n/a"),
    )


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
