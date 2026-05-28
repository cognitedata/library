"""Run one diagram detect page-pack."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from cdf_fn_common.etl_diagram_detect import (
    flatten_detect_items_to_cohort_rows,
    run_pattern_diagram_detect,
    wait_for_diagram_job,
)
from cdf_fn_common.etl_pattern_dump import (
    DiagramDetectCompleteContext,
    invoke_diagram_detect_complete_hooks,
)
from cdf_fn_common.etl_ui_progress import emit_handler_progress


@dataclass
class AnnotationPackContext:
    run_id: str
    workflow_scope: str
    task_id: str
    file_info_map: Dict[int, Dict[str, Any]]
    ref_meta_by_file: Dict[int, Dict[str, Any]]


def run_one_annotation_pack(
    client: Any,
    pack: Sequence[Any],
    entities: List[Dict[str, Any]],
    cfg: Mapping[str, Any],
    ctx: AnnotationPackContext,
    *,
    pack_index: int,
    pack_total: int,
    params: Optional[Mapping[str, Any]] = None,
    log: Any = None,
) -> tuple[int, List[Dict[str, Any]], int]:
    """Submit one detect job, wait, flatten cohort rows. Returns (job_id, rows, pattern_dump_count)."""
    p = dict(params or cfg)
    partial_match = bool(cfg.get("partial_match", p.get("partial_match", True)))
    min_tokens = int(cfg.get("min_tokens") or p.get("min_tokens") or 2)
    diagram_cfg = cfg.get("diagram_detect_config")
    poll_timeout = int(
        cfg.get("diagram_poll_timeout_sec")
        or p.get("diagram_poll_timeout_sec")
        or p.get("child_timeout")
        or 840
    )

    emit_handler_progress(pack_index - 1, total=pack_total, label="detect_jobs")
    job_id = run_pattern_diagram_detect(
        client,
        pack,
        entities,
        partial_match=partial_match,
        min_tokens=min_tokens,
        diagram_detect_config=diagram_cfg if isinstance(diagram_cfg, dict) else None,
    )
    results = wait_for_diagram_job(client, job_id, timeout_sec=poll_timeout)

    pack_file_ids: set[int] = set()
    for ref in pack:
        fid = int(getattr(ref, "file_id", None) or 0)
        if fid:
            pack_file_ids.add(fid)
    for fid in pack_file_ids:
        ctx.ref_meta_by_file.setdefault(fid, {"first_page": 1, "last_page": 1})

    invoke_diagram_detect_complete_hooks(
        DiagramDetectCompleteContext(
            client=client,
            job_id=job_id,
            results=results,
            run_id=ctx.run_id,
            workflow_scope=ctx.workflow_scope,
            task_id=ctx.task_id,
            pack_index=pack_index,
            pack_total=pack_total,
            file_ids=sorted(pack_file_ids),
            log=log,
        )
    )
    cohort_rows = flatten_detect_items_to_cohort_rows(
        results,
        ctx.file_info_map,
        run_id=ctx.run_id,
        scope_key=ctx.workflow_scope,
        file_ref_meta=ctx.ref_meta_by_file,
    )
    emit_handler_progress(pack_index, total=pack_total, label="detect_jobs", force=True)
    return job_id, cohort_rows, 1
