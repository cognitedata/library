"""Run one diagram detect page-pack."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from cdf_fn_common.etl_diagram_detect import (
    flatten_detect_items_to_cohort_rows,
    run_diagram_detect,
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

def _retrieve_file_for_blob_check(client: Any, file_id: int) -> Any:
    files_api = getattr(client, "files", None)
    if files_api is None:
        return None
    if hasattr(files_api, "retrieve"):
        return files_api.retrieve(id=file_id)
    if callable(files_api):
        return files_api(id=file_id)
    return None


def _log_diagram_detect_api_call(
    *,
    log: Any,
    pack: Sequence[Any],
    entities: Sequence[Mapping[str, Any]],
    partial_match: bool,
    min_tokens: int,
    pattern_mode: bool,
    search_field: str,
    diagram_cfg: Any,
) -> None:
    if log is None or not hasattr(log, "info"):
        return
    refs_payload: List[Dict[str, int]] = []
    for ref in pack:
        refs_payload.append(
            {
                "file_id": int(getattr(ref, "file_id", None) or 0),
                "first_page": int(getattr(ref, "first_page", None) or 1),
                "last_page": int(getattr(ref, "last_page", None) or 1),
            }
        )
    payload: Dict[str, Any] = {
        "file_references": refs_payload,
        "entities_count": len(entities),
        "partial_match": partial_match,
        "min_tokens": min_tokens,
        "pattern_mode": pattern_mode,
        "search_field": search_field,
    }
    if isinstance(diagram_cfg, dict):
        payload["configuration"] = dict(diagram_cfg)
    log.info("Diagram detect API call: client.diagrams.detect(%s)", payload)


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
    pattern_mode = bool(cfg.get("pattern_mode", p.get("pattern_mode", False)))
    search_field = str(
        cfg.get("search_field")
        or p.get("search_field")
        or ("sample" if pattern_mode else "aliases")
    ).strip() or ("sample" if pattern_mode else "aliases")
    partial_match = bool(cfg.get("partial_match", p.get("partial_match", True)))
    min_tokens = int(cfg.get("min_tokens") or p.get("min_tokens") or 2)
    diagram_cfg = cfg.get("diagram_detect_config")
    poll_timeout = int(
        cfg.get("diagram_poll_timeout_sec")
        or p.get("diagram_poll_timeout_sec")
        or p.get("child_timeout")
        or 840
    )
    blob_checks: List[Dict[str, Any]] = []
    for ref in pack:
        fid = int(getattr(ref, "file_id", None) or 0)
        if not fid:
            continue
        try:
            f = _retrieve_file_for_blob_check(client, fid)
            blob_checks.append(
                {
                    "file_id": fid,
                    "external_id": getattr(f, "external_id", None),
                    "uploaded": bool(getattr(f, "uploaded", False)),
                    "uploaded_time": str(getattr(f, "uploaded_time", None) or ""),
                    "mime_type": getattr(f, "mime_type", None),
                }
            )
        except Exception as ex:
            blob_checks.append({"file_id": fid, "error": f"{type(ex).__name__}: {ex}"})
    emit_handler_progress(pack_index - 1, total=pack_total, label="detect_jobs")
    _log_diagram_detect_api_call(
        log=log,
        pack=pack,
        entities=entities,
        partial_match=partial_match,
        min_tokens=min_tokens,
        pattern_mode=pattern_mode,
        search_field=search_field,
        diagram_cfg=diagram_cfg,
    )
    job_id = run_diagram_detect(
        client,
        pack,
        entities,
        partial_match=partial_match,
        min_tokens=min_tokens,
        pattern_mode=pattern_mode,
        search_field=search_field,
        diagram_detect_config=diagram_cfg if isinstance(diagram_cfg, dict) else None,
    )
    results = wait_for_diagram_job(client, job_id, timeout_sec=poll_timeout)
    items = results.get("items") if isinstance(results, dict) else []
    first_item = items[0] if isinstance(items, list) and items else {}
    annotations = first_item.get("annotations") if isinstance(first_item, dict) else []
    item_errors: List[str] = []
    has_any_hit_payload = False
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            msg = str(item.get("errorMessage") or "").strip()
            if msg:
                item_errors.append(msg)
            if item.get("text"):
                has_any_hit_payload = True
            ann = item.get("annotations")
            if isinstance(ann, list) and ann:
                has_any_hit_payload = True
    if item_errors and not has_any_hit_payload:
        raise RuntimeError(
            "diagram detect completed without hits and returned item-level errors: "
            + "; ".join(item_errors[:3])
        )

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
