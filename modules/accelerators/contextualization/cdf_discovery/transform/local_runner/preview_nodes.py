"""Run canvas ``node_preview`` snapshots after a local compiled workflow DAG."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from cdf_fn_common.etl_preview_snapshot import snapshot_predecessor_to_preview
from cdf_fn_common.workflow_compile.canvas_dag import CANVAS_ONLY_KINDS, _node_kind

logger = logging.getLogger(__name__)


def _task_succeeded(summary: Mapping[str, Any]) -> bool:
    status = str(summary.get("status") or "succeeded").strip().lower()
    return status not in {"failed", "error", "skipped"}


def _preview_nodes_from_canvas(canvas: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for n in canvas.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        if _node_kind(n) != "node_preview":
            continue
        nid = str(n.get("id") or "").strip()
        if not nid:
            continue
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        out.append({"id": nid, "config": dict(cfg)})
    return out


def _source_for_preview(
    preview_id: str,
    edges: Sequence[Mapping[str, Any]],
) -> str:
    sources: List[str] = []
    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("target") or "").strip() != preview_id:
            continue
        src = str(e.get("source") or "").strip()
        if src:
            sources.append(src)
    if len(sources) != 1:
        raise ValueError(
            f"node_preview {preview_id!r} requires exactly one inbound edge, got {len(sources)}"
        )
    return sources[0]


def _resolve_executable_source(
    source_id: str,
    pred: Mapping[str, Sequence[str]],
    *,
    canvas_only_ids: set[str],
    node_by_id: Mapping[str, Mapping[str, Any]],
) -> str:
    """Walk backward through canvas-only nodes to the executable predecessor canvas id."""
    seen: set[str] = set()
    cur = source_id
    while cur in canvas_only_ids:
        if cur in seen:
            raise ValueError(f"cycle in canvas-only predecessors at {cur!r}")
        seen.add(cur)
        preds = [str(p).strip() for p in (pred.get(cur) or []) if str(p).strip()]
        if len(preds) != 1:
            raise ValueError(
                f"node_preview source {cur!r} must have exactly one predecessor through canvas-only chain"
            )
        cur = preds[0]
    if _node_kind(node_by_id.get(cur) or {}) in CANVAS_ONLY_KINDS:
        raise ValueError(f"node_preview source resolved to non-executable canvas-only node {cur!r}")
    return cur


def run_canvas_preview_snapshots(
    doc: Mapping[str, Any],
    shared_data: MutableMapping[str, Any],
    *,
    client: Any,
    task_summaries: Mapping[str, Any],
    dry_run: bool,
    log: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """Snapshot preview nodes whose upstream task completed successfully."""
    lg = log or logger
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        return []
    previews = _preview_nodes_from_canvas(canvas)
    if not previews:
        return []
    if dry_run:
        lg.info("Skipping %s preview node snapshot(s) (dry_run=True)", len(previews))
        return [{"preview_node_id": p["id"], "skipped": True, "reason": "dry_run"} for p in previews]

    edges = canvas.get("edges") or []
    pred: Dict[str, List[str]] = {}
    for e in edges:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source") or "").strip()
        tgt = str(e.get("target") or "").strip()
        if src and tgt:
            pred.setdefault(tgt, []).append(src)

    node_by_id: Dict[str, Dict[str, Any]] = {}
    canvas_only_ids: set[str] = set()
    for n in canvas.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        nid = str(n.get("id") or "").strip()
        if nid:
            node_by_id[nid] = n
        if _node_kind(n) in CANVAS_ONLY_KINDS:
            canvas_only_ids.add(nid)

    run_id = str(shared_data.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("preview snapshot requires shared_data.run_id")

    results: List[Dict[str, Any]] = []
    for preview in previews:
        preview_id = preview["id"]
        cfg = preview.get("config") or {}
        try:
            direct_source = _source_for_preview(preview_id, edges)
            source_id = _resolve_executable_source(
                direct_source,
                pred,
                canvas_only_ids=canvas_only_ids,
                node_by_id=node_by_id,
            )
            summary = task_summaries.get(source_id)
            if not isinstance(summary, Mapping) or not _task_succeeded(summary):
                results.append(
                    {
                        "preview_node_id": preview_id,
                        "source_canvas_node_id": source_id,
                        "skipped": True,
                        "reason": "source_task_not_succeeded",
                    }
                )
                continue
            record_kind = str(cfg.get("record_kind") or "entity").strip() or None
            row_cap = int(cfg.get("row_cap") or 10_000)
            out = snapshot_predecessor_to_preview(
                client,
                shared_data,
                run_id=run_id,
                preview_node_id=preview_id,
                source_canvas_node_id=source_id,
                preview_config=cfg,
                record_kind=record_kind,
                row_cap=row_cap,
                log=lg,
            )
            results.append(out)
        except Exception as ex:
            lg.warning("Preview snapshot failed for %s: %s", preview_id, ex)
            results.append(
                {
                    "preview_node_id": preview_id,
                    "status": "failed",
                    "error": str(ex),
                }
            )
    return results
