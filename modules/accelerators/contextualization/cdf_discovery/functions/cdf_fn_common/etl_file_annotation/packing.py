"""Page-pack planning for a single file annotation invocation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from cdf_fn_common.etl_diagram_detect import (
    chunk_file_into_page_blocks,
    file_refs_from_serial_pack,
    pack_file_refs_into_detect_requests,
)


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: Mapping[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "1ceb4b",
            "runId": str(run_id or "unknown"),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": dict(data),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        with Path(
            "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-1ceb4b.log"
        ).open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def resolve_detect_packs_for_invocation(
    data: Mapping[str, Any],
    files: List[Dict[str, Any]],
    cfg: Mapping[str, Any],
    *,
    params: Mapping[str, Any] | None = None,
) -> List[List[Any]]:
    """Build page-pack list for this invocation (pre-built pack or multi-file chunking)."""
    p = dict(params or cfg)
    max_ref = int(
        cfg.get("max_pages_per_file_reference") or p.get("max_pages_per_file_reference") or 50
    )
    max_req = int(
        cfg.get("max_pages_per_detect_request") or p.get("max_pages_per_detect_request") or 50
    )
    max_jobs = int(
        cfg.get("max_detect_jobs_per_invocation") or p.get("max_detect_jobs_per_invocation") or 1
    )
    run_id = str(data.get("run_id") or p.get("run_id") or "unknown")
    # region agent log
    _debug_log(
        run_id,
        "H1",
        "etl_file_annotation/packing.py:resolve_detect_packs_for_invocation",
        "pack planner inputs",
        {
            "files_len": len(files),
            "max_ref": max_ref,
            "max_req": max_req,
            "max_jobs": max_jobs,
            "has_detect_pack": isinstance(data.get("detect_pack"), list) and bool(data.get("detect_pack")),
        },
    )
    # endregion

    raw_pack = data.get("detect_pack")
    if isinstance(raw_pack, list) and raw_pack:
        pack = file_refs_from_serial_pack(raw_pack)
        # region agent log
        _debug_log(
            run_id,
            "H2",
            "etl_file_annotation/packing.py:resolve_detect_packs_for_invocation",
            "using serialized detect_pack from payload",
            {"raw_pack_len": len(raw_pack), "resolved_pack_len": len(pack)},
        )
        # endregion
        return [pack] if pack else []

    all_refs: List[Any] = []
    for file_info in files:
        for ref in chunk_file_into_page_blocks(
            file_info, max_pages_per_file_reference=max_ref
        ):
            all_refs.append(ref)
    packs = pack_file_refs_into_detect_requests(all_refs, max_pages_per_detect_request=max_req)
    if max_jobs > 0:
        packs = packs[:max_jobs]
    # region agent log
    _debug_log(
        run_id,
        "H3",
        "etl_file_annotation/packing.py:resolve_detect_packs_for_invocation",
        "pack planner output",
        {"all_refs_len": len(all_refs), "packs_len": len(packs)},
    )
    # endregion
    return packs
