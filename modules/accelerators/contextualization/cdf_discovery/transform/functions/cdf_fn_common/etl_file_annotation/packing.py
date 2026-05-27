"""Page-pack planning for a single file annotation invocation."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from cdf_fn_common.etl_diagram_detect import (
    chunk_file_into_page_blocks,
    file_refs_from_serial_pack,
    pack_file_refs_into_detect_requests,
)


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

    raw_pack = data.get("detect_pack")
    if isinstance(raw_pack, list) and raw_pack:
        pack = file_refs_from_serial_pack(raw_pack)
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
    return packs
