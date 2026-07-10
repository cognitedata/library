"""DM annotation staging rows from detect hits."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

from cdf_fn_common.etl_annotation_map.cohort_hit import DetectHit
from cdf_fn_common.etl_diagram_detect import bounding_box_from_region


def _file_start_node(hit: DetectHit) -> Tuple[str, str]:
    inst = str(hit.file_ref.get("instance_space") or "").strip()
    ext = str(hit.file_ref.get("file_external_id") or hit.external_id).strip()
    if inst and ext:
        return inst, ext
    nid = hit.node_instance_id.strip()
    if ":" in nid:
        space, _, eid = nid.partition(":")
        if space.strip() and eid.strip():
            return space.strip(), eid.strip()
    return inst, ext


def _end_node_from_entity(ent: Mapping[str, Any]) -> Tuple[str, str]:
    ann_type = str(ent.get("annotation_type") or "")
    if ann_type == "diagrams.FileLink":
        return "", ""
    space = str(ent.get("end_node_space") or ent.get("space") or "").strip()
    ext = str(ent.get("end_node_external_id") or ent.get("external_id") or "").strip()
    return space, ext


def dm_annotation_row_from_hit(
    hit: DetectHit,
    *,
    annotation_space: str,
    default_status: str = "Suggested",
) -> List[Dict[str, Any]]:
    bb = bounding_box_from_region(hit.region)
    page = int(hit.file_ref.get("page_number") or hit.region.get("page") or 1)
    start_space, start_ext = _file_start_node(hit)
    rows: List[Dict[str, Any]] = []
    link_targets: List[Tuple[str, str, Any]] = []
    for ent in hit.entities or []:
        end_space, end_ext = _end_node_from_entity(ent)
        if end_space and end_ext:
            link_targets.append((end_space, end_ext, ent.get("annotation_type")))

    for idx, (end_space, end_ext, ann_type) in enumerate(link_targets):
        ext = f"{hit.external_id}_p{page}_{idx}"
        rows.append(
            {
                "annotation_space": annotation_space,
                "annotation_external_id": ext,
                "start_node_space": start_space,
                "start_node_external_id": start_ext,
                "end_node_space": end_space,
                "end_node_external_id": end_ext,
                "page": page,
                "text": hit.text,
                "confidence": hit.confidence,
                "status": default_status,
                "annotation_type": ann_type,
                "x_min": bb.get("x_min"),
                "y_min": bb.get("y_min"),
                "x_max": bb.get("x_max"),
                "y_max": bb.get("y_max"),
                "apply_json": hit.annotation,
            }
        )
    return rows
