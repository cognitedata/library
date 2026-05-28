"""DM annotation staging rows from detect hits."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from cdf_fn_common.etl_annotation_map.cohort_hit import DetectHit
from cdf_fn_common.etl_diagram_detect import bounding_box_from_region


def dm_annotation_row_from_hit(
    hit: DetectHit,
    *,
    annotation_space: str,
    default_status: str = "Suggested",
) -> List[Dict[str, Any]]:
    bb = bounding_box_from_region(hit.region)
    page = int(hit.file_ref.get("page_number") or hit.region.get("page") or 1)
    rows: List[Dict[str, Any]] = []
    for idx, ent in enumerate(hit.entities or [{}]):
        ext = f"{hit.external_id}_p{page}_{idx}"
        rows.append(
            {
                "annotation_space": annotation_space,
                "annotation_external_id": ext,
                "start_node_space": ent.get("start_node_space") or ent.get("space"),
                "start_node_external_id": ent.get("start_node_external_id") or ent.get("external_id"),
                "end_node_space": ent.get("end_node_space"),
                "end_node_external_id": ent.get("end_node_external_id"),
                "page": page,
                "text": hit.text,
                "confidence": hit.confidence,
                "status": default_status,
                "annotation_type": ent.get("annotation_type"),
                "x_min": bb.get("x_min"),
                "y_min": bb.get("y_min"),
                "x_max": bb.get("x_max"),
                "y_max": bb.get("y_max"),
                "apply_json": hit.annotation,
            }
        )
    if not rows:
        bb = bounding_box_from_region(hit.region)
        rows.append(
            {
                "annotation_space": annotation_space,
                "annotation_external_id": f"{hit.external_id}_p{page}",
                "page": page,
                "text": hit.text,
                "confidence": hit.confidence,
                "status": default_status,
                "x_min": bb.get("x_min"),
                "y_min": bb.get("y_min"),
                "x_max": bb.get("x_max"),
                "y_max": bb.get("y_max"),
                "apply_json": hit.annotation,
            }
        )
    return rows
