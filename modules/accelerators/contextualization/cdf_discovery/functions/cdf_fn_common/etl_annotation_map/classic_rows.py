"""Classic annotation staging rows from detect hits."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from cdf_fn_common.etl_annotation_map.cohort_hit import DetectHit
from cdf_fn_common.etl_diagram_detect import bounding_box_from_region


def classic_annotation_row_from_hit(hit: DetectHit) -> Dict[str, Any]:
    bb = bounding_box_from_region(hit.region)
    page = int(hit.file_ref.get("page_number") or hit.region.get("page") or 1)
    ent = hit.entities[0] if hit.entities else {}
    return {
        "file_id": hit.file_ref.get("file_id"),
        "file_external_id": hit.file_ref.get("file_external_id") or hit.external_id,
        "page": page,
        "text": hit.text,
        "confidence": hit.confidence,
        "x_min": bb.get("x_min"),
        "y_min": bb.get("y_min"),
        "x_max": bb.get("x_max"),
        "y_max": bb.get("y_max"),
        "linked_resource_type": ent.get("annotation_type"),
        "linked_resource_external_id": ent.get("external_id") or ent.get("end_node_external_id"),
        "metadata_json": json.dumps(hit.annotation, default=str),
    }
