"""Normalize one file-annotation cohort row."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from cdf_fn_common.etl_diagram_detect import matched_entities_from_annotation


@dataclass
class DetectHit:
    text: str
    region: Dict[str, Any]
    file_ref: Dict[str, Any]
    confidence: Optional[float]
    annotation: Dict[str, Any]
    entities: list[Dict[str, Any]]
    node_instance_id: str
    external_id: str


def read_detect_hit_from_cohort_row(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Optional[DetectHit]:
    text = str(props.get("text") or "").strip()
    if not text:
        return None
    region = props.get("region") if isinstance(props.get("region"), dict) else {}
    file_ref = props.get("file_ref") if isinstance(props.get("file_ref"), dict) else {}
    annotation = props.get("annotation") if isinstance(props.get("annotation"), dict) else {}
    entities = props.get("entities")
    if not isinstance(entities, list) or not entities:
        entities = matched_entities_from_annotation(annotation)
    return DetectHit(
        text=text,
        region=dict(region),
        file_ref=dict(file_ref),
        confidence=props.get("confidence"),
        annotation=dict(annotation),
        entities=[dict(e) for e in entities if isinstance(e, dict)],
        node_instance_id=str(cols.get("node_instance_id") or ""),
        external_id=str(cols.get("external_id") or file_ref.get("file_external_id") or ""),
    )
