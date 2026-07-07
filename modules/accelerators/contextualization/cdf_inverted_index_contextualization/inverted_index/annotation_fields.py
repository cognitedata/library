"""CDM CogniteDiagramAnnotation field mapping (from cdf_file_annotation ApplyService)."""

from __future__ import annotations

from typing import Any

from inverted_index.annotation_identity import (
    apply_external_id_limit,
    build_identity_context,
    resolve_annotation_identity,
)
from inverted_index.config import ANNOTATION_INDEX_CONFIG
from inverted_index.jinja_render import render_template_string

EXTERNAL_ID_LIMIT = 256


def annotation_text(props: dict[str, Any], cfg: dict | None = None) -> str:
    """Primary detected text — CDM ``startNodeText``."""
    c = cfg or ANNOTATION_INDEX_CONFIG
    for key in (
        c.get("text_property", "startNodeText"),
        "startNodeText",
        "detected_text",
        "extracted_text",
        "text",
    ):
        val = props.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def annotation_bbox(props: dict[str, Any], cfg: dict | None = None) -> list[float] | None:
    """Axis-aligned bbox from CDM startNode*Min/Max properties."""
    c = cfg or ANNOTATION_INDEX_CONFIG
    keys = c.get(
        "bbox_properties",
        ["startNodeXMin", "startNodeYMin", "startNodeXMax", "startNodeYMax"],
    )
    if len(keys) != 4:
        return None
    try:
        vals = [float(props[k]) for k in keys if props.get(k) is not None]
        if len(vals) == 4:
            return vals
    except (TypeError, ValueError):
        pass
    region = props.get("region") or {}
    vertices = region.get("vertices") or []
    if vertices:
        xs = [float(v.get("x", 0)) for v in vertices if isinstance(v, dict)]
        ys = [float(v.get("y", 0)) for v in vertices if isinstance(v, dict)]
        if xs and ys:
            return [min(xs), min(ys), max(xs), max(ys)]
    return None


def annotation_page(props: dict[str, Any], cfg: dict | None = None) -> int | None:
    c = cfg or ANNOTATION_INDEX_CONFIG
    for key in (
        c.get("page_property", "startNodePageNumber"),
        "startNodePageNumber",
        "pageNumber",
        "page",
    ):
        if props.get(key) is not None:
            try:
                return int(props[key])
            except (TypeError, ValueError):
                continue
    region = props.get("region") or {}
    if region.get("page") is not None:
        try:
            return int(region["page"])
        except (TypeError, ValueError):
            pass
    return None


def detection_mode_from_annotation(
    props: dict[str, Any],
    external_id: str,
    *,
    cfg: dict | None = None,
) -> str:
    """
    Infer standard vs pattern detection.

    CDM annotations do not always store mode explicitly. Resolution order:
    1. Configured ``detection_mode_property`` when present on the instance
    2. ``tags`` list entries matching ``detection_mode_tags`` patterns
    3. External-id heuristics (``pat`` / ``std`` / ``pattern``)
    4. Default ``default_detection_mode`` (pattern)
    """
    c = cfg or ANNOTATION_INDEX_CONFIG
    prop = c.get("detection_mode_property")
    if prop and props.get(prop) in ("standard", "pattern"):
        return str(props[prop])

    tags = props.get("tags") or []
    tag_set = {str(t).lower() for t in tags if t}
    mode_tags: dict = c.get("detection_mode_tags") or {}
    for mode, markers in mode_tags.items():
        if any(m.lower() in tag_set for m in markers):
            return mode

    eid = external_id.lower()
    if "pattern" in eid or "_pat_" in eid or eid.startswith("pat-"):
        return "pattern"
    if "standard" in eid or "_std_" in eid or eid.startswith("std-"):
        return "standard"
    return str(c.get("default_detection_mode", "standard"))


def build_bbox_hash(
    bbox: list[float] | None,
    *,
    decimal_places: int = 4,
    hex_length: int = 8,
) -> str:
    from inverted_index.annotation_identity import compute_bbox_hash

    return compute_bbox_hash(
        bbox, decimal_places=decimal_places, hex_length=hex_length
    )


def build_detection_key(
    *,
    page: int | None,
    bbox: list[float] | None,
    normalized_term: str = "",
    file_external_id: str = "",
    identity: dict | None = None,
    annotation_config: dict | None = None,
) -> str:
    resolved = identity or resolve_annotation_identity(annotation_config)
    ctx = build_identity_context(
        file_external_id,
        page=page,
        normalized_term=normalized_term,
        bbox=bbox,
        identity=resolved,
    )
    template = resolved.get("detection_key_template")
    if template:
        return render_template_string(str(template), ctx).strip()
    return (
        f"{ctx['page_label']}:bbox_{ctx['bbox_hash']}:{ctx['term_prefix']}"
    )


def build_deterministic_annotation_external_id(
    file_external_id: str,
    *,
    page: int | None,
    normalized_term: str,
    bbox: list[float] | None,
    prefix: str | None = None,
    identity: dict | None = None,
    annotation_config: dict | None = None,
) -> str:
    """Stable annotation edge external id for index-only and upsert paths."""
    resolved = dict(identity or resolve_annotation_identity(annotation_config))
    if prefix is not None:
        resolved["annotation_external_id_prefix"] = prefix
    ctx = build_identity_context(
        file_external_id,
        page=page,
        normalized_term=normalized_term,
        bbox=bbox,
        identity=resolved,
    )
    template = resolved.get("annotation_external_id_template")
    if template:
        raw = render_template_string(str(template), ctx).strip()
    else:
        raw = (
            f"{ctx['prefix']}_{file_external_id}_{ctx['page_external_id']}_"
            f"{ctx['text_hash']}_{ctx['bbox_hash']}"
        )
    return apply_external_id_limit(
        raw, file_external_id=file_external_id, identity=resolved
    )
