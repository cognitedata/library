"""Custom edge and CogniteDiagramAnnotation write-back for target-driven links."""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from inverted_index.annotation_fields import (
    build_deterministic_annotation_external_id,
)
from inverted_index.extract import read_property_path


def _edge_external_id(
    template: str | None,
    *,
    start_space: str,
    start_external_id: str,
    end_space: str,
    end_external_id: str,
) -> str:
    if template:
        raw = (
            template.replace("{start_space}", start_space)
            .replace("{start_external_id}", start_external_id)
            .replace("{end_space}", end_space)
            .replace("{end_external_id}", end_external_id)
        )
        if len(raw) <= 256:
            return raw
    digest = hashlib.sha256(
        f"{start_space}:{start_external_id}:{end_space}:{end_external_id}".encode()
    ).hexdigest()[:16]
    return f"link_{digest}"


def build_custom_edge_apply(
    *,
    edge_view_cfg: dict,
    start_space: str,
    start_external_id: str,
    end_space: str,
    end_external_id: str,
    edge_space: str | None = None,
    external_id: str | None = None,
    external_id_template: str | None = None,
    properties: dict | None = None,
) -> Any:
    """Build an EdgeApply for a project-defined link view."""
    from cognite.client.data_classes.data_modeling import (
        DirectRelationReference,
        EdgeApply,
        NodeOrEdgeData,
    )

    space = edge_space or edge_view_cfg.get("space", start_space)
    ext_id = external_id or _edge_external_id(
        external_id_template,
        start_space=start_space,
        start_external_id=start_external_id,
        end_space=end_space,
        end_external_id=end_external_id,
    )
    view_space = edge_view_cfg.get("space", "cdf_cdm")
    view_external_id = edge_view_cfg.get("external_id", "CustomLink")
    view_version = edge_view_cfg.get("version", "v1")
    view_ref = (view_space, view_external_id, view_version)

    return EdgeApply(
        space=space,
        external_id=ext_id,
        type=DirectRelationReference(space=view_space, external_id=view_external_id),
        start_node=DirectRelationReference(space=start_space, external_id=start_external_id),
        end_node=DirectRelationReference(space=end_space, external_id=end_external_id),
        sources=[
            NodeOrEdgeData(
                source=view_ref,
                properties=properties or {},
            )
        ],
    )


def _annotation_properties_from_hit(hit: dict, ann_cfg: dict) -> dict[str, Any]:
    props: dict[str, Any] = {}
    for dest, src_path in (ann_cfg.get("property_map") or {}).items():
        val = read_property_path(hit, src_path)
        if val is not None:
            props[dest] = val
    meta = hit.get("additional_metadata") or {}
    bbox = meta.get("bbox")
    if isinstance(bbox, list) and len(bbox) == 4:
        props.setdefault("startNodeXMin", bbox[0])
        props.setdefault("startNodeYMin", bbox[1])
        props.setdefault("startNodeXMax", bbox[2])
        props.setdefault("startNodeYMax", bbox[3])
    if "status" not in props and ann_cfg.get("create_status"):
        props["status"] = ann_cfg["create_status"]
    if "startNodeText" not in props and hit.get("term"):
        props["startNodeText"] = hit["term"]
    return props


def _required_paths_present(hit: dict, paths: list[str]) -> bool:
    for path in paths:
        if read_property_path(hit, path) in (None, ""):
            return False
    return True


def upsert_diagram_annotation(
    client: Any,
    hit: dict,
    *,
    start_space: str,
    start_external_id: str,
    end_space: str,
    end_external_id: str,
    diagram_annotation_cfg: dict,
    dr_cfg: dict,
    dry_run: bool = False,
) -> Literal["created", "updated", "skipped"]:
    """Create or patch endNode on a CogniteDiagramAnnotation edge."""
    if not _required_paths_present(hit, diagram_annotation_cfg.get("required_for_create") or []):
        return "skipped"

    meta = hit.get("additional_metadata") or {}
    ann_id_path = diagram_annotation_cfg.get(
        "annotation_id_path", "additional_metadata.annotation_external_id"
    )
    annotation_external_id = read_property_path(hit, ann_id_path) or meta.get(
        "annotation_external_id"
    )
    if not annotation_external_id:
        page = meta.get("page")
        bbox = meta.get("bbox") if isinstance(meta.get("bbox"), list) else None
        annotation_external_id = build_deterministic_annotation_external_id(
            start_external_id,
            page=page,
            normalized_term=hit.get("normalized_term") or "",
            bbox=bbox,
        )

    ann_views = dr_cfg.get("views") or {}
    ann_view = ann_views.get("diagram_annotation") or {}
    annotation_space = (
        read_property_path(hit, diagram_annotation_cfg.get("annotation_space_from"))
        or ann_view.get("space")
        or start_space
    )
    annotation_external_id = str(annotation_external_id)

    if dry_run or client is None:
        return "created"

    existing = None
    try:
        edges = client.data_modeling.instances.retrieve_edges(
            [(annotation_space, annotation_external_id)]
        )
        if edges:
            existing = edges[0]
    except Exception:
        existing = None

    from cognite.client.data_classes.data_modeling import (
        DirectRelationReference,
        EdgeApply,
        NodeOrEdgeData,
    )

    ann_view_space = ann_view.get("space", "cdf_cdm")
    ann_view_external_id = ann_view.get("external_id", "CogniteDiagramAnnotation")
    ann_view_version = ann_view.get("version", "v1")
    view_ref = (ann_view_space, ann_view_external_id, ann_view_version)

    end_ref = DirectRelationReference(space=end_space, external_id=end_external_id)

    if existing is not None:
        if diagram_annotation_cfg.get("update_end_node_only", True):
            current_end = getattr(existing, "end_node", None)
            if (
                current_end
                and getattr(current_end, "space", None) == end_space
                and getattr(current_end, "external_id", None) == end_external_id
            ):
                return "skipped"
            start_ref = DirectRelationReference(
                space=start_space, external_id=start_external_id
            )
            edge_apply = EdgeApply(
                space=annotation_space,
                external_id=annotation_external_id,
                type=DirectRelationReference(
                    space=ann_view_space, external_id=ann_view_external_id
                ),
                start_node=start_ref,
                end_node=end_ref,
            )
            client.data_modeling.instances.apply([edge_apply])
            return "updated"
        return "skipped"

    start_ref = DirectRelationReference(space=start_space, external_id=start_external_id)
    properties = _annotation_properties_from_hit(hit, diagram_annotation_cfg)
    edge_apply = EdgeApply(
        space=annotation_space,
        external_id=annotation_external_id,
        type=DirectRelationReference(
            space=ann_view_space, external_id=ann_view_external_id
        ),
        start_node=start_ref,
        end_node=end_ref,
        sources=[NodeOrEdgeData(source=view_ref, properties=properties)],
    )
    client.data_modeling.instances.apply([edge_apply])
    return "created"
