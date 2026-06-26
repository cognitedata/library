"""Fetch diagram annotations and DM view instances from CDF via instances.query."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, Literal

from inverted_index.annotation_fields import (
    annotation_text,
    detection_mode_from_annotation,
)
from inverted_index.config import ANNOTATION_INDEX_CONFIG, INDEX_FIELD_CONFIG, SCOPE_CONFIG
from inverted_index.dm_query import (
    annotation_select_property_names,
    collect_view_property_paths,
    query_all_edges,
    query_all_nodes,
    top_level_property_names,
)


def _props_dict(inst: Any) -> dict:
    return dict(getattr(inst, "properties", None) or {})


def _flatten_dm_properties(props: Any) -> dict[str, Any]:
    """Merge nested view DM properties into a single flat dict."""
    flat: dict[str, Any] = {}
    items = props.items() if hasattr(props, "items") else ()
    for _key, val in items:
        if isinstance(val, dict):
            if any(not isinstance(v, (dict, list)) for v in val.values()):
                for pk, pv in val.items():
                    if not isinstance(pv, (dict, list)) and pk not in flat:
                        flat[pk] = pv
            else:
                nested = _flatten_dm_properties(val)
                for pk, pv in nested.items():
                    if pk not in flat:
                        flat[pk] = pv
    return flat


def _view_properties(
    inst: Any,
    *,
    view_space: str,
    view: str,
    version: str,
) -> dict[str, Any]:
    """Read flat view properties from a DM node or edge instance."""
    from cognite.client import data_modeling as dm

    view_id = dm.ViewId(space=view_space, external_id=view, version=version)
    props_raw = getattr(inst, "properties", None) or {}
    if hasattr(props_raw, "get"):
        view_props = props_raw.get(view_id)
        if isinstance(view_props, dict):
            return dict(view_props)
    if isinstance(props_raw, dict):
        for key, val in props_raw.items():
            if isinstance(val, dict) and view in str(key):
                return dict(val)
    return _flatten_dm_properties(props_raw)


def _edge_file_ref(inst: Any, file_space: str) -> tuple[str | None, str | None]:
    """CDM annotations are edges: file is ``start_node``."""
    start = getattr(inst, "start_node", None)
    if start is not None:
        return getattr(start, "space", None) or file_space, getattr(
            start, "external_id", None
        )
    props = _props_dict(inst)
    for key in ("startNode", "start_node"):
        node = props.get(key)
        if isinstance(node, dict):
            return node.get("space") or file_space, node.get("externalId") or node.get(
                "external_id"
            )
    return None, None


def _edge_end_ref(inst: Any) -> tuple[str | None, str | None]:
    end = getattr(inst, "end_node", None)
    if end is not None:
        return getattr(end, "space", None), getattr(end, "external_id", None)
    props = _props_dict(inst)
    node = props.get("endNode") or props.get("end_node")
    if isinstance(node, dict):
        return node.get("space"), node.get("externalId") or node.get("external_id")
    return None, None


def list_diagram_annotations(
    client: Any,
    *,
    file_external_id: str | None = None,
    file_space: str = "cdf_cdm",
    annotation_config: dict | None = None,
    instance_spaces: list[str] | None = None,
    filter_updated_after: datetime | None = None,
    detection_mode: Literal["standard", "pattern", "all"] = "all",
    limit: int = 1000,
) -> list[dict]:
    """List CogniteDiagramAnnotation edges with server-side query filters."""
    from cognite.client import data_modeling as dm

    cfg = annotation_config or ANNOTATION_INDEX_CONFIG
    ann_space = cfg.get("view_space", "cdf_cdm")
    ann_view = cfg.get("view", "CogniteDiagramAnnotation")
    ann_version = cfg.get("version", "v1")
    mode_prop = cfg.get("detection_mode_property")
    server_detection_mode = (
        detection_mode if mode_prop and detection_mode in ("standard", "pattern") else None
    )

    view_id = dm.ViewId(
        space=ann_space,
        external_id=ann_view,
        version=ann_version,
    )
    property_names = annotation_select_property_names(cfg)
    results: list[dict] = []
    linked_files: dict[str, dict] = {}
    spaces: list[str | None]
    if instance_spaces:
        spaces = list(instance_spaces)
    else:
        spaces = [ann_space]

    for query_space in spaces:
        edge_iter = query_all_edges(
            client,
            view_id=view_id,
            property_names=property_names,
            edge_space=query_space,
            file_space=file_space,
            file_external_id=file_external_id,
            detection_mode_property=str(mode_prop) if mode_prop else None,
            detection_mode=server_detection_mode,
            text_property=cfg.get("text_property", "startNodeText"),
            filter_updated_after=filter_updated_after,
            page_size=min(limit, 1000),
            max_items=limit,
        )
        for inst in edge_iter:
            props = _view_properties(
                inst,
                view_space=ann_space,
                view=ann_view,
                version=ann_version,
            )
            inst_space = getattr(inst, "space", None) or ann_space
            f_space, f_ext = _edge_file_ref(inst, file_space)

            linked_file = None
            if f_ext and f_space:
                cache_key = f"{f_space}:{f_ext}"
                if cache_key not in linked_files:
                    try:
                        nodes = client.data_modeling.instances.retrieve_nodes(
                            [(f_space, f_ext)]
                        )
                        if nodes:
                            linked_files[cache_key] = {
                                "externalId": nodes[0].external_id,
                                "properties": _props_dict(nodes[0]),
                            }
                    except Exception:
                        linked_files[cache_key] = {}
                linked_file = linked_files.get(cache_key)

            end_space, end_ext = _edge_end_ref(inst)
            text = annotation_text(props, cfg)
            if not text:
                continue

            inferred_mode = detection_mode_from_annotation(
                props, inst.external_id, cfg=cfg
            )
            if detection_mode != "all" and inferred_mode != detection_mode:
                continue

            results.append(
                {
                    "externalId": inst.external_id,
                    "external_id": inst.external_id,
                    "space": inst_space,
                    "reference_type": ann_view,
                    "detection_mode": inferred_mode,
                    "file_external_id": f_ext,
                    "file_space": f_space,
                    "end_node_space": end_space,
                    "end_node_external_id": end_ext,
                    "linked_file": linked_file,
                    "properties": props,
                }
            )
            if len(results) >= limit:
                return results
    return results


def iter_view_instances(
    client: Any,
    *,
    view: str,
    view_space: str,
    version: str = "v1",
    batch_size: int = 1000,
    instance_spaces: list[str] | None = None,
    filter_updated_after: datetime | None = None,
    index_field_config: list[dict] | None = None,
    scope_config: dict | None = None,
) -> Iterator[dict]:
    """Yield DM node instances for a configured metadata view (server-side filtered)."""
    from cognite.client import data_modeling as dm

    field_cfg = index_field_config or INDEX_FIELD_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    view_id = dm.ViewId(space=view_space, external_id=view, version=version)
    paths = collect_view_property_paths(
        view_external_id=view,
        index_field_config=field_cfg,
        scope_config=scope_cfg,
    )
    property_names = top_level_property_names(paths) or ["name", "description"]
    spaces: list[str | None]
    if instance_spaces:
        spaces = list(instance_spaces)
    else:
        spaces = [None]

    for query_space in spaces:
        for inst in query_all_nodes(
            client,
            view_id=view_id,
            property_names=property_names,
            instance_space=query_space,
            filter_updated_after=filter_updated_after,
            page_size=batch_size,
        ):
            yield {
                "externalId": inst.external_id,
                "space": getattr(inst, "space", None) or view_space,
                "properties": _view_properties(
                    inst,
                    view_space=view_space,
                    view=view,
                    version=version,
                ),
            }
