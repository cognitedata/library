import json
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import TypeVar, cast

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.cdm.v1 import (
    CogniteDiagramAnnotation,
    CogniteDiagramAnnotationApply,
)
from cognite.client.exceptions import CogniteAPIError
from core.logger import CogniteFunctionLogger
from core.models import Config, DirectRelationMapping, State

T = TypeVar("T")


def chunker(items: Sequence[T], chunk_size: int) -> Iterable[Sequence[T]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def iterate_new_approved_annotations(
    state: State, client: CogniteClient, annotation_space: str, logger: CogniteFunctionLogger, chunk_size: int = 1000
) -> Iterable[list[CogniteDiagramAnnotation]]:
    query = create_query(state.last_cursor, annotation_space)
    try:
        result = client.data_modeling.instances.sync(query)
    except CogniteAPIError as e:
        if e.code == 400 and "Cursor has expired" in e.message:
            logger.warning("Cursor has expired, starting from the beginning")
            state.last_cursor = None
            query = create_query(state.last_cursor, annotation_space)
            result = client.data_modeling.instances.sync(query)
        else:
            raise
    edges = result["annotations"]
    logger.debug(f"Retrieved {len(edges)} new approved annotations")
    state.last_cursor = edges.cursor
    for edge_list in chunker(list(edges), chunk_size):
        yield [CogniteDiagramAnnotation._load(edge.dump()) for edge in edge_list]


def write_connections(
    annotation_by_source_by_property_by_view: dict[
        dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]
    ],
    client: CogniteClient,
    logger: CogniteFunctionLogger,
) -> int:
    connection_count = 0
    updated_nodes: list[dm.NodeApply] = []
    for view_id, annotation_by_source_by_property in annotation_by_source_by_property_by_view.items():
        node_ids = [node_id for node_id, _ in annotation_by_source_by_property.keys()]
        existing_node_list = client.data_modeling.instances.retrieve(node_ids, sources=[view_id]).nodes
        existing_node_by_id = {node.as_id(): node.as_write() for node in existing_node_list}

        for (node_id, direct_relation_property), direct_relation_ids in annotation_by_source_by_property.items():
            existing_node = existing_node_by_id.get(node_id)
            if existing_node is None:
                logger.warning(f"Node {node_id} not found in view {view_id}")
                continue
            for entity_source in existing_node.sources:
                if entity_source.source == view_id:
                    existing_connections = cast(list[dict], entity_source.properties.get(direct_relation_property, []))
                    before = len(existing_connections)
                    all_connections = {
                        dm.DirectRelationReference.load(connection) for connection in existing_connections
                    } | set(direct_relation_ids)
                    after = len(all_connections)
                    entity_source.properties[direct_relation_property] = [  # type: ignore[index]
                        connection.dump() for connection in all_connections
                    ]
                    connection_count += after - before
                    break
            updated_nodes.append(existing_node)

    updated = client.data_modeling.instances.apply(updated_nodes)
    logger.debug(f"Updated {len(updated.nodes)} nodes")
    return connection_count


def to_direct_relations_by_source_by_node(
    annotations: list[CogniteDiagramAnnotation], mappings: list[DirectRelationMapping], logger: CogniteFunctionLogger
) -> dict[dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]]:
    mapping_by_entity_source: dict[tuple[dm.ViewId, dm.ViewId], DirectRelationMapping] = {
        (mapping.start_node_view.as_view_id(), mapping.end_node_view.as_view_id()): mapping for mapping in mappings
    }
    annotation_by_source_by_node: dict[dm.ViewId, dict[tuple[dm.NodeId, str], list[dm.DirectRelationReference]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for annotation in annotations:
        try:
            source_context = json.loads(annotation.source_context)
        except json.JSONDecodeError:
            logger.error(f"Could not parse source context for annotation {annotation.external_id}")
            continue
        try:
            start_view = dm.ViewId.load(source_context["start"])
            end_view = dm.ViewId.load(source_context["end"])
        except KeyError:
            logger.error(f"Missing start or end in source context for annotation {annotation.external_id}")
            continue
        mapping = mapping_by_entity_source.get((start_view, end_view))
        if mapping is None:
            logger.warning(
                f"No mapping found for entity source {(start_view, end_view)} for annotation {annotation.external_id}"
            )
            continue
        if mapping.start_node_view.direct_relation_property is not None:
            update_node = annotation.start_node
            direct_relation_property = mapping.start_node_view.direct_relation_property
            other_side = annotation.end_node
            view_id = mapping.start_node_view.as_view_id()
        elif mapping.end_node_view.direct_relation_property is not None:
            update_node = annotation.end_node
            direct_relation_property = mapping.end_node_view.direct_relation_property
            other_side = annotation.start_node
            view_id = mapping.end_node_view.as_view_id()
        else:
            raise ValueError(
                f"Neither file source nor entity source has a direct relation property for annotation {annotation.external_id}"
            )
        node = dm.NodeId(update_node.space, update_node.external_id)
        annotation_by_source_by_node[view_id][(node, direct_relation_property)].append(
            dm.DirectRelationReference(other_side.space, other_side.external_id)
        )
    return annotation_by_source_by_node


def create_query(last_cursor: str | None, annotation_space: str) -> dm.query.Query:
    view_id = CogniteDiagramAnnotationApply.get_source()
    is_annotation = dm.filters.And(
        dm.filters.Equals(["edge", "space"], annotation_space),
        dm.filters.HasData(views=[view_id]),
        dm.filters.Equals(view_id.as_property_ref("status"), "Approved"),
    )
    return dm.query.Query(
        with_={
            "annotations": dm.query.EdgeResultSetExpression(
                from_=None,
                filter=is_annotation,
                limit=1000,
            )
        },
        select={
            "annotations": dm.query.Select(
                [
                    dm.query.SourceSelector(
                        source=CogniteDiagramAnnotationApply.get_source(), properties=["sourceContext"]
                    )
                ]
            )
        },
        cursors={"annotations": last_cursor},
    )


def load_config(client: CogniteClient, extraction_pipeline_external_id: str, logger: CogniteFunctionLogger) -> Config:
    raw_config = client.extraction_pipelines.config.retrieve(extraction_pipeline_external_id)
    if raw_config.config is None:
        raise ValueError(f"Config for extraction pipeline {extraction_pipeline_external_id} is empty")
    try:
        return Config.model_validate(yaml.safe_load(raw_config.config))
    except ValueError as e:
        logger.error(f"Invalid config: {e}")
        raise e

