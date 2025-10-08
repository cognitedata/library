from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeOrEdgeData, NodeApply, EdgeApply, ContainerId, ViewId, NodeId, EdgeId, Node, Edge
from cognite.client.data_classes.filters import Equals, And, Not
import datetime
import uuid
import streamlit as st

# Settings for the Industrial Canvas Data Model
CANVAS_SPACE_CANVAS = "cdf_industrial_canvas"
CANVAS_SPACE_INSTANCE = "IndustrialCanvasInstanceSpace"
CANVAS_CONTAINER_CANVAS = "Canvas"
CANVAS_CONTAINER_INSTANCE = "FdmInstanceContainerReference"
CANVAS_CONTAINER_ANNOTATION = "CanvasAnnotation"


def get_time():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_user_id(client: CogniteClient):
    if client:
        return client.iam.user_profiles.me().user_identifier
    return None


def generate_id():
    return str(uuid.uuid4())


def generate_properties(file_node: Node, file_view_id: ViewId, node_id: str, offset_x: int = 0, offset_y: int = 0):
    """Generates the property dictionary for a file node to be displayed on the canvas."""
    return {
        "id": node_id,
        "containerReferenceType": "fdmInstance",
        "label": file_node.properties[file_view_id].get("name", file_node.external_id),
        "x": offset_x,
        "y": offset_y,
        "width": 800,  # Increased default size for better viewing
        "height": 600,
        "maxWidth": 1600,
        "maxHeight": 1200,
        "instanceExternalId": file_node.external_id,
        "instanceSpace": file_node.space,
        "viewExternalId": file_view_id.external_id,
        "viewSpace": file_view_id.space,
        "viewVersion": file_view_id.version,
        "properties": {"zIndex": 0},
    }


def create_canvas(name: str, file_node: Node, client: CogniteClient):
    """Creates the main canvas node."""
    canvas_id = f"file_annotation_canvas_{file_node.external_id}"
    file_annotation_label = {"externalId": "file_annotations_solution_tag", "space": "SolutionTagsInstanceSpace"}
    canvas = NodeApply(
        space=CANVAS_SPACE_INSTANCE,
        external_id=canvas_id,
        sources=[
            NodeOrEdgeData(
                source=ContainerId(CANVAS_SPACE_CANVAS, CANVAS_CONTAINER_CANVAS),
                properties={
                    "name": name,
                    "visibility": "private",
                    "updatedAt": get_time(),
                    "createdBy": get_user_id(client),
                    "updatedBy": get_user_id(client),
                    "solutionTags": [file_annotation_label],
                },
            )
        ],
    )
    return canvas


def fetch_existing_canvas(name: str, file_node: Node, client: CogniteClient):
    existing_canvas = client.data_modeling.instances.retrieve(
        nodes=NodeId(space=CANVAS_SPACE_INSTANCE, external_id=f"file_annotation_canvas_{file_node.external_id}")
    )
   
    return existing_canvas.nodes[0] if existing_canvas.nodes else None


def create_objects(canvas_id: str, file_node: Node, file_view_id: ViewId):
    """Creates the node and edge for the file container, returning its ID."""
    file_container_id = f"file_annotation_file_container_{file_node.external_id}"
    properties = generate_properties(file_node, file_view_id, file_container_id)

    node_apply = NodeApply(
        space=CANVAS_SPACE_INSTANCE,
        external_id=f"{canvas_id}_{file_container_id}",
        sources=[
            NodeOrEdgeData(
                source=ContainerId(CANVAS_SPACE_CANVAS, CANVAS_CONTAINER_INSTANCE),
                properties=properties,
            )
        ],
    )

    edge_apply = EdgeApply(
        space=CANVAS_SPACE_INSTANCE,
        external_id=f"{canvas_id}_{canvas_id}_{file_container_id}",
        type=(CANVAS_SPACE_CANVAS, "referencesFdmInstanceContainerReference"),
        start_node=(CANVAS_SPACE_INSTANCE, canvas_id),
        end_node=(CANVAS_SPACE_INSTANCE, f"{canvas_id}_{file_container_id}"),
    )
    return [node_apply], [edge_apply], file_container_id


def create_bounding_box_annotations(canvas_id: str, file_container_id: str, unmatched_tags: list[dict]):
    """Creates annotation nodes and edges for unmatched tags."""
    annotation_nodes = []
    annotation_edges = []

    for tag_info in unmatched_tags:
        tag_text = tag_info["text"]
        regions = tag_info.get("regions", [])

        for region in regions:
            vertices = region.get("vertices", [])
            if not vertices:
                continue

            x_coords = [v["x"] for v in vertices]
            y_coords = [v["y"] for v in vertices]
            x_min, x_max = min(x_coords), max(x_coords)
            y_min, y_max = min(y_coords), max(y_coords)

            annotation_id = generate_id()
            properties = {
                "id": annotation_id,
                "annotationType": "rectangle",
                "containerId": file_container_id,  # <-- This is the crucial link
                "isSelectable": True,
                "isDraggable": True,
                "isResizable": True,
                "properties": {
                    "x": x_min,
                    "y": y_min,
                    "width": x_max - x_min,
                    "height": y_max - y_min,
                    "label": tag_text,
                    "zIndex": 10,
                    "style": {
                        "fill": "rgba(40, 167, 69, 0.3)",  # Semi-transparent vibrant green
                        "stroke": "rgb(40, 167, 69)",  # Solid vibrant green for the border
                        "strokeWidth": 1,
                        "opacity": 1,
                    },
                },
            }

            annotation_node = NodeApply(
                space=CANVAS_SPACE_INSTANCE,
                external_id=f"{canvas_id}_{annotation_id}",
                sources=[
                    NodeOrEdgeData(
                        source=ContainerId(CANVAS_SPACE_CANVAS, CANVAS_CONTAINER_ANNOTATION),
                        properties=properties,
                    )
                ],
            )
            annotation_nodes.append(annotation_node)

            annotation_edge = EdgeApply(
                space=CANVAS_SPACE_INSTANCE,
                external_id=f"{canvas_id}_{canvas_id}_{annotation_id}",
                type=(CANVAS_SPACE_CANVAS, "referencesCanvasAnnotation"),
                start_node=(CANVAS_SPACE_INSTANCE, canvas_id),
                end_node=(CANVAS_SPACE_INSTANCE, f"{canvas_id}_{annotation_id}"),
            )
            annotation_edges.append(annotation_edge)

    return annotation_nodes, annotation_edges


def dm_generate(
    name: str, file_node: Node, file_view_id: ViewId, client: CogniteClient, unmatched_tags_with_regions: list = []
):
    """Orchestrates the creation of the canvas, its objects, and bounding box annotations."""
    canvas = fetch_existing_canvas(name, file_node, client)

    if canvas:
        file_container_id = f"file_annotation_file_container_{file_node.external_id}"
        reset_canvas_annotations(canvas.external_id, client)
        nodes = []
        edges = []
    else:
        canvas = create_canvas(name, file_node, client)
        nodes, edges, file_container_id = create_objects(canvas.external_id, file_node, file_view_id)

    canvas_id = canvas.external_id

    if unmatched_tags_with_regions:
        annotation_nodes, annotation_edges = create_bounding_box_annotations(
            canvas_id, file_container_id, unmatched_tags_with_regions
        )
        nodes.extend(annotation_nodes)
        edges.extend(annotation_edges)

    client.data_modeling.instances.apply(nodes=[canvas] + nodes, edges=edges)
    st.session_state["canvas_id"] = canvas_id
    return canvas_id


def reset_canvas_annotations(canvas_id: str, client: CogniteClient):
    """Deletes all canvas annotations, which includes nodes and edges"""
    edge_filter = And(
        Equals(property=['edge', 'type'], value={'space': CANVAS_SPACE_CANVAS, 'externalId': 'referencesCanvasAnnotation'}),
        Equals(property=['edge', 'startNode'], value={'space': CANVAS_SPACE_INSTANCE, 'externalId': canvas_id})
    )

    edges_to_delete = client.data_modeling.instances.list(
        instance_type="edge",
        filter=edge_filter,
        limit=-1,
    )

    edges_to_delete_ids = [EdgeId(space=e.space, external_id=e.external_id) for e in edges_to_delete]
    nodes_to_delete_ids = [NodeId(space=e.end_node.space, external_id=e.end_node.external_id) for e in edges_to_delete]

    if edges_to_delete_ids:
        client.data_modeling.instances.delete(
            edges=edges_to_delete_ids
        )

    if nodes_to_delete_ids:
        client.data_modeling.instances.delete(
            nodes=nodes_to_delete_ids
        )        