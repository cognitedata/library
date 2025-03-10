from datetime import datetime
from typing import Optional
from uuid import uuid4

from cognite.client.data_classes.data_modeling import NodeId, ViewId
from industrial_canvas_v7 import CanvasClient  # type: ignore
from industrial_canvas_v7.data_classes import (
    CanvasAnnotationWrite,
    CanvasWrite,  # type: ignore
    DataRecordWrite,
    FdmInstanceContainerReferenceWrite,
)  # type: ignore


def create_canvas(
    client: CanvasClient, external_id: str, name: str, space: str, created_by: str
) -> CanvasWrite:
    canvas = CanvasWrite(
        externalId=external_id,
        name=name,
        space=space,
        createdBy=created_by,
        updatedBy=created_by,
        updatedAt=datetime.now(),
        visibility="public",
    )
    client.upsert(canvas)
    return canvas


def add_annotation(
    client: CanvasClient, canvas: CanvasWrite, text: str, x: int, y: int
) -> None:
    unique_id = str(uuid4())
    annotation = CanvasAnnotationWrite(
        space=canvas.space,
        externalId=f"{canvas.external_id}_{unique_id}",
        annotationType="causeMapNodeAnnotation",
        id=unique_id,
        isSelectable=True,
        isDraggable=True,
        isResizable=True,
        properties={
            "x": x,
            "y": y,
            "text": text,
            "style": {
                "color": "black",
                "padding": 4,
                "lineHeight": 1.2,
                "borderColor": "rgba(83, 88, 127, 0.24)",
                "borderWidth": 2,
                "borderRadius": 4,
                "backgroundColor": "rgb(255, 220, 127)",
            },
            "width": 200,
            "height": 200,
            "zIndex": 0,
        },
    )
    client.upsert(annotation)
    if not canvas.canvas_annotations:
        canvas.canvas_annotations = []
    canvas.canvas_annotations.append(annotation.as_id())

    # Ensure the canvas object is converted to the write version correctly
    canvas_write = canvas.as_write() if hasattr(canvas, "as_write") else canvas
    client.upsert(canvas_write)
    return unique_id


def add_cogniteasset(
    client: CanvasClient,
    canvas: CanvasWrite,
    x: int,
    y: int,
    node_id: NodeId,
    view_id: ViewId,
) -> None:
    unique_id = str(uuid4())
    instance_container_reference = FdmInstanceContainerReferenceWrite(
        space=canvas.space,
        externalId=f"{canvas.external_id}_{unique_id}",
        id=unique_id,
        containerReferenceType="fdmInstance",
        instanceExternalId=node_id.external_id,
        instanceSpace=node_id.space,
        viewExternalId=view_id.external_id,
        viewSpace=view_id.space,
        viewVersion=view_id.version,
        x=x,
        y=y,
        width=200,
        height=200,
        maxWidth=200,
        maxHeight=200,
        properties={"zIndex": 0},
        data_record=DataRecordWrite(),
    )
    client.upsert(instance_container_reference)
    if not canvas.fdm_instance_container_references:
        canvas.fdm_instance_container_references = []
    canvas.fdm_instance_container_references.append(instance_container_reference)
    client.upsert(canvas)


def add_polyline_annotation(
    client: CanvasClient,
    canvas: CanvasWrite,
    from_id: str,
    to_id: str,
    vertices: list,
    x: int,
    y: int,
) -> None:
    print(f"Adding polyline from {from_id} to {to_id}")  # Debug print
    unique_id = str(uuid4())
    annotation = CanvasAnnotationWrite(
        space=canvas.space,
        externalId=f"{canvas.external_id}_{unique_id}",
        annotationType="polylineAnnotation",
        id=unique_id,
        isSelectable=True,
        isDraggable=False,
        isResizable=False,
        properties={
            "fromId": from_id,
            "toId": to_id,
            "vertices": vertices,
            "style": {
                "stroke": "black",
                "opacity": 1,
                "lineType": "elbowed",
                "endEndType": "arrow",
                "strokeWidth": 2,
                "startEndType": "none",
                "shouldEnableStrokeScale": True,
            },
            "zIndex": 2,
            "anchorEndTo": "left",
            "anchorStartTo": "right",
        },
    )
    client.upsert(annotation)
    if not canvas.canvas_annotations:
        canvas.canvas_annotations = []
    canvas.canvas_annotations.append(annotation.as_id())

    canvas_write = canvas.as_write() if hasattr(canvas, "as_write") else canvas
    client.upsert(canvas_write)


def add_annotation_with_connection(
    client: CanvasClient,
    canvas: CanvasWrite,
    parent_id: str,
    annotation_text: str,
    base_x: int,
    base_y: int,
    failure_rate: Optional[float] = None,
    depth: int = 0,
    sibling_index: int = 0,
    sibling_count: int = 1,
    char_width: int = 10,  # Average character width
    line_height: int = 20,  # Line height
    padding: int = 10,  # Padding around the text
) -> str:
    """
    Adds an annotation dynamically spaced based on depth, sibling position, and applies a traffic light system.
    """
    # Calculate horizontal and vertical offsets dynamically
    x_spacing = 300  # Horizontal spacing between siblings
    y_spacing = 300  # Vertical spacing between levels

    x = base_x + (sibling_index - sibling_count // 2) * x_spacing
    y = base_y + depth * y_spacing

    # Calculate the width and height of the box based on the length of the text
    text_length = len(annotation_text)
    box_width = min(300, text_length * char_width + padding * 2)  # Adjust as needed
    box_height = line_height + padding * 2  # Single line height + padding

    background_color = "rgb(250, 250, 250)"  # Default color

    unique_id = str(uuid4())
    annotation = CanvasAnnotationWrite(
        space=canvas.space,
        externalId=f"{canvas.external_id}_{unique_id}",
        annotationType="causeMapNodeAnnotation",
        id=unique_id,
        isSelectable=True,
        isDraggable=True,
        isResizable=True,
        properties={
            "x": x,
            "y": y,
            "text": annotation_text,
            "style": {
                "color": "black",
                "padding": padding,
                "lineHeight": 1.2,
                "borderColor": "rgba(0, 0, 0, 1)",
                "borderWidth": 4,
                "borderRadius": 4,
                "backgroundColor": background_color,
                "overflow": "hidden",  # Ensure text does not overflow the box
                "textOverflow": "ellipsis",  # Add ellipsis for overflow text
                "whiteSpace": "normal",  # Allow text to wrap
                "wordWrap": "break-word",  # Break long words
            },
            "width": box_width,
            "height": box_height,
            "zIndex": 0,
        },
    )
    client.upsert(annotation)

    # Add annotation to the canvas
    if not canvas.canvas_annotations:
        canvas.canvas_annotations = []
    canvas.canvas_annotations.append(annotation.as_id())

    try:
        canvas_write = canvas.as_write() if hasattr(canvas, "as_write") else canvas
        client.upsert(canvas_write)
    except Exception as e:
        print(f"Error: {e}")

        for annotation in canvas.canvas_annotations:
            print(f"Annotation: {annotation}")

        raise e

    # Connect to parent using polyline
    if parent_id:
        vertices = [{"x": (x + base_x) // 2, "y": (y + base_y) // 2}]
        add_polyline_annotation(client, canvas, parent_id, unique_id, vertices, x, y)

    return unique_id
