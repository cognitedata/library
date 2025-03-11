import datetime
import uuid
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import ClassVar, Literal, Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelationReference,
    EdgeApply,
    Node,
    NodeApply,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.query import (
    EdgeResultSetExpression,
    NodeResultSetExpression,
    Query,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import And, ContainsAny, Equals, HasData

# Canvas Data Model
CANVAS_VIEW_SPACE = "cdf_industrial_canvas"
CANVAS_VIEW_EXTERNAL_ID = "Canvas"
CANVAS_VIEW_VERSION = "v7"
CANVAS_DM_REFERENCE_EXTERNAL_ID = "FdmInstanceContainerReference"
CANVAS_DM_TYPE_REFERENCE = "referencesFdmInstanceContainerReference"
CANVAS_INSTANCE_SPACE = "IndustrialCanvasInstanceSpace"
CANVAS_ANNOTATIONS_EXTERNAL_ID = "CanvasAnnotation"
CANVAS_ANNOTATION_TYPE_REFERENCE = "referencesCanvasAnnotation"

CORE_DATA_MODEL_SPACE = "cdf_cdm"
CORE_DATA_MODEL_VIEW_VERSION = "v1"


class DmConfig:
    # Canvas Data Model

    @staticmethod
    def get_view_id(view_name: str) -> ViewId:
        return ViewId(
            DmConfig.CORE_EQUIPMENT_MODEL_SPACE,
            view_name,
            DmConfig.CORE_EQUIPMENT_MODEL_VIEW_VERSION,
        )

    @staticmethod
    def get_core_view_id(view_name: str) -> ViewId:
        return ViewId(
            DmConfig.CORE_DATA_MODEL_SPACE,
            view_name,
            DmConfig.CORE_DATA_MODEL_VIEW_VERSION,
        )


class Utils:
    @staticmethod
    def _get_time(timestamp: Optional[int] = None):
        if timestamp and timestamp > 0:
            dt = datetime.datetime.fromtimestamp(timestamp / 1000)
        else:
            dt = datetime.datetime.now(datetime.timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class CanvasFdmObject:
    # These are the properties that should be shown in the canvas
    property_filter: ClassVar[Optional[list[str]]] = None

    instanceExternalId: Optional[str] = None
    instanceSpace: Optional[str] = None
    viewSpace: str = CORE_DATA_MODEL_SPACE
    viewExternalId: str = "CanvasObject"
    viewVersion: str = CORE_DATA_MODEL_VIEW_VERSION
    containerReferenceType: Optional[Literal["fdmInstance"]] = "fdmInstance"
    label: Optional[str] = "No label defined"
    id: Optional[str] = None
    x: Optional[int] = 0
    y: Optional[int] = 0
    width: int = 600
    height: int = 356
    properties: dict = field(default_factory=lambda: {"zIndex": 0})

    def dump(self) -> dict:
        return asdict(self)


@dataclass
class CanvasCogniteAsset(CanvasFdmObject):
    # These are the properties that should be shown in the canvas
    property_filter: ClassVar[Optional[list[str]]] = [
        "name",
        "description",
        "sourceId",
        "source",
        "parent",
        "root",
        "assetClass",
        "files",
        "children",
        "equipment",
        "activities",
        "timeSeries",
    ]

    viewExternalId: str = "CogniteAsset"
    properties: dict = field(
        default_factory=lambda: {
            "unscaledWidth": 600,
            "unscaledHeight": 356,
            "zIndex": 0,
        }
    )


@dataclass
class CanvasCogniteEvent(CanvasFdmObject):
    viewExternalId: str = "CogniteEvent"
    width: int = 600
    height: int = 500


@dataclass
class CanvasCogniteTimeSeries(CanvasFdmObject):
    viewExternalId: str = "CogniteTimeSeries"
    width: int = 700
    height: int = 400
    # start_timestamp: Optional[int] = 0
    # end_timestamp: Optional[int] = 0

    # def __post_init__(self):
    #     self.properties.update(
    #         {
    #             "startDate": Utils._get_time(self.start_timestamp),
    #             "endDate": Utils._get_time(self.end_timestamp),
    #         }
    #     )


@dataclass
class CanvasCogniteFile(CanvasFdmObject):
    viewExternalId: str = "CogniteFile"
    width: int = 1200
    height: int = 1200
    maxWidth: int = field(init=False)
    maxHeight: int = field(init=False)

    def __post_init__(self):
        self.maxWidth = self.width if self.width != 1200 else 1200
        self.maxHeight = self.height if self.height != 1200 else 1200
        self.properties.update({"page": 1})


@dataclass
class CanvasCogniteMaintenanceOrder(CanvasFdmObject):
    viewExternalId: str = "CogniteMaintenanceOrder"


@dataclass
class CanvasThreedObject(CanvasFdmObject):
    viewExternalId: str = "ThreedObject"
    width: int = 600
    height: int = 400
    initial_asset_id: Optional[int] = 0

    def __post_init__(self):
        self.properties.update(
            {
                "initialAssetId": self.initial_asset_id,
            }
        )


@dataclass
class CanvasCausemapNode:
    annotationType: Literal["causeMapNodeAnnotation"]
    id: str
    isSelectable: bool = True
    isDraggable: bool = True
    isResizable: bool = True
    properties: dict = field(
        default_factory=lambda: {
            "x": 0,
            "y": 0,
            "text": "",
            "style": {
                "color": "black",
                "padding": 10,
                "lineHeight": 1.2,
                "borderColor": "rgba(0, 0, 0, 1)",
                "borderWidth": 4,
                "borderRadius": 4,
                "backgroundColor": "rgb(250, 250, 250)",
                "overflow": "hidden",
                "textOverflow": "ellipsis",
                "whiteSpace": "normal",
                "wordWrap": "break-word",
            },
            "width": 200,
            "height": 100,
            "zIndex": 0,
        }
    )

    def dump(self) -> dict:
        return asdict(self)


@dataclass
class CanvasCausemapPolyline:
    annotationType: Literal["polylineAnnotation"]
    id: str
    isSelectable: bool = True
    isDraggable: bool = False
    isResizable: bool = False
    properties: dict = field(
        default_factory=lambda: {
            "toId": "",
            "fromId": "",
            "startEndType": "arrow",
            "endEndType": "none",
            "style": {
                "stroke": "black",
                "opacity": 1,
                "lineType": "elbowed",
                "strokeWidth": 1,
                "shouldEnableStrokeScale": "true",
            },
            "vertices": [{"x": 0, "y": 0}],
            "zIndex": 0,
        }
    )

    def dump(self) -> dict:
        return asdict(self)


@dataclass
class CustomCanvasObject(CanvasFdmObject):
    viewSpace: str = CORE_DATA_MODEL_SPACE
    viewVersion: str = CORE_DATA_MODEL_VIEW_VERSION
    properties: dict = field(
        default_factory=lambda: {
            "unscaledWidth": 600,
            "unscaledHeight": 356,
            "zIndex": 1,
        }
    )


@dataclass
class CanvasTag(CustomCanvasObject):
    property_filter: ClassVar[Optional[list[str]]] = [
        "name",
        "description",
        "sourceId",
        "parent",
        "root",
        "files",
        "children",
    ]

    viewExternalId: str = "Tag"


@dataclass
class CanvasEquipment(CustomCanvasObject):
    property_filter: ClassVar[Optional[list[str]]] = [
        "name",
        "description",
        "sourceId",
        "source",
        "parent",
        "root",
        "assetClass",
        "files",
        "children",
        "equipment",
        "activities",
        "timeSeries",
    ]

    viewExternalId: str = "Equipment"


@dataclass
class CanvasWorkOrder(CustomCanvasObject):
    viewExternalId: str = "WorkOrder"


@dataclass
class CanvasPart(CustomCanvasObject):
    property_filter: ClassVar[Optional[list[str]]] = [
        "uid",
        "description",
        "spareId",
        "name",
    ]

    viewExternalId: str = "Part"


@dataclass
class CanvasTask(CustomCanvasObject):
    viewExternalId: str = "Task"


def get_canvas_object_subclasses() -> dict[str, type[CanvasFdmObject]]:
    return {
        canvas_class.viewExternalId: canvas_class
        for canvas_class in [
            *CanvasFdmObject.__subclasses__(),
            *CustomCanvasObject.__subclasses__(),
        ]
    }


_CANVAS_OBJECT_SUBCLASSES = get_canvas_object_subclasses()


def get_cls_by_type_name(type_: str) -> type[CanvasFdmObject]:
    if type_ in _CANVAS_OBJECT_SUBCLASSES:
        return _CANVAS_OBJECT_SUBCLASSES[type_]
    else:
        return CustomCanvasObject


def get_user_id(client: CogniteClient) -> Optional[str]:
    if client:
        return client.iam.user_profiles.me().user_identifier
    else:
        return None


def generate_id() -> str:
    return str(uuid.uuid4())


def create_canvas(
    client: CogniteClient,
    canvas_name: str,
    visibility: Literal["private", "public"] = "private",
) -> tuple[NodeApply, str]:
    canvas_id = generate_id()
    properties = {
        "name": canvas_name,
        "visibility": visibility,
        "updatedAt": Utils._get_time(),
        "createdBy": get_user_id(client),
        "updatedBy": get_user_id(client),
    }
    filters = []
    for class_ in [
        *CanvasFdmObject.__subclasses__(),
        *CustomCanvasObject.__subclasses__(),
    ]:
        if class_.property_filter:
            filters.append(
                {
                    "appliesToInstanceType": {
                        "source": "fdm",
                        "viewSpace": class_.viewSpace,
                        "viewExternalId": class_.viewExternalId,
                        "viewVersion": class_.viewVersion,
                    },
                    "properties": class_.property_filter,
                }
            )
    if len(filters) > 0:
        properties["context"] = [{"type": "FILTERS", "payload": {"filters": filters}}]

    canvas = NodeApply(
        space=CANVAS_INSTANCE_SPACE,
        external_id=canvas_id,
        sources=[
            NodeOrEdgeData(
                source=ContainerId(CANVAS_VIEW_SPACE, CANVAS_VIEW_EXTERNAL_ID),
                properties=properties,
            )
        ],
    )
    return canvas, canvas_id


def _create_canvas_content(
    canvas_id: str, resources: Iterable[Union[CogniteResource, Node]]
) -> tuple[list[NodeApply], list[EdgeApply]]:
    print("Creating objects")
    nodes = []
    edges = []
    offset_x = 0
    offset_y = 0
    prev_height = 0
    offsets = {}

    for resource in resources:
        if isinstance(resource, Node):
            view = next(iter(resource.properties.items()))[0]
            canvas_class = get_cls_by_type_name(view.external_id)
        else:
            raise NotImplementedError("Only Node instances are supported at the moment")
        view_properties = next(iter(resource.properties.values()))
        name = str(view_properties.get("name", "No name defined"))
        description = view_properties.get("description", None)
        label = f"{name} - {description}" if description else name
        object_id = generate_id()
        canvas_object: CanvasFdmObject = canvas_class(
            viewExternalId=view.external_id,
            instanceExternalId=resource.external_id,
            instanceSpace=resource.space,
            id=object_id,
            label=label,
        )
        dm_reference_id = f"{canvas_id}_{object_id}"
        if canvas_object.viewExternalId not in offsets:
            offset_y = max(
                (offsets[key]["y"] + prev_height for key in offsets), default=offset_y
            )
            offsets[canvas_object.viewExternalId] = {"x": 0, "y": offset_y}

        offset_x = offsets[canvas_object.viewExternalId]["x"]
        offset_y = offsets[canvas_object.viewExternalId]["y"]

        canvas_object.x = offset_x
        canvas_object.y = offset_y

        offsets[canvas_object.viewExternalId]["x"] = offset_x + canvas_object.width + 20
        prev_height = canvas_object.height + 20
        dm_reference = NodeApply(
            space=CANVAS_INSTANCE_SPACE,
            external_id=dm_reference_id,
            sources=[
                NodeOrEdgeData(
                    source=ContainerId(
                        CANVAS_VIEW_SPACE, CANVAS_DM_REFERENCE_EXTERNAL_ID
                    ),
                    properties=canvas_object.dump(),
                )
            ],
        )
        nodes.append(dm_reference)
        edges.append(
            EdgeApply(
                space=CANVAS_INSTANCE_SPACE,
                external_id=f"{canvas_id}_{dm_reference_id}",
                type=DirectRelationReference(
                    CANVAS_VIEW_SPACE, CANVAS_DM_TYPE_REFERENCE
                ),
                start_node=DirectRelationReference(CANVAS_INSTANCE_SPACE, canvas_id),
                end_node=DirectRelationReference(
                    CANVAS_INSTANCE_SPACE, dm_reference_id
                ),
            )
        )

    return nodes, edges


def add_resources_to_canvas(
    client: CogniteClient,
    resources: Iterable[Union[CogniteResource, Node]],
    canvas: Node,
    visibility: Literal["private", "public"] = "private",
):
    nodes, edges = _create_canvas_content(
        canvas_id=canvas.external_id, resources=resources
    )
    result = client.data_modeling.instances.apply([canvas, *nodes], edges)
    # result = []
    return canvas.external_id, nodes, edges, result


def create_cause_map_annotation(
    canvas: Node,
    annotation_text: str,
    base_x: Optional[int] = 200,
    base_y: Optional[int] = 300,
    failure_rate: Optional[float] = None,
    depth: int = 0,
    sibling_index: int = 0,
    sibling_count: int = 1,
    char_width: int = 10,  # Average character width
    line_height: int = 20,  # Line height
    padding: int = 10,  # Padding around the text
) -> tuple[str, NodeApply, EdgeApply]:
    """
    Adds an annotation dynamically spaced based on depth, sibling position, and applies a traffic light system.
    """
    # Calculate horizontal and vertical offsets dynamically
    x_spacing = 300  # Horizontal spacing between siblings
    y_spacing = 300  # Vertical spacing between levels

    offset_x = base_x + (sibling_index - sibling_count // 2) * x_spacing
    offset_y = base_y or 0 + depth * y_spacing

    # Calculate the width and height of the box based on the length of the text
    text_length = len(annotation_text)
    box_width = min(300, text_length * char_width + padding * 2)  # Adjust as needed
    box_height = line_height + padding * 2  # Single line height + padding

    background_color = "rgb(250, 250, 250)"  # Default color

    node_id = str(uuid.uuid4())
    node_external_id = f"{canvas.external_id}_{node_id}"
    causemapNode: CanvasCausemapNode = CanvasCausemapNode(
        annotationType="causeMapNodeAnnotation",
        id=node_id,
        properties={
            "x": offset_x,
            "y": offset_y,
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

    annotation_node = NodeApply(
        space=canvas.space,
        external_id=node_external_id,
        sources=[
            NodeOrEdgeData(
                source=ContainerId(CANVAS_VIEW_SPACE, CANVAS_ANNOTATIONS_EXTERNAL_ID),
                properties=causemapNode.dump(),
            )
        ],
    )

    annotation_edge = EdgeApply(
        space=CANVAS_INSTANCE_SPACE,
        external_id=f"{canvas.external_id}_{node_external_id}",
        type=DirectRelationReference(
            CANVAS_VIEW_SPACE, CANVAS_ANNOTATION_TYPE_REFERENCE
        ),
        start_node=DirectRelationReference(CANVAS_INSTANCE_SPACE, canvas.external_id),
        end_node=DirectRelationReference(CANVAS_INSTANCE_SPACE, node_external_id),
    )
    return node_id, annotation_node, annotation_edge


def create_cause_map_polyline(
    canvas: Node,
    from_id: str,
    to_id: str,
    vertices: list[dict[str, int]],
) -> tuple[NodeApply, EdgeApply]:
    polyline_id = str(uuid.uuid4())
    polyline_external_id = f"{canvas.external_id}_{polyline_id}"

    polyline: CanvasCausemapPolyline = CanvasCausemapPolyline(
        annotationType="polylineAnnotation",
        id=polyline_id,
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

    polyline_node = NodeApply(
        space=canvas.space,
        external_id=polyline_external_id,
        sources=[
            NodeOrEdgeData(
                source=ContainerId(CANVAS_VIEW_SPACE, CANVAS_ANNOTATIONS_EXTERNAL_ID),
                properties=polyline.dump(),
            )
        ],
    )

    polyline_edge = EdgeApply(
        space=CANVAS_INSTANCE_SPACE,
        external_id=f"{canvas.external_id}_{polyline_external_id}",
        type=DirectRelationReference(
            CANVAS_VIEW_SPACE, CANVAS_ANNOTATION_TYPE_REFERENCE
        ),
        start_node=DirectRelationReference(CANVAS_INSTANCE_SPACE, canvas.external_id),
        end_node=DirectRelationReference(CANVAS_INSTANCE_SPACE, polyline_external_id),
    )
    return polyline_node, polyline_edge

    pass


def create_canvas_for_tag(
    client: CogniteClient,
    tag_name: str,
    canvas_name: str,
):
    """
    Create a canvas for a given tag name, including related entities found
    through traversing the knowledge graph.

    Currently supported entities are
    Tag, WorkOrder, Task, Part, SpareInventory, CogniteTimeSeries, CogniteFile
    """

    canvas_content = []
    node_result_sets = {
        "Tag": NodeResultSetExpression(
            filter=And(
                HasData(views=[DmConfig.get_view_id("Tag")]),
                Equals(
                    property=DmConfig.get_view_id("Tag").as_property_ref("name"),
                    value=tag_name,
                ),
            ),
            limit=100,
        ),
        "WorkOrder": NodeResultSetExpression(
            from_="Tag",
            through=DmConfig.get_view_id("WorkOrder").as_property_ref("tag"),
            direction="inwards",
            filter=HasData(views=[DmConfig.get_view_id("WorkOrder")]),
            limit=100,
        ),
        "Task": NodeResultSetExpression(
            from_="WorkOrder",
            through=DmConfig.get_view_id("Task").as_property_ref("workOrder"),
            direction="inwards",
            filter=HasData(views=[DmConfig.get_view_id("Task")]),
            limit=100,
        ),
        "Part": NodeResultSetExpression(
            from_="TagToPart",
            filter=HasData(views=[DmConfig.get_view_id("Part")]),
            limit=100,
        ),
        "SpareInventory": NodeResultSetExpression(
            from_="Part",
            through=DmConfig.get_view_id("SpareInventory").as_property_ref("part"),
            direction="inwards",
            filter=HasData(views=[DmConfig.get_view_id("SpareInventory")]),
            limit=100,
        ),
    }
    result = client.data_modeling.instances.query(
        Query(
            with_={
                **node_result_sets,
                "TagToPart": EdgeResultSetExpression(
                    from_="Tag",
                    direction="inwards",
                    filter=Equals(
                        property=["edge", "type"],
                        value={
                            "space": DmConfig.CORE_EQUIPMENT_MODEL_SPACE,
                            "externalId": "Part.tags",
                        },
                    ),
                    limit=100,
                ),
            },
            select={
                key: Select([SourceSelector(DmConfig.get_view_id(key), [])])
                for key in node_result_sets
            },
        ),
        include_typing=True,
    )
    for view_name in node_result_sets:
        print(view_name, len(result.get_nodes(view_name)))
        canvas_content.extend(result.get_nodes(view_name))

    value_search_list = []
    for tag_type in ("tag", "equipment", "mainEquipment", "instrument"):
        for space in (
            "sp_inst_domain_uny",
            "temp_sp_inst_domain_uny",
        ):  # FIXME: Remove hardcoded spaces when the data model is finalized
            value_search_list.append(
                {
                    "space": space,
                    "externalId": f"{tag_type}_{tag_name}",
                }
            )

    for resource_type in ("CogniteTimeSeries", "CogniteFile"):
        resource_list = client.data_modeling.instances.search(
            DmConfig.get_core_view_id(resource_type),
            filter=ContainsAny(
                property=DmConfig.get_core_view_id(resource_type).as_property_ref(
                    "assets"
                ),
                values=value_search_list,
            ),
            limit=100,
        )
        print(resource_type, len(resource_list))
        canvas_content.extend(resource_list)

    canvas_id, nodes, edges, result = add_resources_to_canvas(
        client, canvas_content, canvas_name, "public"
    )
    print(f"Canvas (id {canvas_id}) created with {len(nodes)} nodes")
    return canvas_id


# if __name__ == "__main__":
#     from dotenv import load_dotenv

#     load_dotenv()
#     config = ClientConfig(
#         client_name="local",
#         project=os.getenv("CDF_PROJECT"),
#         credentials=OAuthClientCredentials(
#             token_url=os.getenv("IDP_TOKEN_URL"),
#             client_id=os.getenv("IDP_CLIENT_ID"),
#             client_secret=os.getenv("IDP_CLIENT_SECRET"),
#             scopes=[f"https://{os.getenv('CDF_CLUSTER')}.cognitedata.com/.default"],
#         ),
#         base_url=f"https://{os.getenv('CDF_CLUSTER')}.cognitedata.com/",
#     )
#     client = CogniteClient(config)

#     create_canvas_for_tag(
#         client=client,
#         tag_name="771-VEST-0510",
#         canvas_name="Canvas_autogenerate_test_13",
#     )
