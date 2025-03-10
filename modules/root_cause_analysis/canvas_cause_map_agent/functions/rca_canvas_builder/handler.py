import json
import os
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import NewType, Optional

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from dm_canvas import (
    CANVAS_INSTANCE_SPACE,
    CANVAS_VIEW_EXTERNAL_ID,
    CANVAS_VIEW_SPACE,
    CANVAS_VIEW_VERSION,
    add_resources_to_canvas,
    create_cause_map_annotation,
    create_cause_map_polyline,
)

Status = NewType("Status", str)

# Define status constants
PENDING = Status("PENDING")
FAILURE = Status("FAILURE")
SUCCESS = Status("SUCCESS")

# expected data structure: cause_map_<equipment_class_singular>.json;
# like this: /data/cause_map_pump.json
# not like this: /data/cause_map_pumps.json
DATA_DIR = Path(__file__).parent / "data"
SUPPORTED_EQUIPMENT_CLASSES = [
    file.stem.replace("cause_map_", "") for file in DATA_DIR.glob("cause_map_*.json")
]


@dataclass
class AgentInput:
    equipment_external_id: str
    equipment_instance_space: str
    equipment_class: Optional[str] = None
    equipment_view_space: Optional[str] = None
    equipment_view_external_id: Optional[str] = None
    equipment_view_version: Optional[str] = None
    canvas_name: Optional[str] = None
    canvas_external_id: Optional[str] = None
    failure_mode: Optional[str] = None

    @classmethod
    def load(cls, data: dict):
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class Response:
    status: Status = field(default=PENDING)
    _reasoning: list[str] = field(default_factory=list)

    @property
    def message(self) -> str:
        return self._reasoning[-1] if self._reasoning else "something went wrong"

    def add(self, message: str):
        self._reasoning.append(message)
        print(message)

    def failed(self, message: str | None = None) -> dict:
        if message:
            self.add(message)

        self.status = FAILURE
        return asdict(self)

    def succeeded(self, message: str | None = None) -> dict:
        if message:
            self.add(message)

        self.status = SUCCESS
        return asdict(self)


def handle(data: dict, client: CogniteClient) -> dict:
    start_time = time.time()
    print("Received data:", data)  # Debug print

    response = Response()

    agent_input = AgentInput.load(data)

    ### Get the canvas
    canvas_external_id = agent_input.canvas_external_id
    if canvas_external_id:
        result = client.data_modeling.instances.retrieve(
            nodes=[NodeId(CANVAS_INSTANCE_SPACE, canvas_external_id)],
            sources=ViewId(
                CANVAS_VIEW_SPACE, CANVAS_VIEW_EXTERNAL_ID, CANVAS_VIEW_VERSION
            ),
        )
        if result.nodes:
            canvas = result.nodes[0]
            response.add(f"Retrieved canvas: {canvas}")
        else:
            return response.failed(
                f"No canvas found with external_id: {canvas_external_id}"
            )

    else:
        return response.failed("No canvas id provided")

    ### Get Equipment
    if (
        not agent_input.equipment_external_id
        or not agent_input.equipment_instance_space
    ):
        return response.failed("No equipment details provided")

    node_id = NodeId(
        agent_input.equipment_instance_space, agent_input.equipment_external_id
    )
    view_id = ViewId(
        agent_input.equipment_view_space or "cdf_cdm",
        agent_input.equipment_view_external_id or "CogniteEquipment",
        agent_input.equipment_view_version or "v1",
    )

    response.add(f"Retrieving equipment details for {node_id} from {view_id}")

    result = client.data_modeling.instances.retrieve(
        nodes=[node_id], include_typing=False, sources=view_id
    )
    if not result.nodes:
        return response.failed("No equipment found with the given details")

    equipment = result.nodes[0]
    response.add(f"Retrieved equipment: {equipment}")

    equipment_class = None
    try:
        equipment_type_dict = equipment.properties.get(view=view_id)["equipmentType"]
        equipment_type_id = NodeId(
            equipment_type_dict.get("space"), equipment_type_dict.get("externalId")
        )
        equipment_type_node = client.data_modeling.instances.retrieve(
            nodes=equipment_type_id,
            sources=ViewId("cdf_cdm", "CogniteEquipmentType", "v1"),
        ).nodes[0]
        equipment_class = equipment_type_node.properties.get(
            view=ViewId("cdf_cdm", "CogniteEquipmentType", "v1")
        ).get("equipmentClass")
        response.add(f"Equipment class found on Equipment: {equipment_class}")
    except Exception as e:
        response.add(f"Failed to retrieve equipment class from equipment: {e}")

    if not equipment_class:
        if not agent_input.equipment_class:
            return response.failed(
                "Equipment class not found on equipment or provided in input"
            )
        else:
            equipment_class = agent_input.equipment_class

    # Normalize to singular if applicable
    if (
        equipment_class.endswith("s")
        and equipment_class[:-1] in SUPPORTED_EQUIPMENT_CLASSES
    ):
        response.add(f"Normalized equipment class to singular: {equipment_class}")
        equipment_class = equipment_class[:-1]

    if equipment_class not in SUPPORTED_EQUIPMENT_CLASSES:
        response.add(f"Supported equipment classes: {SUPPORTED_EQUIPMENT_CLASSES}")
        return response.failed(
            f"Unsupported equipment class: {agent_input.equipment_class}"
        )

    _, _, _, result = add_resources_to_canvas(
        client=client, canvas=canvas, resources=[equipment]
    )
    if result:
        response.add("Added equipment to canvas")

    ### Get failure mode (or use default)
    if agent_input.failure_mode:
        failure_mode = agent_input.failure_mode
        response.add(f"Using provided failure mode: {failure_mode}")
    else:
        failure_mode = "Abnormal instrument reading (AIR)"
        response.add("No failure mode provided. Using default failure mode (AIR).")

    file_path = DATA_DIR / f"cause_map_{equipment_class}.json"
    if not file_path.exists():
        return response.failed(
            f"Failed to load cause map template for Equipment class {equipment_class}"
        )

    with file_path.open() as f:
        cause_map = json.load(f)

    # Filter the cause map based on the provided failure mode
    matching_cause = cause_map.get(failure_mode)

    if not matching_cause:
        return response.failed(
            f"No matching category found for the cause: {failure_mode}"
        )

    ### Start adding hierarchical annotations
    root_x = 300
    root_y = 200
    stack = deque([(failure_mode, matching_cause, None, root_x, root_y)])

    annotation_nodes = []
    annotation_edges = []

    while stack:
        text, children, parent_id, base_x, base_y = stack.pop()

        # Skip "Failure Rate" annotations
        if text == "Failure Rate":
            continue

        failure_rate = None
        if isinstance(children, dict) and "Failure Rate" in children:
            failure_rate = children.pop("Failure Rate")

        # Create annotation and get its ID with sdk
        # annotation_id = add_annotation_with_connection(
        #     canvas_client, typed_canvas, parent_id, text, x, y, failure_rate
        # )

        # create node in cause map
        annotation_node_id, annotation_node, annotation_edge = (
            create_cause_map_annotation(
                canvas=canvas,
                annotation_text=text,
                base_x=base_x,
                base_y=base_y,
                failure_rate=failure_rate,
            )
        )
        annotation_nodes.append(annotation_node)
        annotation_edges.append(annotation_edge)

        # create polyline
        if parent_id:
            node_offset_x = (
                annotation_node.sources[0].properties.get("properties", {}).get("x")
            )
            node_offset_y = (
                annotation_node.sources[0].properties.get("properties", {}).get("y")
            )

            vertices = [{"x": node_offset_x, "y": node_offset_y}]
            polyline_node, polyline_edge = create_cause_map_polyline(
                canvas=canvas,
                from_id=parent_id,
                to_id=annotation_node_id,
                vertices=vertices,
            )
            annotation_nodes.append(polyline_node)
            annotation_edges.append(polyline_edge)

        # Process children nodes
        if isinstance(children, dict):
            for i, (child_text, child_value) in enumerate(children.items()):
                if child_text == "Failure Rate":
                    continue
                stack.append(
                    (
                        child_text,
                        child_value,
                        annotation_node_id,
                        base_x + 300,
                        base_y + i * 220,
                    )
                )
        elif isinstance(children, list):
            for i, child in enumerate(children):
                stack.append(
                    (child, None, annotation_node_id, base_x + 300, base_y + i * 220)
                )

    client.data_modeling.instances.apply(nodes=annotation_nodes, edges=annotation_edges)

    # Print execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Function execution time: {execution_time:.2f} seconds")

    return response.succeeded(f"Canvas populated for the failure mode: {failure_mode}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    config = ClientConfig(
        client_name="local",
        project=os.getenv("CDF_PROJECT"),
        credentials=OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{os.getenv('IDP_TENANT_ID')}/oauth2/v2.0/token",
            client_id=os.getenv("IDP_CLIENT_ID"),
            client_secret=os.getenv("IDP_CLIENT_SECRET"),
            scopes=[f"https://{os.getenv('CDF_CLUSTER')}.cognitedata.com/.default"],
        ),
        base_url=f"https://{os.getenv('CDF_CLUSTER')}.cognitedata.com/",
    )
    client = CogniteClient(config)

    response = handle(
        data={
            "canvas_external_id": "355e6966-6dad-4b27-b8cd-737717392d7f",
            "equipment_external_id": "23-HA-9115",
            "equipment_instance_space": "springfield_instances",
            # "equipment_class": "heat_exchangers",
        },
        client=client,
    )
    print(json.dumps(response, indent=4))
