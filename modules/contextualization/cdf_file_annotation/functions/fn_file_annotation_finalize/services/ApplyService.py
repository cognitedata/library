import abc
import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    ViewId,
    EdgeApply,
    NodeOrEdgeData,
    Node,
    NodeId,
    NodeApply,
    NodeApplyResultList,
    EdgeId,
    InstancesApplyResult,
)

from cognite.client.data_classes.filters import (
    In,
    Or,
)


from services.ConfigService import Config, ViewPropertyConfig
from utils.DataStructures import DiagramAnnotationStatus
from services.LoggerService import CogniteFunctionLogger


class IApplyService(abc.ABC):
    """
    Interface for applying/deleting annotations to a node
    """

    @abc.abstractmethod
    def apply_annotations(self, result_item: dict, file_node: Node) -> tuple[list, list]:
        pass

    @abc.abstractmethod
    def process_pattern_results(self, result_item: dict, file_node: Node) -> list[RowWrite]:
        pass

    @abc.abstractmethod
    def update_instances(
        self,
        list_node_apply: list[NodeApply] | NodeApply | None = None,
        list_edge_apply: list[EdgeApply] | EdgeApply | None = None,
    ) -> InstancesApplyResult:
        pass

    @abc.abstractmethod
    def delete_annotations_for_file(self, file_id: NodeId) -> tuple[list[str], list[str]]:
        pass


class GeneralApplyService(IApplyService):
    """
    Interface for applying/deleting annotations to a node
    """

    EXTERNAL_ID_LIMIT = 256

    FUNCTION_ID = "fn_file_annotation_finalize"

    def __init__(self, client: CogniteClient, config: Config, logger: CogniteFunctionLogger):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger

        self.core_annotation_view_id: ViewId = self.config.data_model_views.core_annotation_view.as_view_id()
        self.file_view_id: ViewId = self.config.data_model_views.file_view.as_view_id()
        self.file_annotation_type = config.data_model_views.file_view.annotation_type

        self.approve_threshold = self.config.finalize_function.apply_service.auto_approval_threshold
        self.suggest_threshold = self.config.finalize_function.apply_service.auto_suggest_threshold

    # NOTE: could implement annotation edges to be updated in batches for performance gains but leaning towards no. Since it will over complicate error handling.
    def apply_annotations(self, result_item: dict, file_node: Node) -> tuple[list[RowWrite], list[RowWrite]]:
        """
        Push the annotations to the file and set the "AnnotationInProcess" tag to "Annotated"
        """
        node_apply: NodeApply = file_node.as_write()
        node_apply.existing_version = None

        tags_property: list[str] = cast(list[str], node_apply.sources[0].properties["tags"])

        # NOTE: There are cases where the 'annotated' tag is set but a job was queued up again for the file.
        # This is because the rate at which the jobs are processed by finalize is slower than the rate at which launch fills up the queue.
        # So if the wait time that was set in the extractor config file goes passed the time it takes for the finalize function to get to the job. Annotate will appear in the tags list.
        if "AnnotationInProcess" in tags_property:
            index = tags_property.index("AnnotationInProcess")
            tags_property[index] = "Annotated"
        elif "Annotated" not in tags_property:
            raise ValueError("Annotated and AnnotationInProcess not found in tag property of file node")
        source_id: str | None = cast(str, file_node.properties[self.file_view_id].get("sourceId"))
        doc_doc, doc_tag = [], []
        edge_applies: list[EdgeApply] = []
        for detect_annotation in result_item["annotations"]:
            edge_apply_dict: dict[tuple, EdgeApply] = self._detect_annotation_to_edge_applies(
                file_node.as_id(),
                source_id,
                doc_doc,
                doc_tag,
                detect_annotation,
            )
            edge_applies.extend(edge_apply_dict.values())

        self.update_instances(list_node_apply=node_apply, list_edge_apply=edge_applies)

        return doc_doc, doc_tag

    def update_instances(
        self,
        list_node_apply: list[NodeApply] | NodeApply | None = None,
        list_edge_apply: list[EdgeApply] | EdgeApply | None = None,
    ) -> InstancesApplyResult:
        update_results: InstancesApplyResult = self.client.data_modeling.instances.apply(
            nodes=list_node_apply,
            edges=list_edge_apply,
            replace=False,  # ensures we don't delete other properties in the view
        )
        return update_results

    def _detect_annotation_to_edge_applies(
        self,
        file_instance_id: NodeId,
        source_id: str,
        doc_doc: list[RowWrite],
        doc_tag: list[RowWrite],
        detect_annotation: dict[str, Any],
    ) -> dict[tuple, EdgeApply]:

        # NOTE: Using a set to ensure uniqueness and solve the duplicate external edge ID problem
        diagram_annotations: dict[tuple, EdgeApply] = {}
        annotation_schema_space: str = self.config.data_model_views.core_annotation_view.schema_space

        for entity in detect_annotation["entities"]:
            if detect_annotation["confidence"] >= self.approve_threshold:
                annotation_status = DiagramAnnotationStatus.APPROVED.value
            elif detect_annotation["confidence"] >= self.suggest_threshold:
                annotation_status = DiagramAnnotationStatus.SUGGESTED.value
            else:
                continue

            external_id = self._create_annotation_id(
                file_instance_id,
                entity,
                detect_annotation["text"],
                detect_annotation,
            )

            doc_log = {
                "externalId": external_id,
                "startSourceId": source_id,
                "startNode": file_instance_id.external_id,
                "startNodeSpace": file_instance_id.space,
                "endNode": entity["external_id"],
                "endNodeSpace": entity["space"],
                "endNodeResourceType": entity["resource_type"],
                "viewId": self.core_annotation_view_id.external_id,
                "viewSpace": self.core_annotation_view_id.space,
                "viewVersion": self.core_annotation_view_id.version,
            }
            now = datetime.now(timezone.utc).replace(microsecond=0)

            annotation_properties = {
                "name": file_instance_id.external_id,
                "confidence": detect_annotation["confidence"],
                "status": annotation_status,
                "startNodePageNumber": detect_annotation["region"]["page"],
                "startNodeXMin": min(v["x"] for v in detect_annotation["region"]["vertices"]),
                "startNodeYMin": min(v["y"] for v in detect_annotation["region"]["vertices"]),
                "startNodeXMax": max(v["x"] for v in detect_annotation["region"]["vertices"]),
                "startNodeYMax": max(v["y"] for v in detect_annotation["region"]["vertices"]),
                "startNodeText": detect_annotation["text"],
                "sourceCreatedUser": self.FUNCTION_ID,
                "sourceUpdatedUser": self.FUNCTION_ID,
            }

            doc_log.update(annotation_properties)
            annotation_properties["sourceCreatedTime"] = now.isoformat()
            annotation_properties["sourceUpdatedTime"] = now.isoformat()

            edge_apply_instance = EdgeApply(
                space=file_instance_id.space,
                external_id=external_id,
                existing_version=None,
                type=DirectRelationReference(
                    space=annotation_schema_space,
                    external_id=entity["annotation_type"],
                ),
                start_node=DirectRelationReference(
                    space=file_instance_id.space,
                    external_id=file_instance_id.external_id,
                ),
                end_node=DirectRelationReference(space=entity["space"], external_id=entity["external_id"]),
                sources=[
                    NodeOrEdgeData(
                        source=self.core_annotation_view_id,
                        properties=annotation_properties,
                    )
                ],
            )

            edge_apply_key = self._get_edge_apply_unique_key(edge_apply_instance)
            if edge_apply_key not in diagram_annotations:
                diagram_annotations[edge_apply_key] = edge_apply_instance

            if entity["annotation_type"] == self.file_annotation_type:
                doc_doc.append(RowWrite(key=doc_log["externalId"], columns=doc_log))
            else:
                doc_tag.append(RowWrite(key=doc_log["externalId"], columns=doc_log))

        return diagram_annotations

    def _create_annotation_id(
        self,
        file_id: NodeId,
        entity: dict[str, Any],
        text: str,
        raw_annotation: dict[str, Any],
    ) -> str:
        hash_ = sha256(json.dumps(raw_annotation, sort_keys=True).encode()).hexdigest()[:10]
        naive = f"{file_id.space}:{file_id.external_id}:{entity['space']}:{entity['external_id']}:{text}:{hash_}"
        if len(naive) < self.EXTERNAL_ID_LIMIT:
            return naive

        prefix = f"{file_id.external_id}:{entity['external_id']}:{text}"
        shorten = f"{prefix}:{hash_}"
        if len(shorten) < self.EXTERNAL_ID_LIMIT:
            return shorten

        return prefix[: self.EXTERNAL_ID_LIMIT - 10] + hash_

    def delete_annotations_for_file(
        self,
        file_id: NodeId,
    ) -> tuple[list[str], list[str]]:
        """
        Delete all annotation edges for a file node.

        Args:
            client (CogniteClient): The Cognite client instance.
            annotation_view_id (ViewId): The ViewId of the annotation view.
            node (NodeId): The NodeId of the file node.
        """
        annotations = self._list_annotations_for_file(file_id)

        if not annotations:
            return [], []

        doc_annotations_delete: list[str] = []
        tag_annotations_delete: list[str] = []
        edge_ids = []
        for edge in annotations:
            edge_ids.append(EdgeId(space=file_id.space, external_id=edge.external_id))
            if edge.type.external_id == self.file_annotation_type:
                doc_annotations_delete.append(edge.external_id)
            else:
                tag_annotations_delete.append(edge.external_id)
        self.client.data_modeling.instances.delete(edges=edge_ids)

        return doc_annotations_delete, tag_annotations_delete

    def process_pattern_results(self, result_item: dict, file_node: Node) -> list[RowWrite]:
        if not result_item.get("annotations"):
            return []
        if not file_node:
            return []

        file_id: NodeId = file_node.as_id()
        source_id: str | None = cast(str, file_node.properties[self.file_view_id].get("sourceId"))

        # Step 1: Group all detections by their text content
        # The key is the detected tag text, e.g., "P-101A"
        aggregated_detections = {}

        for detect_annotation in result_item["annotations"]:
            tag_text = detect_annotation["text"]

            if tag_text not in aggregated_detections:
                # Initialize the entry for this tag if it's the first time we've seen it
                aggregated_detections[tag_text] = {
                    "regions": [],
                    "resource_type": "Unknown",  # Default resource_type
                }

            # Add the location of the current detection
            # The region dict contains page, vertices, etc.
            aggregated_detections[tag_text]["regions"].append(detect_annotation["region"])

            # Assume the resource_type is consistent for a given tag text
            if "entities" in detect_annotation and detect_annotation["entities"]:
                aggregated_detections[tag_text]["resource_type"] = detect_annotation["entities"][0].get(
                    "resource_type", "Unknown"
                )

        # Step 2: Create one RowWrite object for each unique tag
        doc_patterns: list[RowWrite] = []
        for tag_text, data in aggregated_detections.items():
            # The columns for the RAW table row
            catalog_properties = {
                "startSourceId": source_id,
                "startNode": file_id.external_id,
                "startNodeSpace": file_id.space,
                "text": tag_text,
                "resourceType": data["resource_type"],
                # Store the entire list of region dicts
                # Note: The RAW table will automatically serialize this list of dicts into a JSON string
                "regions": data["regions"],
                "sourceCreatedUser": self.FUNCTION_ID,
                "sourceUpdatedUser": self.FUNCTION_ID,
            }

            # Create a deterministic key based on the tag text and file
            row_key = f"{tag_text}_{source_id}"

            row = RowWrite(key=row_key, columns=catalog_properties)
            doc_patterns.append(row)

        return doc_patterns

    def _list_annotations_for_file(
        self,
        node_id: NodeId,
    ):
        """
        List all annotation edges for a file node.

        Args:
            client (CogniteClient): The Cognite client instance.
            annotation_view_id (ViewId): The ViewId of the annotation view.
            node (NodeId): The NodeId of the file node.

        Returns:
            list: A list of edges (annotations) linked to the file node.
        """
        annotations = self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view_id],
            space=node_id.space,
            filter=Or(In(["edge", "startNode"], [node_id])),
            limit=-1,
        )

        return annotations

    def _get_edge_apply_unique_key(self, edge_apply_instance: EdgeApply) -> tuple:
        """
        Create a hashable value for EdgeApply objects to use as a key for any hashable collection
        """
        start_node_key = (
            edge_apply_instance.start_node.space,
            edge_apply_instance.start_node.external_id,
        )
        end_node_key = (
            edge_apply_instance.end_node.space,
            edge_apply_instance.end_node.external_id,
        )
        type_key = (
            edge_apply_instance.type.space,
            edge_apply_instance.type.external_id,
        )
        return (start_node_key, end_node_key, type_key)
