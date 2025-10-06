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
    EdgeId,
    InstancesApplyResult,
)
from cognite.client.data_classes.filters import And, Equals, Not
from cognite.client import data_modeling as dm

from services.ConfigService import Config, ViewPropertyConfig
from utils.DataStructures import DiagramAnnotationStatus
from services.LoggerService import CogniteFunctionLogger


class IApplyService(abc.ABC):
    """
    Interface for applying/deleting annotations to a node
    """

    @abc.abstractmethod
    def process_and_apply_annotations_for_file(
        self, file_node: Node, regular_item: dict | None, pattern_item: dict | None, clean_old: bool
    ) -> tuple[str, str]:
        pass

    @abc.abstractmethod
    def update_instances(
        self,
        list_node_apply: list[NodeApply] | NodeApply | None = None,
        list_edge_apply: list[EdgeApply] | EdgeApply | None = None,
    ) -> InstancesApplyResult:
        pass


class GeneralApplyService(IApplyService):
    """
    Implementation of the ApplyService interface.
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

        self.sink_node_id = DirectRelationReference(
            space=config.finalize_function.apply_service.sink_node.space,
            external_id=config.finalize_function.apply_service.sink_node.external_id,
        )

    def process_and_apply_annotations_for_file(
        self, file_node: Node, regular_item: dict | None, pattern_item: dict | None, clean_old: bool
    ) -> tuple[str, str]:
        """
        Performs the entire annotation transaction for a single file.
        """
        file_id = file_node.as_id()
        source_id: str | None = cast(str, file_node.properties[self.file_view_id].get("sourceId"))

        # Step 1: Clean old annotations if required
        if clean_old:
            deleted_counts = self._delete_annotations_for_file(file_id)
            self.logger.info(
                f"\t- Deleted {deleted_counts['doc']} doc and {deleted_counts['tag']} tag annotations\n\t- Deleted {deleted_counts['pattern']} pattern annotations."
            )

        # Step 2: Process and apply regular annotations
        regular_edges, doc_rows, tag_rows = [], [], []
        if regular_item and regular_item.get("annotations"):
            for annotation in regular_item["annotations"]:
                edges = self._detect_annotation_to_edge_applies(file_id, source_id, doc_rows, tag_rows, annotation)
                regular_edges.extend(edges.values())

        # Step 3: Process and apply pattern annotations
        pattern_edges, pattern_rows = [], []
        if pattern_item and pattern_item.get("annotations"):
            pattern_edges, pattern_rows = self._process_pattern_results(pattern_item, file_node)

        # Step 4: Apply all changes in batches
        node_apply = file_node.as_write()
        node_apply.existing_version = None
        tags = cast(list[str], node_apply.sources[0].properties["tags"])
        if "AnnotationInProcess" in tags:
            tags[tags.index("AnnotationInProcess")] = "Annotated"
        elif "Annotated" not in tags:
            raise ValueError("Annotated and AnnotationInProcess not found in tag property")

        self.update_instances(list_node_apply=node_apply, list_edge_apply=regular_edges + pattern_edges)

        if doc_rows:
            self.client.raw.rows.insert(
                db_name=self.config.finalize_function.report_service.raw_db,
                table_name=self.config.finalize_function.report_service.raw_table_doc_doc,
                row=doc_rows,
                ensure_parent=True,
            )
        if tag_rows:
            self.client.raw.rows.insert(
                db_name=self.config.finalize_function.report_service.raw_db,
                table_name=self.config.finalize_function.report_service.raw_table_doc_tag,
                row=tag_rows,
                ensure_parent=True,
            )
        if pattern_rows:
            self.client.raw.rows.insert(
                db_name=self.config.finalize_function.report_service.raw_db,
                table_name=self.config.finalize_function.report_service.raw_table_doc_pattern,
                row=pattern_rows,
                ensure_parent=True,
            )

        annotation_msg = f"Applied {len(doc_rows)} doc and {len(tag_rows)} tag annotations."
        pattern_msg = f"Applied {len(pattern_rows)} pattern detections."

        return annotation_msg, pattern_msg

    def update_instances(
        self,
        list_node_apply: list[NodeApply] | NodeApply | None = None,
        list_edge_apply: list[EdgeApply] | EdgeApply | None = None,
    ) -> InstancesApplyResult:
        return self.client.data_modeling.instances.apply(nodes=list_node_apply, edges=list_edge_apply, replace=False)

    def _delete_annotations_for_file(self, file_id: NodeId) -> dict[str, int]:
        """Deletes all standard and pattern edges and their corresponding RAW rows for a file."""
        counts = {"doc": 0, "tag": 0, "pattern": 0}

        # Standard annotations
        std_edges = self._list_annotations_for_file(file_id, self.sink_node_id, negate=True)
        if std_edges:
            edge_ids, doc_keys, tag_keys = [], [], []
            for edge in std_edges:
                edge_ids.append(edge.as_id())
                if edge.type.external_id == self.file_annotation_type:
                    doc_keys.append(edge.external_id)
                else:
                    tag_keys.append(edge.external_id)

            if edge_ids:
                self.client.data_modeling.instances.delete(edges=edge_ids)
            if doc_keys:
                self.client.raw.rows.delete(
                    db_name=self.config.finalize_function.report_service.raw_db,
                    table_name=self.config.finalize_function.report_service.raw_table_doc_doc,
                    key=doc_keys,
                )
            if tag_keys:
                self.client.raw.rows.delete(
                    db_name=self.config.finalize_function.report_service.raw_db,
                    table_name=self.config.finalize_function.report_service.raw_table_doc_tag,
                    key=tag_keys,
                )
            counts["doc"], counts["tag"] = len(doc_keys), len(tag_keys)

        # Pattern annotations
        pattern_edges = self._list_annotations_for_file(file_id, self.sink_node_id, negate=False)
        if pattern_edges:
            edge_ids = [edge.as_id() for edge in pattern_edges]
            row_keys = [edge.external_id for edge in pattern_edges]
            if edge_ids:
                self.client.data_modeling.instances.delete(edges=edge_ids)
            if row_keys:
                self.client.raw.rows.delete(
                    db_name=self.config.finalize_function.report_service.raw_db,
                    table_name=self.config.finalize_function.report_service.raw_table_doc_pattern,
                    key=row_keys,
                )
            counts["pattern"] = len(row_keys)

        return counts

    def _process_pattern_results(self, result_item: dict, file_node: Node) -> tuple[list[EdgeApply], list[RowWrite]]:
        # ... (This method's internal logic remains the same as the previous version)
        file_id: NodeId = file_node.as_id()
        source_id: str | None = cast(str, file_node.properties[self.file_view_id].get("sourceId"))

        doc_patterns, edge_applies = [], []
        for detect_annotation in result_item["annotations"]:
            for entity in detect_annotation.get("entities", []):
                external_id = self._create_pattern_annotation_id(file_id, detect_annotation)
                now = datetime.now(timezone.utc).replace(microsecond=0)
                annotation_type = entity.get(
                    "annotation_type", self.config.data_model_views.target_entities_view.annotation_type
                )

                annotation_properties = {
                    "name": file_id.external_id,
                    "confidence": detect_annotation.get("confidence", 0.0),
                    "status": DiagramAnnotationStatus.SUGGESTED.value,
                    "startNodePageNumber": detect_annotation["region"]["page"],
                    "startNodeXMin": min(v["x"] for v in detect_annotation["region"]["vertices"]),
                    "startNodeYMin": min(v["y"] for v in detect_annotation["region"]["vertices"]),
                    "startNodeXMax": max(v["x"] for v in detect_annotation["region"]["vertices"]),
                    "startNodeYMax": max(v["y"] for v in detect_annotation["region"]["vertices"]),
                    "startNodeText": detect_annotation["text"],
                    "sourceCreatedUser": self.FUNCTION_ID,
                    "sourceUpdatedUser": self.FUNCTION_ID,
                    "sourceCreatedTime": now.isoformat(),
                    "sourceUpdatedTime": now.isoformat(),
                }

                edge_apply = EdgeApply(
                    space=file_id.space,
                    external_id=external_id,
                    type=DirectRelationReference(
                        space=self.core_annotation_view_id.space,
                        external_id=annotation_type,
                    ),
                    start_node=DirectRelationReference(space=file_id.space, external_id=file_id.external_id),
                    end_node=self.sink_node_id,
                    sources=[
                        NodeOrEdgeData(
                            source=self.core_annotation_view_id,
                            properties=annotation_properties,
                        )
                    ],
                )
                edge_applies.append(edge_apply)

                row_columns = {
                    "externalId": external_id,
                    "startSourceId": source_id,
                    "startNode": file_id.external_id,
                    "startNodeSpace": file_id.space,
                    "endNode": self.sink_node_id.external_id,
                    "endNodeSpace": self.sink_node_id.space,
                    "endNodeResourceType": entity.get("resource_type", "Unknown"),
                    "viewId": self.core_annotation_view_id.external_id,
                    "viewSpace": self.core_annotation_view_id.space,
                    "viewVersion": self.core_annotation_view_id.version,
                }
                row_columns.update(annotation_properties)
                doc_patterns.append(RowWrite(key=external_id, columns=row_columns))

        return edge_applies, doc_patterns

    def _detect_annotation_to_edge_applies(
        self,
        file_instance_id: NodeId,
        source_id: str,
        doc_doc: list[RowWrite],
        doc_tag: list[RowWrite],
        detect_annotation: dict[str, Any],
    ) -> dict[tuple, EdgeApply]:

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

    def _create_pattern_annotation_id(self, file_id: NodeId, raw_annotation: dict[str, Any]) -> str:
        text = raw_annotation["text"]
        hash_ = sha256(json.dumps(raw_annotation, sort_keys=True).encode()).hexdigest()[:10]
        prefix = f"pattern:{file_id.external_id}:{text}"

        if len(prefix) > self.EXTERNAL_ID_LIMIT - 11:
            prefix = prefix[: self.EXTERNAL_ID_LIMIT - 11]

        return f"{prefix}:{hash_}"

    def _list_annotations_for_file(self, node_id: NodeId, end_node: DirectRelationReference, negate: bool = False):
        """
        List all annotation edges for a file node, optionally filtering by the end node.
        """
        start_node_filter = Equals(["edge", "startNode"], {"space": node_id.space, "externalId": node_id.external_id})
        end_node_filter = Equals(["edge", "endNode"], {"space": end_node.space, "externalId": end_node.external_id})

        if negate:
            final_filter = And(start_node_filter, dm.filters.Not(end_node_filter))
        else:
            final_filter = And(start_node_filter, end_node_filter)

        annotations = self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view_id],
            space=node_id.space,
            filter=final_filter,
            limit=-1,
        )

        return annotations

    def _get_edge_apply_unique_key(self, edge_apply_instance: EdgeApply) -> tuple:
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
