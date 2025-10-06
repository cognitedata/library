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
    EXTERNAL_ID_LIMIT = 256
    FUNCTION_ID = "fn_file_annotation_finalize"

    def __init__(self, client: CogniteClient, config: Config, logger: CogniteFunctionLogger):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger
        self.core_annotation_view_id: ViewId = config.data_model_views.core_annotation_view.as_view_id()
        self.file_view_id: ViewId = config.data_model_views.file_view.as_view_id()
        self.file_annotation_type = config.data_model_views.file_view.annotation_type
        self.approve_threshold = config.finalize_function.apply_service.auto_approval_threshold
        self.suggest_threshold = config.finalize_function.apply_service.auto_suggest_threshold
        self.sink_node_ref = DirectRelationReference(
            space=config.finalize_function.apply_service.sink_node.space,
            external_id=config.finalize_function.apply_service.sink_node.external_id,
        )

    def process_and_apply_annotations_for_file(
        self, file_node: Node, regular_item: dict | None, pattern_item: dict | None, clean_old: bool
    ) -> tuple[str, str]:
        file_id = file_node.as_id()
        source_id = cast(str, file_node.properties.get(self.file_view_id, {}).get("sourceId"))

        if clean_old:
            deleted_counts = self._delete_annotations_for_file(file_id)
            self.logger.info(
                f"\t- Deleted {deleted_counts['doc']} doc, {deleted_counts['tag']} tag, and {deleted_counts['pattern']} pattern annotations."
            )

        # Step 1: Process regular annotations and collect their stable hashes
        regular_edges, doc_rows, tag_rows = [], [], []
        processed_hashes = set()
        if regular_item and regular_item.get("annotations"):
            for annotation in regular_item["annotations"]:
                stable_hash = self._create_stable_hash(annotation)
                processed_hashes.add(stable_hash)
                edges = self._detect_annotation_to_edge_applies(file_id, source_id, doc_rows, tag_rows, annotation)
                regular_edges.extend(edges.values())

        # Step 2: Process pattern annotations, skipping any that were already processed
        pattern_edges, pattern_rows = [], []
        if pattern_item and pattern_item.get("annotations"):
            pattern_edges, pattern_rows = self._process_pattern_results(pattern_item, file_node, processed_hashes)

        # Step 3: Update the file node tag
        node_apply = file_node.as_write()
        node_apply.existing_version = None
        tags = cast(list[str], node_apply.sources[0].properties["tags"])
        if "AnnotationInProcess" in tags:
            tags[tags.index("AnnotationInProcess")] = "Annotated"
        elif "Annotated" not in tags:
            self.logger.warning(
                f"File {file_id.external_id} was processed, but 'AnnotationInProcess' tag was not found."
            )

        # Step 4: Apply all data model and RAW changes
        self.update_instances(list_node_apply=node_apply, list_edge_apply=regular_edges + pattern_edges)
        db_name = self.config.finalize_function.report_service.raw_db
        if doc_rows:
            self.client.raw.rows.insert(
                db_name=db_name,
                table_name=self.config.finalize_function.report_service.raw_table_doc_doc,
                row=doc_rows,
                ensure_parent=True,
            )
        if tag_rows:
            self.client.raw.rows.insert(
                db_name=db_name,
                table_name=self.config.finalize_function.report_service.raw_table_doc_tag,
                row=tag_rows,
                ensure_parent=True,
            )
        if pattern_rows:
            self.client.raw.rows.insert(
                db_name=db_name,
                table_name=self.config.finalize_function.report_service.raw_table_doc_pattern,
                row=pattern_rows,
                ensure_parent=True,
            )

        return (
            f"Applied {len(doc_rows)} doc and {len(tag_rows)} tag annotations.",
            f"Created {len(pattern_rows)} new pattern detections.",
        )

    def update_instances(self, list_node_apply=None, list_edge_apply=None) -> InstancesApplyResult:
        return self.client.data_modeling.instances.apply(nodes=list_node_apply, edges=list_edge_apply, replace=False)

    def _delete_annotations_for_file(self, file_id: NodeId) -> dict[str, int]:
        counts = {"doc": 0, "tag": 0, "pattern": 0}
        std_edges = self._list_annotations_for_file(file_id, self.sink_node_ref, negate=True)
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

        pattern_edges = self._list_annotations_for_file(file_id, self.sink_node_ref, negate=False)
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

    def _process_pattern_results(
        self, result_item: dict, file_node: Node, existing_hashes: set
    ) -> tuple[list[EdgeApply], list[RowWrite]]:
        file_id = file_node.as_id()
        source_id = cast(str, file_node.properties.get(self.file_view_id, {}).get("sourceId"))
        doc_patterns, edge_applies = [], []
        for detect_annotation in result_item.get("annotations", []):
            stable_hash = self._create_stable_hash(detect_annotation)
            if stable_hash in existing_hashes:
                continue  # Skip creating a pattern edge if a regular one already exists for this detection

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
                    "tags": [],
                    "startNodePageNumber": detect_annotation.get("region", {}).get("page"),
                    "startNodeXMin": min(
                        v.get("x", 0) for v in detect_annotation.get("region", {}).get("vertices", [])
                    ),
                    "startNodeYMin": min(
                        v.get("y", 0) for v in detect_annotation.get("region", {}).get("vertices", [])
                    ),
                    "startNodeXMax": max(
                        v.get("x", 0) for v in detect_annotation.get("region", {}).get("vertices", [])
                    ),
                    "startNodeYMax": max(
                        v.get("y", 0) for v in detect_annotation.get("region", {}).get("vertices", [])
                    ),
                    "startNodeText": detect_annotation.get("text"),
                    "sourceCreatedUser": self.FUNCTION_ID,
                    "sourceUpdatedUser": self.FUNCTION_ID,
                    "sourceCreatedTime": now.isoformat(),
                    "sourceUpdatedTime": now.isoformat(),
                }
                edge_apply = EdgeApply(
                    space=self.sink_node_ref.space,
                    external_id=external_id,
                    type=DirectRelationReference(space=self.core_annotation_view_id.space, external_id=annotation_type),
                    start_node=DirectRelationReference(space=file_id.space, external_id=file_id.external_id),
                    end_node=self.sink_node_ref,
                    sources=[NodeOrEdgeData(source=self.core_annotation_view_id, properties=annotation_properties)],
                )
                edge_applies.append(edge_apply)
                row_columns = {
                    "externalId": external_id,
                    "startSourceId": source_id,
                    "startNode": file_id.external_id,
                    "startNodeSpace": file_id.space,
                    "endNode": self.sink_node_ref.external_id,
                    "endNodeSpace": self.sink_node_ref.space,
                    "endNodeResourceType": entity.get("resource_type", "Unknown"),
                    "viewId": self.core_annotation_view_id.external_id,
                    "viewSpace": self.core_annotation_view_id.space,
                    "viewVersion": self.core_annotation_view_id.version,
                    **annotation_properties,
                }
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
        # ... (This method remains largely the same)
        diagram_annotations = {}
        for entity in detect_annotation.get("entities", []):
            if detect_annotation.get("confidence", 0.0) >= self.approve_threshold:
                status = DiagramAnnotationStatus.APPROVED.value
            elif detect_annotation.get("confidence", 0.0) >= self.suggest_threshold:
                status = DiagramAnnotationStatus.SUGGESTED.value
            else:
                continue

            external_id = self._create_annotation_id(file_instance_id, entity, detect_annotation)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            annotation_properties = {
                "name": file_instance_id.external_id,
                "confidence": detect_annotation.get("confidence"),
                "status": status,
                "startNodePageNumber": detect_annotation.get("region", {}).get("page"),
                "startNodeXMin": min(v.get("x", 0) for v in detect_annotation.get("region", {}).get("vertices", [])),
                "startNodeYMin": min(v.get("y", 0) for v in detect_annotation.get("region", {}).get("vertices", [])),
                "startNodeXMax": max(v.get("x", 0) for v in detect_annotation.get("region", {}).get("vertices", [])),
                "startNodeYMax": max(v.get("y", 0) for v in detect_annotation.get("region", {}).get("vertices", [])),
                "startNodeText": detect_annotation.get("text"),
                "sourceCreatedUser": self.FUNCTION_ID,
                "sourceUpdatedUser": self.FUNCTION_ID,
                "sourceCreatedTime": now.isoformat(),
                "sourceUpdatedTime": now.isoformat(),
            }
            edge = EdgeApply(
                space=file_instance_id.space,
                external_id=external_id,
                type=DirectRelationReference(
                    space=self.core_annotation_view_id.space, external_id=entity.get("annotation_type")
                ),
                start_node=DirectRelationReference(
                    space=file_instance_id.space, external_id=file_instance_id.external_id
                ),
                end_node=DirectRelationReference(space=entity.get("space"), external_id=entity.get("external_id")),
                sources=[NodeOrEdgeData(source=self.core_annotation_view_id, properties=annotation_properties)],
            )
            key = self._get_edge_apply_unique_key(edge)
            if key not in diagram_annotations:
                diagram_annotations[key] = edge

            doc_log = {
                "externalId": external_id,
                "startSourceId": source_id,
                "startNode": file_instance_id.external_id,
                "startNodeSpace": file_instance_id.space,
                "endNode": entity.get("external_id"),
                "endNodeSpace": entity.get("space"),
                "endNodeResourceType": entity.get("resource_type"),
                "viewId": self.core_annotation_view_id.external_id,
                "viewSpace": self.core_annotation_view_id.space,
                "viewVersion": self.core_annotation_view_id.version,
                **annotation_properties,
            }
            if entity.get("annotation_type") == self.file_annotation_type:
                doc_doc.append(RowWrite(key=external_id, columns=doc_log))
            else:
                doc_tag.append(RowWrite(key=external_id, columns=doc_log))
        return diagram_annotations

    def _create_stable_hash(self, raw_annotation: dict[str, Any]) -> str:
        text = raw_annotation.get("text", "")
        region = raw_annotation.get("region", {})
        vertices = region.get("vertices", [])
        sorted_vertices = sorted(vertices, key=lambda v: (v.get("x", 0), v.get("y", 0)))
        stable_representation = {"text": text, "page": region.get("page"), "vertices": sorted_vertices}
        return sha256(json.dumps(stable_representation, sort_keys=True).encode()).hexdigest()[:10]

    def _create_annotation_id(self, file_id: NodeId, entity: dict[str, Any], raw_annotation: dict[str, Any]) -> str:
        hash_ = self._create_stable_hash(raw_annotation)
        text = raw_annotation.get("text", "")
        naive = f"{file_id.external_id}:{entity.get('external_id')}:{text}:{hash_}"
        if len(naive) < self.EXTERNAL_ID_LIMIT:
            return naive
        prefix = f"{file_id.external_id}:{entity.get('external_id')}:{text}"
        if len(prefix) > self.EXTERNAL_ID_LIMIT - 11:
            prefix = prefix[: self.EXTERNAL_ID_LIMIT - 11]
        return f"{prefix}:{hash_}"

    def _create_pattern_annotation_id(self, file_id: NodeId, raw_annotation: dict[str, Any]) -> str:
        hash_ = self._create_stable_hash(raw_annotation)
        text = raw_annotation.get("text", "")
        prefix = f"pattern:{file_id.external_id}:{text}"
        if len(prefix) > self.EXTERNAL_ID_LIMIT - 11:
            prefix = prefix[: self.EXTERNAL_ID_LIMIT - 11]
        return f"{prefix}:{hash_}"

    def _list_annotations_for_file(self, node_id: NodeId, end_node: DirectRelationReference, negate: bool = False):
        start_node_filter = Equals(["edge", "startNode"], {"space": node_id.space, "externalId": node_id.external_id})
        end_node_filter = Equals(["edge", "endNode"], {"space": end_node.space, "externalId": end_node.external_id})
        if negate:
            final_filter = And(start_node_filter, dm.filters.Not(end_node_filter))
            space = node_id.space
        else:
            space = self.sink_node_ref.space
            final_filter = And(start_node_filter, end_node_filter)
        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view_id],
            space=space,
            filter=final_filter,
            limit=-1,
        )

    def _get_edge_apply_unique_key(self, edge_apply_instance: EdgeApply) -> tuple:
        start_node = edge_apply_instance.start_node
        end_node = edge_apply_instance.end_node
        type_ = edge_apply_instance.type
        return (
            (start_node.space, start_node.external_id) if start_node else None,
            (end_node.space, end_node.external_id) if end_node else None,
            (type_.space, type_.external_id) if type_ else None,
        )
