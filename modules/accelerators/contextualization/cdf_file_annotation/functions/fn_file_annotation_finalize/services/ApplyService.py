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
    InstancesApplyResult,
)
from cognite.client.data_classes.filters import Equals
from cognite.client.data_classes.annotation_types.primitives import BoundingBox

from services.ConfigService import Config
from utils.DataStructures import DiagramAnnotationStatus
from services.LoggerService import CogniteFunctionLogger


class IApplyService(abc.ABC):
    """
    Interface for applying/deleting annotations to a node
    """

    @abc.abstractmethod
    def process_and_apply_annotations_for_file(
        self,
        file_node: Node,
        regular_item: dict | None,
        pattern_item: dict | None,
        clean_old: bool,
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
        self.raw_tables = config.raw_tables
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
        self,
        file_node: Node,
        regular_item: dict[str, Any] | None,
        pattern_item: dict[str, Any] | None,
        clean_old: bool,
    ) -> tuple[str, str]:
        """
        Performs the complete annotation workflow for a single file.

        Processes diagram detection results (regular and pattern mode), removes old annotations if needed,
        creates annotation edges in the data model, writes annotation data to RAW tables,
        and updates the file node's tag status.

        The function uses spatial deduplication to prevent creating duplicate annotations when the same
        region is detected by both regular and pattern modes. Only annotations from regular mode that
        meet confidence thresholds are tracked, ensuring pattern annotations aren't incorrectly filtered.

        Args:
            file_node: The file node instance to annotate.
            regular_item: Dictionary containing regular diagram detect results with 'annotations' key.
            pattern_item: Dictionary containing pattern mode diagram detect results with 'annotations' key.
            clean_old: Whether to delete existing annotations before applying new ones.

        Returns:
            A tuple containing:
                - Summary message of regular annotations applied
                - Summary message of pattern annotations created
        """
        file_id = file_node.as_id()
        source_id = cast(str, file_node.properties.get(self.file_view_id, {}).get("sourceId"))

        if clean_old:
            deleted_counts = self._delete_annotations_for_file(file_id)
            self.logger.info(
                f"\t- Deleted {deleted_counts['doc']} doc, {deleted_counts['tag']} tag, and {deleted_counts['pattern']} pattern annotations."
            )

        # Step 1: Process regular annotations and collect their spatial locations
        # Set stores (page, bounding_box_yaml) tuples to prevent duplicate annotations
        regular_edges, doc_rows, tag_rows = [], [], []
        processed_bounding_boxes: set[tuple[int, str]] = set()
        if regular_item and regular_item.get("annotations"):
            for annotation in regular_item["annotations"]:
                edges = self._detect_annotation_to_edge_applies(
                    file_id,
                    source_id,
                    doc_rows,
                    tag_rows,
                    annotation,
                    processed_bounding_boxes,
                )
                regular_edges.extend(edges)

        # Step 2: Process pattern annotations, skipping those with spatial overlap
        pattern_edges, pattern_rows = [], []
        if pattern_item and pattern_item.get("annotations"):
            pattern_edges, pattern_rows = self._process_pattern_results(
                pattern_item, file_node, processed_bounding_boxes
            )

        # Step 3: Apply all data model and RAW changes
        self.update_instances(list_edge_apply=regular_edges + pattern_edges)
        if doc_rows:
            self.client.raw.rows.insert(
                db_name=self.raw_tables.raw_db,
                table_name=self.raw_tables.raw_table_doc_doc,
                row=doc_rows,
                ensure_parent=True,
            )
        if tag_rows:
            self.client.raw.rows.insert(
                db_name=self.raw_tables.raw_db,
                table_name=self.raw_tables.raw_table_doc_tag,
                row=tag_rows,
                ensure_parent=True,
            )
        if pattern_rows:
            self.client.raw.rows.insert(
                db_name=self.raw_tables.raw_db,
                table_name=self.raw_tables.raw_table_doc_pattern,
                row=pattern_rows,
                ensure_parent=True,
            )

        return (
            f"Applied {len(doc_rows)} doc and {len(tag_rows)} tag annotations.",
            f"Created {len(pattern_rows)} new pattern detections.",
        )

    def update_instances(
        self,
        list_node_apply: list[NodeApply] | NodeApply | None = None,
        list_edge_apply: list[EdgeApply] | EdgeApply | None = None,
    ) -> InstancesApplyResult:
        """
        Applies node and/or edge updates to the data model.

        Args:
            list_node_apply: Optional NodeApply or list of NodeApply objects to update.
            list_edge_apply: Optional EdgeApply or list of EdgeApply objects to update.

        Returns:
            InstancesApplyResult containing the results of the apply operation.
        """
        return self.client.data_modeling.instances.apply(nodes=list_node_apply, edges=list_edge_apply, replace=False)

    def _delete_annotations_for_file(self, file_id: NodeId) -> dict[str, int]:
        """
        Removes all existing annotations for a file from both data model and RAW tables.

        Deletes annotation edges (doc-to-doc, doc-to-tag, and pattern annotations) and their
        corresponding RAW table entries to prepare for fresh annotations.

        Args:
            file_id: NodeId of the file whose annotations should be deleted.

        Returns:
            Dictionary with counts of deleted annotations: {"doc": int, "tag": int, "pattern": int}.
        """

        counts = {"doc": 0, "tag": 0, "pattern": 0}
        std_edges = self._list_annotations_for_file(
            file_id, file_id.space
        )  # NOTE: Annotations produced from regular diagram detect are stored in the same instance space as the file node
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
                    db_name=self.raw_tables.raw_db,
                    table_name=self.raw_tables.raw_table_doc_doc,
                    key=doc_keys,
                )
            if tag_keys:
                self.client.raw.rows.delete(
                    db_name=self.raw_tables.raw_db,
                    table_name=self.raw_tables.raw_table_doc_tag,
                    key=tag_keys,
                )
            counts["doc"], counts["tag"] = len(doc_keys), len(tag_keys)

        pattern_edges = self._list_annotations_for_file(
            file_id, self.sink_node_ref.space
        )  # NOTE: Annotations produced from pattern mode are stored in the same instance space as the sink node
        if pattern_edges:
            edge_ids = [edge.as_id() for edge in pattern_edges]
            row_keys = [edge.external_id for edge in pattern_edges]
            if edge_ids:
                self.client.data_modeling.instances.delete(edges=edge_ids)
            if row_keys:
                self.client.raw.rows.delete(
                    db_name=self.raw_tables.raw_db,
                    table_name=self.raw_tables.raw_table_doc_pattern,
                    key=row_keys,
                )
            counts["pattern"] = len(row_keys)
        return counts

    def _list_annotations_for_file(self, node_id: NodeId, edge_instance_space: str):
        """
        Retrieves all annotation edges for a specific file from a given instance space.

        Args:
            node_id: NodeId of the file to query annotations for.
            edge_instance_space: Instance space where the annotation edges are stored.

        Returns:
            EdgeList of all annotation edges connected to the file node.
        """
        start_node_filter = Equals(
            ["edge", "startNode"],
            {"space": node_id.space, "externalId": node_id.external_id},
        )

        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view_id],
            space=edge_instance_space,
            filter=start_node_filter,
            limit=-1,
        )

    def _process_pattern_results(
        self, result_item: dict[str, Any], file_node: Node, existing_bounding_boxes: set[tuple[int, str]]
    ) -> tuple[list[EdgeApply], list[RowWrite]]:
        """
        Processes pattern mode detection results into annotation edges and RAW rows.

        Creates pattern-based annotations that link to a sink node rather than specific entities,
        allowing review and approval of pattern-detected annotations before linking to actual entities.
        Uses spatial deduplication to skip patterns already covered by regular detection results that
        met confidence thresholds.

        Args:
            result_item: Dictionary containing pattern mode detection results with 'annotations' key.
            file_node: The file node being annotated.
            existing_bounding_boxes: Set of (page, bounding_box_yaml) tuples from regular annotations
                                    that met confidence thresholds. Used to avoid duplicate annotations.

        Returns:
            A tuple containing:
                - List of EdgeApply objects for pattern annotations
                - List of RowWrite objects for RAW table entries
        """
        file_id = file_node.as_id()
        source_id = cast(str, file_node.properties.get(self.file_view_id, {}).get("sourceId"))
        doc_patterns, edge_applies = [], []
        for detect_annotation in result_item.get("annotations", []):
            bounding_box: BoundingBox = self._extract_bounding_box_from_region(detect_annotation["region"])
            page = detect_annotation["region"].get("page")
            if (page, bounding_box.dump_yaml()) in existing_bounding_boxes:
                continue  # Skip creating a pattern edge if a regular one already exists for this detection

            entities = detect_annotation.get("entities", [])
            if not entities:
                continue
            entity = entities[0]

            external_id = self._create_pattern_annotation_id(file_id, detect_annotation, bounding_box)
            annotation_type = entity.get(
                "annotation_type",
                self.config.data_model_views.target_entities_view.annotation_type,
            )
            annotation_properties = self._create_annotation_properties_from_detection(
                file_id=file_id,
                detect_annotation=detect_annotation,
                status=DiagramAnnotationStatus.SUGGESTED.value,
            )
            # Add pattern-specific property
            annotation_properties["tags"] = []
            edge_apply = EdgeApply(
                space=self.sink_node_ref.space,
                external_id=external_id,
                type=DirectRelationReference(
                    space=self.core_annotation_view_id.space,
                    external_id=annotation_type,
                ),
                start_node=DirectRelationReference(space=file_id.space, external_id=file_id.external_id),
                end_node=self.sink_node_ref,
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
        processed_bounding_boxes: set[tuple[int, str]],
    ) -> list[EdgeApply]:
        """
        Converts a single detection annotation into edge applies and RAW row writes.

        Creates annotation edges linking the file to detected entities, applying confidence thresholds
        to determine approval/suggestion status. Also creates corresponding RAW table entries.
        Only annotations meeting confidence thresholds are added to the processed_bounding_boxes set for spatial deduplication with pattern mode results.

        Args:
            file_instance_id: NodeId of the file being annotated.
            source_id: Source ID of the file for RAW table logging.
            doc_doc: List to append doc-to-doc annotation RAW rows to (modified in place).
            doc_tag: List to append doc-to-tag annotation RAW rows to (modified in place).
            detect_annotation: Dictionary containing a single detection result with 'region', 'entities', 'confidence', and 'text' keys.
            processed_bounding_boxes: Set of (page, bounding_box_yaml) tuples to track annotations meeting confidence thresholds (modified in place).

        Returns:
            List of EdgeApply objects for each entity in the detection that meets confidence thresholds.
        """
        edges = []
        # NOTE: File annotation endpoint returns multiple of the same entities when matched on different aliases
        edge_external_id: list[str] = []
        bounding_box: BoundingBox = self._extract_bounding_box_from_region(detect_annotation["region"])
        page = detect_annotation["region"].get("page")
        for entity in detect_annotation.get("entities", []):
            if detect_annotation.get("confidence", 0.0) >= self.approve_threshold:
                status = DiagramAnnotationStatus.APPROVED.value
            elif detect_annotation.get("confidence", 0.0) >= self.suggest_threshold:
                status = DiagramAnnotationStatus.SUGGESTED.value
            else:
                continue

            processed_bounding_boxes.add((page, bounding_box.dump_yaml()))

            external_id = self._create_annotation_id(file_instance_id, entity, detect_annotation, bounding_box)

            # skip when duplicate is present
            if external_id in edge_external_id:
                continue
            edge_external_id.append(external_id)

            annotation_properties = self._create_annotation_properties_from_detection(
                file_id=file_instance_id,
                detect_annotation=detect_annotation,
                status=status,
                bounding_box=bounding_box,
            )
            edge = EdgeApply(
                space=file_instance_id.space,
                external_id=external_id,
                type=DirectRelationReference(
                    space=self.core_annotation_view_id.space,
                    external_id=entity.get("annotation_type"),
                ),
                start_node=DirectRelationReference(
                    space=file_instance_id.space,
                    external_id=file_instance_id.external_id,
                ),
                end_node=DirectRelationReference(space=entity.get("space"), external_id=entity.get("external_id")),
                sources=[
                    NodeOrEdgeData(
                        source=self.core_annotation_view_id,
                        properties=annotation_properties,
                    )
                ],
            )
            edges.append(edge)

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
        return edges

    def _create_stable_hash(self, raw_annotation: dict[str, Any], bounding_box: BoundingBox) -> str:
        """
        Generates a stable hash for an annotation to enable unique identification.

        Creates a deterministic hash based on annotation text, page, and bounding box coordinates.
        This hash is used as part of the annotation external ID to ensure stable, reproducible annotation identifiers across re-runs.

        Args:
            raw_annotation: Dictionary containing annotation detection data with 'text' and 'region' keys.
            bounding_box: BoundingBox object containing the spatial coordinates of the annotation.

        Returns:
            10-character hash string representing the annotation.
        """
        text = raw_annotation.get("text", "")
        region = raw_annotation.get("region", {})
        stable_representation = {
            "text": text,
            "page": region.get("page"),
            "bounding_box": bounding_box.dump_yaml(),
        }
        return sha256(json.dumps(stable_representation, sort_keys=True).encode()).hexdigest()[:10]

    def _create_annotation_id(
        self, file_id: NodeId, entity: dict[str, Any], raw_annotation: dict[str, Any], bounding_box: BoundingBox
    ) -> str:
        """
        Creates a unique external ID for a regular annotation edge.

        Combines file ID, entity ID, detected text, and a stable hash to create a human-readable yet unique identifier, truncating if necessary to stay within CDF's 256 character limit.

        Args:
            file_id: NodeId of the file being annotated.
            entity: Dictionary containing the detected entity information with 'external_id' key.
            raw_annotation: Dictionary containing annotation detection data with 'text' and 'region' keys.
            bounding_box: BoundingBox object used for generating the stable hash component.

        Returns:
            Unique external ID string for the annotation edge (max 256 characters).
        """
        hash_ = self._create_stable_hash(raw_annotation, bounding_box)
        text = raw_annotation.get("text", "")
        naive = f"{file_id.external_id}:{entity.get('external_id')}:{text}:{hash_}"
        if len(naive) < self.EXTERNAL_ID_LIMIT:
            return naive
        prefix = f"{file_id.external_id}:{entity.get('external_id')}:{text}"
        if len(prefix) > self.EXTERNAL_ID_LIMIT - 11:
            prefix = prefix[: self.EXTERNAL_ID_LIMIT - 11]
        return f"{prefix}:{hash_}"

    def _create_pattern_annotation_id(
        self, file_id: NodeId, raw_annotation: dict[str, Any], bounding_box: BoundingBox
    ) -> str:
        """
        Creates a unique external ID for a pattern annotation edge.

        Similar to regular annotations but prefixed with "pattern:" to distinguish pattern-detected
        annotations that link to sink nodes rather than specific entities.

        Args:
            file_id: NodeId of the file being annotated.
            raw_annotation: Dictionary containing annotation detection data with 'text' and 'region' keys.
            bounding_box: BoundingBox object used for generating the stable hash component.

        Returns:
            Unique external ID string for the pattern annotation edge (max 256 characters).
        """
        hash_ = self._create_stable_hash(raw_annotation, bounding_box)
        text = raw_annotation.get("text", "")
        prefix = f"pattern:{file_id.external_id}:{text}"
        if len(prefix) > self.EXTERNAL_ID_LIMIT - 11:
            prefix = prefix[: self.EXTERNAL_ID_LIMIT - 11]
        return f"{prefix}:{hash_}"

    def _extract_bounding_box_from_region(self, region: dict[str, Any]) -> BoundingBox:
        """
        Extracts and creates a BoundingBox from a diagram detection region.

        Converts the vertices array from the diagram detect API response into a proper
        BoundingBox object by computing the min/max coordinates from all vertices.

        Args:
            region: Dictionary containing 'vertices' list with 'x' and 'y' coordinates.

        Returns:
            BoundingBox object with computed x_min, x_max, y_min, y_max boundaries.
        """
        vertices = region.get("vertices", [])
        x_coords = [v.get("x", 0) for v in vertices]
        y_coords = [v.get("y", 0) for v in vertices]

        return BoundingBox(x_min=min(x_coords), x_max=max(x_coords), y_min=min(y_coords), y_max=max(y_coords))

    def _create_annotation_properties_from_detection(
        self,
        file_id: NodeId,
        detect_annotation: dict[str, Any],
        status: str,
        bounding_box: BoundingBox | None = None,
    ) -> dict[str, Any]:
        """
        Creates annotation properties dictionary from a detection result.

        Extracts common annotation properties including confidence, status, text, page number, and bounding box coordinates for use in EdgeApply objects or RAW table entries.

        Args:
            file_id: NodeId of the file being annotated.
            detect_annotation: Dictionary containing detection result data with 'confidence', 'text',
                             and 'region' keys.
            status: Annotation status string (e.g., 'Approved', 'Suggested').
            bounding_box: Optional pre-computed BoundingBox object. If None, will be extracted from
                         the detection region.

        Returns:
            Dictionary of annotation properties ready for EdgeApply or RAW table insertion, including standard fields and bounding box coordinates.
        """
        region = detect_annotation.get("region", {})
        if bounding_box is None:
            bounding_box = self._extract_bounding_box_from_region(region)

        now = datetime.now(timezone.utc).replace(microsecond=0)
        properties = {
            "name": file_id.external_id,
            "confidence": detect_annotation.get("confidence", 0.0),
            "status": status,
            "startNodePageNumber": region.get("page"),
            "startNodeText": detect_annotation.get("text"),
            "sourceCreatedUser": self.FUNCTION_ID,
            "sourceUpdatedUser": self.FUNCTION_ID,
            "sourceCreatedTime": now.isoformat(),
            "sourceUpdatedTime": now.isoformat(),
        }

        # Add bounding box coordinates if available
        if bounding_box:
            properties.update(
                {
                    "startNodeXMin": bounding_box.x_min,
                    "startNodeYMin": bounding_box.y_min,
                    "startNodeXMax": bounding_box.x_max,
                    "startNodeYMax": bounding_box.y_max,
                }
            )

        return properties
