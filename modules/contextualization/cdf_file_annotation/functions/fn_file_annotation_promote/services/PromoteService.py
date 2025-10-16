import abc
from typing import Any
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite
from cognite.client.data_classes.data_modeling import (
    Edge,
    EdgeList,
    EdgeApply,
    Node,
    NodeOrEdgeData,
    DirectRelationReference,
    NodeList,
)
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger
from services.CacheService import CacheService
from services.EntitySearchService import EntitySearchService
from utils.DataStructures import DiagramAnnotationStatus


class IPromoteService(abc.ABC):
    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralPromoteService(IPromoteService):
    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        entity_search_service: EntitySearchService,
        cache_service: CacheService,
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.core_annotation_view = self.config.data_model_views.core_annotation_view
        self.file_view = self.config.data_model_views.file_view
        self.target_entities_view = self.config.data_model_views.target_entities_view
        self.sink_node_ref = DirectRelationReference(
            space=self.config.finalize_function.apply_service.sink_node.space,
            external_id=self.config.finalize_function.apply_service.sink_node.external_id,
        )
        self.raw_db = self.config.finalize_function.apply_service.raw_db
        self.raw_pattern_table = self.config.finalize_function.apply_service.raw_table_doc_pattern
        self.raw_doc_doc_table = self.config.finalize_function.apply_service.raw_table_doc_doc
        self.raw_doc_tag_table = self.config.finalize_function.apply_service.raw_table_doc_tag

        # Injected service dependencies
        self.entity_search_service = entity_search_service
        self.cache_service = cache_service

    def run(self) -> str | None:
        """Main entrypoint for the Promote service."""
        candidates = self._get_promote_candidates()
        if not candidates:
            self.logger.info("No Promote candidates found.")
            return "Done"

        self.logger.info(f"Found {len(candidates)} Promote candidates. Starting processing.")

        # Group candidates by (startNodeText, annotationType) for deduplication
        grouped_candidates: dict[tuple[str, str], list[Edge]] = {}
        for edge in candidates:
            properties = edge.properties[self.core_annotation_view.as_view_id()]
            text = properties.get("startNodeText")
            annotation_type = edge.type.external_id

            if text and annotation_type:
                key = (text, annotation_type)
                if key not in grouped_candidates:
                    grouped_candidates[key] = []
                grouped_candidates[key].append(edge)

        self.logger.info(
            f"Grouped {len(candidates)} candidates into {len(grouped_candidates)} unique text/type combinations. "
            f"Deduplication savings: {len(candidates) - len(grouped_candidates)} queries avoided."
        )

        edges_to_update = []
        raw_rows_to_update = []

        # Process each unique text/type combination once
        for (text_to_find, annotation_type), edges_with_same_text in grouped_candidates.items():
            entity_space = (
                self.file_view.instance_space
                if annotation_type == "diagrams.FileLink"
                else self.target_entities_view.instance_space
            )

            if not entity_space:
                self.logger.warning(f"Could not determine entity space for type '{annotation_type}'. Skipping.")
                continue

            # Strategy: Check cache → query edges → fallback to global search
            found_nodes = self._find_entity_with_cache(text_to_find, annotation_type, entity_space)

            # Apply the same result to ALL edges with this text
            for edge in edges_with_same_text:
                edge_apply, raw_row = self._prepare_edge_update(edge, found_nodes)

                if edge_apply:
                    edges_to_update.append(edge_apply)
                if raw_row:
                    raw_rows_to_update.append(raw_row)

        if edges_to_update:
            self.client.data_modeling.instances.apply(edges=edges_to_update)
            self.logger.info(f"Successfully updated {len(edges_to_update)} edges in data model.")

        if raw_rows_to_update:
            self.client.raw.rows.insert(
                db_name=self.raw_db,
                table_name=self.raw_pattern_table,
                row=raw_rows_to_update,
                ensure_parent=True,
            )
            self.logger.info(f"Successfully updated {len(raw_rows_to_update)} rows in RAW table.")

        if not edges_to_update and not raw_rows_to_update:
            self.logger.info("No edges were updated in this run.")

        return None  # Continue running if more candidates might exist

    def _get_promote_candidates(self) -> EdgeList | None:
        """Queries for suggested edges pointing to the sink node that haven't been PromoteAttempted."""
        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view.as_view_id()],
            filter={
                "and": [
                    {
                        "equals": {
                            "property": ["edge", "endNode"],
                            "value": {"space": self.sink_node_ref.space, "externalId": self.sink_node_ref.external_id},
                        }
                    },
                    {"equals": {"property": self.core_annotation_view.as_property_ref("status"), "value": "Suggested"}},
                    {
                        "not": {
                            "containsAny": {
                                "property": self.core_annotation_view.as_property_ref("tags"),
                                "values": ["PromoteAttempted"],
                            }
                        }
                    },
                ]
            },
            limit=500,  # Batch size
        )

    def _find_entity_with_cache(self, text: str, annotation_type: str, entity_space: str) -> list | None:
        """
        Finds entity for text using multi-tier caching strategy.

        Strategy:
        1. Check cache (in-memory + persistent RAW)
        2. Use EntitySearchService (annotation edges → global search)
        3. Update cache if unambiguous match found

        Args:
            text: Text to search for
            annotation_type: Type of annotation
            entity_space: Space to search in

        Returns:
            List of matched nodes (empty if no match, 2+ if ambiguous)
        """
        # TIER 1 & 2: Check cache (in-memory + persistent)
        cached_node = self.cache_service.get(text, annotation_type)
        if cached_node is not None:
            return [cached_node]

        # Check if we've already determined there's no match
        # (negative caching is handled internally by cache service)
        if self.cache_service.get_from_memory(text, annotation_type) is None:
            # We've checked this before in this run and found nothing
            if (text, annotation_type) in self.cache_service._memory_cache:
                return []

        # TIER 3 & 4: Use EntitySearchService (edges → global search)
        found_nodes = self.entity_search_service.find_entity(text, annotation_type, entity_space)

        # Update cache based on result
        if found_nodes and len(found_nodes) == 1:
            # Unambiguous match - cache it
            self.cache_service.set(text, annotation_type, found_nodes[0])
        elif not found_nodes:
            # No match - cache negative result
            self.cache_service.set(text, annotation_type, None)
        # Don't cache ambiguous results (len > 1)

        return found_nodes

    def _prepare_edge_update(self, edge: Edge, found_nodes) -> tuple[EdgeApply | None, RowWrite | None]:
        """
        Prepares the EdgeApply and RowWrite objects for updating both data model and RAW table.
        Returns a tuple of (edge_apply, raw_row) where either can be None if update is not needed.
        """
        # Get the current edge properties before creating the write version
        edge_props = edge.properties.get(self.core_annotation_view.as_view_id(), {})
        current_tags = edge_props.get("tags", [])
        updated_tags = list(current_tags) if isinstance(current_tags, list) else []

        # Now create the write version
        edge_apply = edge.as_write()

        # Fetch existing RAW row to preserve all data
        raw_data: dict[str, Any] = {}
        try:
            existing_row = self.client.raw.rows.retrieve(
                db_name=self.raw_db, table_name=self.raw_pattern_table, key=edge.external_id
            )
            if existing_row and existing_row.columns:
                raw_data = {k: v for k, v in existing_row.columns.items()}
        except Exception as e:
            self.logger.warning(f"Could not retrieve RAW row for edge {edge.external_id}: {e}")

        # Prepare update properties for the edge
        update_properties: dict = {}

        if len(found_nodes) == 1:  # Success - single match found
            matched_node = found_nodes[0]
            self.logger.info(f"Found single match for '{edge_props.get('startNodeText')}'. Promoting edge.")

            # Update edge to point to the found entity
            edge_apply.end_node = DirectRelationReference(matched_node.space, matched_node.external_id)
            update_properties["status"] = DiagramAnnotationStatus.APPROVED.value
            updated_tags.append("PromotedAuto")

            # Update RAW row with new end node information
            raw_data["endNode"] = matched_node.external_id
            raw_data["endNodeSpace"] = matched_node.space
            raw_data["status"] = DiagramAnnotationStatus.APPROVED.value

            # Get resource type from the matched entity
            entity_props = matched_node.properties.get(self.target_entities_view.as_view_id(), {})
            resource_type = entity_props.get("resourceType") or entity_props.get("type")
            if resource_type:
                raw_data["endNodeResourceType"] = resource_type

        elif len(found_nodes) == 0:  # Failure - no match found
            self.logger.info(f"Found no match for '{edge_props.get('startNodeText')}'. Rejecting edge.")
            update_properties["status"] = DiagramAnnotationStatus.REJECTED.value
            updated_tags.append("PromoteAttempted")

            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        else:  # Ambiguous - multiple matches found
            self.logger.info(f"Found multiple matches for '{edge_props.get('startNodeText')}'. Marking as ambiguous.")
            updated_tags.extend(["PromoteAttempted", "AmbiguousMatch"])

            # Don't change status, just add tags to RAW
            raw_data["status"] = edge_props.get("status", DiagramAnnotationStatus.SUGGESTED.value)

        # Update edge properties
        update_properties["tags"] = updated_tags
        raw_data["tags"] = updated_tags
        edge_apply.sources[0] = NodeOrEdgeData(
            source=self.core_annotation_view.as_view_id(), properties=update_properties
        )

        # Create RowWrite object for RAW table update
        raw_row = RowWrite(key=edge.external_id, columns=raw_data) if raw_data else None

        return edge_apply, raw_row
