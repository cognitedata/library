import abc
import time
from dataclasses import dataclass
from typing import Any, Literal
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite
from cognite.client.data_classes.data_modeling import (
    Edge,
    EdgeId,
    EdgeList,
    EdgeApply,
    Node,
    NodeOrEdgeData,
    DirectRelationReference,
)
from services.ConfigService import Config, build_filter_from_query, get_limit_from_query
from services.LoggerService import CogniteFunctionLogger
from services.CacheService import CacheService, CachedEntityInfo
from services.EntitySearchService import EntitySearchService
from utils.DataStructures import DiagramAnnotationStatus, PromoteTracker


@dataclass
class MatchedEntity:
    """
    Unified representation of a matched entity from either cache or search.

    This dataclass provides a consistent interface for working with matched entities
    regardless of whether they came from the cache (CachedEntityInfo) or from
    a fresh Elasticsearch search (Node).
    """

    space: str
    external_id: str
    resource_type: str | None = None

    @classmethod
    def from_node(cls, node: Node, target_view_id: Any) -> "MatchedEntity":
        """Creates MatchedEntity from a Node object."""
        entity_props = node.properties.get(target_view_id, {}) if node.properties else {}
        resource_type_value = entity_props.get("resourceType") or entity_props.get("type")
        # Ensure resource_type is a string or None
        resource_type: str | None = str(resource_type_value) if resource_type_value is not None else None
        return cls(
            space=node.space,
            external_id=node.external_id,
            resource_type=resource_type,
        )

    @classmethod
    def from_cached_info(cls, cached: CachedEntityInfo) -> "MatchedEntity":
        """Creates MatchedEntity from CachedEntityInfo."""
        return cls(
            space=cached.space,
            external_id=cached.external_id,
            resource_type=cached.resource_type,
        )


class IPromoteService(abc.ABC):
    """
    Interface for services that promote pattern-mode annotations by finding entities
    and updating annotation edges.
    """

    @abc.abstractmethod
    def run(self) -> Literal["Done"] | None:
        """
        Main execution method for promoting pattern-mode annotations.

        Returns:
            "Done" if no more candidates need processing, None if processing should continue.
        """
        pass


class GeneralPromoteService(IPromoteService):
    """
    Promotes pattern-mode annotations by finding matching entities and updating annotation edges.

    This service retrieves candidate pattern-mode annotations (edges pointing to sink node),
    searches for matching entities using EntitySearchService (with caching via CacheService),
    and updates both the data model edges and RAW tables with the results.

    Pattern-mode annotations are created during diagram detection when entities can't be
    matched to the provided entity list but match regex patterns. This service attempts to
    resolve those annotations by searching existing annotations and entity aliases.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PromoteTracker,
        entity_search_service: EntitySearchService,
        cache_service: CacheService,
    ):
        """
        Initialize the promote service with required dependencies.

        Args:
            client: CogniteClient for API interactions
            config: Configuration object containing data model views and settings
            logger: Logger instance for tracking execution
            tracker: Performance tracker for metrics (edges promoted/rejected/ambiguous)
            entity_search_service: Service for finding entities by text (injected)
            cache_service: Service for caching text→entity mappings (injected)
        """
        self.client = client
        self.config = config
        self.logger = logger
        self.tracker = tracker
        self.core_annotation_view = self.config.data_model_views.core_annotation_view
        self.file_view = self.config.data_model_views.file_view
        self.target_entities_view = self.config.data_model_views.target_entities_view

        # Sink node reference (from finalize_function config as it's shared)
        self.sink_node_ref = DirectRelationReference(
            space=self.config.finalize_function.apply_service.sink_node.space,
            external_id=self.config.finalize_function.apply_service.sink_node.external_id,
        )

        # RAW database and table configuration
        self.raw_db = self.config.raw_tables.raw_db
        self.raw_pattern_table = self.config.raw_tables.raw_table_doc_pattern
        self.raw_doc_doc_table = self.config.raw_tables.raw_table_doc_doc
        self.raw_doc_tag_table = self.config.raw_tables.raw_table_doc_tag

        # Promote flags
        self.delete_rejected_edges: bool = self.config.promote_function.delete_rejected_edges
        self.delete_suggested_edges: bool = self.config.promote_function.delete_suggested_edges

        # Injected service dependencies
        self.entity_search_service = entity_search_service
        self.cache_service = cache_service

    def run(self) -> Literal["Done"] | None:
        """
        Main execution method for promoting pattern-mode annotations.

        Process flow:
        1. Retrieve candidate edges (pattern-mode annotations not yet promoted)
        2. Group candidates by (text, type) for deduplication
        3. For each unique text/type:
           - Check cache for previous results
           - Search for matching entity via EntitySearchService
           - Update cache with results
        4. Prepare edge and RAW table updates
        5. Apply updates to data model and RAW tables

        Args:
            None

        Returns:
            "Done" if no candidates found (processing complete),
            None if candidates were processed (more batches may exist).

        Raises:
            Exception: Any unexpected errors during processing are logged and re-raised.
        """
        self.logger.info("Starting Promote batch", section="START")

        try:
            candidates: EdgeList | None = self._get_promote_candidates()
            if not candidates:
                self.logger.info("No Promote candidates found.", section="END")
                return "Done"
        except Exception as e:
            self.logger.error("Ran into the following error", error=e)
            self.logger.info("Retrying in 15 seconds")
            time.sleep(15)
            return

        self.logger.info(f"Found {len(candidates)} Promote candidates. Starting processing.")

        # Group candidates by (startNodeText, annotationType) for deduplication
        grouped_candidates: dict[tuple[str, str], list[Edge]] = {}
        for edge in candidates:
            properties: dict[str, Any] = edge.properties[self.core_annotation_view.as_view_id()]
            text: Any = properties.get("startNodeText")
            annotation_type: str = edge.type.external_id

            if text and annotation_type:
                key: tuple[str, str] = (text, annotation_type)
                if key not in grouped_candidates:
                    grouped_candidates[key] = []
                grouped_candidates[key].append(edge)

        self.logger.debug(
            message=f"Grouped {len(candidates)} candidates into {len(grouped_candidates)} unique text/type combinations.",
        )
        self.logger.debug(
            message=f"Deduplication savings: {len(candidates) - len(grouped_candidates)} queries avoided.",
            section="END",
        )

        edges_to_update: list[EdgeApply] = []
        raw_rows_to_update: list[RowWrite] = []
        # TODO: think about whether we need to delete the cooresponding raw row of edges that we delete OR if it should be placed in another RAW table when rejected
        # raw_rows_to_delete: list[RowWrite] = []
        edges_to_delete: list[EdgeId] = []

        # Track results for this batch
        batch_promoted: int = 0
        batch_rejected: int = 0
        batch_ambiguous: int = 0

        try:
            # Process each unique text/type combination once
            for (text_to_find, annotation_type), edges_with_same_text in grouped_candidates.items():
                entity_space: str | None = (
                    self.file_view.instance_space
                    if annotation_type == "diagrams.FileLink"
                    else self.target_entities_view.instance_space
                )

                # NOTE: This occurs when no instance space is set in the data model views section extraction pipelines config file
                if not entity_space:
                    self.logger.warning(
                        f"Could not determine entity space for type '{annotation_type}'.\nPlease ensure an instance space is set in the Files and Target Entities data model views section of the extraction pipeline configuration.\nSkipping."
                    )
                    continue

                # Strategy: Check cache → Elasticsearch search
                found_entities: list[MatchedEntity] = self._find_entity_with_cache(
                    text_to_find, annotation_type, entity_space
                )

                # Determine result type for tracking AND deletion decision
                num_edges: int = len(edges_with_same_text)
                should_delete: bool = False

                if len(found_entities) == 1:
                    batch_promoted += num_edges
                    should_delete = False  # Never delete promoted edges
                elif len(found_entities) == 0:
                    batch_rejected += num_edges
                    should_delete = self.delete_rejected_edges
                else:  # Multiple matches
                    batch_ambiguous += num_edges
                    should_delete = self.delete_suggested_edges

                # Apply the same result to ALL edges with this text
                for edge in edges_with_same_text:
                    edge_apply, raw_row = self._prepare_edge_update(edge, found_entities)

                    if should_delete:
                        # Delete the edge but still update RAW row to track what happened
                        edges_to_delete.append(EdgeId(edge.space, edge.external_id))
                        if raw_row is not None:
                            raw_rows_to_update.append(raw_row)
                    else:
                        # Update both edge and RAW row
                        if edge_apply is not None:
                            edges_to_update.append(edge_apply)
                        if raw_row is not None:
                            raw_rows_to_update.append(raw_row)
        finally:
            # Update tracker with batch results
            self.tracker.add_edges(promoted=batch_promoted, rejected=batch_rejected, ambiguous=batch_ambiguous)

            try:
                if edges_to_update:
                    self.client.data_modeling.instances.apply(edges=edges_to_update)
                    self.logger.info(
                        f"Successfully updated {len(edges_to_update)} edges in data model:\n"
                        f"  ├─ Promoted: {batch_promoted}\n"
                        f"  ├─ Rejected: {batch_rejected}\n"
                        f"  └─ Ambiguous: {batch_ambiguous}",
                        section="BOTH",
                    )
            except Exception as e:
                self.logger.error("Error updating edges", error=e, section="BOTH")

            try:
                if edges_to_delete:
                    self.client.data_modeling.instances.delete(edges=edges_to_delete)
                    self.logger.info(
                        f"Successfully deleted {len(edges_to_delete)} edges from data model.", section="END"
                    )
            except Exception as e:
                self.logger.error("Error deleting edges", error=e, section="BOTH")

            try:
                if raw_rows_to_update:
                    self.client.raw.rows.insert(
                        db_name=self.raw_db,
                        table_name=self.raw_pattern_table,
                        row=raw_rows_to_update,
                        ensure_parent=True,
                    )
                    self.logger.info(
                        f"Successfully updated {len(raw_rows_to_update)} rows in RAW table.", section="END"
                    )
            except Exception as e:
                self.logger.error("Error updating RAW table", error=e, section="BOTH")

            if not edges_to_update and not edges_to_delete and not raw_rows_to_update:
                self.logger.info("No edges were updated in this run.", section="END")

        return None  # Continue running if more candidates might exist

    def _get_promote_candidates(self) -> EdgeList | None:
        """
        Retrieves pattern-mode annotation edges that are candidates for promotion.

        Uses query configuration from promote_function config.

        Args:
            None

        Returns:
            EdgeList of candidate edges, or None if no candidates found.
            Limited by getCandidatesQuery.limit (default 500 if -1/unlimited).
        """
        query_filter = build_filter_from_query(self.config.promote_function.get_candidates_query)
        limit = get_limit_from_query(self.config.promote_function.get_candidates_query)
        # If limit is -1 (unlimited), use sensible default
        if limit == -1:
            limit = 500  # NOTE: This may or may not be needed. The main benefit of this is having the ability to ensure edges are processed in the 10minute time constraint of Serverless Functions

        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view.as_view_id()],
            filter=query_filter,
            limit=limit,
            space=self.sink_node_ref.space,
        )

    def _find_entity_with_cache(self, text: str, annotation_type: str, entity_space: str) -> list[MatchedEntity]:
        """
        Finds entity for text using multi-tier caching strategy.

        Caching strategy (fastest to slowest):
        - TIER 1: In-memory cache (this run only, no API calls)
        - TIER 2: Persistent RAW cache (all runs, single RAW query, no retrieve_nodes)
        - TIER 3: EntitySearchService (Elasticsearch search on aliases)

        Caching behavior:
        - Only caches unambiguous single matches (len(found) == 1)
        - Caches negative results (no match found) to avoid repeated lookups
        - Does NOT cache ambiguous results (multiple matches)

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in for global fallback

        Returns:
            List of MatchedEntity objects:
            - Empty list [] if no match found
            - Single-element list [entity] if unambiguous match
            - Two-element list [entity1, entity2] if ambiguous (data quality issue)
        """
        # TIER 1 & 2: Check cache (in-memory + persistent) - no API calls on hit
        cached_info: CachedEntityInfo | None = self.cache_service.get(text, annotation_type)
        if cached_info is not None:
            return [MatchedEntity.from_cached_info(cached_info)]

        # Check if we've already determined there's no match
        # (negative caching is handled internally by cache service)
        if self.cache_service.get_from_memory(text, annotation_type) is None:
            # We've checked this before in this run and found nothing
            if (text, annotation_type) in self.cache_service._memory_cache:
                return []

        # TIER 3: Use EntitySearchService (Elasticsearch)
        found_nodes: list[Node] = self.entity_search_service.find_entity(text, annotation_type, entity_space)

        # Determine view for extracting resource type
        target_view_id = self.target_entities_view.as_view_id()

        # Convert nodes to MatchedEntity and update cache
        if found_nodes and len(found_nodes) == 1:
            # Unambiguous match - extract resource type and cache it
            node = found_nodes[0]
            matched = MatchedEntity.from_node(node, target_view_id)
            # Cache with resource type to avoid future retrieve_nodes calls
            self.cache_service.set(text, annotation_type, node, matched.resource_type)
            return [matched]
        elif not found_nodes:
            # No match - cache negative result
            self.cache_service.set(text, annotation_type, None)
            return []
        else:
            # Ambiguous results - don't cache, convert all to MatchedEntity
            return [MatchedEntity.from_node(node, target_view_id) for node in found_nodes]

    def _prepare_edge_update(
        self, edge: Edge, found_entities: list[MatchedEntity]
    ) -> tuple[EdgeApply | None, RowWrite | None]:
        """
        Prepares updates for both data model edge and RAW table based on entity search results.

        Handles three scenarios:
        1. Single match (len==1): Mark as "Approved", point edge to entity, add "PromotedAuto" tag
        2. No match (len==0): Mark as "Rejected", keep pointing to sink, add "PromoteAttempted" tag
        3. Ambiguous (len>=2): Keep "Suggested", add "PromoteAttempted" and "AmbiguousMatch" tags

        For all cases:
        - Retrieves existing RAW row to preserve all data
        - Updates edge properties (status, tags, endNode if match found)
        - Updates RAW row with same changes
        - Returns both for atomic update

        Args:
            edge: The annotation edge to update (pattern-mode annotation)
            found_entities: List of matched entities from cache or search
                - [] = no match
                - [entity] = single unambiguous match
                - [entity1, entity2] = ambiguous (multiple matches)

        Returns:
            Tuple of (EdgeApply, RowWrite):
            - EdgeApply: Edge update for data model
            - RowWrite: Row update for RAW table
            Both will always be returned (never None).
        """
        # Get the current edge properties before creating the write version
        edge_props: Any = edge.properties.get(self.core_annotation_view.as_view_id(), {})
        current_tags: Any = edge_props.get("tags", [])
        updated_tags: list[str] = list(current_tags) if isinstance(current_tags, list) else []

        # Now create the write version
        edge_apply: EdgeApply = edge.as_write()

        # Fetch existing RAW row to preserve all data
        raw_data: dict[str, Any] = {}
        try:
            existing_row: Any = self.client.raw.rows.retrieve(
                db_name=self.raw_db, table_name=self.raw_pattern_table, key=edge.external_id
            )
            if existing_row and existing_row.columns:
                raw_data = {k: v for k, v in existing_row.columns.items()}
        except Exception as e:
            self.logger.warning(f"Could not retrieve RAW row for edge {edge.external_id}: {e}")

        # Prepare update properties for the edge
        update_properties: dict[str, Any] = {}

        if len(found_entities) == 1:  # Success - single match found
            matched_entity: MatchedEntity = found_entities[0]
            self.logger.debug(
                f"✓ Found single match for '{edge_props.get('startNodeText')}' → {matched_entity.external_id}. \n\t- Promoting edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )

            # Update edge to point to the found entity
            edge_apply.end_node = DirectRelationReference(matched_entity.space, matched_entity.external_id)
            update_properties["status"] = DiagramAnnotationStatus.APPROVED.value
            updated_tags.append("PromotedAuto")

            # Update RAW row with new end node information
            raw_data["endNode"] = matched_entity.external_id
            raw_data["endNodeSpace"] = matched_entity.space
            raw_data["status"] = DiagramAnnotationStatus.APPROVED.value

            # Use resource type from matched entity (cached or freshly extracted)
            if matched_entity.resource_type:
                raw_data["endNodeResourceType"] = matched_entity.resource_type

        elif len(found_entities) == 0:  # Failure - no match found
            self.logger.debug(
                f"✗ No match found for '{edge_props.get('startNodeText')}'.\n\t- Rejecting edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )
            update_properties["status"] = DiagramAnnotationStatus.REJECTED.value
            updated_tags.append("PromoteAttempted")

            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        else:  # Ambiguous - multiple matches found
            self.logger.debug(
                f"⚠ Multiple matches found for '{edge_props.get('startNodeText')}'.\n\t- Ambiguous edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )
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
        raw_row: RowWrite | None = RowWrite(key=edge.external_id, columns=raw_data) if raw_data else None

        return edge_apply, raw_row
