import abc
import time
from typing import Any, Literal
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
from services.ConfigService import Config, build_filter_from_query, get_limit_from_query
from services.LoggerService import CogniteFunctionLogger
from services.CacheService import CacheService
from services.EntitySearchService import EntitySearchService
from utils.DataStructures import DiagramAnnotationStatus, PromoteTracker


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
        # Prefer promote_function config if available, otherwise fallback to finalize_function config
        if self.config.promote_function:
            self.raw_db = self.config.promote_function.raw_db
            self.raw_pattern_table = self.config.promote_function.raw_table_doc_pattern
            self.raw_doc_doc_table = self.config.promote_function.raw_table_doc_doc
            self.raw_doc_tag_table = self.config.promote_function.raw_table_doc_tag
        else:
            # Backward compatibility: use finalize_function config
            self.logger.warning(
                "promote_function config not found. Using finalize_function config for backward compatibility."
            )
            self.raw_db = self.config.finalize_function.apply_service.raw_db
            self.raw_pattern_table = self.config.finalize_function.apply_service.raw_table_doc_pattern
            self.raw_doc_doc_table = self.config.finalize_function.apply_service.raw_table_doc_doc
            self.raw_doc_tag_table = self.config.finalize_function.apply_service.raw_table_doc_tag

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
            self.logger.error(f"Ran into the following error: {str(e)}")
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

        self.logger.info(
            message=f"Grouped {len(candidates)} candidates into {len(grouped_candidates)} unique text/type combinations.",
        )
        self.logger.info(
            message=f"Deduplication savings: {len(candidates) - len(grouped_candidates)} queries avoided.",
            section="END",
        )

        edges_to_update: list[EdgeApply] = []
        raw_rows_to_update: list[RowWrite] = []

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

                if not entity_space:
                    self.logger.warning(f"Could not determine entity space for type '{annotation_type}'. Skipping.")
                    continue

                # Strategy: Check cache → query edges → fallback to global search
                found_nodes: list[Node] | list = self._find_entity_with_cache(
                    text_to_find, annotation_type, entity_space
                )

                # Determine result type for tracking
                num_edges: int = len(edges_with_same_text)
                if len(found_nodes) == 1:
                    batch_promoted += num_edges
                elif len(found_nodes) == 0:
                    batch_rejected += num_edges
                else:  # Multiple matches
                    batch_ambiguous += num_edges

                # Apply the same result to ALL edges with this text
                for edge in edges_with_same_text:
                    edge_apply, raw_row = self._prepare_edge_update(edge, found_nodes)

                    if edge_apply is not None:
                        edges_to_update.append(edge_apply)
                    if raw_row is not None:
                        raw_rows_to_update.append(raw_row)
        finally:
            # Update tracker with batch results
            self.tracker.add_edges(promoted=batch_promoted, rejected=batch_rejected, ambiguous=batch_ambiguous)

            if edges_to_update:
                self.client.data_modeling.instances.apply(edges=edges_to_update)
                self.logger.info(
                    f"Successfully updated {len(edges_to_update)} edges in data model:\n"
                    f"  ├─ Promoted: {batch_promoted}\n"
                    f"  ├─ Rejected: {batch_rejected}\n"
                    f"  └─ Ambiguous: {batch_ambiguous}",
                    section="END",
                )

            if raw_rows_to_update:
                self.client.raw.rows.insert(
                    db_name=self.raw_db,
                    table_name=self.raw_pattern_table,
                    row=raw_rows_to_update,
                    ensure_parent=True,
                )
                self.logger.info(f"Successfully updated {len(raw_rows_to_update)} rows in RAW table.", section="END")

            if not edges_to_update and not raw_rows_to_update:
                self.logger.info("No edges were updated in this run.", section="END")

        return None  # Continue running if more candidates might exist

    def _get_promote_candidates(self) -> EdgeList | None:
        """
        Retrieves pattern-mode annotation edges that are candidates for promotion.

        Uses query configuration from promote_function config if available, otherwise falls back
        to hardcoded filter for backward compatibility.

        Default query criteria (when no config):
        - End node is the sink node (placeholder for unresolved entities)
        - Status is "Suggested" (not yet approved/rejected)
        - Tags do not contain "PromoteAttempted" (haven't been processed yet)

        Args:
            None

        Returns:
            EdgeList of candidate edges, or None if no candidates found.
            Limited by getCandidatesQuery.limit (default 500 if -1/unlimited).
        """
        # Use query config if available
        if self.config.promote_function and self.config.promote_function.get_candidates_query:
            query_filter = build_filter_from_query(self.config.promote_function.get_candidates_query)
            limit = get_limit_from_query(self.config.promote_function.get_candidates_query)
            # If limit is -1 (unlimited), use sensible default
            if limit == -1:
                limit = 500
        else:
            # Backward compatibility: hardcoded filter
            query_filter = {
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
            }
            limit = 500  # Default batch size

        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view.as_view_id()],
            filter=query_filter,
            limit=limit,
        )

    def _find_entity_with_cache(self, text: str, annotation_type: str, entity_space: str) -> list[Node] | list:
        """
        Finds entity for text using multi-tier caching strategy.

        Caching strategy (fastest to slowest):
        - TIER 1: In-memory cache (this run only, in-memory dictionary)
        - TIER 2: Persistent RAW cache (all runs, single database query)
        - TIER 3: EntitySearchService (global entity search, server-side IN filter on aliases)

        Caching behavior:
        - Only caches unambiguous single matches (len(found_nodes) == 1)
        - Caches negative results (no match found) to avoid repeated lookups
        - Does NOT cache ambiguous results (multiple matches)

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in for global fallback

        Returns:
            List of matched Node objects:
            - Empty list [] if no match found
            - Single-element list [node] if unambiguous match
            - Two-element list [node1, node2] if ambiguous (data quality issue)
        """
        # TIER 1 & 2: Check cache (in-memory + persistent)
        cached_node: Node | None = self.cache_service.get(text, annotation_type)
        if cached_node is not None:
            return [cached_node]

        # Check if we've already determined there's no match
        # (negative caching is handled internally by cache service)
        if self.cache_service.get_from_memory(text, annotation_type) is None:
            # We've checked this before in this run and found nothing
            if (text, annotation_type) in self.cache_service._memory_cache:
                return []

        # TIER 3 & 4: Use EntitySearchService (edges → global search)
        found_nodes: list[Node] = self.entity_search_service.find_entity(text, annotation_type, entity_space)

        # Update cache based on result
        if found_nodes and len(found_nodes) == 1:
            # Unambiguous match - cache it
            self.cache_service.set(text, annotation_type, found_nodes[0])
        elif not found_nodes:
            # No match - cache negative result
            self.cache_service.set(text, annotation_type, None)
        # Don't cache ambiguous results (len > 1)

        return found_nodes

    def _prepare_edge_update(
        self, edge: Edge, found_nodes: list[Node] | list
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
            found_nodes: List of matched entity nodes from entity search
                - [] = no match
                - [node] = single unambiguous match
                - [node1, node2] = ambiguous (multiple matches)

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

        if len(found_nodes) == 1:  # Success - single match found
            matched_node: Node = found_nodes[0]
            self.logger.info(
                f"✓ Found single match for '{edge_props.get('startNodeText')}' → {matched_node.external_id}. \n\t- Promoting edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )

            # Update edge to point to the found entity
            edge_apply.end_node = DirectRelationReference(matched_node.space, matched_node.external_id)
            update_properties["status"] = DiagramAnnotationStatus.APPROVED.value
            updated_tags.append("PromotedAuto")

            # Update RAW row with new end node information
            raw_data["endNode"] = matched_node.external_id
            raw_data["endNodeSpace"] = matched_node.space
            raw_data["status"] = DiagramAnnotationStatus.APPROVED.value

            # Get resource type from the matched entity
            entity_props: Any = matched_node.properties.get(self.target_entities_view.as_view_id(), {})
            resource_type: Any = entity_props.get("resourceType") or entity_props.get("type")
            if resource_type:
                raw_data["endNodeResourceType"] = resource_type

        elif len(found_nodes) == 0:  # Failure - no match found
            self.logger.info(
                f"✗ No match found for '{edge_props.get('startNodeText')}'.\n\t- Rejecting edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )
            update_properties["status"] = DiagramAnnotationStatus.REJECTED.value
            updated_tags.append("PromoteAttempted")

            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        else:  # Ambiguous - multiple matches found
            self.logger.info(
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
