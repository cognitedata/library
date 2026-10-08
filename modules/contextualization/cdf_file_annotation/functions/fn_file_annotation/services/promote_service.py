import abc
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.data_classes import Row, RowWrite
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeId,
    EdgeList,
    Node,
    NodeId,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.filters import And, ContainsAny, Equals, Filter, Or
from cognite.client.exceptions import CogniteAPIError
from fa_constants import (
    PROMOTE_RAW_FETCH_WORKERS,
    TAG_PROMOTE_ATTEMPTED,
    TAG_SCOPE_WIDE_DETECT,
    TRANSIENT_HTTP_CODES,
)
from services.config_service import Config, build_filter_from_query, get_limit_from_query
from services.entity_search_service import EntitySearchService
from services.logger_service import CogniteFunctionLogger
from services.promote_cache_service import CachedEntityInfo, CacheService
from utils.data_structures import DiagramAnnotationStatus, PromoteTracker, add_unique_tags


@dataclass
class MatchedEntity:
    """
    Unified representation of a matched entity from either cache or search.

    This dataclass provides a consistent interface for working with matched entities
    regardless of whether they came from the cache (CachedEntityInfo) or from
    a query on edges.
    """

    space: str
    external_id: str
    resource_type: str | None = None

    @classmethod
    def from_node(cls, node: Node, target_view_id: ViewId) -> "MatchedEntity":
        """Creates MatchedEntity from a Node object."""
        entity_props = (node.properties or {}).get(target_view_id) or {}
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


def _scope_text(properties: dict[str, object], name: str) -> str:
    """Text value of a scope property, or empty when the property is absent."""
    if not name:
        return ""
    value = properties.get(name)
    if isinstance(value, str):
        return value.strip()
    return ""


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
        self.promote_file_entities: bool = self.config.promote_function.promote_file_entities
        self.promote_target_entities: bool = self.config.promote_function.promote_target_entities
        apply_config = self.config.finalize_function.apply_service
        self.asset_suggest_threshold: float = apply_config.asset_auto_suggest_threshold
        self.file_suggest_threshold: float = apply_config.file_auto_suggest_threshold

        # Injected service dependencies
        self.entity_search_service = entity_search_service
        self.cache_service = cache_service
        # RAW pattern rows of the current batch, keyed by edge external id
        self._raw_rows: dict[str, dict[str, object]] = {}

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
        except CogniteAPIError as e:
            if e.code not in TRANSIENT_HTTP_CODES:
                self.logger.error("Could not read Promote candidates", error=e, section="BOTH")
                raise
            self.logger.error("Ran into a transient error", error=e)
            self.logger.info("Retrying in 15 seconds")
            time.sleep(15)
            return None

        self.logger.info(f"Found {len(candidates)} Promote candidates. Starting processing.")

        scope_by_file = self._scope_values_by_file(list(candidates))
        self._raw_rows = self._fetch_raw_rows([edge.external_id for edge in candidates])

        # Group by text, type, space, and scope so two sites do not share one search result.
        grouped_candidates: dict[tuple[str, str, str, str, str], list[Edge]] = {}
        # Edges without text can never match. They are rejected so the candidates query stops returning them.
        textless_edges: list[Edge] = []
        for edge in candidates:
            properties: dict[str, object] = (edge.properties or {}).get(self.core_annotation_view.as_view_id()) or {}
            text: object = properties.get("startNodeText")
            annotation_type: str = edge.type.external_id

            if not (isinstance(text, str) and text):
                textless_edges.append(edge)
            elif annotation_type:
                primary_value, secondary_value = scope_by_file.get(
                    (edge.start_node.space, edge.start_node.external_id), ("", "")
                )
                key = (
                    text,
                    annotation_type,
                    self._entity_space(annotation_type, edge),
                    primary_value,
                    secondary_value,
                )
                grouped_candidates.setdefault(key, []).append(edge)

        grouped_by_type: dict[str, dict[tuple[str, str, str, str], list[Edge]]] = {}

        for (
            text_to_find,
            annotation_type,
            entity_space,
            primary_value,
            secondary_value,
        ), edges_with_same_text in grouped_candidates.items():
            grouped_by_type.setdefault(annotation_type, {})[
                (text_to_find, entity_space, primary_value, secondary_value)
            ] = edges_with_same_text

        total_grouped = sum(len(m) for m in grouped_by_type.values())

        self.logger.info(
            message=(
                f"Grouped {len(candidates)} candidates into {total_grouped} unique "
                f"text/type combinations across {len(grouped_by_type)} types."
            ),
        )

        self.logger.debug(
            message=f"Deduplication savings: {len(candidates) - total_grouped} queries avoided.",
            section="END",
        )

        edges_to_update: list[EdgeApply] = []
        raw_rows_to_update: list[RowWrite] = []
        edges_to_delete: list[EdgeId] = []
        rejected_to_delete: int = 0
        ambiguous_to_delete: int = 0

        # Track results for this batch
        batch_promoted: int = 0
        batch_rejected: int = 0
        batch_ambiguous: int = 0

        try:
            for edge in textless_edges:
                batch_rejected += 1
                edge_apply, raw_row, _ = self._prepare_edge_update(edge, [])
                if self.delete_rejected_edges:
                    edges_to_delete.append(EdgeId(edge.space, edge.external_id))
                    rejected_to_delete += 1
                elif edge_apply is not None:
                    edges_to_update.append(edge_apply)
                if raw_row is not None:
                    raw_rows_to_update.append(raw_row)

            # Process each unique text/type combination once
            # Iterate per annotation type so we can check the search flag once per type
            for annotation_type, texts_map in grouped_by_type.items():
                if annotation_type == "diagrams.FileLink":
                    is_searching_annotation_type = self.promote_file_entities
                else:
                    is_searching_annotation_type = self.promote_target_entities

                if not is_searching_annotation_type:
                    self.logger.info(
                        f"Search disabled for annotation type '{annotation_type}'. "
                        f"Rejecting those edges without searching ({len(texts_map)} nodes).",
                        section="START",
                    )

                for (
                    text_to_find,
                    entity_space,
                    primary_value,
                    secondary_value,
                ), edges_with_same_text in texts_map.items():
                    # Strategy: Check cache → query edges → fallback to global search
                    found_entities: list[MatchedEntity] | list = []

                    if is_searching_annotation_type:
                        # Strategy: Check cache → query edges → fallback to global search
                        found_entities = self._find_entity_with_cache(
                            text_to_find,
                            annotation_type,
                            entity_space,
                            scope_filter=self._scope_filter(annotation_type, primary_value, secondary_value),
                            scope_key=self._scope_cache_key(primary_value, secondary_value),
                        )

                    for edge in edges_with_same_text:
                        is_self_reference = (
                            len(found_entities) == 1
                            and found_entities[0].space == edge.start_node.space
                            and found_entities[0].external_id == edge.start_node.external_id
                        )

                        if len(found_entities) >= 2:
                            edge_apply, raw_row, original_to_delete = self._prepare_ambiguous_edge(edge, found_entities)
                            is_rejected = (
                                raw_row is not None
                                and raw_row.columns.get("status") == DiagramAnnotationStatus.REJECTED.value
                            )
                            if is_rejected:
                                batch_rejected += 1
                            else:
                                batch_ambiguous += 1
                            if edge_apply is not None:
                                edges_to_update.append(edge_apply)
                            if raw_row is not None:
                                raw_rows_to_update.append(raw_row)
                            if original_to_delete is not None:
                                edges_to_delete.append(original_to_delete)
                                if is_rejected:
                                    rejected_to_delete += 1
                                else:
                                    ambiguous_to_delete += 1
                            continue

                        if len(found_entities) == 1 and not is_self_reference:
                            batch_promoted += 1
                            should_delete = False
                        else:  # No match or self-reference
                            batch_rejected += 1
                            should_delete = self.delete_rejected_edges

                        edge_apply, raw_row, edge_to_relocate = self._prepare_edge_update(edge, found_entities)

                        if should_delete:
                            edges_to_delete.append(EdgeId(edge.space, edge.external_id))
                            rejected_to_delete += 1
                            if raw_row is not None:
                                raw_rows_to_update.append(raw_row)
                        else:
                            if edge_to_relocate is not None:
                                edges_to_delete.append(edge_to_relocate)
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
            except CogniteAPIError as e:
                self.logger.error("Error updating edges", error=e, section="BOTH")
                raise

            try:
                if edges_to_delete:
                    self.client.data_modeling.instances.delete(edges=edges_to_delete)
                    if rejected_to_delete:
                        self.logger.info(
                            f"Sent {rejected_to_delete} rejected edges to the data model for deletion.",
                            section="END",
                        )
                    if ambiguous_to_delete:
                        self.logger.info(
                            f"Replaced {ambiguous_to_delete} ambiguous sink edge(s) with Suggested candidate edge(s).",
                            section="END",
                        )
            except CogniteAPIError as e:
                self.logger.error("Error deleting edges", error=e, section="BOTH")
                raise

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
            except CogniteAPIError as e:
                self.logger.error("Error updating RAW table", error=e, section="BOTH")
                raise

            if not edges_to_update and not edges_to_delete and not raw_rows_to_update:
                self.logger.info("No edges were updated in this run.", section="END")

        return None  # Continue running if more candidates might exist

    def _scope_property_names(self) -> tuple[str, str]:
        """Configured scope property names. Empty string means that level is off."""
        primary = (self.config.parameters.primary_scope_property or "").strip()
        secondary = (self.config.parameters.secondary_scope_property or "").strip()
        return primary, secondary

    def _promote_scope_enabled(self) -> bool:
        """True when the flag is on and at least one scope property name is set."""
        if not self.config.parameters.pattern_promote.filter_pattern_promote_by_scope:
            return False
        primary_name, secondary_name = self._scope_property_names()
        return bool(primary_name or secondary_name)

    def _scope_values_by_file(self, edges: list[Edge]) -> dict[tuple[str, str], tuple[str, str]]:
        """Primary and secondary scope values on each annotated file. Empty when scoping is off."""
        if not self._promote_scope_enabled():
            return {}

        primary_name, secondary_name = self._scope_property_names()
        file_ids: list[NodeId] = []
        seen: set[tuple[str, str]] = set()
        for edge in edges:
            key = (edge.start_node.space, edge.start_node.external_id)
            if key in seen:
                continue
            seen.add(key)
            file_ids.append(NodeId(space=key[0], external_id=key[1]))
        if not file_ids:
            return {}

        retrieved = self.client.data_modeling.instances.retrieve_nodes(
            nodes=file_ids, sources=self.file_view.as_view_id()
        )
        if retrieved is None:
            nodes: list[Node] = []
        elif isinstance(retrieved, Node):
            nodes = [retrieved]
        else:
            nodes = list(retrieved)

        by_id = {(node.space, node.external_id): node for node in nodes}
        view_id = self.file_view.as_view_id()
        values: dict[tuple[str, str], tuple[str, str]] = {}
        for file_id in file_ids:
            key = (file_id.space, file_id.external_id)
            node = by_id.get(key)
            properties: dict[str, object] = {}
            if node is not None and node.properties:
                raw_properties = node.properties.get(view_id)
                if isinstance(raw_properties, dict):
                    properties = raw_properties
            primary_value = _scope_text(properties, primary_name)
            secondary_value = _scope_text(properties, secondary_name)
            if primary_name and not primary_value:
                self.logger.warning(
                    f"File {key[0]}/{key[1]} has no '{primary_name}' value. "
                    "Promote will not apply that scope filter for its annotations."
                )
            if secondary_name and not secondary_value:
                self.logger.warning(
                    f"File {key[0]}/{key[1]} has no '{secondary_name}' value. "
                    "Promote will not apply that scope filter for its annotations."
                )
            values[key] = (primary_value, secondary_value)
        return values

    def _scope_filter(self, annotation_type: str, primary_value: str, secondary_value: str) -> Filter | None:
        """Equals filters for the file's scope. Secondary also keeps ScopeWideDetect entities."""
        if not self._promote_scope_enabled():
            return None
        view = self.file_view if annotation_type == "diagrams.FileLink" else self.target_entities_view
        primary_name, secondary_name = self._scope_property_names()
        clauses: list[Filter] = []
        if primary_name and primary_value:
            clauses.append(Equals(view.as_property_ref(primary_name), primary_value))
        if secondary_name and secondary_value:
            clauses.append(
                Or(
                    Equals(view.as_property_ref(secondary_name), secondary_value),
                    ContainsAny(view.as_property_ref("tags"), [TAG_SCOPE_WIDE_DETECT]),
                )
            )
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return And(*clauses)

    def _scope_cache_key(self, primary_value: str, secondary_value: str) -> str:
        """Cache discriminator so a match in one scope is not reused in another."""
        if not self._promote_scope_enabled():
            return ""
        primary_name, secondary_name = self._scope_property_names()
        parts: list[str] = []
        if primary_name and primary_value:
            parts.append(f"{primary_name}={primary_value}")
        if secondary_name and secondary_value:
            parts.append(f"{secondary_name}={secondary_value}")
        return "|".join(parts)

    def _entity_space(self, annotation_type: str, edge: Edge) -> str:
        """The space to search for the entity: the view's instanceSpace, else the space of the annotated file."""
        view = self.file_view if annotation_type == "diagrams.FileLink" else self.target_entities_view
        return view.instance_space or edge.start_node.space

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
        debug_file = self.config.debug_file
        if debug_file:
            query_filter &= Equals(
                ["edge", "startNode"], {"space": debug_file.space, "externalId": debug_file.external_id}
            )
        limit = get_limit_from_query(self.config.promote_function.get_candidates_query)
        # If limit is -1 (unlimited), use sensible default
        if limit == -1:
            # NOTE: Caps batch size so edges can finish within the 10-minute serverless limit.
            limit = 500

        return self.client.data_modeling.instances.list(
            instance_type="edge",
            sources=[self.core_annotation_view.as_view_id()],
            filter=query_filter,
            limit=limit,
            space=self.sink_node_ref.space,
        )

    def _find_entity_with_cache(
        self,
        text: str,
        annotation_type: str,
        entity_space: str,
        *,
        scope_filter: Filter | None = None,
        scope_key: str = "",
    ) -> list[MatchedEntity]:
        """
        Finds entity for text using multi-tier caching strategy.

        Caching strategy (fastest to slowest):
        - TIER 1: In-memory cache (this run only, no API calls)
        - TIER 2: Persistent RAW cache (all runs, single RAW query, no retrieve_nodes)
        - TIER 3: EntitySearchService (global entity search, server-side IN filter on aliases)

        Caching behavior:
        - Only caches unambiguous single matches (len(found_entities) == 1)
        - Caches CacheMarkers for ambiguous and no match cases to avoid repeated lookups

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in for global fallback
            scope_filter: Scope constraint applied to the entity search. None searches the whole space.
            scope_key: Cache discriminator for that scope. Empty when the search is not scope-filtered.

        Returns:
            List of MatchedEntity objects:
            - Empty list [] if no match found
            - Single-element list [entity] if unambiguous match
            - Two-element list [entity1, entity2] if ambiguous (data quality issue)
        """
        # Gate: source normalizePatterns must match before cache/search
        if not self.entity_search_service.generate_text_variations(text, annotation_type):
            pattern_label = (
                "fileNormalizationPatterns" if annotation_type == "diagrams.FileLink" else "entityNormalizationPatterns"
            )
            self.logger.debug(f"✗ Text '{text}' does not match {pattern_label} — skipping API search.")
            self.cache_service.set_no_match(
                text,
                annotation_type,
                entity_space,
                reason=f"{pattern_label} did not match; API search skipped",
            )
            return []

        # TIER 1 & 2: Check cache (in-memory + persistent) - no API calls on hit
        cached_info: CachedEntityInfo | None = self.cache_service.get(
            text, annotation_type, entity_space, scope_key=scope_key
        )

        if cached_info is not None:
            return [MatchedEntity.from_cached_info(cached_info)]

        if self.cache_service.is_ambiguous_in_memory(text, annotation_type, entity_space, scope_key=scope_key):
            cached_ambiguous = self.cache_service.get_ambiguous_entities(
                text, annotation_type, entity_space, scope_key=scope_key
            )
            if cached_ambiguous:
                self.logger.debug(f"✓ [CACHE] Using in-memory ambiguous candidates for '{text}' (skipping search)")
                return [MatchedEntity.from_cached_info(info) for info in cached_ambiguous]
            self.logger.debug(f"✓ [CACHE] Ambiguous marker for '{text}' without candidates — re-searching")

        if self.cache_service.is_no_match_in_memory(text, annotation_type, entity_space, scope_key=scope_key):
            self.logger.debug(f"✓ [CACHE] Using in-memory NO_MATCH marker for '{text}' (skipping search)")
            return []

        # TIER 3: Use EntitySearchService
        if scope_filter is None:
            found_nodes: list[Node] = self.entity_search_service.find_entity(text, annotation_type, entity_space)
        else:
            found_nodes = self.entity_search_service.find_entity(
                text, annotation_type, entity_space, scope_filter=scope_filter
            )

        # Determine view for extracting resource type
        target_view_id = self.target_entities_view.as_view_id()

        # Convert nodes to MatchedEntity and update cache
        if found_nodes and len(found_nodes) == 1:
            # Unambiguous match - extract resource type and cache it
            node = found_nodes[0]
            matched = MatchedEntity.from_node(node, target_view_id)
            # Cache with resource type to avoid future retrieve_nodes calls
            self.cache_service.set(
                text, annotation_type, entity_space, node, matched.resource_type, scope_key=scope_key
            )
            return [matched]
        elif not found_nodes:
            # API search ran and returned nothing — cache that for the rest of this run
            self.cache_service.set_no_match(
                text,
                annotation_type,
                entity_space,
                reason="API search returned no entity",
                scope_key=scope_key,
            )
            return []
        else:
            # Ambiguous - cache candidates in-memory for Suggested edge expansion
            matched_entities = [MatchedEntity.from_node(node, target_view_id) for node in found_nodes]
            try:
                self.cache_service.set_ambiguous(
                    text,
                    annotation_type,
                    entity_space,
                    scope_key=scope_key,
                    entities=[
                        CachedEntityInfo(
                            space=matched.space,
                            external_id=matched.external_id,
                            resource_type=matched.resource_type,
                        )
                        for matched in matched_entities
                    ],
                )
                self.logger.debug(f"✓ [CACHE] Marked '{text}' as ambiguous in memory")
            except (CogniteAPIError, ValueError, TypeError) as e:
                self.logger.debug(f"[CACHE] Failed to set ambiguous marker for '{text}' (continuing): {e}")

            return matched_entities

    def _prepare_edge_update(
        self, edge: Edge, found_entities: list[MatchedEntity]
    ) -> tuple[EdgeApply | None, RowWrite | None, EdgeId | None]:
        """
        Prepares updates for both data model edge and RAW table based on entity search results.

        Handles three scenarios:
        1. Single match (len==1): Mark as "Approved", point edge to entity, add "PromotedAuto" tag
        2. No match (len==0): Mark as "Rejected", keep pointing to sink, add "PromoteAttempted" tag
        3. Self-reference (len==1 to start node): Mark as "Rejected"

        Ambiguous matches (len>=2) are handled by ``_prepare_ambiguous_edge``.

        For all cases:
        - Retrieves existing RAW row to preserve all data
        - Updates edge properties (status, tags, endNode if match found)
        - Updates RAW row with same changes
        - Returns both for atomic update

        When a pattern-mode edge is promoted, it is written in the file instance space (same as
        regular diagram-detect annotations) and the caller deletes the copy in the pattern space.

        Args:
            edge: The annotation edge to update (pattern-mode annotation)
            found_entities: List of matched entities from cache or search
                - [] = no match
                - [entity] = single unambiguous match (or self-reference)

        Returns:
            Tuple of (EdgeApply, RowWrite, EdgeId | None):
            - EdgeApply: Edge update for data model
            - RowWrite: Row update for RAW table
            - EdgeId: Pattern-space edge to delete after a successful relocate, if any
        """
        # Get the current edge properties before creating the write version
        edge_props: dict[str, object] = edge.properties.get(self.core_annotation_view.as_view_id(), {})
        current_tags: object = edge_props.get("tags", [])
        updated_tags: list[str] = list(current_tags) if isinstance(current_tags, list) else []

        file_instance_space = edge.start_node.space
        edge_to_relocate: EdgeId | None = None

        # Now create the write version
        edge_apply: EdgeApply = edge.as_write()

        # Existing RAW row, to preserve all data
        raw_data: dict[str, object] = self._existing_raw_columns(edge.external_id)

        # Prepare update properties for the edge
        update_properties: dict[str, object] = {}

        if len(found_entities) == 1 and not (
            found_entities[0].space == edge.start_node.space
            and found_entities[0].external_id == edge.start_node.external_id
        ):  # Success - single match found
            matched_entity: MatchedEntity = found_entities[0]
            self.logger.debug(
                f"✓ Found single match for '{edge_props.get('startNodeText')}' → "
                f"{matched_entity.external_id}.\n"
                f"\t- Promoting edge: ({edge.space}, {edge.external_id})\n"
                f"\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )

            # Update edge to point to the found entity
            edge_apply.end_node = DirectRelationReference(matched_entity.space, matched_entity.external_id)
            update_properties["status"] = DiagramAnnotationStatus.APPROVED.value
            updated_tags = add_unique_tags(updated_tags, "PromotedAuto")

            if edge.space != file_instance_space:
                edge_to_relocate = EdgeId(edge.space, edge.external_id)
                edge_apply.space = file_instance_space
                # as_write() copies the pattern-space version; the file-space edge is a create.
                edge_apply.existing_version = None
                self.logger.debug(
                    f"\t- Relocating promoted edge from ({edge.space}, {edge.external_id}) "
                    f"to file space {file_instance_space}."
                )

            # Update RAW row with new end node information
            raw_data["endNode"] = matched_entity.external_id
            raw_data["endNodeSpace"] = matched_entity.space
            raw_data["status"] = DiagramAnnotationStatus.APPROVED.value

            # Use resource type from matched entity (cached or freshly extracted)
            if matched_entity.resource_type:
                raw_data["endNodeResourceType"] = matched_entity.resource_type

        elif len(found_entities) == 1 and (
            found_entities[0].space == edge.start_node.space
            and found_entities[0].external_id == edge.start_node.external_id
        ):  # Failure - single match, but it's a self-reference
            self.logger.info(
                f"✗ Only self-reference match for '{edge_props.get('startNodeText')}'.\n"
                f"\t- Rejecting edge: ({edge.space}, {edge.external_id})\n"
                f"\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )
            update_properties["status"] = DiagramAnnotationStatus.REJECTED.value
            updated_tags = add_unique_tags(updated_tags, TAG_PROMOTE_ATTEMPTED)
            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        else:  # Failure - no match found (or normalizePatterns filtered the text)
            start_text = edge_props.get("startNodeText")
            start_text_str = str(start_text) if start_text is not None else ""
            annotation_type = edge.type.external_id
            pattern_label = (
                "fileNormalizationPatterns" if annotation_type == "diagrams.FileLink" else "entityNormalizationPatterns"
            )
            if not self.entity_search_service.generate_text_variations(start_text_str, annotation_type):
                self.logger.debug(
                    f"✗ Text '{start_text}' does not match {pattern_label} — rejecting without search.\n"
                    f"\t- Rejecting edge: ({edge.space}, {edge.external_id})\n"
                    f"\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
                )
            else:
                self.logger.debug(
                    f"✗ No match found for '{start_text}'.\n"
                    f"\t- Rejecting edge: ({edge.space}, {edge.external_id})\n"
                    f"\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
                )
            update_properties["status"] = DiagramAnnotationStatus.REJECTED.value
            updated_tags = add_unique_tags(updated_tags, TAG_PROMOTE_ATTEMPTED)

            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        # Update edge properties. Merge into the existing props so a relocate create
        # (new space) still carries startNodeText, bbox, sourceCreatedUser, etc.
        update_properties["tags"] = updated_tags
        raw_data["tags"] = updated_tags
        merged_properties = dict(edge_props)
        merged_properties.update(update_properties)
        edge_apply.sources[0] = NodeOrEdgeData(
            source=self.core_annotation_view.as_view_id(), properties=merged_properties
        )

        # Create RowWrite object for RAW table update
        raw_row: RowWrite | None = RowWrite(key=edge.external_id, columns=raw_data) if raw_data else None

        return edge_apply, raw_row, edge_to_relocate

    def _retrieve_raw_columns(self, key: str) -> dict[str, object]:
        """Columns of one RAW pattern row, or {} when it is missing or cannot be read."""
        try:
            row: Row | None = self.client.raw.rows.retrieve(
                db_name=self.raw_db, table_name=self.raw_pattern_table, key=key
            )
        except CogniteAPIError as e:
            self.logger.warning(f"Could not retrieve RAW row for edge {key}: {e}")
            return {}
        return dict(row.columns.items()) if row and row.columns else {}

    def _fetch_raw_rows(self, keys: list[str]) -> dict[str, dict[str, object]]:
        """RAW pattern rows of one batch, read in parallel.

        Args:
            keys: Edge external ids, which are also the RAW row keys.

        Returns:
            Columns per key.
        """
        unique_keys = list(dict.fromkeys(keys))
        with ThreadPoolExecutor(max_workers=PROMOTE_RAW_FETCH_WORKERS) as pool:
            return dict(zip(unique_keys, pool.map(self._retrieve_raw_columns, unique_keys), strict=True))

    def _existing_raw_columns(self, key: str) -> dict[str, object]:
        """A copy of the batch's RAW row for this edge, read now when the batch did not fetch it."""
        if key not in self._raw_rows:
            self._raw_rows[key] = self._retrieve_raw_columns(key)
        return dict(self._raw_rows[key])

    def _suggest_threshold_for_type(self, annotation_type: str) -> float:
        """Configured auto-suggest threshold for this annotation type."""
        if annotation_type == "diagrams.FileLink":
            return self.file_suggest_threshold
        return self.asset_suggest_threshold

    def _prepare_ambiguous_edge(
        self, edge: Edge, found_entities: list[MatchedEntity]
    ) -> tuple[EdgeApply | None, RowWrite | None, EdgeId | None]:
        """
        Replaces a sink-pointing pattern edge with one Suggested edge to the first candidate.

        Other candidates are listed in ``description`` for a custom picker. Confidence is set to
        the configured auto-suggest threshold (not the pattern detect confidence of 1). The edge
        is written in the file instance space so Fusion can resolve the end node; the caller
        deletes the original pattern-space edge when relocating.

        When every candidate is unusable (self-reference or missing external id), the edge is
        rejected like a failed promote so it is not selected again on the next run.

        Args:
            edge: Pattern-mode annotation still pointing at the sink.
            found_entities: Two or more matched entities from search/cache.

        Returns:
            EdgeApply, RAW row, and the original edge id to delete when space changes (or always
            when replacing the sink stub in pattern space). On reject-with-delete, EdgeApply is
            None and the third value is the edge to delete.
        """
        view_id = self.core_annotation_view.as_view_id()
        edge_props: dict[str, object] = dict(edge.properties.get(view_id, {}) or {})
        current_tags: object = edge_props.get("tags", [])
        base_tags: list[str] = list(current_tags) if isinstance(current_tags, list) else []
        candidate_tags = add_unique_tags(base_tags, TAG_PROMOTE_ATTEMPTED, "AmbiguousMatch")
        file_instance_space = edge.start_node.space
        annotation_type = edge.type.external_id
        confidence = self._suggest_threshold_for_type(annotation_type)

        raw_data: dict[str, object] = self._existing_raw_columns(edge.external_id)

        candidates = [
            entity
            for entity in found_entities
            if entity.external_id
            and not (entity.space == edge.start_node.space and entity.external_id == edge.start_node.external_id)
        ]
        if not candidates:
            self.logger.warning(
                f"Ambiguous match for '{edge_props.get('startNodeText')}' had no usable candidates; "
                f"rejecting edge ({edge.space}, {edge.external_id})."
            )
            edge_apply, raw_row, _ = self._prepare_edge_update(edge, [])
            edge_to_delete = EdgeId(edge.space, edge.external_id) if self.delete_rejected_edges else None
            if self.delete_rejected_edges:
                edge_apply = None
            return edge_apply, raw_row, edge_to_delete

        primary = candidates[0]
        alternatives = candidates[1:]
        description = self._ambiguous_alternatives_description(alternatives)
        self.logger.debug(
            f"⚠ Ambiguous match for '{edge_props.get('startNodeText')}': "
            f"linking Suggested edge to {primary.external_id} "
            f"(alternatives: {', '.join(f'{a.space}/{a.external_id}' for a in alternatives) or 'none'}); "
            f"confidence={confidence}."
        )

        properties = dict(edge_props)
        properties["status"] = DiagramAnnotationStatus.SUGGESTED.value
        properties["confidence"] = confidence
        properties["tags"] = list(candidate_tags)
        properties["description"] = description

        edge_to_delete: EdgeId | None = None
        edge_space = edge.space
        existing_version: int | None = edge.version if hasattr(edge, "version") else None
        if edge.space != file_instance_space:
            edge_to_delete = EdgeId(edge.space, edge.external_id)
            edge_space = file_instance_space
            existing_version = None

        edge_apply = EdgeApply(
            space=edge_space,
            external_id=edge.external_id,
            type=edge.type,
            start_node=edge.start_node,
            end_node=DirectRelationReference(primary.space, primary.external_id),
            existing_version=existing_version,
            sources=[NodeOrEdgeData(source=view_id, properties=properties)],
        )

        raw_data["endNode"] = primary.external_id
        raw_data["endNodeSpace"] = primary.space
        raw_data["status"] = DiagramAnnotationStatus.SUGGESTED.value
        raw_data["confidence"] = confidence
        raw_data["tags"] = list(candidate_tags)
        raw_data["description"] = description
        if primary.resource_type:
            raw_data["endNodeResourceType"] = primary.resource_type
        raw_row = RowWrite(key=edge.external_id, columns=raw_data)

        return edge_apply, raw_row, edge_to_delete

    @staticmethod
    def _ambiguous_alternatives_description(alternatives: list[MatchedEntity]) -> str:
        """Parseable description listing candidate entities not used as endNode."""
        if not alternatives:
            return "AmbiguousMatch alternatives: none"
        refs = "; ".join(f"{entity.space}/{entity.external_id}" for entity in alternatives)
        return f"AmbiguousMatch alternatives: {refs}"
