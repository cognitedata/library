import abc
import time
from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote

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
from fa_constants import TAG_SCOPE_WIDE_DETECT
from services.ConfigService import Config, build_filter_from_query, get_limit_from_query
from services.EntitySearchService import EntitySearchService
from services.LoggerService import CogniteFunctionLogger
from services.PromoteCacheService import CachedEntityInfo, CacheService
from utils.DataStructures import DiagramAnnotationStatus, PromoteTracker, add_unique_tags


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
    _DIAGRAM_PARSING_VERSION = "20230101-alpha"
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
        except CogniteAPIError as e:
            self.logger.error("Ran into the following error", error=e)
            self.logger.info("Retrying in 15 seconds")
            time.sleep(15)
            return None

        self.logger.info(f"Found {len(candidates)} Promote candidates. Starting processing.")

        scope_by_file = self._scope_values_by_file(list(candidates))

        # Group by text, type, space, and scope so two sites do not share one search result.
        grouped_candidates: dict[tuple[str, str, str, str, str], list[Edge]] = {}
        for edge in candidates:
            properties: dict[str, object] = (edge.properties or {}).get(self.core_annotation_view.as_view_id()) or {}
            text: object = properties.get("startNodeText")
            annotation_type: str = edge.type.external_id

            if isinstance(text, str) and text and annotation_type:
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
            message=f"Grouped {len(candidates)} candidates into {total_grouped} unique text/type combinations across {len(grouped_by_type)} types.",
        )

        self.logger.debug(
            message=f"Deduplication savings: {len(candidates) - total_grouped} queries avoided.",
            section="END",
        )

        edges_to_update: list[EdgeApply] = []
        raw_rows_to_update: list[RowWrite] = []
        # Think about whether we need to delete the corresponding raw row of edges that we delete OR if it should be placed in another RAW table when rejected
        # raw_rows_to_delete: list[RowWrite] = []
        edges_to_delete: list[EdgeId] = []
        rejected_to_delete: int = 0
        ambiguous_to_delete: int = 0

        # Track results for this batch
        batch_promoted: int = 0
        batch_rejected: int = 0
        batch_ambiguous: int = 0
        promoted_edges: list[Edge] = []

        try:
            # Process each unique text/type combination once
            # Iterate per annotation type so we can check the search flag once per type
            for annotation_type, texts_map in grouped_by_type.items():
                if annotation_type == "diagrams.FileLink":
                    is_searching_annotation_type = self.promote_file_entities
                else:
                    is_searching_annotation_type = self.promote_target_entities

                if not is_searching_annotation_type:
                    self.logger.info(
                        f"Search disabled for annotation type '{annotation_type}'. It will reject those edges without searching ({len(texts_map)} nodes).",
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

                        if len(found_entities) == 1 and not is_self_reference:
                            batch_promoted += 1
                            should_delete = False
                            promoted_edges.append(edge)
                        elif len(found_entities) == 0 or is_self_reference:
                            batch_rejected += 1
                            should_delete = self.delete_rejected_edges
                        else:  # Multiple matches
                            batch_ambiguous += 1
                            should_delete = self.delete_suggested_edges

                        edge_apply, raw_row = self._prepare_edge_update(edge, found_entities)

                        if should_delete:
                            edges_to_delete.append(EdgeId(edge.space, edge.external_id))
                            if len(found_entities) == 0 or is_self_reference:
                                rejected_to_delete += 1
                            else:
                                ambiguous_to_delete += 1
                            if raw_row is not None:
                                raw_rows_to_update.append(raw_row)
                        else:
                            if edge_apply is not None:
                                edges_to_update.append(edge_apply)
                            if raw_row is not None:
                                raw_rows_to_update.append(raw_row)
        finally:
            # Update tracker with batch results
            self.tracker.add_edges(promoted=batch_promoted, rejected=batch_rejected, ambiguous=batch_ambiguous)

            edges_applied = False
            try:
                if edges_to_update:
                    self.client.data_modeling.instances.apply(edges=edges_to_update)
                    edges_applied = True
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
            if edges_applied:
                self._verify_promoted_diagram_entities(promoted_edges)

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
                            f"Sent {ambiguous_to_delete} ambiguous edges to the data model for deletion.",
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
            limit = 500  # NOTE: This may or may not be needed. The main benefit of this is having the ability to ensure edges are processed in the 10minute time constraint of Serverless Functions

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
            self.logger.debug(f"✓ [CACHE] Using in-memory ambiguous marker for '{text}' (skipping search)")
            return [MatchedEntity(space="", external_id=""), MatchedEntity(space="", external_id="")]

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
            # Ambiguous - cache negative result (in-memory AMBIGUOUS)
            try:
                self.cache_service.set_ambiguous(text, annotation_type, entity_space, scope_key=scope_key)
                self.logger.debug(f"✓ [CACHE] Marked '{text}' as ambiguous in memory")
            except (CogniteAPIError, ValueError, TypeError) as e:
                self.logger.debug(f"[CACHE] Failed to set ambiguous marker for '{text}' (continuing): {e}")

            return [MatchedEntity.from_node(node, target_view_id) for node in found_nodes]

    def _verify_promoted_diagram_entities(self, edges: list[Edge]) -> None:
        """Set isAssetVerified on diagram-parsing entities linked to single-match promotions.

        Diagram parsing keeps its own entity per tag. A CogniteDiagramAnnotation status of
        Approved does not set that entity's isAssetVerified flag. Only entities whose
        annotationId is one of these edges, and whose flag is not already true, are updated.

        Args:
            edges: Pattern edges that this batch promoted with a single match.
        """
        annotations_by_diagram: dict[tuple[str, str, int], set[tuple[str, str]]] = {}
        for edge in edges:
            diagram_key = (edge.start_node.space, edge.start_node.external_id, self._annotation_page(edge))
            annotations_by_diagram.setdefault(diagram_key, set()).add((edge.space, edge.external_id))

        updates: list[dict[str, object]] = []
        for (file_space, file_external_id, page), annotation_ids in annotations_by_diagram.items():
            entities = self._list_diagram_entities(file_space, file_external_id, page)
            linked: set[tuple[str, str]] = set()
            for entity in entities:
                annotation = entity.get("annotationId")
                if not isinstance(annotation, dict):
                    continue
                annotation_space = annotation.get("space")
                annotation_external_id = annotation.get("externalId")
                if not isinstance(annotation_space, str) or not isinstance(annotation_external_id, str):
                    continue
                identity = (annotation_space, annotation_external_id)
                if identity not in annotation_ids:
                    continue
                linked.add(identity)
                if entity.get("isAssetVerified") is True:
                    continue
                entity_external_id = entity.get("externalId")
                if isinstance(entity_external_id, str) and entity_external_id:
                    updates.append({"externalId": entity_external_id, "update": {"isAssetVerified": True}})
            missing = len(annotation_ids - linked)
            if missing:
                self.logger.debug(
                    f"No diagram entity linked to {missing} approved annotation(s) on "
                    f"({file_space}, {file_external_id}) page {page}."
                )

        if not updates:
            return
        self._update_diagram_entities(updates)

    def _annotation_page(self, edge: Edge) -> int:
        """Page number Diagram parsing uses for this annotation. The first page is 1."""
        properties: dict[str, object] = (edge.properties or {}).get(self.core_annotation_view.as_view_id()) or {}
        page = properties.get("startNodePageNumber")
        if isinstance(page, int) and page >= 1:
            return page
        return 1

    def _diagram_parsing_path(self, suffix: str) -> str:
        project = quote(self.client.config.project, safe="")
        return f"/api/v1/projects/{project}{suffix}"

    def _diagram_parsing_headers(self) -> dict[str, str]:
        return {"cdf-version": self._DIAGRAM_PARSING_VERSION}

    def _list_diagram_entities(self, file_space: str, file_external_id: str, page: int) -> list[dict[str, object]]:
        path = f"/diagram-parsing/diagrams/{quote(file_space, safe='')}/{quote(file_external_id, safe='')}/{page}"
        try:
            response = self.client.get(self._diagram_parsing_path(path), headers=self._diagram_parsing_headers())
        except CogniteAPIError as error:
            if error.code == 404:
                self.logger.debug(
                    f"No parsed diagram for ({file_space}, {file_external_id}) page {page}; isAssetVerified was not set."
                )
            else:
                self.logger.warning(
                    f"Could not read diagram entities for ({file_space}, {file_external_id}) page {page}: {error}"
                )
            return []
        try:
            payload: object = response.json()
        except ValueError as error:
            self.logger.warning(f"Diagram parsing returned a body that is not JSON: {error}")
            return []
        if not isinstance(payload, dict):
            return []
        entities = payload.get("entities")
        if not isinstance(entities, list):
            return []
        return [entity for entity in entities if isinstance(entity, dict)]

    def _update_diagram_entities(self, items: list[dict[str, object]]) -> None:
        try:
            self.client.post(
                self._diagram_parsing_path("/diagram-parsing/entities/update"),
                json={"items": items},
                headers=self._diagram_parsing_headers(),
            )
        except CogniteAPIError as error:
            self.logger.warning(f"Could not set isAssetVerified on {len(items)} diagram entities: {error}")
            return
        self.logger.info(f"Set isAssetVerified on {len(items)} diagram entities.")

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
        edge_props: dict[str, object] = edge.properties.get(self.core_annotation_view.as_view_id(), {})
        current_tags: object = edge_props.get("tags", [])
        updated_tags: list[str] = list(current_tags) if isinstance(current_tags, list) else []

        # Now create the write version
        edge_apply: EdgeApply = edge.as_write()

        # Fetch existing RAW row to preserve all data
        raw_data: dict[str, object] = {}
        try:
            existing_row: Row | None = self.client.raw.rows.retrieve(
                db_name=self.raw_db, table_name=self.raw_pattern_table, key=edge.external_id
            )
            if existing_row and existing_row.columns:
                raw_data = dict(existing_row.columns.items())
        except CogniteAPIError as e:
            self.logger.warning(f"Could not retrieve RAW row for edge {edge.external_id}: {e}")

        # Prepare update properties for the edge
        update_properties: dict[str, object] = {}

        if len(found_entities) == 1 and not (
            found_entities[0].space == edge.start_node.space
            and found_entities[0].external_id == edge.start_node.external_id
        ):  # Success - single match found
            matched_entity: MatchedEntity = found_entities[0]
            self.logger.debug(
                f"✓ Found single match for '{edge_props.get('startNodeText')}' → {matched_entity.external_id}. \n\t- Promoting edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )

            # Update edge to point to the found entity
            edge_apply.end_node = DirectRelationReference(matched_entity.space, matched_entity.external_id)
            update_properties["status"] = DiagramAnnotationStatus.APPROVED.value
            updated_tags = add_unique_tags(updated_tags, "PromotedAuto")

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
            updated_tags = add_unique_tags(updated_tags, "PromoteAttempted")
            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        elif len(found_entities) == 0:  # Failure - no match found (or normalizePatterns filtered the text)
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
            updated_tags = add_unique_tags(updated_tags, "PromoteAttempted")

            # Update RAW row status
            raw_data["status"] = DiagramAnnotationStatus.REJECTED.value

        else:  # Ambiguous - multiple matches found
            self.logger.debug(
                f"⚠ Multiple matches found for '{edge_props.get('startNodeText')}'.\n\t- Ambiguous edge: ({edge.space}, {edge.external_id})\n\t- Start node: ({edge.start_node.space}, {edge.start_node.external_id})."
            )
            updated_tags = add_unique_tags(updated_tags, "PromoteAttempted", "AmbiguousMatch")

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
