import abc

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList, ViewId
from cognite.client.data_classes.filters import And, ContainsAny, Filter
from cognite.client.exceptions import CogniteAPIError
from fa_constants import MAX_ENTITY_SEARCH_LIMIT
from normalization import normalize_text, text_variations
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger

# Token search on these text properties. AND requires every token of the input;
# the property value may contain additional tokens.
TEXT_QUERY_PROPERTIES: tuple[str, ...] = ("name", "description")


class IEntitySearchService(abc.ABC):
    """
    Interface for services that find entities by text using various search strategies.
    """

    @abc.abstractmethod
    def find_entity(
        self,
        text: str,
        annotation_type: str,
        entity_space: str,
        *,
        scope_filter: Filter | None = None,
    ) -> list[Node]:
        """
        Finds entities matching the given text using multiple strategies.

        Args:
            text: Text to search for
            annotation_type: Type of annotation being searched
            entity_space: Space to search in for global fallback
            scope_filter: Extra filter ANDed with the alias match and applied to the text query.

        Returns:
            List of matched Node objects
        """
        pass


class EntitySearchService(IEntitySearchService):
    """
    Finds entities by text using server-side filtering on entity aliases.

    This service queries entities directly using a containsAny filter on the aliases property,
    which is more efficient than querying annotation edges:

    **Why query entities directly instead of annotation edges?**
    - Entity dataset is smaller and stable (~1,000-10,000 entities)
    - Annotation edges grow quadratically (Files x Entities = potentially millions)
    - Neither startNodeText nor aliases properties are indexed
    - Without indexes, smaller dataset = better performance
    - Entity count doesn't increase as more files are annotated

    **Search Strategy:**
    - Generate text variations (e.g., "V-0912" → ["V-0912", "v-0912", "V-912", "v912", ...])
    - Match the configured list property (usually aliases) with containsAny
    - When that misses, search name and description with query operator AND
    - Returns matches from specified entity space

    **Utilities:**
    - `generate_text_variations()`: Creates common variations (case, leading zeros, special chars)
    - `normalize()`: Normalizes text for cache keys (removes special chars, strips zeros)
    """

    def __init__(
        self,
        config: Config,
        client: CogniteClient,
        logger: CogniteFunctionLogger,
    ):
        """
        Initializes the entity search service.

        Args:
            config: Configuration object containing data model views and entity search settings
            client: Cognite client
            logger: Logger instance
        """
        self.client = client
        self.logger = logger
        self.config = config

        # Extract view IDs
        self.core_annotation_view_id = config.data_model_views.core_annotation_view.as_view_id()
        self.file_view_id = config.data_model_views.file_view.as_view_id()
        self.target_entities_view_id = config.data_model_views.target_entities_view.as_view_id()
        self.search_properties = {
            self.file_view_id: config.data.file_view.search_property,
            self.target_entities_view_id: config.data.target_entities_view.search_property,
        }

        # Extract text normalization config
        self.text_normalization_config = config.promote_function.entity_search_service.text_normalization

    def find_entity(
        self,
        text: str,
        annotation_type: str,
        entity_space: str,
        *,
        scope_filter: Filter | None = None,
    ) -> list[Node]:
        """
        Finds entities matching the given text by querying entity aliases.

        This is the main entry point for entity search.

        Strategy:
        1. Generate text variations (e.g., "V-0912" → ["V-0912", "v-0912", "V-912", "v912", ...])
        2. Search the configured list property. On a miss, query name and description with AND

        Note: We query entities directly rather than annotation edges because:
        - Entity dataset is smaller and more stable (~1,000-10,000 entities)
        - Annotation edges grow quadratically (Files x Entities = potentially millions)
        - Neither startNodeText nor aliases properties are indexed
        - Without indexes, smaller dataset = better performance

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in
            scope_filter: Optional scope constraint from the annotated file. None searches the whole space.

        Returns:
            List of matched nodes:
            - [] if no match found
            - [node] if single unambiguous match
            - [node1, node2] if ambiguous (multiple matches)
        """
        # Normalize first: no pattern match → do not search (e.g. drawing words like "REPEATED")
        search_texts: list[str] = self.generate_text_variations(text, annotation_type)
        if not search_texts:
            self.logger.debug(
                f"✗ Skipping search for '{text}': does not match "
                f"{'file' if annotation_type == 'diagrams.FileLink' else 'entity'}NormalizationPatterns."
            )
            return []

        self.logger.debug(f"Generated {len(search_texts)} text variation(s) for '{text}': {search_texts}")

        # Determine which view to query based on annotation type
        if annotation_type == "diagrams.FileLink":
            source: ViewId = self.file_view_id
        else:
            source = self.target_entities_view_id

        found_nodes: list[Node] = self.find_global_entity(
            search_texts, source, entity_space, text, scope_filter=scope_filter
        )

        return found_nodes

    def find_global_entity(
        self,
        text_variations: list[str],
        source: ViewId,
        entity_space: str,
        query_text: str,
        *,
        scope_filter: Filter | None = None,
    ) -> list[Node]:
        """
        Performs a global, un-scoped search for an entity matching the given text variations.

        The configured property (usually aliases) is a list, matched with containsAny.
        When that returns nothing, name and description are searched with a query.
        AND requires every token of query_text; those fields may contain further tokens.
        A /list filter on aliases is not index-backed and times out on large spaces.

        Args:
            text_variations: List of text variations to search for (e.g., ["V-0912", "v-0912", "V-912", ...])
            source: View to query (file_view or target_entities_view)
            entity_space: Space to search in
            query_text: Original input, tokenized against name and description
            scope_filter: ANDed with the alias filter, and applied as-is to the name/description query.

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        list_filter: Filter = ContainsAny(source.as_property_ref(self.search_properties[source]), text_variations)
        if scope_filter is not None:
            list_filter = And(list_filter, scope_filter)
        list_matches: list[Node] = self._search_nodes(query_text, source, entity_space, search_filter=list_filter)
        if list_matches:
            return self._cap_matches(list_matches, query_text, entity_space)

        text_matches: list[Node] = self._search_nodes(
            query_text,
            source,
            entity_space,
            query=query_text,
            properties=list(TEXT_QUERY_PROPERTIES),
            search_filter=scope_filter,
        )
        return self._cap_matches(text_matches, query_text, entity_space)

    def _search_nodes(
        self,
        log_text: str,
        source: ViewId,
        entity_space: str,
        *,
        query: str | None = None,
        properties: list[str] | None = None,
        search_filter: Filter | None = None,
    ) -> list[Node]:
        """Run one instances.search call. Returns [] when the API call fails.

        Args:
            log_text: Text used in the error log
            source: View to query
            entity_space: Space to search in
            query: Token query. None keeps the call filter-only.
            properties: Properties the query searches. None searches every text property.
            search_filter: Hard filter applied together with the query.

        Returns:
            Matching nodes, or [] on API error.
        """
        try:
            # operator AND: every query token must match. Unused when query is omitted.
            # Set explicitly; the SDK default changes from OR to AND in v8.
            entities: NodeList[Node] = self.client.data_modeling.instances.search(
                view=source,
                instance_type="node",
                query=query,
                properties=properties,
                filter=search_filter,
                space=entity_space,
                limit=MAX_ENTITY_SEARCH_LIMIT,
                operator="AND",
            )
        except CogniteAPIError as e:
            self.logger.error(f"Error searching for entity '{log_text}' in space '{entity_space}': {e}")
            return []
        return list(entities)

    def _cap_matches(self, matched_entities: list[Node], text: str, entity_space: str) -> list[Node]:
        """Keep a single match, or the first two when several entities match.

        Args:
            matched_entities: Nodes returned by one search call
            text: Text that was searched, for logs
            entity_space: Space that was searched, for logs

        Returns:
            [] , [node], or the first two nodes
        """
        if len(matched_entities) > 1:
            self.logger.warning(
                f"Found more than one entity matching '{text}' in space '{entity_space}'. "
                f"This is ambiguous. Returning first 2 for ambiguity detection."
            )
            return matched_entities[:2]

        if matched_entities:
            self.logger.debug(f"Found {len(matched_entities)} match(es) for '{text}' via global entity search")

        return matched_entities

    def generate_text_variations(self, text: str, annotation_type: str) -> list[str]:
        """
        Builds search variations from the longest form extracted by source normalize patterns.

        Uses entityNormalizationPatterns for AssetLink and fileNormalizationPatterns for FileLink.
        Returns an empty list when patterns are set and text matches none of them.
        When patterns are empty for that source, hygiene variations of the original text are returned.

        Args:
            text: Original text from pattern detection
            annotation_type: ``diagrams.FileLink`` or ``diagrams.AssetLink``

        Returns:
            Search strings, or [] when configured patterns do not match.
        """
        return text_variations(
            text,
            self.text_normalization_config.patterns_for_annotation_type(annotation_type),
        )

    def normalize(self, s: str, annotation_type: str = "diagrams.AssetLink") -> str:
        """
        Canonical cache-key form: longest source-specific extraction then built-in hygiene.

        Falls back to the original string (with hygiene) when no pattern matches or patterns
        are empty. Defaults to entity patterns when annotation_type is omitted.

        Args:
            s: String to normalize
            annotation_type: ``diagrams.FileLink`` or ``diagrams.AssetLink``

        Returns:
            Normalized string based on config settings
        """
        if not isinstance(s, str):
            return ""
        return normalize_text(
            s,
            self.text_normalization_config.patterns_for_annotation_type(annotation_type),
        )
