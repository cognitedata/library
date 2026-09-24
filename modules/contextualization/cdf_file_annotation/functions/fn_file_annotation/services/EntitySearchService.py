import abc

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList, ViewId
from cognite.client.data_classes.filters import Filter, In
from cognite.client.exceptions import CogniteAPIError
from fa_constants import MAX_ENTITY_SEARCH_LIMIT
from normalization import normalize_text, text_variations
from services.ConfigService import Config
from services.LoggerService import CogniteFunctionLogger


class IEntitySearchService(abc.ABC):
    """
    Interface for services that find entities by text using various search strategies.
    """

    @abc.abstractmethod
    def find_entity(self, text: str, annotation_type: str, entity_space: str) -> list[Node]:
        """
        Finds entities matching the given text using multiple strategies.

        Args:
            text: Text to search for
            annotation_type: Type of annotation being searched
            entity_space: Space to search in for global fallback

        Returns:
            List of matched Node objects
        """
        pass


class EntitySearchService(IEntitySearchService):
    """
    Finds entities by text using server-side filtering on entity aliases.

    This service queries entities directly using an IN filter on the aliases property,
    which is more efficient than querying annotation edges:

    **Why query entities directly instead of annotation edges?**
    - Entity dataset is smaller and stable (~1,000-10,000 entities)
    - Annotation edges grow quadratically (Files x Entities = potentially millions)
    - Neither startNodeText nor aliases properties are indexed
    - Without indexes, smaller dataset = better performance
    - Entity count doesn't increase as more files are annotated

    **Search Strategy:**
    - Generate text variations (e.g., "V-0912" → ["V-0912", "v-0912", "V-912", "v912", ...])
    - Query entities with server-side IN filter on aliases property
    - Uses text variations to handle different naming conventions
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

        Raises:
            ValueError: If regular_annotation_space (file_view.instance_space) is None
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

        # Extract regular annotation space
        self.regular_annotation_space: str | None = config.data_model_views.file_view.instance_space
        if not self.regular_annotation_space:
            raise ValueError("regular_annotation_space (file_view.instance_space) is required but was None")

        # Extract text normalization config
        self.text_normalization_config = config.promote_function.entity_search_service.text_normalization

    def find_entity(self, text: str, annotation_type: str, entity_space: str) -> list[Node]:
        """
        Finds entities matching the given text by querying entity aliases.

        This is the main entry point for entity search.

        Strategy:
        1. Generate text variations (e.g., "V-0912" → ["V-0912", "v-0912", "V-912", "v912", ...])
        2. Query entities with server-side IN filter on aliases property

        Note: We query entities directly rather than annotation edges because:
        - Entity dataset is smaller and more stable (~1,000-10,000 entities)
        - Annotation edges grow quadratically (Files x Entities = potentially millions)
        - Neither startNodeText nor aliases properties are indexed
        - Without indexes, smaller dataset = better performance

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in

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

        self.logger.info(f"Generated {len(search_texts)} text variation(s) for '{text}': {search_texts}")

        # Determine which view to query based on annotation type
        if annotation_type == "diagrams.FileLink":
            source: ViewId = self.file_view_id
        else:
            source = self.target_entities_view_id

        # Query entities directly by aliases
        found_nodes: list[Node] = self.find_global_entity(search_texts, source, entity_space)

        return found_nodes

    def find_global_entity(self, text_variations: list[str], source: ViewId, entity_space: str) -> list[Node]:
        """
        Performs a global, un-scoped search for an entity matching the given text variations.
        Uses server-side IN filter with text variations to handle different naming conventions.

        This approach uses server-side filtering on the aliases property, making it efficient
        and scalable even with large numbers of entities in a space.

        Args:
            text_variations: List of text variations to search for (e.g., ["V-0912", "v-0912", "V-912", ...])
            source: View to query (file_view or target_entities_view)
            entity_space: Space to search in

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        # Use first text variation (original text) for logging
        original_text: str = text_variations[0] if text_variations else "unknown"

        try:
            search_filter: Filter = In(source.as_property_ref(self.search_properties[source]), text_variations)

            entities: NodeList[Node] = self.client.data_modeling.instances.list(
                instance_type="node",
                sources=source,
                filter=search_filter,
                space=entity_space,
                limit=MAX_ENTITY_SEARCH_LIMIT,
            )

            if not entities:
                return []

            # Convert to list and check for ambiguity
            matched_entities: list[Node] = list(entities)

            if len(matched_entities) > 1:
                self.logger.warning(
                    f"Found {len(matched_entities)} entities with aliases matching '{original_text}' in space '{entity_space}'. "
                    f"This is ambiguous. Returning first 2 for ambiguity detection."
                )
                return matched_entities[:2]

            if matched_entities:
                self.logger.debug(
                    f"Found {len(matched_entities)} match(es) for '{original_text}' via global entity search"
                )

            return matched_entities

        except CogniteAPIError as e:
            self.logger.error(f"Error searching for entity '{original_text}' in space '{entity_space}': {e}")
            return []

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
