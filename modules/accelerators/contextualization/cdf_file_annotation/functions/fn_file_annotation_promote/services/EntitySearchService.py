import abc
import re
from typing import Any
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList, ViewId
from cognite.client.data_classes.filters import Filter, In
from services.LoggerService import CogniteFunctionLogger
from services.ConfigService import Config


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
    Finds entities by text using Elasticsearch-based search on entity aliases.

    This service uses the CDF Data Modeling search endpoint (Elasticsearch backend)
    which provides faster full-text search with built-in fuzzy matching.

    **Why Elasticsearch instead of PostgreSQL filters?**
    - Elasticsearch uses inverted indexes for O(1) text lookups
    - Built-in fuzzy matching eliminates need for text variation generation
    - Better performance for text-based searches at scale

    **Why query entities directly instead of annotation edges?**
    - Entity dataset is smaller and stable (~1,000-10,000 entities)
    - Annotation edges grow quadratically (Files × Entities = potentially millions)
    - Entity count doesn't increase as more files are annotated

    **Search Strategy:**
    - Use Elasticsearch search endpoint with single query text
    - Search on aliases property with built-in fuzzy matching
    - Returns matches from specified entity space

    **Utilities:**
    - `normalize()`: Normalizes text for cache keys (removes special chars, lowercase, strips zeros)
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

        # Extract regular annotation space
        self.regular_annotation_space: str | None = config.data_model_views.file_view.instance_space
        if not self.regular_annotation_space:
            raise ValueError("regular_annotation_space (file_view.instance_space) is required but was None")

        # Extract text normalization config
        self.text_normalization_config = config.promote_function.entity_search_service.text_normalization

    def find_entity(self, text: str, annotation_type: str, entity_space: str) -> list[Node]:
        """
        Finds entities matching the given text using Elasticsearch search on aliases.

        This is the main entry point for entity search.

        Strategy:
        - Use Elasticsearch search endpoint with built-in fuzzy matching
        - Search on aliases property
        - No text variation generation needed - ES handles this natively

        Note: We query entities directly rather than annotation edges because:
        - Entity dataset is smaller and more stable (~1,000-10,000 entities)
        - Annotation edges grow quadratically (Files x Entities = potentially millions)

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
        self.logger.debug(f"Searching for entity with text '{text}' using Elasticsearch")

        # Determine which view to query based on annotation type
        if annotation_type == "diagrams.FileLink":
            source: ViewId = self.file_view_id
        else:
            source = self.target_entities_view_id

        # Query entities using Elasticsearch search (handles fuzzy matching natively)
        found_nodes: list[Node] = self.search_entity(text, source, entity_space)

        return found_nodes

    def find_from_existing_annotations(self, text_variations: list[str], annotation_type: str) -> list[Node]:
        """
        [UNUSED] Searches for existing successful annotations with matching startNodeText.

        ** WHY THIS FUNCTION IS NOT USED: **
        While this was originally designed as a "smart" optimization to find proven matches,
        it actually queries the LARGER dataset:

        - Annotation edges grow quadratically: O(Files × Entities) = potentially millions
        - Entity/file nodes grow linearly: O(Entities) = thousands
        - Neither startNodeText nor aliases properties are indexed
        - Without indexes, querying the smaller dataset (entities) is always faster

        Performance comparison at scale:
        - This function: Scans ~500,000+ annotation edges (grows over time)
        - Global entity search: Scans ~1,000-10,000 entities (relatively stable)

        Result: Global entity search is 50-500x faster at scale.

        This function is kept for reference but should not be used in production.

        Args:
            text_variations: List of text variations to search for (e.g., ["V-0912", "v-0912", "V-912", ...])
            annotation_type: "diagrams.FileLink" or "diagrams.AssetLink"

        Returns:
            List of matched entity nodes (0, 1, or 2+ for ambiguous)
        """
        # Use first text variation (original text) for logging
        original_text: str = text_variations[0] if text_variations else "unknown"

        try:
            # Query edges directly with IN filter
            # These are annotation edges that are from regular diagram detect (not pattern mode)
            # NOTE: manually promoted results from pattern mode are added to the
            text_filter: Filter = In(self.core_annotation_view_id.as_property_ref("startNodeText"), text_variations)
            edges: Any = self.client.data_modeling.instances.list(
                instance_type="edge",
                sources=[self.core_annotation_view_id],
                filter=text_filter,
                space=self.regular_annotation_space,  # Where regular annotations live
                limit=1000,  # Reasonable limit
            )

            if not edges:
                return []

            # Count occurrences of each endNode
            matched_end_nodes: dict[tuple[str, str], int] = {}  # {(space, externalId): count}
            for edge in edges:
                # Check annotation type matches
                edge_props: dict[str, Any] = edge.properties.get(self.core_annotation_view_id, {})
                edge_type: Any = edge_props.get("type")

                if edge_type != annotation_type:
                    continue  # Skip edges of different type

                # Extract endNode from the edge
                end_node_ref: Any = edge.end_node
                if end_node_ref:
                    key: tuple[str, str] = (end_node_ref.space, end_node_ref.external_id)
                    matched_end_nodes[key] = matched_end_nodes.get(key, 0) + 1

            if not matched_end_nodes:
                return []

            # If multiple different endNodes found, it's ambiguous
            top_matches: list[tuple[str, str]]
            if len(matched_end_nodes) > 1:
                self.logger.warning(
                    f"Found {len(matched_end_nodes)} different entities for '{original_text}' in existing annotations. "
                    f"This indicates data quality issues or legitimate ambiguity."
                )
                # Return list of most common matches (limit to 2 for ambiguity detection)
                sorted_matches: list[tuple[tuple[str, str], int]] = sorted(
                    matched_end_nodes.items(), key=lambda x: x[1], reverse=True
                )
                top_matches = [match[0] for match in sorted_matches[:2]]
            else:
                # Single consistent match found
                top_matches = [list(matched_end_nodes.keys())[0]]

            # Fetch the actual node objects for the matched entities
            view_to_use: ViewId = (
                self.file_view_id if annotation_type == "diagrams.FileLink" else self.target_entities_view_id
            )

            matched_nodes: list[Node] = []
            for space, ext_id in top_matches:
                retrieved: Any = self.client.data_modeling.instances.retrieve_nodes(
                    nodes=(space, ext_id), sources=view_to_use
                )
                # Handle both single Node and NodeList returns
                if retrieved:
                    if isinstance(retrieved, list):
                        matched_nodes.extend(retrieved)
                    else:
                        matched_nodes.append(retrieved)

            if matched_nodes:
                self.logger.info(
                    f"Found {len(matched_nodes)} match(es) for '{original_text}' from existing annotations "
                    f"(appeared {matched_end_nodes.get((matched_nodes[0].space, matched_nodes[0].external_id), 0)} times)"
                )

            return matched_nodes

        except Exception as e:
            self.logger.error(f"Error searching existing annotations for '{original_text}': {e}")
            return []

    def search_entity(self, text: str, source: ViewId, entity_space: str) -> list[Node]:
        """
        Searches for entities matching the given text using Elasticsearch.

        Uses the CDF Data Modeling search endpoint which provides:
        - Elasticsearch-backed full-text search
        - Built-in fuzzy matching (handles case, special chars, etc.)
        - Faster performance via inverted index lookups

        Args:
            text: Text to search for (e.g., "V-0912")
            source: View to query (file_view or target_entities_view)
            entity_space: Space to search in

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        try:
            # Use Elasticsearch search endpoint instead of PostgreSQL list with IN filter
            search_result: NodeList = self.client.data_modeling.instances.search(
                view=source,
                query=text,
                instance_type="node",
                space=entity_space,
                properties=["aliases"],
                operator="AND",
                limit=50,  # Increase to inspect more ambiguous matches
            )

            if not search_result:
                self.logger.debug(f"No matches found for '{text}' via Elasticsearch search")
                return []

            # Convert to list and check for ambiguity
            matched_entities: list[Node] = list(search_result)

            if len(matched_entities) > 1:

                def _aliases_for_node(node: Node) -> list[str]:
                    props = node.properties.get(source, {}) if node.properties else {}
                    aliases = props.get("aliases")
                    if isinstance(aliases, list):
                        return [str(a) for a in aliases]
                    if aliases:
                        return [str(aliases)]
                    return []

                self.logger.debug(
                    f"Ambiguous matches for '{text}' in space '{entity_space}': "
                    f"{[{'external_id': node.external_id, 'aliases': _aliases_for_node(node)} for node in matched_entities]}"
                )
                self.logger.warning(
                    f"Found {len(matched_entities)} entities with aliases matching '{text}' in space '{entity_space}'. "
                    f"This is ambiguous. Returning first 2 for ambiguity detection."
                )
                return matched_entities[:2]

            if matched_entities:
                self.logger.debug(f"Found {len(matched_entities)} match(es) for '{text}' via Elasticsearch search")

            return matched_entities

        except Exception as e:
            self.logger.error(f"Error searching for entity '{text}' in space '{entity_space}': {e}")
            return []

    def find_global_entity(self, text_variations: list[str], source: ViewId, entity_space: str) -> list[Node]:
        """
        [DEPRECATED] Use search_entity() instead.

        This method used PostgreSQL IN filter with text variations.
        Kept for backwards compatibility but search_entity() is preferred.

        Args:
            text_variations: List of text variations to search for
            source: View to query (file_view or target_entities_view)
            entity_space: Space to search in

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        # Use first text variation (original text) and delegate to new search method
        original_text: str = text_variations[0] if text_variations else ""
        return self.search_entity(original_text, source, entity_space)

    def generate_text_variations(self, text: str) -> list[str]:
        """
        [DEPRECATED] No longer needed - Elasticsearch handles fuzzy matching natively.

        This method was used to generate text variations for PostgreSQL IN filter queries.
        With the switch to Elasticsearch search, fuzzy matching is handled server-side.

        Kept for backwards compatibility only.

        Args:
            text: Original text from pattern detection

        Returns:
            List containing only the original text (no variations generated)
        """
        # No longer generate variations - ES handles fuzzy matching
        return [text]

    def normalize(self, s: str) -> str:
        """
        Normalizes a string for comparison based on text_normalization_config settings.

        Applies transformations in sequence based on config:
        1. removeSpecialCharacters: Remove non-alphanumeric characters
        2. convertToLowercase: Convert to lowercase
        3. stripLeadingZeros: Remove leading zeros from number sequences

        Examples (all flags enabled):
            "V-0912" -> "v912"
            "FT-101A" -> "ft101a"
            "P&ID-0001" -> "pid1"

        Examples (all flags disabled):
            "V-0912" -> "V-0912"  # No transformation

        Examples (only removeSpecialCharacters):
            "V-0912" -> "V0912"  # Special chars removed, case and zeros preserved

        Args:
            s: String to normalize

        Returns:
            Normalized string based on config settings
        """
        if not isinstance(s, str):
            return ""

        # Apply transformations based on config
        if self.text_normalization_config.remove_special_characters:
            s = re.sub(r"[^a-zA-Z0-9]", "", s)

        if self.text_normalization_config.convert_to_lowercase:
            s = s.lower()

        if self.text_normalization_config.strip_leading_zeros:
            # Define a replacer function that converts any matched number to an int and back to a string
            def strip_leading_zeros(match):
                # match.group(0) is the matched string (e.g., "0912")
                return str(int(match.group(0)))

            # Apply the replacer function to all sequences of digits (\d+) in the string
            s = re.sub(r"\d+", strip_leading_zeros, s)

        return s
