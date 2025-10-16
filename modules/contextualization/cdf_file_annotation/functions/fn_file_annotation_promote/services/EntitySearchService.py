import abc
import re
from typing import Callable, Any
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList, ViewId
from cognite.client.data_classes.filters import Filter, Equals, In
from services.LoggerService import CogniteFunctionLogger
from services.ConfigService import Config, ViewPropertyConfig


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
    Finds entities by text using multiple search strategies with automatic fallback.

    This service implements a two-tier search strategy for finding entities:

    **Strategy 1 - Existing Annotations** (Primary, Fast):
    - Queries annotation edges from regular diagram detect
    - Uses server-side IN filter with text variations on edge startNodeText
    - Returns entities that were successfully annotated before
    - Handles cross-scope scenarios naturally (entity in different site/unit)
    - Most efficient: Queries proven successful matches first

    **Strategy 2 - Global Entity Search** (Fallback):
    - Queries all entities in specified space
    - Uses server-side IN filter with text variations on entity aliases
    - Comprehensive search across all entities when no previous annotation exists
    - Efficient: Server-side indexed filtering on aliases property

    **Utilities:**
    - `generate_text_variations()`: Creates common variations (case, leading zeros, special chars)
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
        Finds entities matching the given text using multiple strategies.

        This is the main entry point for entity search.

        Strategy:
        1. Generate text variations once (e.g., "V-0912" → ["V-0912", "v-0912", "V-912", "v-912", ...])
        2. Try existing annotations (fast, queries edges from previous successful matches)
        3. Fall back to global search (queries all entities in space with IN filter on aliases)

        Both strategies use server-side filtering with text variations for efficiency.

        Args:
            text: Text to search for (e.g., "V-123", "G18A-921")
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")
            entity_space: Space to search in for global fallback

        Returns:
            List of matched nodes:
            - [] if no match found
            - [node] if single unambiguous match
            - [node1, node2] if ambiguous (multiple matches)
        """
        # Generate text variations once for use in both strategies
        text_variations: list[str] = self.generate_text_variations(text)
        self.logger.info(f"Generated {len(text_variations)} text variation(s) for '{text}': {text_variations}")

        # STRATEGY 1: Query existing annotations (primary, fast)
        found_nodes: list[Node] = self.find_from_existing_annotations(text_variations, annotation_type)

        if not found_nodes:
            # STRATEGY 2: Global entity search (fallback)
            self.logger.debug(f"No match in existing annotations for '{text}'. Trying global entity search.")
            found_nodes = self.find_global_entity(text_variations, entity_space)

        return found_nodes

    def find_from_existing_annotations(self, text_variations: list[str], annotation_type: str) -> list[Node]:
        """
        Searches for existing successful annotations with matching startNodeText.

        This is MUCH faster than querying all entity aliases because:
        1. Queries edges directly with server-side filtering (indexed and fast)
        2. Uses IN filter with text variations to handle common differences
        3. Only searches proven successful annotations
        4. Handles cross-scope scenarios naturally (entity in different site/unit)

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

    def find_global_entity(self, text_variations: list[str], entity_space: str) -> list[Node]:
        """
        Performs a global, un-scoped search for an entity matching the given text variations.
        Uses server-side IN filter with text variations to handle different naming conventions.

        This approach uses server-side filtering on the aliases property, making it efficient
        and scalable even with large numbers of entities in a space.

        Args:
            text_variations: List of text variations to search for (e.g., ["V-0912", "v-0912", "V-912", ...])
            entity_space: Space to search in

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        # Use first text variation (original text) for logging
        original_text: str = text_variations[0] if text_variations else "unknown"

        try:
            # Query entities with IN filter on aliases property
            aliases_filter: Filter = In(self.target_entities_view_id.as_property_ref("aliases"), text_variations)

            entities: Any = self.client.data_modeling.instances.list(
                instance_type="node",
                sources=[self.target_entities_view_id],
                filter=aliases_filter,
                space=entity_space,
                limit=1000,  # Reasonable limit to prevent timeouts
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
                self.logger.info(
                    f"Found {len(matched_entities)} match(es) for '{original_text}' via global entity search"
                )

            return matched_entities

        except Exception as e:
            self.logger.error(f"Error searching for entity '{original_text}' in space '{entity_space}': {e}")
            return []

    def generate_text_variations(self, text: str) -> list[str]:
        """
        Generates common variations of a text string to improve matching.

        Respects text_normalization_config settings:
        - removeSpecialCharacters: Generate variations without special characters
        - convertToLowercase: Generate lowercase variations
        - stripLeadingZeros: Generate variations with leading zeros removed

        Examples (all flags enabled):
            "V-0912" → ["V-0912", "v-0912", "V-912", "v-912", "V0912", "v0912", "V912", "v912"]
            "P&ID-001" → ["P&ID-001", "p&id-001", "P&ID-1", "p&id-1", "PID001", "pid001", "PID1", "pid1"]

        Examples (all flags disabled):
            "V-0912" → ["V-0912"]  # Only original

        Args:
            text: Original text from pattern detection

        Returns:
            List of text variations based on config settings
        """
        variations: set[str] = set()
        variations.add(text)  # Always include original

        # Helper function to strip leading zeros
        def strip_leading_zeros_in_text(s: str) -> str:
            return re.sub(r"\b0+(\d+)", r"\1", s)

        # Helper function to remove special characters
        def remove_special_chars(s: str) -> str:
            return re.sub(r"[^a-zA-Z0-9]", "", s)

        # Generate all combinations of transformations systematically
        # We'll build up variations by applying each transformation flag
        base_variations: set[str] = {text}

        # Apply removeSpecialCharacters transformations
        if self.text_normalization_config.remove_special_characters:
            new_variations: set[str] = set()
            for v in base_variations:
                new_variations.add(remove_special_chars(v))
            base_variations.update(new_variations)

        # Apply convertToLowercase transformations
        if self.text_normalization_config.convert_to_lowercase:
            new_variations = set()
            for v in base_variations:
                new_variations.add(v.lower())
            base_variations.update(new_variations)

        # Apply stripLeadingZeros transformations
        if self.text_normalization_config.strip_leading_zeros:
            new_variations = set()
            for v in base_variations:
                new_variations.add(strip_leading_zeros_in_text(v))
            base_variations.update(new_variations)

        return list(base_variations)

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
