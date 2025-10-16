import abc
import re
from typing import Callable
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList, ViewId
from cognite.client.data_classes.filters import Filter, Equals, In
from services.LoggerService import CogniteFunctionLogger


class IEntitySearchService(abc.ABC):
    @abc.abstractmethod
    def find_entity(self, text: str, annotation_type: str, entity_space: str) -> list[Node]:
        """Finds entities matching the given text using multiple strategies."""
        pass


class EntitySearchService(IEntitySearchService):
    """
    Finds entities by text using multiple search strategies.

    Search Strategy:
    1. Existing annotations (fast, reliable, leverages proven connections)
    2. Global entity search (slow, comprehensive, fallback)

    Utilities:
    - Text variation generation (handles case, leading zeros)
    - Text normalization (for comparison)
    """

    def __init__(
        self,
        client: CogniteClient,
        logger: CogniteFunctionLogger,
        core_annotation_view_id: ViewId,
        file_view_id: ViewId,
        target_entities_view_id: ViewId,
        regular_annotation_space: str,
    ):
        """
        Initializes the entity search service.

        Args:
            client: Cognite client
            logger: Logger instance
            core_annotation_view_id: View ID for annotation edges
            file_view_id: View ID for file entities
            target_entities_view_id: View ID for target entities (assets, etc.)
            regular_annotation_space: Space where regular (non-pattern) annotations are stored
        """
        self.client = client
        self.logger = logger
        self.core_annotation_view_id = core_annotation_view_id
        self.file_view_id = file_view_id
        self.target_entities_view_id = target_entities_view_id
        self.regular_annotation_space = regular_annotation_space

    def find_entity(self, text: str, annotation_type: str, entity_space: str) -> list[Node]:
        """
        Finds entities matching the given text using multiple strategies.

        This is the main entry point for entity search.

        Strategy:
        1. Try existing annotations (fast, 50-100ms)
        2. Fall back to global search (slow, 500ms-2s)

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
        # STRATEGY 1: Query existing annotations (primary, fast)
        found_nodes = self.find_from_existing_annotations(text, annotation_type)

        if not found_nodes:
            # STRATEGY 2: Global entity search (fallback, slow)
            self.logger.debug(f"No match in existing annotations for '{text}'. Trying global entity search.")
            found_nodes = self.find_global_entity(text, entity_space)

        return found_nodes

    def find_from_existing_annotations(self, text: str, annotation_type: str) -> list[Node]:
        """
        Searches for existing successful annotations with matching startNodeText.

        This is MUCH faster than querying all entity aliases because:
        1. Queries edges directly with server-side filtering (indexed and fast)
        2. Uses IN filter with text variations to handle common differences
        3. Only searches proven successful annotations
        4. Handles cross-scope scenarios naturally (entity in different site/unit)

        Args:
            text: The text to search for (e.g., "G18A-921")
            annotation_type: "diagrams.FileLink" or "diagrams.AssetLink"

        Returns:
            List of matched entity nodes (0, 1, or 2+ for ambiguous)
        """
        try:
            # Generate variations of the search text
            text_variations = self.generate_text_variations(text)
            self.logger.debug(f"Searching for text variations: {text_variations}")

            # Query edges directly with IN filter
            # These are annotation edges that are from regular diagram detect (not pattern mode)
            # NOTE: manually promoted results from pattern mode are added to the 
            text_filter: Filter = In(self.core_annotation_view_id.as_property_ref("startNodeText"), text_variations)
            edges = self.client.data_modeling.instances.list(
                instance_type="edge",
                sources=[self.core_annotation_view_id],
                filter=text_filter,
                space=self.regular_annotation_space,  # Where regular annotations live
                limit=1000,  # Reasonable limit
            )

            if not edges:
                return []

            # Count occurrences of each endNode
            matched_end_nodes = {}  # {(space, externalId): count}
            for edge in edges:
                # Check annotation type matches
                edge_props = edge.properties.get(self.core_annotation_view_id, {})
                edge_type = edge_props.get("type")

                if edge_type != annotation_type:
                    continue  # Skip edges of different type

                # Extract endNode from the edge
                end_node_ref = edge.end_node
                if end_node_ref:
                    key = (end_node_ref.space, end_node_ref.external_id)
                    matched_end_nodes[key] = matched_end_nodes.get(key, 0) + 1

            if not matched_end_nodes:
                return []

            # If multiple different endNodes found, it's ambiguous
            if len(matched_end_nodes) > 1:
                self.logger.warning(
                    f"Found {len(matched_end_nodes)} different entities for '{text}' in existing annotations. "
                    f"This indicates data quality issues or legitimate ambiguity."
                )
                # Return list of most common matches (limit to 2 for ambiguity detection)
                sorted_matches = sorted(matched_end_nodes.items(), key=lambda x: x[1], reverse=True)
                top_matches = [match[0] for match in sorted_matches[:2]]
            else:
                # Single consistent match found
                top_matches = [list(matched_end_nodes.keys())[0]]

            # Fetch the actual node objects for the matched entities
            view_to_use = self.file_view_id if annotation_type == "diagrams.FileLink" else self.target_entities_view_id

            matched_nodes = []
            for space, ext_id in top_matches:
                retrieved = self.client.data_modeling.instances.retrieve_nodes(
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
                    f"Found {len(matched_nodes)} match(es) for '{text}' from existing annotations "
                    f"(appeared {matched_end_nodes.get((matched_nodes[0].space, matched_nodes[0].external_id), 0)} times)"
                )

            return matched_nodes

        except Exception as e:
            self.logger.error(f"Error searching existing annotations for '{text}': {e}")
            return []

    def find_global_entity(self, text: str, entity_space: str) -> list[Node]:
        """
        Performs a global, un-scoped search for an entity matching the given text.
        Uses normalized matching to handle variations like "V-0912" vs "V-912".

        NOTE: This approach queries all instances in a given space.
        Pros: The most accurate and guaranteed approach
        Cons: Will likely timeout as the amount of instances in a given space increase

        Args:
            text: Text to search for
            entity_space: Space to search in

        Returns:
            List of matched nodes (0, 1, or 2 for ambiguity detection)
        """
        # Normalize the search text
        normalized_text = self.normalize(text)

        # Fetch all entities in the space (with reasonable limit)
        # NOTE: We can't do normalized matching server-side, so we fetch and filter client-side
        try:
            entities = self.client.data_modeling.instances.list(
                instance_type="node",
                sources=[self.target_entities_view_id],
                space=entity_space,
                limit=1000,  # Reasonable limit to prevent timeouts
            )
        except Exception as e:
            self.logger.error(f"Error fetching entities from space '{entity_space}': {e}")
            return []

        # Client-side normalized matching against aliases
        matches = []
        for entity in entities:
            entity_props = entity.properties.get(self.target_entities_view_id, {})
            aliases = entity_props.get("aliases", [])

            # Ensure aliases is iterable
            if not isinstance(aliases, list):
                continue

            # Check if any alias matches after normalization
            for alias in aliases:
                if isinstance(alias, str) and self.normalize(alias) == normalized_text:
                    matches.append(entity)
                    # Stop after finding 2 matches (ambiguous case)
                    if len(matches) >= 2:
                        self.logger.warning(
                            f"Found multiple entities with alias matching '{text}' (normalized: '{normalized_text}'). "
                            f"This is ambiguous."
                        )
                        return matches[:2]
                    break  # Move to next entity after finding match

        if matches:
            self.logger.info(f"Found {len(matches)} match(es) for '{text}' via global entity search")

        return matches

    def generate_text_variations(self, text: str) -> list[str]:
        """
        Generates common variations of a text string to improve matching.

        Examples:
            "14-V-0937" → ["14-V-0937", "14-V-937", "14-v-0937", "14-v-937"]
            "P&ID-001" → ["P&ID-001", "P&ID-1", "p&id-001", "p&id-1"]

        Args:
            text: Original text from pattern detection

        Returns:
            List of text variations (original + common transformations)
        """
        variations = set()
        variations.add(text)  # Always include original

        # Add lowercase version
        variations.add(text.lower())

        # Remove leading zeros from number sequences
        def strip_leading_zeros_in_text(s: str) -> str:
            return re.sub(r"\b0+(\d+)", r"\1", s)

        variations.add(strip_leading_zeros_in_text(text))
        variations.add(strip_leading_zeros_in_text(text.lower()))

        return list(variations)

    def normalize(self, s: str) -> str:
        """
        Normalizes a string for comparison.

        Process:
        1. Ensures it's a string
        2. Removes all non-alphanumeric characters
        3. Converts to lowercase
        4. Removes leading zeros from any sequence of digits

        Examples:
            "V-0912" -> "v912"
            "FT-101A" -> "ft101a"
            "P&ID-0001" -> "pid1"

        Args:
            s: String to normalize

        Returns:
            Normalized string
        """
        if not isinstance(s, str):
            return ""

        # Step 1: Basic cleaning (e.g., "V-0912" -> "v0912")
        s = re.sub(r"[^a-zA-Z0-9]", "", s).lower()

        # Step 2: Define a replacer function that converts any matched number to an int and back to a string
        def strip_leading_zeros(match):
            # match.group(0) is the matched string (e.g., "0912")
            return str(int(match.group(0)))

        # Step 3: Apply the replacer function to all sequences of digits (\d+) in the string
        # This turns "v0912" into "v912"
        return re.sub(r"\d+", strip_leading_zeros, s)
