import abc
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Any
from utils.DataStructures import CacheMarker
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Node, NodeList
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.raw import Row
from services.LoggerService import CogniteFunctionLogger
from services.ConfigService import Config, ViewPropertyConfig


@dataclass
class CachedEntityInfo:
    """
    Stores cached entity information to avoid retrieve_nodes API calls.

    This dataclass contains all the information needed from a matched entity,
    eliminating the need to call retrieve_nodes() on cache hits.
    """

    space: str
    external_id: str
    resource_type: str | None = None

    def to_tuple(self) -> tuple[str, str, str | None]:
        """Returns tuple representation for storage."""
        return (self.space, self.external_id, self.resource_type)

    @classmethod
    def from_tuple(cls, data: tuple[str, str, str | None]) -> "CachedEntityInfo":
        """Creates instance from tuple."""
        return cls(space=data[0], external_id=data[1], resource_type=data[2] if len(data) > 2 else None)





class ICacheService(abc.ABC):
    """
    Interface for services that cache text → entity mappings to improve lookup performance.
    """

    @abc.abstractmethod
    def get(self, text: str, annotation_type: str) -> CachedEntityInfo | None:
        """
        Retrieves cached entity info for the given text and annotation type.

        Args:
            text: Text to look up
            annotation_type: Type of annotation

        Returns:
            CachedEntityInfo if found, None if cache miss
        """
        pass

    @abc.abstractmethod
    def set(self, text: str, annotation_type: str, node: Node | None, resource_type: str | None = None) -> None:
        """
        Caches an entity node for the given text and annotation type.

        Args:
            text: Text being cached
            annotation_type: Type of annotation
            node: Entity node to cache, or None for negative caching
            resource_type: Optional resource type to cache alongside the node
        """
        pass

    @abc.abstractmethod
    def get_from_memory(self, text: str, annotation_type: str) -> CachedEntityInfo | None:
        """
        Retrieves from in-memory cache only (no persistent storage lookup).

        Args:
            text: Text to look up
            annotation_type: Type of annotation

        Returns:
            CachedEntityInfo if found in memory, None otherwise
        """
        pass


class CacheService(ICacheService):
    """
    Manages two-tier caching for text → entity mappings to dramatically improve performance.

    **OPTIMIZATION: No API calls on cache hit**
    - Stores CachedEntityInfo (space, external_id, resourceType) instead of just IDs
    - Eliminates retrieve_nodes() calls on cache hits
    - Returns cached data directly without additional API roundtrips

        **TIER 1: In-Memory Cache** (This Run Only):
        - Ultra-fast lookup (in-memory dictionary)
        - Dictionary stored in memory: {(text, type): CachedEntityInfo or CacheMarker}
        - Used for: positive matches and transient results (`NO_MATCH` / `AMBIGUOUS`) that
            are recorded in-memory to avoid repeated server-side lookups during the same run.
        - Transient markers are not persisted to RAW and are cleared when the function execution ends

    **TIER 2: Persistent RAW Cache** (All Runs):
    - Fast lookup (single database query)
    - Stored in RAW table: promote_text_to_entity_cache
    - Benefits all future function runs indefinitely
    - Includes resourceType column for complete entity info
    - **Only persists positive matches** (unambiguous single entities found)
    - Does NOT persist negative or ambiguous in-memory markers (to allow for new entities added over time)

    **Performance Impact:**
    - First lookup: Slowest (query annotation edges)
    - Cached lookup (same run): Fastest (in-memory dictionary, no API calls)
    - Cached lookup (future run): Fast (single RAW query, no retrieve_nodes call)
    - Self-improving: Gets faster as cache fills
    """

    def __init__(
        self,
        config: Config,
        client: CogniteClient,
        logger: CogniteFunctionLogger,
        normalize_fn: Callable[[str], str],
    ):
        """
        Initializes the cache service.

        Args:
            config: Configuration object containing data model views and cache settings
            client: Cognite client
            logger: Logger instance
            normalize_fn: Function to normalize text for cache keys
        """
        self.client = client
        self.logger = logger
        self.config = config
        self.normalize = normalize_fn

        # Extract view configurations
        file_view: ViewPropertyConfig = config.data_model_views.file_view
        target_entities_view: ViewPropertyConfig = config.data_model_views.target_entities_view

        # Extract view IDs
        self.file_view_id = file_view.as_view_id()
        self.target_entities_view_id = target_entities_view.as_view_id()

        # Extract RAW database and cache table configuration
        self.raw_db: str = config.raw_tables.raw_db
        self.cache_table_name: str = config.raw_tables.raw_table_promote_cache

        self.function_id = "fn_file_annotation_promote"

        # In-memory cache: {(text, type): CachedEntityInfo or CacheMarker}
        # Memory cache values can be:
        # - CachedEntityInfo: positive single match
        # - CacheMarker.NO_MATCH: negative cache (no match) stored in-memory only
        # - CacheMarker.AMBIGUOUS: ambiguous marker (more than one match)
        self._memory_cache: dict[tuple[str, str], CachedEntityInfo | CacheMarker] = {}

    def get(self, text: str, annotation_type: str) -> CachedEntityInfo | None:
        """
        Retrieves cached entity info for the given text and annotation type.

        Checks in-memory cache first, then falls back to persistent RAW cache.
        Returns CachedEntityInfo directly without making API calls.

        Args:
            text: The text to look up
            annotation_type: Type of annotation ("diagrams.FileLink" or "diagrams.AssetLink")

        Returns:
            CachedEntityInfo if found, None if cache miss
        """
        cache_key: tuple[str, str] = (text, annotation_type)

        # TIER 1: In-memory cache (instant, no API calls)
        if cache_key in self._memory_cache:
            cached_result: CachedEntityInfo | CacheMarker = self._memory_cache[cache_key]
            # 'No Match' in-memory marker
            if cached_result is CacheMarker.NO_MATCH:
                self.logger.debug(f"✓ [CACHE] In-memory 'No Match' marker HIT for '{text}'")
                return None

            # 'Ambiguous' in-memory marker
            if cached_result is CacheMarker.AMBIGUOUS:
                self.logger.debug(f"✓ [CACHE] In-memory 'Ambiguous' marker HIT for '{text}'")
                return None

            self.logger.debug(f"✓ [CACHE] In-memory cache HIT for '{text}'")
            return cached_result

        # TIER 2: Persistent RAW cache (fast, no retrieve_nodes call)
        cached_info: CachedEntityInfo | None = self._get_from_persistent_cache(text, annotation_type)
        if cached_info:
            self.logger.info(f"✓ [CACHE] Persistent cache HIT for '{text}'")
            # Populate in-memory cache for future lookups in this run
            self._memory_cache[cache_key] = cached_info
            return cached_info

        # Cache miss
        return None

    def get_from_memory(self, text: str, annotation_type: str) -> CachedEntityInfo | None:
        """
        Retrieves from in-memory cache only (no persistent storage lookup, no API calls).

        Useful for checking if we've already looked up this text in this run.

        Args:
            text: The text to look up
            annotation_type: Type of annotation

        Returns:
            CachedEntityInfo if found in memory, None otherwise
        """
        cache_key: tuple[str, str] = (text, annotation_type)
        if cache_key not in self._memory_cache:
            return None

        # Return cached info directly - no API call needed
        # If the value is a CacheMarker, treat it as a cache miss (None)
        val = self._memory_cache[cache_key]
        return None if val is CacheMarker.AMBIGUOUS or val is CacheMarker.NO_MATCH else val

    def is_ambiguous_in_memory(self, text: str, annotation_type: str) -> bool:
        """
        Returns True if this text/type combination was previously seen as ambiguous
        during the current run (in-memory only).
        """
        cache_key: tuple[str, str] = (text, annotation_type)
        return cache_key in self._memory_cache and self._memory_cache[cache_key] is CacheMarker.AMBIGUOUS

    def is_no_match_in_memory(self, text: str, annotation_type: str) -> bool:
        """
        Returns True if this text/type combination was determined to have no match
        during the current run (in-memory only).
        """
        cache_key: tuple[str, str] = (text, annotation_type)
        return cache_key in self._memory_cache and self._memory_cache[cache_key] is CacheMarker.NO_MATCH
    

    def set_ambiguous(self, text: str, annotation_type: str) -> None:
        """
        Marks the given text/type as ambiguous in in-memory cache only.

        This avoids re-querying repeatedly for known ambiguous cases
        within the same function run. Ambiguous entries are NOT persisted to RAW.
        """
        cache_key: tuple[str, str] = (text, annotation_type)
        self._memory_cache[cache_key] = CacheMarker.AMBIGUOUS
        self.logger.debug(f"✓ [CACHE] Cached ambiguous marker for '{text}' (in-memory only)")

    def set_no_match(self, text: str, annotation_type: str) -> None:
        """
        Marks the given text/type as a negative (no match) result in in-memory cache only.

        Useful to avoid repeated searches within the same function run. Not persisted.
        """
        cache_key: tuple[str, str] = (text, annotation_type)
        self._memory_cache[cache_key] = CacheMarker.NO_MATCH
        self.logger.debug(f"✓ [CACHE] Cached NO_MATCH marker for '{text}' (in-memory only)")

    def set(self, text: str, annotation_type: str, node: Node | None, resource_type: str | None = None) -> None:
        """
        Caches an entity node for the given text and annotation type.

        Caching behavior:
        - Positive matches (node provided): Cached in BOTH in-memory AND persistent RAW
        - Transient results (`NO_MATCH` or `AMBIGUOUS`): Cached ONLY in-memory
            to avoid repeated server-side lookups during the same run. Use
            `set_no_match` to record a negative (no match) result or
            `set_ambiguous` to mark ambiguous search outcomes. Passing
            `node=None` to `set` will also record a `NO_MATCH` marker for
            backward compatibility.

        Args:
            text: The text being cached
            annotation_type: Type of annotation
            node: The entity node to cache. For negative caching (no match), use `set_no_match`
                or pass `None` to record an in-memory NO_MATCH marker. For ambiguous
                search outcomes prefer `set_ambiguous` to mark the key as ambiguous in-memory.
            resource_type: Optional resource type to cache (avoids needing to retrieve node later)
        """
        cache_key: tuple[str, str] = (text, annotation_type)

        if node is None:
            # Negative cache entry (IN-MEMORY ONLY - not persisted to RAW)
            # Store explicit NO_MATCH marker to make cache states self-descriptive
            self._memory_cache[cache_key] = CacheMarker.NO_MATCH
            self.logger.debug(f"✓ [CACHE] Cached NO_MATCH marker for '{text}' (in-memory only)")
            return

        # Create CachedEntityInfo with all needed properties
        cached_info = CachedEntityInfo(
            space=node.space,
            external_id=node.external_id,
            resource_type=resource_type,
        )

        # Positive cache entry (BOTH in-memory AND persistent RAW)
        self._memory_cache[cache_key] = cached_info
        self._set_in_persistent_cache(text, annotation_type, node, resource_type)
        self.logger.debug(f"✓ [CACHE] Cached positive match for '{text}' → {node.external_id} (in-memory + RAW)")

    def _get_from_persistent_cache(self, text: str, annotation_type: str) -> CachedEntityInfo | None:
        """
        Checks persistent RAW cache for text → entity mapping.

        Returns CachedEntityInfo directly without calling retrieve_nodes.

        Returns:
            CachedEntityInfo if cache hit, None if miss
        """
        try:
            # Normalize text for consistent cache keys
            cache_key: str = self.normalize(text)

            row: Any = self.client.raw.rows.retrieve(
                db_name=self.raw_db,
                table_name=self.cache_table_name,
                key=cache_key,
            )

            if not row or not row.columns:
                return None

            # Verify annotation type matches
            if row.columns.get("annotationType") != annotation_type:
                return None

            # Extract cached entity info directly from RAW row
            end_node_space: Any = row.columns.get("endNodeSpace")
            end_node_ext_id: Any = row.columns.get("endNode")
            resource_type: Any = row.columns.get("resourceType")

            if not end_node_space or not end_node_ext_id:
                return None

            # Return CachedEntityInfo directly - no retrieve_nodes call needed
            return CachedEntityInfo(
                space=end_node_space,
                external_id=end_node_ext_id,
                resource_type=resource_type,
            )

        except Exception as e:
            # Cache miss or error - just continue without cache
            self.logger.debug(f"[CACHE] Cache check failed for '{text}': {e}")
            return None

    def _set_in_persistent_cache(
        self, text: str, annotation_type: str, node: Node, resource_type: str | None = None
    ) -> None:
        """
        Updates persistent RAW cache with text → entity mapping.
        Only caches unambiguous single matches.
        Includes resourceType to avoid needing retrieve_nodes on cache hits.

        NOTE: This cache has two entry points:
        1. Automatically generated connections from this code
        2. Manual promotions through the streamlit app

        The sourceCreatedUser will be the functionId for auto generated cache rows
        and will be a usersId for the manual promotions.
        """
        try:
            cache_key: str = self.normalize(text)

            cache_columns: dict[str, Any] = {
                "originalText": text,
                "endNode": node.external_id,
                "endNodeSpace": node.space,
                "annotationType": annotation_type,
                "lastUpdateTimeUtcIso": datetime.now(timezone.utc).isoformat(),
                "sourceCreatedUser": self.function_id,
            }

            # Include resourceType if available
            if resource_type:
                cache_columns["resourceType"] = resource_type

            cache_data: Row = Row(key=cache_key, columns=cache_columns)

            self.client.raw.rows.insert(
                db_name=self.raw_db,
                table_name=self.cache_table_name,
                row=cache_data,
                ensure_parent=True,
            )

        except Exception as e:
            # Don't fail the run if cache update fails
            self.logger.warning(f"Failed to update cache for '{text}': {e}")

    def _extract_single_node(self, retrieved: Node | NodeList) -> Node | None:
        """
        Extracts a single Node from the retrieved result.

        Handles both single Node and NodeList returns from the SDK.
        """
        if isinstance(retrieved, NodeList) and len(retrieved) > 0:
            first_node = list(retrieved)[0]
            return first_node if isinstance(first_node, Node) else None
        elif isinstance(retrieved, Node):
            return retrieved
        else:
            return None

    def get_stats(self) -> dict[str, int]:
        """
        Returns statistics about the in-memory cache.

        Returns:
            Dictionary with cache statistics
        """
        total_entries = len(self._memory_cache)
        negative_entries = sum(1 for v in self._memory_cache.values() if v is CacheMarker.NO_MATCH)
        positive_entries = total_entries - negative_entries

        return {
            "total_entries": total_entries,
            "positive_entries": positive_entries,
            "negative_entries": negative_entries,
        }

    def clear_memory_cache(self) -> None:
        """Clears the in-memory cache. Useful for testing."""
        self._memory_cache.clear()
        self.logger.debug("In-memory cache cleared")
