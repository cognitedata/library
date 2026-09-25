"""Reads the match entities of a view without paging the whole view every run.

A filtered query on tags and scope timed out on large asset views: the scope property and
`tags` sit in different containers, and the OR with ScopeWideDetect keeps DMS from paging
the filter with an index. Instead a view is read through the sync endpoint with only a
space and hasData filter, and tags and scope are applied in memory. The sync cursor lives
in the RAW entity cache table and the entities in a JSON file in CDF, so a run that syncs
no changes downloads the file rather than reading the data model again.

Sync reports every change to an instance of the view, a changed tag included, so the
cache holds exactly the instances carrying one of the tags it is kept for.
"""

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import Node, ViewId
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import Equals, Filter, HasData
from cognite.client.exceptions import CogniteAPIError, CogniteException
from fa_constants import (
    ENTITY_SYNC_BATCH_SIZE,
    ENTITY_SYNC_BATCH_SIZE_FACTOR,
    ENTITY_SYNC_CACHE_FILE_PREFIX,
    ENTITY_SYNC_CACHE_VERSION,
    ENTITY_SYNC_CHECKPOINT_SECONDS,
    ENTITY_SYNC_MAX_RETRIES,
    ENTITY_SYNC_MIN_BATCH_SIZE,
    ENTITY_SYNC_QUERY_NAME,
    ENTITY_SYNC_RETRY_BACKOFF_SECONDS,
    ENTITY_SYNC_STATE_KEY_PREFIX,
)
from services.ConfigService import Config, ViewPropertyConfig
from services.LoggerService import CogniteFunctionLogger

HTTP_STATUS_BAD_REQUEST = 400
HTTP_STATUS_REQUEST_TIMEOUT = 408
HTTP_STATUS_TOO_MANY_REQUESTS = 429


@dataclass(frozen=True)
class EntityInstance:
    """A match entity carrying only the view properties Launch reads, keyed by view like a Node."""

    space: str
    external_id: str
    properties: dict[ViewId, dict[str, object]]


class EntitySyncIncompleteError(RuntimeError):
    """A read of a view was stopped part way; what was read is stored and the next load continues it."""


def entity_cache_key(view: ViewPropertyConfig, space: str | None, properties: list[str], tags: list[str]) -> str:
    """Fingerprint of the read a sync cursor and a cache file belong to."""
    configuration = json.dumps(
        {
            "cacheVersion": ENTITY_SYNC_CACHE_VERSION,
            "view": [view.schema_space, view.external_id, view.version],
            "space": space,
            "properties": sorted(properties),
            "tags": sorted(tags),
        },
        sort_keys=True,
    )
    return hashlib.sha256(configuration.encode("utf-8")).hexdigest()[:16]


def _has_any_tag(properties: dict[str, object], tags: set[str]) -> bool:
    instance_tags = properties.get("tags")
    return isinstance(instance_tags, list) and not tags.isdisjoint(instance_tags)


class EntitySyncService:
    """Keeps the tagged instances of a view in a CDF file, brought up to date through DMS sync."""

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        data_set_id: int | None = None,
        deadline: float | None = None,
    ):
        """
        Args:
            data_set_id: Data set the cache files are written to.
            deadline: `time.monotonic()` value a read stops at, so it is stored before the function is killed.
        """
        self.client = client
        self.logger = logger
        self.db_name = config.raw_tables.raw_db
        self.table_name = config.raw_tables.raw_table_cache
        self.data_set_id = data_set_id
        self.deadline = deadline

    def load(
        self, view: ViewPropertyConfig, space: str | None, properties: list[str], tags: list[str]
    ) -> list[EntityInstance]:
        """Every instance of the view in the space that carries one of the tags.

        Args:
            view: View the entities are read from.
            space: Instance space to read from; None reads every space.
            properties: View properties to keep; `tags` must be one of them.
            tags: The instances carrying none of these are left out.

        Returns:
            The entities, with only the requested properties.

        Raises:
            EntitySyncIncompleteError: The read reached a checkpoint or the deadline before it caught up.
        """
        key = entity_cache_key(view, space, properties, tags)
        cursor, batch_size = self._read_state(key)
        cached = self._read_cache(key, view.as_view_id()) if cursor else None
        if cached is None:
            cursor = None

        entities = dict(cached or {})
        try:
            changes, next_cursor, batch_size, caught_up = self._sync(
                view, space, properties, set(tags), cursor, batch_size, entities
            )
        except CogniteAPIError as e:
            if cursor is None or e.code != HTTP_STATUS_BAD_REQUEST:
                raise
            # An expired cursor leaves no way to tell what changed, so the view is read again.
            self.logger.warning(f"Sync cursor for {view.external_id} rejected ({e.message}) - reading it again")
            cached, entities = None, {}
            changes, next_cursor, batch_size, caught_up = self._sync(
                view, space, properties, set(tags), None, batch_size, entities
            )

        result = list(entities.values())
        self.logger.info(
            f"Synced {changes} change(s) of {view.external_id} in space {space!r} - {len(result)} tagged entities"
            + (" (from the cached file)" if cached is not None and not changes else "")
        )
        # The cursor only moves once the content it describes is stored, so the two never disagree.
        if (cached is not None and not changes) or self._write_cache(key, result):
            self._write_state(key, next_cursor, batch_size, len(result), view, space)

        if not caught_up:
            raise EntitySyncIncompleteError(
                f"Read {len(result)} tagged entities of {view.external_id} so far; the next run continues the read"
            )
        return result

    def _sync(
        self,
        view: ViewPropertyConfig,
        space: str | None,
        properties: list[str],
        tags: set[str],
        cursor: str | None,
        batch_size: int,
        entities: dict[tuple[str, str], EntityInstance],
    ) -> tuple[int, str | None, int, bool]:
        """Applies every change the cursor has not seen to `entities`, page by page.

        Returns:
            The number of changes, the cursor to store, the page size that worked, and
            whether the read caught up rather than running out of time.
        """
        view_id = view.as_view_id()
        read_filter: Filter = HasData(views=[view_id])
        if space:
            read_filter = Equals(["node", "space"], space) & read_filter
        expression = NodeResultSetExpression(filter=read_filter, limit=batch_size)
        query = Query(
            with_={ENTITY_SYNC_QUERY_NAME: expression},
            select={ENTITY_SYNC_QUERY_NAME: Select([SourceSelector(view_id, properties)])},
            cursors={ENTITY_SYNC_QUERY_NAME: cursor},
        )

        deadline = time.monotonic() + ENTITY_SYNC_CHECKPOINT_SECONDS
        if self.deadline is not None:
            deadline = min(deadline, self.deadline)
        changes = 0
        while True:
            result, batch_size = self._sync_page(query, expression, batch_size)
            page: list[Node] = list(result[ENTITY_SYNC_QUERY_NAME])
            for node in page:
                node_properties = node.properties.get(view_id) if node.properties else None
                instance_key = (node.space, node.external_id)
                if node.deleted_time is not None or not node_properties or not _has_any_tag(node_properties, tags):
                    entities.pop(instance_key, None)
                else:
                    entities[instance_key] = EntityInstance(
                        node.space, node.external_id, {view_id: dict(node_properties)}
                    )
            changes += len(page)
            query.cursors = result.cursors
            next_cursor = result.cursors.get(ENTITY_SYNC_QUERY_NAME)
            if len(page) < batch_size:
                return changes, next_cursor, batch_size, True
            if time.monotonic() > deadline:
                return changes, next_cursor, batch_size, False

    def _sync_page(self, query: Query, expression: NodeResultSetExpression, batch_size: int) -> tuple[QueryResult, int]:
        """One sync call; a timeout is retried with a 20% smaller page, other transient errors with backoff."""
        attempt = 0
        while True:
            expression.limit = batch_size
            try:
                return self.client.data_modeling.instances.sync(query), batch_size
            except CogniteAPIError as e:
                attempt += 1
                transient = e.code in (HTTP_STATUS_REQUEST_TIMEOUT, HTTP_STATUS_TOO_MANY_REQUESTS) or e.code >= 500
                if attempt > ENTITY_SYNC_MAX_RETRIES or not transient:
                    raise
                if e.code == HTTP_STATUS_REQUEST_TIMEOUT and batch_size > ENTITY_SYNC_MIN_BATCH_SIZE:
                    batch_size = max(ENTITY_SYNC_MIN_BATCH_SIZE, int(batch_size * ENTITY_SYNC_BATCH_SIZE_FACTOR))
                    self.logger.warning(f"Entity read timed out - reading again with page size {batch_size}")
                    continue
                sleep_seconds = ENTITY_SYNC_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                self.logger.warning(
                    f"Retry {attempt}/{ENTITY_SYNC_MAX_RETRIES} of entity read in {sleep_seconds}s: {e}"
                )
                time.sleep(sleep_seconds)

    def _read_state(self, key: str) -> tuple[str | None, int]:
        """The cursor and page size the previous run stored, or none and the default page size."""
        try:
            row = self.client.raw.rows.retrieve(self.db_name, self.table_name, f"{ENTITY_SYNC_STATE_KEY_PREFIX}{key}")
        except CogniteAPIError as e:
            self.logger.warning(f"Could not read the entity sync state - reading all entities: {e}")
            row = None
        columns = row.columns if row and row.columns else {}
        cursor = columns.get("cursor")
        batch_size = columns.get("batchSize")
        return (
            str(cursor) if cursor else None,
            int(batch_size) if isinstance(batch_size, int) and batch_size > 0 else ENTITY_SYNC_BATCH_SIZE,
        )

    def _write_state(
        self,
        key: str,
        cursor: str | None,
        batch_size: int,
        entity_count: int,
        view: ViewPropertyConfig,
        space: str | None,
    ) -> None:
        columns: dict[str, object] = {
            "cursor": cursor,
            "batchSize": batch_size,
            "entityCount": entity_count,
            "view": str(view.as_view_id()),
            "space": space,
            "cacheFile": self._file_external_id(key),
            "updatedAt": datetime.now(UTC).isoformat(),
        }
        try:
            self.client.raw.rows.insert(
                self.db_name, self.table_name, Row(f"{ENTITY_SYNC_STATE_KEY_PREFIX}{key}", columns), ensure_parent=True
            )
        except CogniteAPIError as e:
            # The stored cursor still matches the stored file, so the next run just syncs the same changes again.
            self.logger.warning(f"Could not store the entity sync cursor: {e}")

    @staticmethod
    def _file_external_id(key: str) -> str:
        return f"{ENTITY_SYNC_CACHE_FILE_PREFIX}{key}.json"

    def _read_cache(self, key: str, view_id: ViewId) -> dict[tuple[str, str], EntityInstance] | None:
        """The entities an earlier run stored, or None when the file cannot be read."""
        file_external_id = self._file_external_id(key)
        try:
            content = self.client.files.download_bytes(external_id=file_external_id)
            rows = json.loads(content)
            return {
                (space, external_id): EntityInstance(space, external_id, {view_id: properties})
                for space, external_id, properties in rows
            }
        except (CogniteException, ValueError, TypeError) as e:
            self.logger.warning(f"Cached entities in {file_external_id} could not be read - reading them again: {e}")
            return None

    def _write_cache(self, key: str, entities: list[EntityInstance]) -> bool:
        """Stores the entities; False when they could not be, so the cursor is left where it was."""
        file_external_id = self._file_external_id(key)
        content = json.dumps(
            [[e.space, e.external_id, next(iter(e.properties.values()))] for e in entities], separators=(",", ":")
        ).encode("utf-8")
        try:
            self.client.files.upload_bytes(
                content=content,
                name=file_external_id,
                external_id=file_external_id,
                mime_type="application/json",
                data_set_id=self.data_set_id,
                overwrite=True,
            )
        except CogniteAPIError as e:
            self.logger.warning(f"Could not cache {len(entities)} entities in {file_external_id}: {e}")
            return False
        return True
