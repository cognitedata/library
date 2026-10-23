"""Reading the instances to alias through the sync endpoint, from where the last run stopped.

The cursor of each view lives in the state store, so a normal run reads only the instances
created or changed since the previous one, a page at a time. Writing aliases changes an
instance too, so instances written by one run come back once more on the next; they
produce no update the second time.
"""

import hashlib
import json
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import Node
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.exceptions import CogniteAPIError, CogniteException

from alias_optimizations import is_retryable  # isort: skip
from config import Parameters, ViewPropertyConfig  # isort: skip
from constants import (  # isort: skip
    ALIAS_MIN_PAGE_SIZE,
    ALIAS_PAGE_SIZE,
    ALIAS_PAGE_SIZE_FACTOR,
    ALIAS_SOURCE_PROPERTIES,
    ALIAS_SYNC_MAX_RETRIES,
    ALIAS_SYNC_RETRY_BACKOFF_SECONDS,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
    HTTP_STATUS_REQUEST_TIMEOUT,
    ITEMS_QUERY_NAME,
    SYNC_COL_BATCH_SIZE,
    SYNC_COL_CURSOR,
    SYNC_COL_FULL_READ,
    SYNC_COL_UPDATED_AT,
    SYNC_COL_VIEW,
    SYNC_STATE_KEY_PREFIX,
    SYNC_STATE_VERSION,
)
from logger import CogniteFunctionLogger  # isort: skip


@dataclass(frozen=True)
class SyncState:
    """Where a read of one view stands: the cursor to continue from, and the page size that works.

    `full_read_in_progress` marks a cursor taken part way through a read of every instance,
    so a run stopped before the end continues that read instead of starting it again.
    """

    cursor: str | None
    batch_size: int
    full_read_in_progress: bool = False


def sync_state_key(view_config: ViewPropertyConfig, label: str) -> str:
    """The state store row the cursor for this view and filter is kept in."""
    configuration = json.dumps(
        {
            "version": SYNC_STATE_VERSION,
            "view": [view_config.schema_space, view_config.external_id, view_config.version],
            "instanceSpaces": sorted(view_config.instance_spaces),
            "filterProperty": view_config.filter_property,
            "filterValues": sorted(view_config.filter_values) if view_config.filter_values else None,
        },
        sort_keys=True,
    )
    return f"{SYNC_STATE_KEY_PREFIX}{label}_{hashlib.sha256(configuration.encode('utf-8')).hexdigest()[:16]}"


def _missing_state_store(error: CogniteAPIError) -> bool:
    """Whether the state database or table does not exist yet.

    A missing row never arrives here: the SDK returns None for it. A missing
    database or table is a 400 that names it. A 404 is the same situation when
    it reaches the caller.
    """
    if error.code == HTTP_STATUS_NOT_FOUND:
        return True
    if error.code != HTTP_STATUS_BAD_REQUEST:
        return False
    message = (error.message or "").casefold()
    return "database not found" in message or "table not found" in message


def read_sync_state(
    client: CogniteClient, parameters: Parameters, logger: CogniteFunctionLogger, key: str
) -> SyncState:
    """The cursor and page size the previous run stored, or none and the default size on a first run.

    A missing database or table reads as a missing row; the first write creates them.
    """
    try:
        row = client.raw.rows.retrieve(parameters.raw_db, parameters.raw_table_state, key)
    except CogniteAPIError as e:
        if not _missing_state_store(e):
            raise
        row = None
    columns = row.columns if row and row.columns else {}
    cursor = columns.get(SYNC_COL_CURSOR)
    batch_size = columns.get(SYNC_COL_BATCH_SIZE)
    state = SyncState(
        str(cursor) if cursor else None,
        int(batch_size) if batch_size else ALIAS_PAGE_SIZE,
        bool(columns.get(SYNC_COL_FULL_READ)),
    )
    logger.debug(
        f"Sync state {key} - cursor: {state.cursor}, page size: {state.batch_size}, "
        f"full read in progress: {state.full_read_in_progress}"
    )
    return state


def write_sync_state(
    client: CogniteClient,
    parameters: Parameters,
    logger: CogniteFunctionLogger,
    key: str,
    view_config: ViewPropertyConfig,
    state: SyncState,
) -> None:
    """Record where the next run should continue reading this view from."""
    columns: dict[str, object] = {
        SYNC_COL_CURSOR: state.cursor,
        SYNC_COL_BATCH_SIZE: state.batch_size,
        SYNC_COL_FULL_READ: state.full_read_in_progress,
        SYNC_COL_VIEW: str(view_config.as_view_id()),
        SYNC_COL_UPDATED_AT: datetime.now(UTC).isoformat(),
    }
    try:
        client.raw.rows.insert(parameters.raw_db, parameters.raw_table_state, Row(key, columns), ensure_parent=True)
    except CogniteException as e:
        # The stored cursor is older but still valid, so the next run reads more than it
        # has to rather than skipping anything.
        logger.warning(f"Could not store sync cursor {key} - the next run reads from the previous one: {e}")
        return
    logger.debug(f"Stored sync cursor in {parameters.raw_db}.{parameters.raw_table_state} - key: {key}")


def sync_filter(view_config: ViewPropertyConfig) -> dm.filters.Filter:
    """The instances of the view in the configured spaces, narrowed by the property filter when one is set.

    A sync read is scoped by its filter alone: without HasData it returns every node in
    the spaces, whichever views they have data in.
    """
    filters: list[dm.filters.Filter] = [
        dm.filters.In(["node", "space"], view_config.instance_spaces),
        dm.filters.HasData(views=[view_config.as_view_id()]),
    ]
    if view_config.filter_property and view_config.filter_values:
        filters.append(
            dm.filters.In(view_config.as_property_ref(view_config.filter_property), view_config.filter_values)
        )
    return dm.filters.And(*filters)


def _sync_page(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    view_config: ViewPropertyConfig,
    cursor: str | None,
    batch_size: int,
    label: str,
) -> tuple[QueryResult, int]:
    """One sync call, with a page size that gives way to a timeout.

    A 408 is read again 20% smaller, down to the minimum page size. Anything else
    transient is retried unchanged with an exponential backoff.

    Returns:
        The result, and the page size that produced it.
    """
    attempt = 0
    while True:
        query = Query(
            with_={ITEMS_QUERY_NAME: NodeResultSetExpression(filter=sync_filter(view_config), limit=batch_size)},
            select={ITEMS_QUERY_NAME: Select([SourceSelector(view_config.as_view_id(), ALIAS_SOURCE_PROPERTIES)])},
            cursors={ITEMS_QUERY_NAME: cursor} if cursor else None,
        )
        if not cursor:
            # Query fills in {name: None} when cursors is omitted, and that serializes as null.
            query.cursors = None
        try:
            return client.data_modeling.instances.sync(query), batch_size
        # Deliberately broad: `is_retryable` decides what is worth another attempt, and
        # everything else is re-raised unchanged.
        except Exception as e:
            attempt += 1
            timed_out = isinstance(e, CogniteAPIError) and e.code == HTTP_STATUS_REQUEST_TIMEOUT
            if timed_out and batch_size > ALIAS_MIN_PAGE_SIZE and attempt <= ALIAS_SYNC_MAX_RETRIES:
                batch_size = max(ALIAS_MIN_PAGE_SIZE, int(batch_size * ALIAS_PAGE_SIZE_FACTOR))
                logger.warning(f"Reading {label} timed out - reading again with page size: {batch_size}")
                continue
            if attempt > ALIAS_SYNC_MAX_RETRIES or not is_retryable(e):
                raise
            sleep_seconds = ALIAS_SYNC_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(f"Transient error reading {label} (attempt {attempt}), retrying in {sleep_seconds}s: {e}")
            time.sleep(sleep_seconds)


def iter_synced_pages(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    view_config: ViewPropertyConfig,
    state: SyncState,
    label: str,
) -> Iterator[tuple[list[Node], SyncState]]:
    """The instances changed since the cursor - every instance when there is none - one page at a time.

    Each page comes with the state to store once the caller has processed it. Deleted
    instances are left out of the page.

    Raises:
        Exception: A read that fails for good, so the run fails instead of reporting that
            there was nothing to update.
    """
    cursor, batch_size = state.cursor, state.batch_size
    stored_cursor_unread = cursor is not None
    while True:
        try:
            result, batch_size = _sync_page(client, logger, view_config, cursor, batch_size, label)
        except CogniteAPIError as e:
            if not stored_cursor_unread or e.code != HTTP_STATUS_BAD_REQUEST:
                raise
            # An expired cursor, or one the API no longer recognises, leaves no way to tell
            # what changed, so the view is read from the start.
            logger.warning(f"Stored {label} sync cursor rejected: {e.message} - reading every {label} instance")
            cursor, stored_cursor_unread = None, False
            continue
        stored_cursor_unread = False
        synced = result[ITEMS_QUERY_NAME]
        last_page = len(synced) < batch_size
        nodes = [node for node in synced if node.deleted_time is None]
        cursor = result.cursors.get(ITEMS_QUERY_NAME)
        logger.debug(f"Synced a page of {len(synced)} {label} instance(s), {len(nodes)} not deleted")
        # Dropped before the next read so only one page is held at a time.
        del result, synced
        yield nodes, SyncState(cursor, batch_size)
        del nodes
        if last_page:
            return
