"""Reading the targets to match against, without reading them all every run.

Paging every target instance out of the data model is the slowest step of a submit run,
and on a large model it is what times the read out with a 408. Targets change rarely, so
they are read through the sync endpoint instead: the cursor from the previous run lives
in the state store, the target content lives in a JSON file in CDF, and a run that syncs
no changes downloads that file rather than paging the data model again. Both are keyed
by a fingerprint of the target configuration, so several functions - each with its own
extraction pipeline configuration - keep caches of their own.

Sync reports instances that were created, changed or deleted, but not ones that stopped
matching the configured filter, so a cached copy can hold a target the filter no longer
selects. Running with `runAll` reads every target again and rebuilds the cache.
"""

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

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

from em_config import Config, ViewPropertyConfig  # isort: skip
from em_constants import (  # isort: skip
    COL_KEY_RULE_REGEXP_TARGET,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_REQUEST_TIMEOUT,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE,
    KEY_RULE_KEYS,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_SPACE,
    PROP_COL_NAME,
    QUERY_FILTER_TYPE_TARGETS,
    STAT_STORE_TARGET_SYNC_PREFIX,
    TARGET_CACHE_FILE_PREFIX,
    TARGET_CACHE_MAX_RETRIES,
    TARGET_CACHE_VERSION,
    TARGET_SYNC_BATCH_SIZE,
    TARGET_SYNC_BATCH_SIZE_FACTOR,
    TARGET_SYNC_COL_BATCH_SIZE,
    TARGET_SYNC_COL_COUNT,
    TARGET_SYNC_COL_CURSOR,
    TARGET_SYNC_COL_FILE,
    TARGET_SYNC_COL_UPDATED_AT,
    TARGET_SYNC_COL_VIEW,
    TARGET_SYNC_MAX_RETRIES,
    TARGET_SYNC_MIN_BATCH_SIZE,
    TARGET_SYNC_QUERY_NAME,
    TARGET_SYNC_RETRY_BACKOFF_SECONDS,
)
from em_logger import CogniteFunctionLogger  # isort: skip
from em_pipeline import (  # isort: skip
    create_table,
    get_query_filter,
    is_retryable,
    match_values,
    warn_on_cross_space_duplicates,
)
from em_pipeline_types import RuleMappingDefinition, TargetMatchRecord  # isort: skip


@dataclass(frozen=True)
class TargetSyncState:
    """What an earlier run left behind: how far it read, and where it put the content."""

    cursor: str | None
    file_external_id: str
    batch_size: int


def target_cache_key(view: ViewPropertyConfig) -> str:
    """Fingerprint of the target configuration a cursor and a cache file belong to.

    Functions reading the same view, spaces and filter share one cache; change any of
    them and the run gets a cache of its own, so a cursor is never applied to content it
    was not taken from. `TARGET_CACHE_VERSION` does the same for a change to what the
    read itself selects, which no configuration reflects.
    """
    configuration = json.dumps(
        {
            "cacheVersion": TARGET_CACHE_VERSION,
            "schemaSpace": view.schema_space,
            "externalId": view.external_id,
            "version": view.version,
            "instanceSpaces": sorted(view.instance_spaces),
            "searchProperty": view.search_property,
            "filterProperty": view.filter_property,
            "filterValues": sorted(view.filter_values) if view.filter_values else None,
        },
        sort_keys=True,
    )
    return hashlib.sha256(configuration.encode("utf-8")).hexdigest()[:16]


def target_state_row_key(cache_key: str) -> str:
    """The state store row the sync cursor for this configuration is kept in."""
    return f"{STAT_STORE_TARGET_SYNC_PREFIX}{cache_key}"


def cache_file_external_id(cache_key: str) -> str:
    """The CDF file the target content for this configuration is kept in."""
    return f"{TARGET_CACHE_FILE_PREFIX}{cache_key}.json"


def read_sync_state(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    cache_key: str,
) -> TargetSyncState:
    """The cursor, cache file and page size of the previous run, with defaults if it is the first."""
    db = config.parameters.raw_db
    table = config.parameters.raw_table_state
    create_table(client, db, table)

    row = client.raw.rows.retrieve(db, table, target_state_row_key(cache_key))
    columns = row.columns if row and row.columns else {}

    batch_size = columns.get(TARGET_SYNC_COL_BATCH_SIZE)
    state = TargetSyncState(
        cursor=str(columns[TARGET_SYNC_COL_CURSOR]) if columns.get(TARGET_SYNC_COL_CURSOR) else None,
        file_external_id=str(columns.get(TARGET_SYNC_COL_FILE) or cache_file_external_id(cache_key)),
        batch_size=int(batch_size) if batch_size else TARGET_SYNC_BATCH_SIZE,
    )
    logger.debug(
        f"Target sync state - cursor: {state.cursor}, file: {state.file_external_id}, "
        f"page size: {state.batch_size}"
    )
    return state


def write_sync_state(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    cache_key: str,
    state: TargetSyncState,
    target_count: int,
) -> None:
    """Record where the next run should pick up from."""
    db = config.parameters.raw_db
    table = config.parameters.raw_table_state
    create_table(client, db, table)

    columns: dict[str, Any] = {
        TARGET_SYNC_COL_CURSOR: state.cursor,
        TARGET_SYNC_COL_FILE: state.file_external_id,
        TARGET_SYNC_COL_BATCH_SIZE: state.batch_size,
        TARGET_SYNC_COL_COUNT: target_count,
        TARGET_SYNC_COL_VIEW: str(config.data.target_view.as_view_id()),
        TARGET_SYNC_COL_UPDATED_AT: datetime.now(UTC).isoformat(),
    }
    row_key = target_state_row_key(cache_key)
    try:
        with_retries(
            logger,
            f"store the target sync cursor in {db}.{table} - key: {row_key}",
            lambda: client.raw.rows.insert(db, table, Row(row_key, columns)),
        )
    except CogniteException as e:
        # The cursor that is already stored still matches the cached content, so the next
        # run syncs the same changes again rather than reading from the wrong place.
        logger.warning(f"Could not store the target sync cursor - the next run syncs from the previous one. Error: {type(e)}({e})")
        return

    logger.debug(f"Stored target sync cursor in {db}.{table} - key: {row_key}")


def with_retries[T](
    logger: CogniteFunctionLogger,
    description: str,
    operation: Callable[[], T],
) -> T:
    """Run a cache operation, retrying the failures that are worth retrying.

    Args:
        description: What the operation does, for the log line on a retry.

    Raises:
        Exception: Whatever the operation raised, once it is out of attempts or the
            failure is one that repeating cannot fix.
    """
    attempt = 0
    while True:
        try:
            return operation()
        # Deliberately broad: `is_retryable` decides what another attempt is worth, and
        # everything else is re-raised for the caller to fall back on.
        except Exception as e:
            attempt += 1
            if attempt > TARGET_CACHE_MAX_RETRIES or not is_retryable(e):
                raise
            sleep_seconds = TARGET_SYNC_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                f"Retry {attempt}/{TARGET_CACHE_MAX_RETRIES} to {description} in {sleep_seconds}s. "
                f"Error: {type(e)}({e})"
            )
            time.sleep(sleep_seconds)


def read_cached_targets(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_external_id: str,
) -> list[Node] | None:
    """The targets an earlier run stored, or None when the file cannot be read.

    A cache that is gone, unreachable or unreadable is not an error: the targets are
    still in the data model, and the caller falls back to reading them from there.
    """
    try:
        content = with_retries(
            logger,
            f"download cached {QUERY_FILTER_TYPE_TARGETS} from file: {file_external_id}",
            lambda: client.files.download_bytes(external_id=file_external_id),
        )
        targets = [Node.load(item) for item in json.loads(content)]
    except (CogniteException, ValueError, TypeError, KeyError) as e:
        logger.warning(
            f"Cached {QUERY_FILTER_TYPE_TARGETS} in file: {file_external_id} could not be read, "
            f"reading them from the data model instead - error: {type(e)}({e})"
        )
        return None

    logger.info(f"Read {len(targets)} cached {QUERY_FILTER_TYPE_TARGETS} from file: {file_external_id}")
    return targets


def write_cached_targets(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_external_id: str,
    targets: list[Node],
) -> bool:
    """Store the target content so the next run can read it instead of the data model.

    Returns:
        Whether the content was stored. A run that could not store it keeps the targets
        it has read, and its caller leaves the cursor where it is, so the file and the
        cursor still describe the same targets.
    """
    content = json.dumps([target.dump(camel_case=True) for target in targets]).encode("utf-8")
    try:
        with_retries(
            logger,
            f"cache {QUERY_FILTER_TYPE_TARGETS} in file: {file_external_id}",
            lambda: client.files.upload_bytes(
                content=content,
                name=file_external_id,
                external_id=file_external_id,
                mime_type="application/json",
                overwrite=True,
            ),
        )
    except CogniteException as e:
        logger.warning(
            f"Could not cache {len(targets)} {QUERY_FILTER_TYPE_TARGETS} in file: {file_external_id} - "
            f"the next run reads them from the data model. Error: {type(e)}({e})"
        )
        return False

    logger.info(f"Cached {len(targets)} {QUERY_FILTER_TYPE_TARGETS} in file: {file_external_id}")
    return True


def sync_page(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    query: Query,
    expression: NodeResultSetExpression,
    batch_size: int,
) -> tuple[QueryResult, int]:
    """One sync call, with a page size that gives way to a timeout.

    A 408 says the read did not finish in time, not that it was wrong, so the same read
    over 20% fewer instances is worth trying - repeatedly, down to the minimum page size.
    Anything else transient is retried unchanged, with an exponential backoff.

    Returns:
        The result, and the page size that produced it.
    """
    attempt = 0
    while True:
        expression.limit = batch_size
        try:
            return client.data_modeling.instances.sync(query), batch_size
        # Deliberately broad: `is_retryable` decides what is worth another attempt, and
        # everything else is logged with the page size it died on and re-raised unchanged.
        except Exception as e:
            timed_out = isinstance(e, CogniteAPIError) and e.code == HTTP_STATUS_REQUEST_TIMEOUT
            if timed_out and batch_size > TARGET_SYNC_MIN_BATCH_SIZE:
                batch_size = max(TARGET_SYNC_MIN_BATCH_SIZE, int(batch_size * TARGET_SYNC_BATCH_SIZE_FACTOR))
                logger.warning(f"Reading {QUERY_FILTER_TYPE_TARGETS} timed out - reading again with page size: {batch_size}")
                continue

            attempt += 1
            if attempt > TARGET_SYNC_MAX_RETRIES or not is_retryable(e):
                logger.error(
                    f"Failed to sync {QUERY_FILTER_TYPE_TARGETS} after {attempt} attempt(s) "
                    f"with page size: {batch_size}. Error: {type(e)}({e})"
                )
                raise

            sleep_seconds = TARGET_SYNC_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                f"Retry {attempt}/{TARGET_SYNC_MAX_RETRIES} of {QUERY_FILTER_TYPE_TARGETS} sync in {sleep_seconds}s. "
                f"Error: {type(e)}({e})"
            )
            time.sleep(sleep_seconds)


def sync_target_changes(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    cursor: str | None,
    batch_size: int,
) -> tuple[list[Node], str | None, int]:
    """Every target change the cursor has not seen, or every target when there is none.

    Only the properties the matching needs are selected, which is what keeps a page of
    targets small enough to come back inside the API timeout.

    Returns:
        The changed instances - a deleted one carries `deleted_time` - the cursor to
        store for the next run, and the page size that worked.
    """
    view = config.data.target_view
    view_id = view.as_view_id()

    # A sync read is scoped by its filter alone: `sources` in the select decides which
    # properties come back, not which instances do. Without HasData the read returns
    # every node in the configured spaces - state nodes, annotation nodes and all.
    filters: list[dm.filters.Filter] = [dm.filters.In(["node", "space"], view.instance_spaces)]
    is_selected = get_query_filter(
        QUERY_FILTER_TYPE_TARGETS,
        view,
        config.parameters.run_all,
        logger,
        include_has_data=True,
    )
    if is_selected is not None:
        filters.append(is_selected)

    properties = [PROP_COL_NAME]
    if view.search_property != PROP_COL_NAME:
        properties.append(view.search_property)

    expression = NodeResultSetExpression(filter=dm.filters.And(*filters), limit=batch_size)
    query = Query(
        with_={TARGET_SYNC_QUERY_NAME: expression},
        select={TARGET_SYNC_QUERY_NAME: Select([SourceSelector(view_id, properties)])},
        cursors={TARGET_SYNC_QUERY_NAME: cursor},
    )

    changes: list[Node] = []
    while True:
        result, batch_size = sync_page(client, logger, query, expression, batch_size)
        page = list(result[TARGET_SYNC_QUERY_NAME])
        next_cursor = result.cursors.get(TARGET_SYNC_QUERY_NAME)
        query.cursors = result.cursors
        changes.extend(page)

        logger.debug(f"Synced {len(page)} {QUERY_FILTER_TYPE_TARGETS} change(s), total so far: {len(changes)}")
        if len(page) < batch_size:
            break

    return changes, next_cursor, batch_size


def merge_target_changes(cached: list[Node], changes: list[Node]) -> list[Node]:
    """The cached targets with the synced changes applied.

    Every change is an upsert unless the instance carries `deleted_time`, which is how
    sync reports one that no longer exists.
    """
    merged = {(target.space, target.external_id): target for target in cached}
    for change in changes:
        key = (change.space, change.external_id)
        if change.deleted_time is not None:
            merged.pop(key, None)
        else:
            merged[key] = change
    return list(merged.values())


def load_targets(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
) -> list[Node]:
    """Every target instance matching the configuration, synced on top of the cached copy.

    The data model is read for changes only. A run where nothing changed reads the
    content from the cache file and writes back the new cursor; a run with changes merges
    them into that content and stores it again.
    """
    view = config.data.target_view
    cache_key = target_cache_key(view)
    state = read_sync_state(client, config, logger, cache_key)

    cached: list[Node] | None = None
    if state.cursor and not config.parameters.run_all:
        cached = read_cached_targets(client, logger, state.file_external_id)
    elif state.cursor:
        logger.info(f"runAll enabled - reading all {QUERY_FILTER_TYPE_TARGETS} again instead of syncing changes")

    cursor = state.cursor if cached is not None else None
    try:
        changes, next_cursor, batch_size = sync_target_changes(client, config, logger, cursor, state.batch_size)
    except CogniteAPIError as e:
        if cursor is None or e.code != HTTP_STATUS_BAD_REQUEST:
            raise
        # A cursor the API will not take - expired, or from a configuration it no longer
        # recognises - leaves no way to tell what changed, so the targets are read again.
        logger.warning(f"Sync cursor rejected: {e.message} - reading all {QUERY_FILTER_TYPE_TARGETS} again")
        cached = None
        changes, next_cursor, batch_size = sync_target_changes(client, config, logger, None, state.batch_size)

    synced = TargetSyncState(next_cursor, state.file_external_id, batch_size)
    if cached is not None and not changes:
        logger.info(f"No {QUERY_FILTER_TYPE_TARGETS} changed since the last run - using the cached content")
        write_sync_state(client, config, logger, cache_key, synced, len(cached))
        return cached

    targets = merge_target_changes(cached or [], changes)
    logger.info(f"Synced {len(changes)} {QUERY_FILTER_TYPE_TARGETS} change(s) - {len(targets)} in total")

    # The cursor is only moved on once the content it describes has been stored. Moving
    # it on a failed write would leave the next run merging changes onto the targets of
    # an earlier one.
    if write_cached_targets(client, logger, state.file_external_id, targets):
        write_sync_state(client, config, logger, cache_key, synced, len(targets))
    return targets


def get_all_targets(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    rule_mappings: list[RuleMappingDefinition] | None = None,
) -> list[TargetMatchRecord]:

    targets: list[TargetMatchRecord] = []
    all_targets = load_targets(client, config, logger)

    warn_on_cross_space_duplicates(all_targets, QUERY_FILTER_TYPE_TARGETS, config.data.target_view, logger)

    logger.info(
        f"Number of {QUERY_FILTER_TYPE_TARGETS} to process: {len(all_targets)}, "
        f"NOTE: Rule based regular expressions are applied to the '{PROP_COL_NAME}' property"
    )
    search_property = config.data.target_view.search_property
    view_id = config.data.target_view.as_view_id()
    for target in all_targets:
        properties = target.properties.get(view_id) if target.properties else None
        if not properties or PROP_COL_NAME not in properties:
            logger.warning(f"Target: {target.external_id} is missing properties or name, skipping")
            continue
        org_name = str(properties[PROP_COL_NAME])

        rule_keys = []
        if rule_mappings:
            for rule in rule_mappings:
                # Pattern was pre-compiled in read_rule_mappings (re.Pattern object).
                pattern = rule[COL_KEY_RULE_REGEXP_TARGET]
                match = pattern.search(org_name)

                if match:
                    # Concatenate the captured groups directly. An operator's regex may
                    # make a group optional, and one that does not participate in the
                    # match captures None, which cannot be joined.
                    matched_groups = [group for group in match.groups() if group is not None]
                    cleaned_value = rule[KEY_RULE] + "_" + "".join(matched_groups)
                    logger.debug(f"Cleaned value (using capture groups): {cleaned_value}")
                    rule_keys.append(cleaned_value)

        match_properties = match_values(properties, search_property, org_name)

        for match_property in match_properties:
            targets.append(
                {
                    KEY_TARGET_EXT_ID: target.external_id,
                    KEY_TARGET_SPACE: target.space,
                    KEY_ORG_NAME: org_name,
                    KEY_NAME: match_property,
                    KEY_RULE_KEYS: rule_keys if rule_keys else None,
                }
            )
    logger.debug(f"Number {QUERY_FILTER_TYPE_TARGETS} added as entities: {len(targets)}")

    return targets
