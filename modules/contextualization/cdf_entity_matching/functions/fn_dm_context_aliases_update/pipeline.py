"""
Optimized Metadata Update Pipeline

This module provides optimized metadata update functionality for timeseries, assets and
files with improved performance, caching, batch processing, and error handling.
"""

import sys
import time
import traceback
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRun
from cognite.client.data_classes.data_modeling import Node, NodeApply
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import HasData
from cognite.client.utils._text import shorten

from alias_optimizations import (  # isort: skip
    AliasRule,
    BatchProcessor,
    OptimizedMetadataProcessor,
    is_retryable,
    time_operation,
)
from config import Config, ViewPropertyConfig  # isort: skip
from constants import (  # isort: skip
    ALIAS_PAGE_SIZE,
    ALIAS_SOURCE_PROPERTIES,
    ASSET_NODE,
    DEFAULT_ALIAS_PATTERN,
    FILE_NODE,
    ITEMS_QUERY_NAME,
    TS_NODE,
)
from logger import CogniteFunctionLogger  # isort: skip

sys.path.append(str(Path(__file__).parent))


def effective_run_all(config: Config) -> bool:
    """Return whether to fetch all instances (not only those missing aliases)."""
    return config.parameters.run_all or config.parameters.update_all or config.parameters.remove_old_aliases


def alias_rule(view: ViewPropertyConfig | None) -> AliasRule:
    """The alias rule a view configures.

    Args:
        view: The view configuration, or None when the view is not configured at all.

    Returns:
        The compiled rule, or the default one when there is no view to read it from.
    """
    if view is None:
        return AliasRule.from_config([DEFAULT_ALIAS_PATTERN])

    return AliasRule.from_config(view.alias_patterns, view.alias_selection)


def describe_processing_mode(config: Config) -> str:
    """Human-readable description of the configured fetch/update mode."""
    if config.parameters.update_all:
        return "updateAll — all instances, managed metadata reset before recompute"
    if config.parameters.remove_old_aliases:
        return "removeOldAliases — all instances, existing aliases cleared before recompute"
    if config.parameters.run_all:
        return "runAll — all instances, merge with existing metadata"
    return "incremental — instances without aliases only"


def metadata_update(client: CogniteClient, logger: CogniteFunctionLogger, data: dict[str, Any], config: Config) -> None:
    """Write aliases for timeseries, assets and files, reporting each pass to the extraction pipeline."""
    pipeline_ext_id = data["ExtractionPipelineExtId"]
    try:
        if config.parameters.update_all:
            logger.warning(
                "updateAll enabled — fetching all instances and resetting "
                "managed metadata properties before reprocessing"
            )
        elif config.parameters.remove_old_aliases:
            logger.warning(
                "removeOldAliases enabled — fetching all instances and replacing "
                "every alias with freshly produced values"
            )

        # Process configuration
        with time_operation("Configuration processing", logger):
            # Initialize processors. BatchProcessor keeps its own write batch size,
            # independent of the read page size.
            file_view = config.data.job.file_view
            metadata_processor = OptimizedMetadataProcessor(
                logger,
                alias_rule(config.data.job.timeseries_view),
                alias_rule(config.data.job.asset_view),
                alias_rule(file_view),
            )
            batch_processor = BatchProcessor()

        # Process timeseries
        with time_operation("Timeseries processing", logger):
            ts_updates = _process_timeseries_optimized(client, logger, config, metadata_processor, batch_processor)

            if ts_updates > 0:
                msg = (
                    f"Timeseries metadata finished — {ts_updates} instance(s) updated "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = f"Timeseries metadata finished — no updates required ({describe_processing_mode(config)})"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

        # Process assets
        with time_operation("Asset processing", logger):
            asset_updates = _process_assets_optimized(client, logger, config, metadata_processor, batch_processor)

            if asset_updates > 0:
                msg = (
                    f"Asset metadata finished — {asset_updates} instance(s) updated "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = f"Asset metadata finished — no updates required ({describe_processing_mode(config)})"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

        # Process files, when a fileView is configured
        if file_view:
            with time_operation("File processing", logger):
                file_updates = _process_files_optimized(client, logger, config, metadata_processor, batch_processor)

                if file_updates > 0:
                    msg = (
                        f"File metadata finished — {file_updates} instance(s) updated "
                        f"({describe_processing_mode(config)})"
                    )
                else:
                    msg = f"File metadata finished — no updates required ({describe_processing_mode(config)})"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
        else:
            logger.info("File metadata skipped — no fileView configured")

        # Log performance statistics
        processor_stats = metadata_processor.get_stats()
        logger.info(
            f"📊 Processing Stats: {processor_stats['processed']} processed, "
            f"{processor_stats['updated']} updated, "
            f"{processor_stats['update_rate']:.2%} update rate"
        )
        # Counted across all three passes, so this has to wait until they are all done.
        metadata_processor.log_missing_alias_summary()

    except Exception as e:
        msg = f"Aliases Update failed: {e!s}, traceback:\n{traceback.format_exc()}"
        logger.error(msg)
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", msg)
        raise


def _process_timeseries_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor,
) -> int:
    """Process timeseries metadata"""
    return _process_view(
        client,
        logger,
        config,
        config.data.job.timeseries_view,
        TS_NODE,
        metadata_processor.process_timeseries_metadata,
        batch_processor,
    )


def _process_assets_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor,
) -> int:
    """Process asset metadata"""
    return _process_view(
        client,
        logger,
        config,
        config.data.job.asset_view,
        ASSET_NODE,
        metadata_processor.process_asset_metadata,
        batch_processor,
    )


def _process_files_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor,
) -> int:
    """Process file metadata, when a fileView is configured"""
    file_view = config.data.job.file_view
    if file_view is None:
        logger.info("Files skipped — no fileView configured")
        return 0

    return _process_view(
        client,
        logger,
        config,
        file_view,
        FILE_NODE,
        metadata_processor.process_file_metadata,
        batch_processor,
    )


def _process_view(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    view_config: ViewPropertyConfig,
    label: str,
    process_node: Callable[..., NodeApply | None],
    batch_processor: BatchProcessor,
) -> int:
    """Generate aliases for the instances of one view, writing each page before reading the next.

    Returns:
        The number of instances updated.
    """
    mode = describe_processing_mode(config)
    run_all = effective_run_all(config)
    logger.info(f"Starting {label} metadata — mode: {mode}")

    view_id = view_config.as_view_id()
    examined = 0
    total_updates = 0
    for page in iter_new_items(client, logger, view_config, run_all, label):
        examined += len(page)
        updates = [
            update
            for node in page
            if (
                update := process_node(
                    node,
                    view_id,
                    node.space,
                    update_all=config.parameters.update_all,
                    remove_old_aliases=config.parameters.remove_old_aliases,
                )
            )
        ]
        if updates:
            total_updates += batch_processor.apply_updates_in_batches(client, updates, logger)

    fetch_scope = "all instances in scope" if run_all else "instances missing aliases"
    logger.info(f"{label} complete — {mode}: {examined} examined ({fetch_scope}), {total_updates} updated")
    return total_updates


def update_pipeline_run(
    client: CogniteClient, logger: CogniteFunctionLogger, xid: str, status: str, msg: str | None = None
) -> None:
    """
    Update extraction pipeline run status with enhanced error handling
    """

    try:
        if status == "success":
            logger.info(msg or "Success")
        else:
            logger.error(msg or "Error")

        # Truncate message to avoid API limits
        truncated_msg = shorten(msg, 1000) if msg else ""

        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(extpipe_external_id=xid, status=status, message=truncated_msg)
        )

    except Exception as e:
        logger.warning(f"Failed to update pipeline run: {e}")


def iter_new_items(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    view_config: ViewPropertyConfig,
    run_all: bool,
    label: str,
) -> Iterator[list[Node]]:
    """The instances to generate aliases for, one page at a time.

    Only the properties alias generation reads are selected. The caller writes a page
    before the next is read, so in incremental mode the instances it has just given
    aliases have already left the filter and the cursor carries on past them.

    Raises:
        Exception: A read that fails for good - not transient, or out of retries - so the
            run fails instead of reporting that there was nothing to update.
    """
    expression = NodeResultSetExpression(
        filter=dm.filters.And(
            dm.filters.In(["node", "space"], view_config.instance_spaces),
            get_alias_filter(view_config, logger, run_all),
        ),
        limit=ALIAS_PAGE_SIZE,
    )
    query = Query(
        with_={ITEMS_QUERY_NAME: expression},
        select={ITEMS_QUERY_NAME: Select([SourceSelector(view_config.as_view_id(), ALIAS_SOURCE_PROPERTIES)])},
    )
    while True:
        result = _query_with_retries(client, logger, query)
        page = list(result[ITEMS_QUERY_NAME])
        logger.debug(f"Read a page of {len(page)} {label} instances")
        if page:
            yield page
        cursor = result.cursors.get(ITEMS_QUERY_NAME)
        if not cursor or len(page) < ALIAS_PAGE_SIZE:
            return
        query.cursors = {ITEMS_QUERY_NAME: cursor}


def _query_with_retries(client: CogniteClient, logger: CogniteFunctionLogger, query: Query) -> QueryResult:
    """One query call, retried with an exponential backoff while the failure is transient."""
    max_attempts = 3
    retry_backoff_seconds = 2
    attempt = 0
    while True:
        try:
            return client.data_modeling.instances.query(query)
        # Deliberately broad: `is_retryable` decides what is worth another attempt, and
        # everything else is re-raised unchanged.
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts or not is_retryable(e):
                raise
            sleep_seconds = retry_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(f"Transient error (attempt {attempt}), sleeping {sleep_seconds}s before retry: {e}")
            time.sleep(sleep_seconds)


def get_alias_filter(
    view_config: ViewPropertyConfig,
    logger: CogniteFunctionLogger,
    run_all: bool,
) -> dm.filters.Filter:
    """Select instances of a view, or only those still missing aliases.

    Used for time series, assets and files alike, which are all fetched on nothing but
    the presence of aliases.
    """

    logger.debug(f"Creating alias filter for {view_config.external_id}")

    filters: list[dm.filters.Filter] = [HasData(views=[view_config.as_view_id()])]

    if not run_all:
        has_alias = dm.filters.Exists(view_config.as_property_ref("aliases"))
        not_alias = dm.filters.Not(has_alias)
        filters.append(not_alias)

    return dm.filters.And(*filters) if len(filters) > 1 else filters[0]


# Export all functions for backward compatibility
__all__ = [
    "alias_rule",
    "describe_processing_mode",
    "effective_run_all",
    "get_alias_filter",
    "iter_new_items",
    "metadata_update",
    "update_pipeline_run",
]
