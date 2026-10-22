"""
Optimized Metadata Update Pipeline

This module provides optimized metadata update functionality for timeseries, assets and
files with improved performance, caching, batch processing, and error handling.
"""

import gc
import sys
import traceback
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRun
from cognite.client.data_classes.data_modeling import NodeApply
from cognite.client.utils._text import shorten

from alias_optimizations import (  # isort: skip
    AliasRule,
    BatchProcessor,
    OptimizedMetadataProcessor,
    time_operation,
)
from alias_sync import SyncState, iter_synced_pages, read_sync_state, sync_state_key, write_sync_state  # isort: skip
from config import Config, ViewPropertyConfig  # isort: skip
from constants import ASSET_NODE, DEFAULT_ALIAS_PATTERN, FILE_NODE, TS_NODE  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip

sys.path.append(str(Path(__file__).parent))


def effective_run_all(config: Config) -> bool:
    """Return whether to read every instance rather than only those changed since the last run."""
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
    return "incremental — instances changed since the last run"


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

    The sync cursor is stored after each page is written, so a run stopped part way - by
    the function time limit or a failure - leaves the next run to continue from the last
    written page. A full read (`runAll`, `updateAll`, `removeOldAliases`) is marked in
    progress until its last page, so an interrupted one is continued rather than started
    again. The memory the view used is released before the next view is read.

    Returns:
        The number of instances updated.
    """
    mode = describe_processing_mode(config)
    run_all = effective_run_all(config)
    logger.info(f"Starting {label} metadata — mode: {mode}")
    if view_config.filter_property and view_config.filter_values:
        logger.info(f"{label} filtered on {view_config.filter_property} in {view_config.filter_values}")

    state_key = sync_state_key(view_config, label)
    stored = read_sync_state(client, config.parameters, logger, state_key)
    if stored.full_read_in_progress and stored.cursor:
        logger.info(f"{label}: continuing the interrupted full read from the stored cursor")
        start_cursor = stored.cursor
    elif run_all:
        if stored.cursor:
            logger.info(f"{label}: reading every instance instead of syncing changes from the stored cursor")
        start_cursor = None
    else:
        start_cursor = stored.cursor
    start = SyncState(start_cursor, stored.batch_size)

    view_id = view_config.as_view_id()
    examined = 0
    total_updates = 0
    state = start
    for page, page_state in iter_synced_pages(client, logger, view_config, start, label):
        state = replace(page_state, full_read_in_progress=run_all)
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
        del page, updates
        write_sync_state(client, config.parameters, logger, state_key, view_config, state)

    if state.full_read_in_progress:
        write_sync_state(
            client, config.parameters, logger, state_key, view_config, replace(state, full_read_in_progress=False)
        )
    logger.info(f"{label}: stored sync cursor in {config.parameters.raw_db}.{config.parameters.raw_table_state}")
    OptimizedMetadataProcessor.clear_alias_caches()
    gc.collect()

    fetch_scope = "all instances in scope" if run_all else "instances changed since the last run"
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


# Export all functions for backward compatibility
__all__ = [
    "alias_rule",
    "describe_processing_mode",
    "effective_run_all",
    "metadata_update",
    "update_pipeline_run",
]
