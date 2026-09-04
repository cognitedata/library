"""
Optimized Metadata Update Pipeline

This module provides optimized metadata update functionality for timeseries, assets and
files with improved performance, caching, batch processing, and error handling.
"""

import sys
import time
import traceback
from pathlib import Path
from typing import Any

# Import optimizations
from alias_optimizations import (
    AliasRule,
    BatchProcessor,
    OptimizedMetadataProcessor,
    PerformanceBenchmark,
    cleanup_memory,
    is_retryable,
    monitor_memory_usage,
    optimize_metadata_processing,
    time_operation,
)
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import ExtractionPipelineRun
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
    ViewId,
)
from cognite.client.data_classes.filters import HasData
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import shorten
from config import Config, ViewPropertyConfig
from constants import (
    ASSET_NODE,
    BATCH_SIZE,
    DEFAULT_ALIAS_PATTERN,
    FILE_NODE,
    TS_NODE,
)
from logger import CogniteFunctionLogger

sys.path.append(str(Path(__file__).parent))


def effective_run_all(config: Config) -> bool:
    """Return whether to fetch all instances (not only those missing aliases)."""
    return config.parameters.run_all or config.parameters.update_all


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
    if config.parameters.run_all:
        return "runAll — all instances, merge with existing metadata"
    return "incremental — instances without aliases only"


def metadata_update(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config
) -> None:
    """
    Optimized main function for metadata update process.
    
    Includes performance optimizations, better error handling, and monitoring.
    """
    
    # Apply global optimizations
    optimize_metadata_processing()
    
    # Initialize performance monitoring
    benchmark = PerformanceBenchmark(logger)
    
    pipeline_ext_id = data["ExtractionPipelineExtId"]
    try:
        if config.parameters.update_all:
            logger.warning(
                "updateAll enabled — fetching all instances and resetting "
                "managed metadata properties before reprocessing"
            )

        # Monitor initial memory usage
        monitor_memory_usage(logger, "Pipeline start")
        
        # Process configuration
        with time_operation("Configuration processing", logger):
            # Initialize processors. BATCH_SIZE is the instance fetch limit, not an
            # apply batch size, so BatchProcessor keeps its own default.
            file_view = config.data.job.file_view
            metadata_processor = OptimizedMetadataProcessor(
                logger,
                alias_rule(config.data.job.timeseries_view),
                alias_rule(config.data.job.asset_view),
                alias_rule(file_view),
            )
            if config.parameters.debug:
                logger.debug("Debug mode enabled - processing limited data")
                batch_processor = BatchProcessor(batch_size=100)
            else:
                batch_processor = BatchProcessor()
        
        # Process timeseries
        with time_operation("Timeseries processing", logger):
            ts_updates = benchmark.benchmark_function(
                "Process timeseries metadata",
                _process_timeseries_optimized,
                client, logger, config, metadata_processor, batch_processor
            )
            
            if ts_updates > 0:
                msg = (
                    f"Timeseries metadata finished — {ts_updates} instance(s) updated "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = (
                    f"Timeseries metadata finished — no updates required "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
        
        # Process assets
        with time_operation("Asset processing", logger):
            asset_updates = benchmark.benchmark_function(
                "Process asset metadata",
                _process_assets_optimized,
                client, logger, config, metadata_processor, batch_processor
            )
            
            if asset_updates > 0:
                msg = (
                    f"Asset metadata finished — {asset_updates} instance(s) updated "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = (
                    f"Asset metadata finished — no updates required "
                    f"({describe_processing_mode(config)})"
                )
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

        # Process files, when a fileView is configured
        if file_view:
            with time_operation("File processing", logger):
                file_updates = benchmark.benchmark_function(
                    "Process file metadata",
                    _process_files_optimized,
                    client, logger, config, metadata_processor, batch_processor
                )

                if file_updates > 0:
                    msg = (
                        f"File metadata finished — {file_updates} instance(s) updated "
                        f"({describe_processing_mode(config)})"
                    )
                else:
                    msg = (
                        f"File metadata finished — no updates required "
                        f"({describe_processing_mode(config)})"
                    )
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
        
        # Final cleanup and monitoring
        cleanup_memory()
        monitor_memory_usage(logger, "Pipeline end")
        benchmark.log_summary()
        
    except Exception as e:
        msg = f"Optimized metadata update failed: {e!s}, traceback:\n{traceback.format_exc()}"
        logger.error(msg)
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", msg)
        raise


def _process_timeseries_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor
) -> int:
    """Process timeseries metadata with optimizations"""

    total_updates = 0
    mode = describe_processing_mode(config)
    run_all = effective_run_all(config)

    logger.info(f"Starting timeseries metadata — mode: {mode}")

    ts_view_id = config.data.job.timeseries_view.as_view_id()

    # BATCH_SIZE is -1, so a single call returns every instance in scope.
    with time_operation("Fetch timeseries", logger):
        new_timeseries = get_new_items(client, logger, ts_view_id, config, TS_NODE)

    if not new_timeseries:
        logger.info("Timeseries complete — no instances returned")
        return total_updates

    batch_count = len(new_timeseries)
    fetch_scope = "all instances in scope" if run_all else "instances missing aliases"
    logger.info(f"Timeseries: fetched {batch_count} instances ({fetch_scope})")

    with time_operation(f"Process {batch_count} timeseries", logger):
        updates = []

        for node in new_timeseries:
            update = metadata_processor.process_timeseries_metadata(
                node,
                ts_view_id,
                node.space,
                update_all=config.parameters.update_all,
            )
            if update:
                updates.append(update)

        if updates:
            total_updates = batch_processor.apply_updates_in_batches(
                client, updates, logger
            )
            logger.info(
                f"Timeseries: applied {total_updates} updates "
                f"({len(updates)} of {batch_count} examined instances changed)"
            )
        else:
            logger.info(
                f"Timeseries: no metadata changes needed ({batch_count} instances examined)"
            )

    cleanup_memory()

    logger.info(
        f"Timeseries complete — {mode}: {batch_count} examined, {total_updates} updated"
    )

    return total_updates


def _process_assets_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor
) -> int:
    """Process asset metadata with optimizations"""

    total_updates = 0
    mode = describe_processing_mode(config)
    run_all = effective_run_all(config)

    logger.info(f"Starting asset metadata — mode: {mode}")

    asset_view_id = config.data.job.asset_view.as_view_id()

    # BATCH_SIZE is -1, so a single call returns every instance in scope.
    with time_operation("Fetch assets", logger):
        new_assets = get_new_items(client, logger, asset_view_id, config, ASSET_NODE)

    if not new_assets:
        logger.info("Assets complete — no instances returned")
        return total_updates

    batch_count = len(new_assets)
    fetch_scope = "all instances in scope" if run_all else "instances missing aliases"
    logger.info(f"Assets: fetched {batch_count} instances ({fetch_scope})")

    with time_operation(f"Process {batch_count} assets", logger):
        updates = []

        for node in new_assets:
            update = metadata_processor.process_asset_metadata(
                node,
                asset_view_id,
                node.space,
                update_all=config.parameters.update_all,
            )
            if update:
                updates.append(update)

        if updates:
            total_updates = batch_processor.apply_updates_in_batches(
                client, updates, logger
            )
            logger.info(
                f"Assets: applied {total_updates} updates "
                f"({len(updates)} of {batch_count} examined instances changed)"
            )
        else:
            logger.info(
                f"Assets: no metadata changes needed ({batch_count} instances examined)"
            )

    cleanup_memory()

    logger.info(
        f"Assets complete — {mode}: {batch_count} examined, {total_updates} updated"
    )

    return total_updates


def _process_files_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor
) -> int:
    """Process file metadata with optimizations"""

    file_view = config.data.job.file_view
    if file_view is None:
        logger.info("Files skipped — no fileView configured")
        return 0

    total_updates = 0
    mode = describe_processing_mode(config)
    run_all = effective_run_all(config)

    logger.info(f"Starting file metadata — mode: {mode}")

    file_view_id = file_view.as_view_id()

    # BATCH_SIZE is -1, so a single call returns every instance in scope.
    with time_operation("Fetch files", logger):
        new_files = get_new_items(client, logger, file_view_id, config, FILE_NODE)

    if not new_files:
        logger.info("Files complete — no instances returned")
        return total_updates

    batch_count = len(new_files)
    fetch_scope = "all instances in scope" if run_all else "instances missing aliases"
    logger.info(f"Files: fetched {batch_count} instances ({fetch_scope})")

    with time_operation(f"Process {batch_count} files", logger):
        updates = []

        for node in new_files:
            update = metadata_processor.process_file_metadata(
                node,
                file_view_id,
                node.space,
                update_all=config.parameters.update_all,
            )
            if update:
                updates.append(update)

        if updates:
            total_updates = batch_processor.apply_updates_in_batches(
                client, updates, logger
            )
            logger.info(
                f"Files: applied {total_updates} updates "
                f"({len(updates)} of {batch_count} examined instances changed)"
            )
        else:
            logger.info(
                f"Files: no metadata changes needed ({batch_count} instances examined)"
            )

    cleanup_memory()

    logger.info(
        f"Files complete — {mode}: {batch_count} examined, {total_updates} updated"
    )

    return total_updates


def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    msg: str | None = None
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
            ExtractionPipelineRun(
                extpipe_external_id=xid,
                status=status,
                message=truncated_msg
            )
        )
        
    except Exception as e:
        logger.warning(f"Failed to update pipeline run: {e}")



def get_new_items(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    view_id: ViewId,
    config: Config,
    node_type: str,
) -> NodeList[Node] | None:
    """
    Get new items with enhanced error handling and retry logic
    """
    
    try:
        logger.debug(f"Getting new {node_type} from view: {view_id} ")
        
        # Set the filter for the query
        if node_type == TS_NODE:
            view_config = config.data.job.timeseries_view
            filter_query = get_alias_filter(view_config, logger, effective_run_all(config))
        elif node_type == FILE_NODE:
            view_config = config.data.job.file_view
            if view_config is None:
                raise ValueError("Cannot fetch files without a fileView in the configuration")
            filter_query = get_alias_filter(view_config, logger, effective_run_all(config))
        else:  # ASSET_NODE
            view_config = config.data.job.asset_view
            filter_query = get_alias_filter(view_config, logger, effective_run_all(config))
        
        # Query with retry logic
        max_retries = 3
        retry_backoff_seconds = 2
        for attempt in range(max_retries):
            try:
                result = client.data_modeling.instances.list(
                    instance_type="node",
                    space=view_config.instance_spaces,
                    sources=[view_id],
                    filter=filter_query,
                    limit=BATCH_SIZE
                )
                
                logger.debug(f"Query returned {len(result)} {node_type} instances")
                return result
                
            except CogniteAPIError as e:
                if is_retryable(e) and attempt < max_retries - 1:
                    # Rate limiting is the main reason to be here, and retrying it
                    # immediately only spends another request on the same limit.
                    sleep_seconds = retry_backoff_seconds * (2 ** attempt)
                    logger.warning(
                        f"Transient API error (attempt {attempt + 1}), sleeping "
                        f"{sleep_seconds}s before retry: {e}"
                    )
                    time.sleep(sleep_seconds)
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to get new items: {e}")
        return None


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
    'alias_rule',
    'describe_processing_mode',
    'effective_run_all',
    'get_alias_filter',
    'get_new_items',
    'metadata_update',
    'update_pipeline_run'
]