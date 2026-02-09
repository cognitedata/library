#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local Script: Context Quality Metrics Collection.

This script runs locally on your machine to compute contextualization quality
metrics and upload them to CDF. It provides the same functionality as the
Cognite Function but without timeout limitations.

Advantages over Cognite Functions:
- No 15-minute timeout limit
- Uses local compute resources (more memory/CPU)
- Easier to debug with local breakpoints
- No deployment required for changes
- Progress bars for visibility

Usage:
    # Set environment variables (cognite-toolkit compatible)
    export CDF_PROJECT="your-project"
    export CDF_CLUSTER="aws-dub-dev"
    export IDP_CLIENT_ID="your-client-id"
    export IDP_CLIENT_SECRET="your-client-secret"
    export IDP_TENANT_ID="your-tenant-id"
    
    # Or use a .env file from cognite-toolkit
    # The script will read the same variables
    
    # Run with defaults
    python run_metrics.py
    
    # Run with custom config
    python run_metrics.py --max-ts 100000 --max-assets 50000
    
    # Dry run (don't upload results)
    python run_metrics.py --dry-run

Requirements:
    pip install -r requirements.txt
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add the metrics module to path
SCRIPT_DIR = Path(__file__).parent
METRICS_DIR = SCRIPT_DIR.parent / "functions" / "context_quality_handler"
sys.path.insert(0, str(METRICS_DIR))

# Load .env file if present (for cognite-toolkit compatibility)
try:
    from dotenv import load_dotenv
    # Try loading from multiple locations
    env_paths = [
        SCRIPT_DIR / ".env",                    # scripts/.env
        SCRIPT_DIR.parent.parent.parent.parent / ".env",  # templates/.env
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass  # python-dotenv not installed, use environment variables directly

from cognite.client.data_classes.data_modeling import ViewId

# Import from metrics modules
from metrics import (
    # Config and constants
    DEFAULT_CONFIG,
    LOG_EVERY_N_BATCHES,
    # Utilities
    format_elapsed,
    # Accumulator
    CombinedAccumulator,
    # Batch processors
    process_timeseries_batch,
    process_asset_batch,
    process_equipment_batch,
    process_notification_batch,
    process_maintenance_order_batch,
    process_failure_notification_batch,
    compute_historical_gaps_batch,
    # File annotation processors
    FileAnnotationAccumulator,
    process_annotation_batch,
    # 3D model processors
    Model3DAccumulator,
    process_asset_3d_batch,
    process_3d_object_batch,
    # File contextualization processors
    process_file_batch,
    # Metric computers
    compute_ts_metrics,
    compute_asset_hierarchy_metrics,
    compute_equipment_metrics,
    compute_maintenance_metrics,
    compute_file_annotation_metrics,
    compute_3d_metrics,
    compute_file_metrics,
    # Storage
    save_metrics_to_file,
)

from client import get_client

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None


# ----------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------
def setup_logging(verbose: bool = False):
    """Configure logging for local execution."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S',
    )
    
    # Reduce noise from HTTP libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("cognite.client").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ----------------------------------------------------
# PROGRESS WRAPPER
# ----------------------------------------------------
def progress_iterator(iterator, desc: str, total: int = None):
    """Wrap an iterator with a progress bar if tqdm is available."""
    if HAS_TQDM and total:
        return tqdm(iterator, desc=desc, total=total, unit="batch")
    return iterator


# ----------------------------------------------------
# CACHE MANAGEMENT
# ----------------------------------------------------
def load_cached_metrics(client, file_external_id: str) -> dict:
    """
    Load previously computed metrics from CDF.
    
    Args:
        client: Authenticated CogniteClient
        file_external_id: External ID of the metrics file
        
    Returns:
        Dictionary of cached metrics, or empty dict if not found
    """
    import tempfile
    
    try:
        # Check if file exists
        files = client.files.retrieve(external_id=file_external_id)
        if not files:
            logger.warning(f"No cached metrics found: {file_external_id}")
            return {}
        
        # Download to temp file
        temp_path = os.path.join(tempfile.gettempdir(), "cached_metrics.json")
        client.files.download(directory=tempfile.gettempdir(), external_id=file_external_id)
        
        # The downloaded file has the original name, find it
        downloaded_path = os.path.join(tempfile.gettempdir(), files.name)
        
        with open(downloaded_path, "r") as f:
            cached = json.load(f)
        
        # Clean up
        try:
            os.remove(downloaded_path)
        except Exception:
            pass
        
        logger.info(f"Loaded cached metrics from: {file_external_id}")
        logger.info(f"  Cached at: {cached.get('metadata', {}).get('computed_at', 'unknown')}")
        return cached
        
    except Exception as e:
        logger.warning(f"Could not load cached metrics: {e}")
        return {}


def merge_metrics(cached: dict, new_metrics: dict, recomputed_sections: set) -> dict:
    """
    Merge newly computed metrics with cached metrics.
    
    Only the sections listed in recomputed_sections will be taken from new_metrics,
    everything else comes from cached.
    
    Args:
        cached: Previously computed metrics
        new_metrics: Newly computed metrics
        recomputed_sections: Set of section names that were recomputed
            Options: 'ts', 'assets', 'equipment', 'maintenance', 'annotations', '3d', 'files'
    
    Returns:
        Merged metrics dictionary
    """
    if not cached:
        return new_metrics
    
    result = dict(cached)
    
    # Map section names to metric keys
    section_mapping = {
        'ts': ['timeseries_metrics'],
        'assets': ['hierarchy_metrics'],
        'equipment': ['equipment_metrics'],
        'maintenance': ['maintenance_metrics'],
        'annotations': ['file_annotation_metrics'],
        '3d': ['model_3d_metrics'],
        'files': ['file_metrics'],
    }
    
    # Instance count mappings
    instance_count_mapping = {
        'ts': 'timeseries',
        'assets': 'assets',
        'equipment': 'equipment',
        'maintenance': ['notifications', 'maintenance_orders', 'failure_notifications'],
        'annotations': 'annotations',
        '3d': '3d_objects',
        'files': 'files',
    }
    
    # Update metadata
    result['metadata'] = new_metrics.get('metadata', result.get('metadata', {}))
    result['metadata']['partial_recompute'] = True
    result['metadata']['recomputed_sections'] = list(recomputed_sections)
    
    # Merge instance counts
    if 'instance_counts' not in result['metadata']:
        result['metadata']['instance_counts'] = {}
    
    for section in recomputed_sections:
        # Update metric sections
        for key in section_mapping.get(section, []):
            if key in new_metrics:
                result[key] = new_metrics[key]
        
        # Update instance counts
        ic_keys = instance_count_mapping.get(section, [])
        if isinstance(ic_keys, str):
            ic_keys = [ic_keys]
        
        for ic_key in ic_keys:
            if ic_key in new_metrics.get('metadata', {}).get('instance_counts', {}):
                result['metadata']['instance_counts'][ic_key] = \
                    new_metrics['metadata']['instance_counts'][ic_key]
    
    return result


# Valid section names for --only flag
VALID_SECTIONS = {'ts', 'assets', 'equipment', 'maintenance', 'annotations', '3d', 'files'}


# ----------------------------------------------------
# MAIN PROCESSING FUNCTION
# ----------------------------------------------------
def run_metrics_collection(
    config: dict, 
    dry_run: bool = False,
    use_cache: bool = False,
    recompute_sections: set = None,
) -> dict:
    """
    Run the complete metrics collection process.
    
    Args:
        config: Configuration dictionary (merged with DEFAULT_CONFIG)
        dry_run: If True, don't upload results to CDF
        use_cache: If True, load previous results and only recompute specified sections
        recompute_sections: Set of sections to recompute (only used with use_cache)
            Valid values: 'ts', 'assets', 'equipment', 'maintenance', 'annotations', '3d', 'files'
            If None with use_cache=True, recomputes nothing (just validates cache)
        
    Returns:
        Dictionary containing all computed metrics
    """
    start_time = time.time()
    
    # Get authenticated client
    logger.info("Authenticating with CDF...")
    client = get_client()
    logger.info(f"Connected to project: {client.config.project}")
    
    # Handle caching
    cached_metrics = {}
    if use_cache:
        file_external_id = config.get("file_external_id", "contextualization_quality_metrics")
        cached_metrics = load_cached_metrics(client, file_external_id)
        if not cached_metrics:
            logger.warning("No cache found, running full computation")
            use_cache = False
            recompute_sections = None
    
    # Determine what to recompute
    if recompute_sections is None:
        recompute_sections = VALID_SECTIONS.copy()  # Recompute everything
    
    # Log what will be recomputed
    if use_cache and recompute_sections:
        logger.info(f"Partial recompute mode: {', '.join(sorted(recompute_sections))}")
        skipped = VALID_SECTIONS - recompute_sections
        if skipped:
            logger.info(f"Using cached data for: {', '.join(sorted(skipped))}")
    
    # Extract configuration
    chunk_size = config["chunk_size"]
    max_ts = config["max_timeseries"]
    max_assets = config["max_assets"]
    max_eq = config["max_equipment"]
    max_notif = config["max_notifications"]
    max_orders = config["max_maintenance_orders"]
    max_annotations = config["max_annotations"]
    max_3d_objects = config["max_3d_objects"]
    max_files = config["max_files"]
    freshness_days = config["freshness_days"]
    enable_gaps = config["enable_historical_gaps"]
    enable_maintenance = config["enable_maintenance_metrics"]
    enable_file_annotations = config["enable_file_annotation_metrics"]
    enable_3d = config["enable_3d_metrics"]
    enable_files = config["enable_file_metrics"]
    gap_sample_rate = config["gap_sample_rate"]
    gap_threshold_days = config["gap_threshold_days"]
    gap_lookback = config["gap_lookback"]
    file_external_id = config["file_external_id"]
    file_name = config["file_name"]
    
    logger.info("=" * 70)
    logger.info("CONTEXT QUALITY METRICS - LOCAL EXECUTION")
    logger.info("=" * 70)
    logger.info(f"Limits: TS={max_ts:,}, Assets={max_assets:,}, Equipment={max_eq:,}")
    if enable_maintenance:
        logger.info(f"Maintenance: Notifications={max_notif:,}, Orders={max_orders:,}")
    if enable_file_annotations:
        logger.info(f"Annotations: max={max_annotations:,}")
    if enable_3d:
        logger.info(f"3D Objects: max={max_3d_objects:,}")
    if enable_files:
        logger.info(f"Files: max={max_files:,}")
    
    # Build view IDs
    ts_view = ViewId(
        config["ts_view_space"],
        config["ts_view_external_id"],
        config["ts_view_version"]
    )
    asset_view = ViewId(
        config["asset_view_space"],
        config["asset_view_external_id"],
        config["asset_view_version"]
    )
    equipment_view = ViewId(
        config["equipment_view_space"],
        config["equipment_view_external_id"],
        config["equipment_view_version"]
    )
    notification_view = ViewId(
        config["notification_view_space"],
        config["notification_view_external_id"],
        config["notification_view_version"]
    )
    order_view = ViewId(
        config["maintenance_order_view_space"],
        config["maintenance_order_view_external_id"],
        config["maintenance_order_view_version"]
    )
    failure_notification_view = ViewId(
        config["failure_notification_view_space"],
        config["failure_notification_view_external_id"],
        config["failure_notification_view_version"]
    )
    annotation_view = ViewId(
        config["annotation_view_space"],
        config["annotation_view_external_id"],
        config["annotation_view_version"]
    )
    object_3d_view = ViewId(
        config["object3d_view_space"],
        config["object3d_view_external_id"],
        config["object3d_view_version"]
    )
    file_view = ViewId(
        config["file_view_space"],
        config["file_view_external_id"],
        config["file_view_version"]
    )
    
    # Initialize accumulators
    acc = CombinedAccumulator(freshness_days=freshness_days)
    annotation_acc = FileAnnotationAccumulator()
    model3d_acc = Model3DAccumulator()
    
    batch_counts = {
        "ts": 0, "assets": 0, "equipment": 0, 
        "notifications": 0, "orders": 0, "failure_notifications": 0,
        "annotations": 0, "3d_objects": 0, "files": 0
    }
    
    # ============================================================
    # PHASE 1: Process Time Series
    # ============================================================
    if 'ts' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 1: Processing Time Series")
        logger.info(f"  View: {ts_view.space}/{ts_view.external_id}/{ts_view.version}")
        logger.info("-" * 50)
        phase1_start = time.time()
        
        logger.info("[TS] Querying data model instances...")
        for ts_batch in client.data_modeling.instances(
            chunk_size=chunk_size,
            instance_type="node",
            sources=ts_view,
        ):
            batch_counts["ts"] += 1
            batch_size = len(ts_batch.data) if hasattr(ts_batch, 'data') else 0
            
            # Log first batch immediately to show progress
            if batch_counts["ts"] == 1:
                elapsed = time.time() - phase1_start
                logger.info(f"[TS] First batch received ({batch_size} items) after {elapsed:.1f}s")
            
            process_timeseries_batch(ts_batch, ts_view, acc)
            
            # Historical gap analysis
            if enable_gaps and (batch_counts["ts"] == 1 or batch_counts["ts"] % gap_sample_rate == 0):
                compute_historical_gaps_batch(
                    ts_batch, client, acc,
                    gap_threshold_days=gap_threshold_days,
                    lookback=gap_lookback
                )
            
            if batch_counts["ts"] % LOG_EVERY_N_BATCHES == 0:
                elapsed = time.time() - phase1_start
                logger.info(f"[TS] Batch {batch_counts['ts']:,} | Total: {acc.total_ts:,} | Elapsed: {format_elapsed(elapsed)}")
            
            if acc.total_ts >= max_ts:
                logger.info(f"[TS] Reached limit ({max_ts:,})")
                break
        
        if batch_counts["ts"] == 0:
            logger.warning("[TS] No time series found in view")
        
        logger.info(f"PHASE 1 Complete: {acc.total_ts:,} TS in {format_elapsed(time.time() - phase1_start)}")
    else:
        logger.info("[TS] Skipped - using cached data")
    
    # ============================================================
    # PHASE 2: Process Assets
    # ============================================================
    if 'assets' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 2: Processing Assets")
        logger.info(f"  View: {asset_view.space}/{asset_view.external_id}/{asset_view.version}")
        logger.info("-" * 50)
        phase2_start = time.time()
        
        logger.info("[Assets] Querying data model instances...")
        for asset_batch in client.data_modeling.instances(
            chunk_size=chunk_size,
            instance_type="node",
            sources=asset_view,
        ):
            batch_counts["assets"] += 1
            batch_size = len(asset_batch.data) if hasattr(asset_batch, 'data') else 0
            
            if batch_counts["assets"] == 1:
                elapsed = time.time() - phase2_start
                logger.info(f"[Assets] First batch received ({batch_size} items) after {elapsed:.1f}s")
            
            process_asset_batch(asset_batch, asset_view, acc)
            
            # Also process for 3D association if enabled
            if enable_3d and '3d' in recompute_sections:
                process_asset_3d_batch(asset_batch, asset_view, model3d_acc)
            
            if batch_counts["assets"] % LOG_EVERY_N_BATCHES == 0:
                elapsed = time.time() - phase2_start
                logger.info(f"[Assets] Batch {batch_counts['assets']:,} | Total: {acc.total_assets:,} | Elapsed: {format_elapsed(elapsed)}")
        
            if acc.total_assets >= max_assets:
                logger.info(f"[Assets] Reached limit ({max_assets:,})")
                break
        
        if batch_counts["assets"] == 0:
            logger.warning("[Assets] No assets found in view")
        
        logger.info(f"PHASE 2 Complete: {acc.total_assets:,} Assets in {format_elapsed(time.time() - phase2_start)}")
    else:
        logger.info("[Assets] Skipped - using cached data")
    
    # ============================================================
    # PHASE 3: Process Equipment
    # ============================================================
    if 'equipment' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 3: Processing Equipment")
        logger.info(f"  View: {equipment_view.space}/{equipment_view.external_id}/{equipment_view.version}")
        logger.info("-" * 50)
        phase3_start = time.time()
        
        logger.info("[Equipment] Querying data model instances...")
        for eq_batch in client.data_modeling.instances(
            chunk_size=chunk_size,
            instance_type="node",
            sources=equipment_view,
        ):
            batch_counts["equipment"] += 1
            batch_size = len(eq_batch.data) if hasattr(eq_batch, 'data') else 0
            
            if batch_counts["equipment"] == 1:
                elapsed = time.time() - phase3_start
                logger.info(f"[Equipment] First batch received ({batch_size} items) after {elapsed:.1f}s")
            
            process_equipment_batch(eq_batch, equipment_view, acc)
            
            if batch_counts["equipment"] % LOG_EVERY_N_BATCHES == 0:
                elapsed = time.time() - phase3_start
                logger.info(f"[Equipment] Batch {batch_counts['equipment']:,} | Total: {acc.total_equipment:,} | Elapsed: {format_elapsed(elapsed)}")
            
            if acc.total_equipment >= max_eq:
                logger.info(f"[Equipment] Reached limit ({max_eq:,})")
                break
        
        if batch_counts["equipment"] == 0:
            logger.warning("[Equipment] No equipment found in view")
        
        logger.info(f"PHASE 3 Complete: {acc.total_equipment:,} Equipment in {format_elapsed(time.time() - phase3_start)}")
    else:
        logger.info("[Equipment] Skipped - using cached data")
    
    # ============================================================
    # PHASE 4: Process Maintenance Data
    # ============================================================
    maintenance_metrics = {}
    
    if enable_maintenance and 'maintenance' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 4: Processing Maintenance Data")
        logger.info(f"  Notifications: {notification_view.space}/{notification_view.external_id}/{notification_view.version}")
        logger.info(f"  Orders: {order_view.space}/{order_view.external_id}/{order_view.version}")
        logger.info("-" * 50)
        phase4_start = time.time()
        
        # Notifications
        logger.info("[Notifications] Querying data model instances...")
        notif_start = time.time()
        try:
            for notif_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=notification_view,
            ):
                batch_counts["notifications"] += 1
                if batch_counts["notifications"] == 1:
                    logger.info(f"[Notifications] First batch received after {time.time() - notif_start:.1f}s")
                
                process_notification_batch(notif_batch, notification_view, acc)
                
                if batch_counts["notifications"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Notifications] Batch {batch_counts['notifications']:,} | Total: {acc.total_notifications:,}")
                
                if acc.total_notifications >= max_notif:
                    logger.info(f"[Notifications] Reached limit ({max_notif:,})")
                    break
        except Exception as e:
            logger.warning(f"[Notifications] Could not process: {e}")
        
        # Maintenance Orders
        logger.info("[Orders] Querying data model instances...")
        order_start = time.time()
        try:
            for order_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=order_view,
            ):
                batch_counts["orders"] += 1
                if batch_counts["orders"] == 1:
                    logger.info(f"[Orders] First batch received after {time.time() - order_start:.1f}s")
                
                process_maintenance_order_batch(order_batch, order_view, acc)
                
                if batch_counts["orders"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Orders] Batch {batch_counts['orders']:,} | Total: {acc.total_orders:,}")
                
                if acc.total_orders >= max_orders:
                    logger.info(f"[Orders] Reached limit ({max_orders:,})")
                    break
        except Exception as e:
            logger.warning(f"[Orders] Could not process: {e}")
        
        # Failure Notifications
        logger.info("[FailureNotif] Querying data model instances...")
        failure_start = time.time()
        try:
            for failure_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=failure_notification_view,
            ):
                batch_counts["failure_notifications"] += 1
                if batch_counts["failure_notifications"] == 1:
                    logger.info(f"[FailureNotif] First batch received after {time.time() - failure_start:.1f}s")
                
                process_failure_notification_batch(failure_batch, failure_notification_view, acc)
        except Exception as e:
            logger.warning(f"[FailureNotif] Could not process: {e}")
        
        logger.info(f"PHASE 4 Complete: Notifications={acc.total_notifications:,}, Orders={acc.total_orders:,}, Failures={acc.total_failure_notifications:,} in {format_elapsed(time.time() - phase4_start)}")
        
        # Compute maintenance metrics
        if acc.total_notifications > 0 or acc.total_orders > 0:
            maintenance_metrics = compute_maintenance_metrics(acc)
    elif not enable_maintenance:
        logger.info("[Maintenance] Skipped - disabled in config")
    else:
        logger.info("[Maintenance] Skipped - using cached data")
    
    # ============================================================
    # PHASE 5: Process File Annotations
    # ============================================================
    file_annotation_metrics = {}
    
    if enable_file_annotations and 'annotations' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 5: Processing File Annotations")
        logger.info(f"  View: {annotation_view.space}/{annotation_view.external_id}/{annotation_view.version}")
        logger.info("-" * 50)
        phase5_start = time.time()
        
        logger.info("[Annotations] Querying data model instances (edges)...")
        try:
            for annot_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="edge",
                sources=annotation_view,
            ):
                batch_counts["annotations"] += 1
                if batch_counts["annotations"] == 1:
                    logger.info(f"[Annotations] First batch received after {time.time() - phase5_start:.1f}s")
                
                process_annotation_batch(annot_batch, annotation_view, annotation_acc)
                
                if batch_counts["annotations"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Annotations] Batch {batch_counts['annotations']:,} | Total: {annotation_acc.unique_annotations:,}")
                
                if annotation_acc.unique_annotations >= max_annotations:
                    logger.info(f"[Annotations] Reached limit ({max_annotations:,})")
                    break
        except Exception as e:
            logger.warning(f"[Annotations] Could not process: {e}")
        
        logger.info(f"PHASE 5 Complete: {annotation_acc.unique_annotations:,} Annotations in {format_elapsed(time.time() - phase5_start)}")
        
        file_annotation_metrics = compute_file_annotation_metrics(annotation_acc)
    elif not enable_file_annotations:
        logger.info("[Annotations] Skipped - disabled in config")
    else:
        logger.info("[Annotations] Skipped - using cached data")
    
    # ============================================================
    # PHASE 6: Process 3D Objects
    # ============================================================
    model_3d_metrics = {}
    
    if enable_3d and '3d' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 6: Processing 3D Objects")
        logger.info(f"  View: {object_3d_view.space}/{object_3d_view.external_id}/{object_3d_view.version}")
        logger.info("-" * 50)
        phase6_start = time.time()
        
        logger.info("[3D Objects] Querying data model instances...")
        try:
            for obj_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=object_3d_view,
            ):
                batch_counts["3d_objects"] += 1
                if batch_counts["3d_objects"] == 1:
                    logger.info(f"[3D Objects] First batch received after {time.time() - phase6_start:.1f}s")
                
                process_3d_object_batch(obj_batch, object_3d_view, model3d_acc)
                
                if batch_counts["3d_objects"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[3D Objects] Batch {batch_counts['3d_objects']:,} | Total: {model3d_acc.total_3d_objects:,}")
                
                if model3d_acc.total_3d_objects >= max_3d_objects:
                    logger.info(f"[3D Objects] Reached limit ({max_3d_objects:,})")
                    break
        except Exception as e:
            logger.warning(f"[3D Objects] Could not process: {e}")
        
        logger.info(f"PHASE 6 Complete: {model3d_acc.total_3d_objects:,} 3D Objects in {format_elapsed(time.time() - phase6_start)}")
        
        model_3d_metrics = compute_3d_metrics(model3d_acc)
    elif not enable_3d:
        logger.info("[3D] Skipped - disabled in config")
    else:
        logger.info("[3D] Skipped - using cached data")
    
    # ============================================================
    # PHASE 7: Process Files
    # ============================================================
    file_metrics = {}
    
    if enable_files and 'files' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 7: Processing Files")
        logger.info(f"  View: {file_view.space}/{file_view.external_id}/{file_view.version}")
        logger.info("-" * 50)
        phase7_start = time.time()
        
        logger.info("[Files] Querying data model instances...")
        try:
            for file_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=file_view,
            ):
                batch_counts["files"] += 1
                if batch_counts["files"] == 1:
                    logger.info(f"[Files] First batch received after {time.time() - phase7_start:.1f}s")
                
                process_file_batch(file_batch, file_view, acc)
                
                if batch_counts["files"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Files] Batch {batch_counts['files']:,} | Total: {acc.total_files:,}")
                
                if acc.total_files >= max_files:
                    logger.info(f"[Files] Reached limit ({max_files:,})")
                    break
        except Exception as e:
            logger.warning(f"[Files] Could not process: {e}")
        
        logger.info(f"PHASE 7 Complete: {acc.total_files:,} Files in {format_elapsed(time.time() - phase7_start)}")
        
        file_metrics = compute_file_metrics(acc)
    elif not enable_files:
        logger.info("[Files] Skipped - disabled in config")
    else:
        logger.info("[Files] Skipped - using cached data")
    
    # ============================================================
    # PHASE 8: Compute All Metrics
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 8: Computing Final Metrics")
    logger.info("-" * 50)
    
    # Only compute metrics for sections that were recomputed
    ts_metrics = compute_ts_metrics(acc) if 'ts' in recompute_sections else {}
    hierarchy_metrics = compute_asset_hierarchy_metrics(acc) if 'assets' in recompute_sections else {}
    equipment_metrics = compute_equipment_metrics(acc) if 'equipment' in recompute_sections else {}
    
    total_elapsed = time.time() - start_time
    
    # ============================================================
    # PHASE 9: Compile and Save Results
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 9: Saving Results")
    logger.info("-" * 50)
    
    all_metrics = {
        "metadata": {
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "execution_time_seconds": round(total_elapsed, 2),
            "execution_mode": "local_script",
            "instance_counts": {
                "timeseries": {
                    "total_instances": acc.total_ts_instances,
                    "unique": acc.total_ts,
                    "duplicates": acc.ts_duplicates,
                    "duplicate_ids": acc.ts_duplicate_ids,
                },
                "assets": {
                    "total_instances": acc.total_asset_instances,
                    "unique": acc.total_assets,
                    "duplicates": acc.asset_duplicates,
                    "duplicate_ids": acc.asset_duplicate_ids,
                },
                "equipment": {
                    "total_instances": acc.total_equipment_instances,
                    "unique": acc.total_equipment,
                    "duplicates": acc.equipment_duplicates,
                    "duplicate_ids": acc.equipment_duplicate_ids,
                },
                "notifications": {
                    "total_instances": acc.total_notification_instances,
                    "unique": acc.total_notifications,
                    "duplicates": acc.notification_duplicates,
                    "duplicate_ids": acc.notification_duplicate_ids,
                },
                "maintenance_orders": {
                    "total_instances": acc.total_order_instances,
                    "unique": acc.total_orders,
                    "duplicates": acc.order_duplicates,
                    "duplicate_ids": acc.order_duplicate_ids,
                },
                "failure_notifications": {
                    "unique": acc.total_failure_notifications,
                },
                "annotations": {
                    "unique": annotation_acc.unique_annotations if enable_file_annotations else 0,
                },
                "3d_objects": {
                    "unique": model3d_acc.total_3d_objects if enable_3d else 0,
                    "assets_with_3d": model3d_acc.assets_with_3d if enable_3d else 0,
                },
                "files": {
                    "total_instances": acc.total_file_instances if enable_files else 0,
                    "unique": acc.total_files if enable_files else 0,
                    "duplicates": acc.file_duplicates if enable_files else 0,
                    "duplicate_ids": acc.file_duplicate_ids if enable_files else [],
                },
            },
            "limits_reached": {
                "timeseries": acc.total_ts >= max_ts,
                "assets": acc.total_assets >= max_assets,
                "equipment": acc.total_equipment >= max_eq,
                "notifications": acc.total_notifications >= max_notif if enable_maintenance else False,
                "maintenance_orders": acc.total_orders >= max_orders if enable_maintenance else False,
                "annotations": annotation_acc.unique_annotations >= max_annotations if enable_file_annotations else False,
                "3d_objects": model3d_acc.total_3d_objects >= max_3d_objects if enable_3d else False,
                "files": acc.total_files >= max_files if enable_files else False,
            },
            "config": config,
        },
        "timeseries_metrics": ts_metrics,
        "hierarchy_metrics": hierarchy_metrics,
        "equipment_metrics": equipment_metrics,
        "maintenance_metrics": maintenance_metrics,
        "file_annotation_metrics": file_annotation_metrics,
        "model_3d_metrics": model_3d_metrics,
        "file_metrics": file_metrics,
    }
    
    # Merge with cached metrics if using partial recompute
    if use_cache and cached_metrics:
        logger.info("Merging with cached metrics...")
        all_metrics = merge_metrics(cached_metrics, all_metrics, recompute_sections)
    
    # Save to CDF or local file
    if dry_run:
        # Save locally for inspection
        local_path = SCRIPT_DIR / "metrics_output.json"
        with open(local_path, "w") as f:
            json.dump(all_metrics, f, indent=2, default=str)
        logger.info(f"[DRY RUN] Saved metrics locally: {local_path}")
    else:
        save_metrics_to_file(client, all_metrics, file_external_id, file_name)
        logger.info(f"Saved metrics to CDF: {file_external_id}")
    
    # ============================================================
    # SUMMARY
    # ============================================================
    logger.info("=" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info("Instance Counts (Total / Unique / Duplicates):")
    logger.info(f"  Time Series:  {acc.total_ts_instances:,} / {acc.total_ts:,} / {acc.ts_duplicates:,}")
    logger.info(f"  Assets:       {acc.total_asset_instances:,} / {acc.total_assets:,} / {acc.asset_duplicates:,}")
    logger.info(f"  Equipment:    {acc.total_equipment_instances:,} / {acc.total_equipment:,} / {acc.equipment_duplicates:,}")
    if enable_maintenance:
        logger.info(f"  Notifications:{acc.total_notification_instances:,} / {acc.total_notifications:,} / {acc.notification_duplicates:,}")
        logger.info(f"  Orders:       {acc.total_order_instances:,} / {acc.total_orders:,} / {acc.order_duplicates:,}")
        logger.info(f"  FailureNotif: {acc.total_failure_notifications:,}")
    if enable_file_annotations:
        logger.info(f"  Annotations:  {annotation_acc.total_annotations:,} / {annotation_acc.unique_annotations:,}")
    if enable_3d:
        logger.info(f"  3D Objects:   {model3d_acc.total_3d_objects:,}")
    if enable_files:
        logger.info(f"  Files:        {acc.total_file_instances:,} / {acc.total_files:,} / {acc.file_duplicates:,}")
    logger.info("-" * 50)
    # Get metrics from either newly computed or merged result
    final_ts = all_metrics.get('timeseries_metrics', ts_metrics)
    final_hierarchy = all_metrics.get('hierarchy_metrics', hierarchy_metrics)
    final_equipment = all_metrics.get('equipment_metrics', equipment_metrics)
    if final_ts:
        logger.info(f"TS to Asset Rate:        {final_ts.get('ts_to_asset_rate', 'N/A')}%")
        logger.info(f"Asset Monitoring:        {final_ts.get('ts_asset_monitoring_coverage', 'N/A')}%")
    if final_hierarchy:
        logger.info(f"Hierarchy Completion:    {final_hierarchy.get('hierarchy_completion_rate', 'N/A')}%")
    if final_equipment:
        logger.info(f"Equipment Association:   {final_equipment.get('eq_association_rate', 'N/A')}%")
    logger.info("-" * 50)
    logger.info(f"Total execution time: {format_elapsed(total_elapsed)}")
    logger.info("=" * 70)
    
    return all_metrics


# ----------------------------------------------------
# CLI INTERFACE
# ----------------------------------------------------
def parse_view_string(view_str: str) -> tuple:
    """
    Parse a view string in format 'space/view_id/version'.
    
    Args:
        view_str: String like 'cdf_cdm/CogniteTimeSeries/v1'
        
    Returns:
        Tuple of (space, external_id, version)
        
    Raises:
        ValueError: If format is invalid
    """
    parts = view_str.split("/")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid view format: '{view_str}'. "
            f"Expected 'space/view_id/version' (e.g., 'cdf_cdm/CogniteTimeSeries/v1')"
        )
    return parts[0], parts[1], parts[2]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Context Quality Metrics collection locally.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with defaults
    python run_metrics.py
    
    # Custom limits
    python run_metrics.py --max-ts 100000 --max-assets 50000
    
    # Dry run (save locally, don't upload)
    python run_metrics.py --dry-run
    
    # Disable optional metrics
    python run_metrics.py --no-maintenance --no-3d
    
    # Partial recompute - only refresh assets metrics
    python run_metrics.py --use-cache --only assets
    
    # Recompute multiple sections, reuse rest from cache
    python run_metrics.py --use-cache --only assets --only equipment
    
    # Fast run (skip slow gap analysis)
    python run_metrics.py --no-gaps
    
    # Use custom views (for non-CDM data models)
    python run_metrics.py --ts-view "rmdm/YourOrgTimeSeries/v1" --asset-view "rmdm/YourOrgAsset/v1"
    
Environment Variables (cognite-toolkit compatible):
    CDF_PROJECT         CDF project name (required)
    CDF_CLUSTER         CDF cluster (e.g., aws-dub-dev)
    CDF_URL             Full CDF URL (optional)
    CDF_TOKEN           Direct bearer token (optional)
    IDP_CLIENT_ID       OAuth client ID
    IDP_CLIENT_SECRET   OAuth client secret
    IDP_TENANT_ID       Azure tenant ID
    IDP_TOKEN_URL       OAuth token URL (optional)
    IDP_SCOPES          OAuth scopes (optional)
        """
    )
    
    # Limits
    parser.add_argument("--max-ts", type=int, default=None,
                        help="Maximum time series to process")
    parser.add_argument("--max-assets", type=int, default=None,
                        help="Maximum assets to process")
    parser.add_argument("--max-equipment", type=int, default=None,
                        help="Maximum equipment to process")
    parser.add_argument("--max-notifications", type=int, default=None,
                        help="Maximum notifications to process")
    parser.add_argument("--max-orders", type=int, default=None,
                        help="Maximum maintenance orders to process")
    parser.add_argument("--max-annotations", type=int, default=None,
                        help="Maximum annotations to process")
    parser.add_argument("--max-3d", type=int, default=None,
                        help="Maximum 3D objects to process")
    parser.add_argument("--max-files", type=int, default=None,
                        help="Maximum files to process")
    
    # Feature toggles
    parser.add_argument("--no-maintenance", action="store_true",
                        help="Disable maintenance metrics")
    parser.add_argument("--no-annotations", action="store_true",
                        help="Disable file annotation metrics")
    parser.add_argument("--no-3d", action="store_true",
                        help="Disable 3D model metrics")
    parser.add_argument("--no-files", action="store_true",
                        help="Disable file contextualization metrics")
    parser.add_argument("--no-gaps", action="store_true",
                        help="Disable historical gap analysis")
    
    # View configuration overrides (format: space/view_id/version)
    parser.add_argument("--ts-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Time Series view (e.g., 'cdf_cdm/CogniteTimeSeries/v1')")
    parser.add_argument("--asset-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Asset view (e.g., 'cdf_cdm/CogniteAsset/v1')")
    parser.add_argument("--equipment-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Equipment view (e.g., 'cdf_cdm/CogniteEquipment/v1')")
    parser.add_argument("--file-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="File view (e.g., 'cdf_cdm/CogniteFile/v1')")
    parser.add_argument("--notification-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Notification view (e.g., 'rmdm/Notification/v1')")
    parser.add_argument("--order-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Maintenance Order view (e.g., 'rmdm/MaintenanceOrder/v1')")
    parser.add_argument("--annotation-view", type=str, metavar="SPACE/VIEW/VERSION",
                        help="Annotation view (e.g., 'cdf_cdm/CogniteDiagramAnnotation/v1')")
    parser.add_argument("--3d-view", type=str, dest="view_3d", metavar="SPACE/VIEW/VERSION",
                        help="3D Object view (e.g., 'cdf_cdm/Cognite3DObject/v1')")
    
    # Caching and partial recompute options
    parser.add_argument("--use-cache", action="store_true",
                        help="Load previous metrics and only recompute specified sections")
    parser.add_argument("--only", action="append", dest="only_sections",
                        choices=['ts', 'assets', 'equipment', 'maintenance', 'annotations', '3d', 'files'],
                        metavar="SECTION",
                        help="Only recompute specified section(s). Can be repeated. "
                             "Requires --use-cache. Options: ts, assets, equipment, "
                             "maintenance, annotations, 3d, files")
    
    # Output options
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't upload to CDF, save locally instead")
    parser.add_argument("--output-file", type=str, default=None,
                        help="Custom external ID for output file")
    
    # Verbosity
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose logging")
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    # Build config from defaults and CLI args
    config = dict(DEFAULT_CONFIG)
    
    # Apply CLI overrides
    if args.max_ts is not None:
        config["max_timeseries"] = args.max_ts
    if args.max_assets is not None:
        config["max_assets"] = args.max_assets
    if args.max_equipment is not None:
        config["max_equipment"] = args.max_equipment
    if args.max_notifications is not None:
        config["max_notifications"] = args.max_notifications
    if args.max_orders is not None:
        config["max_maintenance_orders"] = args.max_orders
    if args.max_annotations is not None:
        config["max_annotations"] = args.max_annotations
    if args.max_3d is not None:
        config["max_3d_objects"] = args.max_3d
    if args.max_files is not None:
        config["max_files"] = args.max_files
    
    # Feature toggles
    if args.no_maintenance:
        config["enable_maintenance_metrics"] = False
    if args.no_annotations:
        config["enable_file_annotation_metrics"] = False
    if args.no_3d:
        config["enable_3d_metrics"] = False
    if args.no_files:
        config["enable_file_metrics"] = False
    if args.no_gaps:
        config["enable_historical_gaps"] = False
    
    # View configuration overrides
    try:
        if args.ts_view:
            space, ext_id, version = parse_view_string(args.ts_view)
            config["ts_view_space"] = space
            config["ts_view_external_id"] = ext_id
            config["ts_view_version"] = version
            logger.info(f"Using custom TS view: {args.ts_view}")
        
        if args.asset_view:
            space, ext_id, version = parse_view_string(args.asset_view)
            config["asset_view_space"] = space
            config["asset_view_external_id"] = ext_id
            config["asset_view_version"] = version
            logger.info(f"Using custom Asset view: {args.asset_view}")
        
        if args.equipment_view:
            space, ext_id, version = parse_view_string(args.equipment_view)
            config["equipment_view_space"] = space
            config["equipment_view_external_id"] = ext_id
            config["equipment_view_version"] = version
            logger.info(f"Using custom Equipment view: {args.equipment_view}")
        
        if args.file_view:
            space, ext_id, version = parse_view_string(args.file_view)
            config["file_view_space"] = space
            config["file_view_external_id"] = ext_id
            config["file_view_version"] = version
            logger.info(f"Using custom File view: {args.file_view}")
        
        if args.notification_view:
            space, ext_id, version = parse_view_string(args.notification_view)
            config["notification_view_space"] = space
            config["notification_view_external_id"] = ext_id
            config["notification_view_version"] = version
            logger.info(f"Using custom Notification view: {args.notification_view}")
        
        if args.order_view:
            space, ext_id, version = parse_view_string(args.order_view)
            config["maintenance_order_view_space"] = space
            config["maintenance_order_view_external_id"] = ext_id
            config["maintenance_order_view_version"] = version
            logger.info(f"Using custom Order view: {args.order_view}")
        
        if args.annotation_view:
            space, ext_id, version = parse_view_string(args.annotation_view)
            config["annotation_view_space"] = space
            config["annotation_view_external_id"] = ext_id
            config["annotation_view_version"] = version
            logger.info(f"Using custom Annotation view: {args.annotation_view}")
        
        if args.view_3d:
            space, ext_id, version = parse_view_string(args.view_3d)
            config["object3d_view_space"] = space
            config["object3d_view_external_id"] = ext_id
            config["object3d_view_version"] = version
            logger.info(f"Using custom 3D view: {args.view_3d}")
            
    except ValueError as e:
        logger.error(str(e))
        return 1
    
    # Output file
    if args.output_file:
        config["file_external_id"] = args.output_file
        config["file_name"] = f"{args.output_file}.json"
    
    # Handle partial recompute
    use_cache = args.use_cache
    recompute_sections = None
    
    if args.only_sections:
        if not use_cache:
            logger.warning("--only requires --use-cache. Enabling cache mode.")
            use_cache = True
        recompute_sections = set(args.only_sections)
        logger.info(f"Partial recompute mode: will only recompute {recompute_sections}")
    
    # Run metrics collection
    try:
        result = run_metrics_collection(
            config, 
            dry_run=args.dry_run,
            use_cache=use_cache,
            recompute_sections=recompute_sections,
        )
        logger.info("Metrics collection completed successfully!")
        return 0
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
