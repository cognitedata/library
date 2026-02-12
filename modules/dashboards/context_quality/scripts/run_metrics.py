#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local Script: Context Quality Metrics Collection.

This script runs locally on your machine to compute contextualization quality
metrics and upload them to CDF. It provides the same functionality as the
Cognite Function but without timeout limitations.

All dashboard tabs are treated equally - use --only to selectively recompute.

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
    
    # Full recompute - all tabs/sections
    python run_metrics.py
    
    # Selective recompute - only specific tabs (others preserved from cache)
    python run_metrics.py --only staging
    python run_metrics.py --only ts --only assets
    
    # Custom limits
    python run_metrics.py --max-ts 100000 --max-assets 50000
    
    # Reduce chunk size if maintenance (or other views) hit graph query timeout
    python run_metrics.py --only maintenance_idi --chunk-size 200
    
    # Dry run (don't upload results)
    python run_metrics.py --dry-run

Available sections for --only:
    ts, assets, equipment, maintenance_idi, annotations, 3d, files, others, staging

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
    # SCRIPT_DIR = templates/modules/solutions/context_quality/scripts/
    env_paths = [
        SCRIPT_DIR / ".env",                              # scripts/.env
        SCRIPT_DIR.parent.parent.parent.parent.parent / ".env",  # templates/.env (root)
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
    compute_file_annotation_metrics,
    compute_3d_metrics,
    compute_file_metrics,
    # Maintenance IDI metrics
    collect_maintenance_idi_metrics,
    compute_maintenance_idi_metrics,
    # Others metrics
    collect_others_metrics,
    compute_others_metrics,
    # Staging metrics
    compute_staging_metrics,
    DEFAULT_STAGING_CONFIG,
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
            Options: 'ts', 'assets', 'equipment', 'maintenance', 'annotations', '3d', 'files', 'staging'
    
    Returns:
        Merged metrics dictionary
    """
    if not cached:
        return new_metrics
    
    result = dict(cached)
    
    # Map section names to metric keys - all sections treated equally
    section_mapping = {
        'ts': ['timeseries_metrics'],
        'assets': ['hierarchy_metrics'],
        'equipment': ['equipment_metrics'],
        'maintenance': ['maintenance_metrics'],
        'maintenance_idi': ['maintenance_idi_metrics'],
        'annotations': ['file_annotation_metrics'],
        '3d': ['model3d_metrics'],
        'files': ['file_metrics'],
        'others': ['others_metrics'],
        'activities': [],  # Activity metrics are included in equipment_metrics
        'staging': ['staging_metrics'],
    }
    
    # Instance count mappings
    instance_count_mapping = {
        'ts': 'timeseries',
        'assets': 'assets',
        'equipment': 'equipment',
        'maintenance': ['notifications', 'maintenance_orders', 'failure_notifications'],
        'maintenance_idi': 'maintenance_idi',
        'annotations': 'annotations',
        '3d': '3d_objects',
        'files': 'files',
        'others': 'others',
        'activities': 'activities',
        'staging': 'staging',
    }
    
    # Preserve cached instance counts before updating metadata
    cached_instance_counts = result.get('metadata', {}).get('instance_counts', {}).copy()
    
    # Update metadata (but preserve instance counts)
    new_metadata = new_metrics.get('metadata', {})
    result['metadata'] = {
        **result.get('metadata', {}),  # Keep cached metadata as base
        'computed_at': new_metadata.get('computed_at', result.get('metadata', {}).get('computed_at')),
        'execution_time_seconds': new_metadata.get('execution_time_seconds', 0),
        'partial_recompute': True,
        'recomputed_sections': list(recomputed_sections),
        'config': new_metadata.get('config', result.get('metadata', {}).get('config', {})),
    }
    
    # Restore cached instance counts, then selectively update
    result['metadata']['instance_counts'] = cached_instance_counts
    
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
# All sections are treated equally - no special enable/disable logic
# Note: 'maintenance' uses IDI views only (maintenance_idi)
VALID_SECTIONS = {'ts', 'assets', 'equipment', 'maintenance_idi', 'annotations', '3d', 'files', 'others', 'activities', 'staging'}


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
    
    All sections are treated equally. By default, all sections are computed.
    When using selective recompute (--only), only specified sections are 
    computed and others are preserved from cached metrics.
    
    Args:
        config: Configuration dictionary (merged with DEFAULT_CONFIG)
        dry_run: If True, don't upload results to CDF
        use_cache: If True, load previous results and merge with new computations
        recompute_sections: Set of sections to recompute
            Valid values: 'ts', 'assets', 'equipment', 'maintenance', 
                         'annotations', '3d', 'files', 'staging'
            If None, all sections are computed (full recompute)
        
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
    freshness_days = config["freshness_days"]
    file_external_id = config["file_external_id"]
    file_name = config["file_name"]
    
    # Local runs have no limits by default (that's the whole point of running locally)
    # Use config values only if explicitly set via CLI, otherwise unlimited
    max_ts = config.get("max_timeseries") or float('inf')
    max_assets = config.get("max_assets") or float('inf')
    max_eq = config.get("max_equipment") or float('inf')
    max_annotations = config.get("max_annotations") or float('inf')
    max_3d_objects = config.get("max_3d_objects") or float('inf')
    max_files = config.get("max_files") or float('inf')
    
    logger.info("=" * 70)
    logger.info("CONTEXT QUALITY METRICS - LOCAL EXECUTION")
    logger.info("=" * 70)
    
    # Log what sections will be computed
    if recompute_sections == VALID_SECTIONS:
        logger.info("Mode: Full recompute (all sections)")
    else:
        logger.info(f"Mode: Selective recompute ({', '.join(sorted(recompute_sections))})")
    
    logger.info("Limits: None (local run processes all data)")
    if 'staging' in recompute_sections:
        logger.info(f"Staging: db={config.get('staging_raw_database', 'oracle:db')}, space={config.get('staging_dm_space', 'rmdm')}")
    
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
            
            if batch_counts["ts"] % LOG_EVERY_N_BATCHES == 0:
                elapsed = time.time() - phase1_start
                logger.info(f"[TS] Batch {batch_counts['ts']:,} | Total: {acc.total_ts:,} | Elapsed: {format_elapsed(elapsed)}")
            
            if max_ts and acc.total_ts >= max_ts:
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
            
            # Also process for 3D association if included
            if '3d' in recompute_sections:
                process_asset_3d_batch(asset_batch, asset_view, model3d_acc)
            
            if batch_counts["assets"] % LOG_EVERY_N_BATCHES == 0:
                elapsed = time.time() - phase2_start
                logger.info(f"[Assets] Batch {batch_counts['assets']:,} | Total: {acc.total_assets:,} | Elapsed: {format_elapsed(elapsed)}")
        
            if max_assets and acc.total_assets >= max_assets:
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
            
            if max_eq and acc.total_equipment >= max_eq:
                logger.info(f"[Equipment] Reached limit ({max_eq:,})")
                break
        
        if batch_counts["equipment"] == 0:
            logger.warning("[Equipment] No equipment found in view")
        
        logger.info(f"PHASE 3 Complete: {acc.total_equipment:,} Equipment in {format_elapsed(time.time() - phase3_start)}")
    else:
        logger.info("[Equipment] Skipped - using cached data")
    
    # ============================================================
    # PHASE 4: Process File Annotations (Maintenance uses IDI views)
    # ============================================================
    file_annotation_metrics = {}
    
    if 'annotations' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 4: Processing File Annotations")
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
                
                if max_annotations and annotation_acc.unique_annotations >= max_annotations:
                    logger.info(f"[Annotations] Reached limit ({max_annotations:,})")
                    break
        except Exception as e:
            logger.warning(f"[Annotations] Could not process: {e}")
        
        logger.info(f"PHASE 5 Complete: {annotation_acc.unique_annotations:,} Annotations in {format_elapsed(time.time() - phase5_start)}")
        
        file_annotation_metrics = compute_file_annotation_metrics(annotation_acc)
    else:
        logger.info("[Annotations] Skipped - using cached data")
    
    # ============================================================
    # PHASE 5: Process 3D Objects
    # ============================================================
    model_3d_metrics = {}
    
    if '3d' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 5: Processing 3D Objects")
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
                
                if max_3d_objects and model3d_acc.total_3d_objects >= max_3d_objects:
                    logger.info(f"[3D Objects] Reached limit ({max_3d_objects:,})")
                    break
        except Exception as e:
            logger.warning(f"[3D Objects] Could not process: {e}")
        
        logger.info(f"PHASE 6 Complete: {model3d_acc.total_3d_objects:,} 3D Objects in {format_elapsed(time.time() - phase6_start)}")
        
        model_3d_metrics = compute_3d_metrics(model3d_acc)
    else:
        logger.info("[3D] Skipped - using cached data")
    
    # ============================================================
    # PHASE 6: Process Files
    # ============================================================
    file_metrics = {}
    
    if 'files' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 6: Processing Files")
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
                
                if max_files and acc.total_files >= max_files:
                    logger.info(f"[Files] Reached limit ({max_files:,})")
                    break
        except Exception as e:
            logger.warning(f"[Files] Could not process: {e}")
        
        logger.info(f"PHASE 7 Complete: {acc.total_files:,} Files in {format_elapsed(time.time() - phase7_start)}")
        
        file_metrics = compute_file_metrics(acc)
    else:
        logger.info("[Files] Skipped - using cached data")
    
    # ============================================================
    # PHASE 7: Staging vs DM Comparison
    # ============================================================
    staging_metrics = {}
    
    if 'staging' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 7: Computing Staging vs DM Comparison")
        logger.info("-" * 50)
        
        phase8_start = time.time()
        
        try:
            staging_config = {
                "staging_raw_database": config.get("staging_raw_database", "oracle:db"),
                "staging_dm_space": config.get("staging_dm_space", "rmdm"),
                "staging_dm_version": config.get("staging_dm_version", "v1"),
            }
            staging_metrics = compute_staging_metrics(client, staging_config)
            logger.info(f"[Staging] Matched: {staging_metrics.get('staging_views_matched', 0)}, "
                       f"Gaps: {staging_metrics.get('staging_views_with_gaps', 0)}, "
                       f"Overall: {staging_metrics.get('staging_overall_match_rate', 0)}%")
        except Exception as e:
            logger.warning(f"[Staging] Could not compute staging metrics: {e}")
            staging_metrics = {"staging_has_data": False, "error": str(e)}
        
        logger.info(f"PHASE 8 Complete: Staging comparison in {format_elapsed(time.time() - phase8_start)}")
    else:
        logger.info("[Staging] Skipped - using cached data")
    
    # ============================================================
    # PHASE 8: Maintenance IDI Metrics
    # ============================================================
    maintenance_idi_metrics = {}
    
    if 'maintenance_idi' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 9: Computing Maintenance IDI Metrics")
        logger.info("-" * 50)
        
        phase_maint_start = time.time()
        
        try:
            maint_idi_config = {
                "maintenance_idi_dm_space": config.get("maintenance_idi_dm_space", "rmdm"),
                "maintenance_idi_dm_version": config.get("maintenance_idi_dm_version", "v1"),
                "max_maintenance_idi_instances": config.get("max_maintenance_idi_instances"),
                "chunk_size": chunk_size,
            }
            maint_idi_acc = collect_maintenance_idi_metrics(client, maint_idi_config)
            maintenance_idi_metrics = compute_maintenance_idi_metrics(maint_idi_acc, acc.total_assets)
            logger.info(f"[MaintenanceIDI] Views with data: {maintenance_idi_metrics.get('maint_idi_views_with_data', 0)}, "
                       f"Total instances: {maintenance_idi_metrics.get('maint_idi_total_instances', 0):,}")
        except Exception as e:
            logger.warning(f"[MaintenanceIDI] Could not compute metrics: {e}")
            maintenance_idi_metrics = {"maint_idi_has_data": False, "error": str(e)}
        
        logger.info(f"PHASE 9 Complete: Maintenance IDI in {format_elapsed(time.time() - phase_maint_start)}")
    else:
        logger.info("[MaintenanceIDI] Skipped - using cached data")
    
    # ============================================================
    # PHASE 9: Others IDI Metrics
    # ============================================================
    others_metrics = {}
    
    if 'others' in recompute_sections:
        logger.info("-" * 50)
        logger.info("PHASE 10: Computing Others IDI Metrics")
        logger.info("-" * 50)
        
        phase_others_start = time.time()
        
        try:
            others_config = {
                "others_dm_space": config.get("others_dm_space", "rmdm"),
                "others_dm_version": config.get("others_dm_version", "v1"),
                "max_others_instances": config.get("max_others_instances"),
                "chunk_size": chunk_size,
            }
            others_acc = collect_others_metrics(client, others_config)
            others_metrics = compute_others_metrics(others_acc)
            logger.info(f"[Others] Views with data: {others_metrics.get('others_views_with_data', 0)}, "
                       f"Total instances: {others_metrics.get('others_total_instances', 0):,}")
        except Exception as e:
            logger.warning(f"[Others] Could not compute metrics: {e}")
            others_metrics = {"others_has_data": False, "error": str(e)}
        
        logger.info(f"PHASE 10 Complete: Others IDI in {format_elapsed(time.time() - phase_others_start)}")
    else:
        logger.info("[Others] Skipped - using cached data")
    
    # ============================================================
    # PHASE 10: Compute All Metrics
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 10: Computing Final Metrics")
    logger.info("-" * 50)
    
    # Only compute metrics for sections that were recomputed
    ts_metrics = compute_ts_metrics(acc) if 'ts' in recompute_sections else {}
    hierarchy_metrics = compute_asset_hierarchy_metrics(acc) if 'assets' in recompute_sections else {}
    equipment_metrics = compute_equipment_metrics(acc) if 'equipment' in recompute_sections else {}
    
    total_elapsed = time.time() - start_time
    
    # ============================================================
    # PHASE 11: Compile and Save Results
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 11: Saving Results")
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
                "annotations": {
                    "unique": annotation_acc.unique_annotations if 'annotations' in recompute_sections else 0,
                },
                "3d_objects": {
                    "unique": model3d_acc.total_3d_objects if '3d' in recompute_sections else 0,
                    "assets_with_3d": model3d_acc.assets_with_3d if '3d' in recompute_sections else 0,
                },
                "files": {
                    "total_instances": acc.total_file_instances if 'files' in recompute_sections else 0,
                    "unique": acc.total_files if 'files' in recompute_sections else 0,
                    "duplicates": acc.file_duplicates if 'files' in recompute_sections else 0,
                    "duplicate_ids": acc.file_duplicate_ids if 'files' in recompute_sections else [],
                },
                "staging": {
                    "computed": 'staging' in recompute_sections,
                },
            },
            "limits_reached": {
                "timeseries": (max_ts and acc.total_ts >= max_ts) or False,
                "assets": (max_assets and acc.total_assets >= max_assets) or False,
                "equipment": (max_eq and acc.total_equipment >= max_eq) or False,
                "annotations": (max_annotations and annotation_acc.unique_annotations >= max_annotations) if 'annotations' in recompute_sections else False,
                "3d_objects": (max_3d_objects and model3d_acc.total_3d_objects >= max_3d_objects) if '3d' in recompute_sections else False,
                "files": (max_files and acc.total_files >= max_files) if 'files' in recompute_sections else False,
            },
            "config": config,
        },
        "timeseries_metrics": ts_metrics,
        "hierarchy_metrics": hierarchy_metrics,
        "equipment_metrics": equipment_metrics,
        "file_annotation_metrics": file_annotation_metrics,
        "model3d_metrics": model_3d_metrics,
        "file_metrics": file_metrics,
        "staging_metrics": staging_metrics,
        "maintenance_idi_metrics": maintenance_idi_metrics,
        "others_metrics": others_metrics,
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
    if 'ts' in recompute_sections:
        logger.info(f"  Time Series:  {acc.total_ts_instances:,} / {acc.total_ts:,} / {acc.ts_duplicates:,}")
    if 'assets' in recompute_sections:
        logger.info(f"  Assets:       {acc.total_asset_instances:,} / {acc.total_assets:,} / {acc.asset_duplicates:,}")
    if 'equipment' in recompute_sections:
        logger.info(f"  Equipment:    {acc.total_equipment_instances:,} / {acc.total_equipment:,} / {acc.equipment_duplicates:,}")
    if 'annotations' in recompute_sections:
        logger.info(f"  Annotations:  {annotation_acc.total_annotations:,} / {annotation_acc.unique_annotations:,}")
    if '3d' in recompute_sections:
        logger.info(f"  3D Objects:   {model3d_acc.total_3d_objects:,}")
    if 'files' in recompute_sections:
        logger.info(f"  Files:        {acc.total_file_instances:,} / {acc.total_files:,} / {acc.file_duplicates:,}")
    if 'staging' in recompute_sections:
        logger.info(f"  Staging:      {staging_metrics.get('staging_total_mappings', 0)} mappings processed")
    if 'maintenance_idi' in recompute_sections:
        logger.info(f"  Maint IDI:    {maintenance_idi_metrics.get('maint_idi_total_instances', 0):,} instances across {maintenance_idi_metrics.get('maint_idi_views_with_data', 0)} views")
    if 'others' in recompute_sections:
        logger.info(f"  Others:       {others_metrics.get('others_total_instances', 0):,} instances across {others_metrics.get('others_views_with_data', 0)} views")
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
    # Full recompute - compute all metrics for all tabs
    python run_metrics.py
    
    # Custom limits
    python run_metrics.py --max-ts 100000 --max-assets 50000
    
    # Dry run (save locally, don't upload)
    python run_metrics.py --dry-run
    
    # Selective recompute - only refresh specific tabs (others preserved from cache)
    python run_metrics.py --only staging
    python run_metrics.py --only assets --only equipment
    python run_metrics.py --only ts --only maintenance_idi --only staging
    
    # Use custom views (for non-CDM data models)
    python run_metrics.py --ts-view "rmdm/YourOrgTimeSeries/v1" --asset-view "rmdm/YourOrgAsset/v1"
    
    # Custom staging configuration
    python run_metrics.py --only staging --staging-db "my_db" --staging-space "my_space"

Available sections for --only:
    ts              Time Series metrics
    assets          Asset hierarchy metrics
    equipment       Equipment metrics
    maintenance_idi Maintenance IDI views metrics
    annotations     File annotation metrics
    3d              3D model metrics
    files           File contextualization metrics
    others          Other IDI views metrics
    staging         Staging vs Data Model comparison
    equipment   Equipment metrics
    maintenance Maintenance (notifications, orders) metrics
    annotations File annotation metrics
    3d          3D model metrics
    files       File contextualization metrics
    staging     Staging vs Data Model comparison

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
    parser.add_argument("--max-annotations", type=int, default=None,
                        help="Maximum annotations to process")
    parser.add_argument("--max-3d", type=int, default=None,
                        help="Maximum 3D objects to process")
    parser.add_argument("--max-files", type=int, default=None,
                        help="Maximum files to process")
    parser.add_argument("--max-maintenance-idi", type=int, default=None,
                        help="Maximum instances per maintenance IDI view")
    parser.add_argument("--max-others", type=int, default=None,
                        help="Maximum instances per Others IDI view")
    parser.add_argument("--max-activities", type=int, default=None,
                        help="Maximum activities to process")
    
    # Request page size (helps avoid graph query timeouts on heavy views)
    parser.add_argument("--chunk-size", type=int, default=None, metavar="N",
                        help="Page size for Data Model instances requests (default: 500). "
                             "Use 200 or 100 if maintenance or other views hit graph query timeout.")
    
    # Staging configuration (staging is always computed unless excluded via --only)
    parser.add_argument("--staging-db", type=str, default="oracle:db",
                        help="Raw database for staging comparison (default: oracle:db)")
    parser.add_argument("--staging-space", type=str, default="rmdm",
                        help="DM space for staging comparison (default: rmdm)")
    parser.add_argument("--staging-version", type=str, default="v1",
                        help="DM version for staging comparison (default: v1)")
    
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
    
    # Selective recompute (automatically preserves other sections from cache)
    parser.add_argument("--only", action="append", dest="only_sections",
                        choices=['ts', 'assets', 'equipment', 'maintenance_idi', 'annotations', '3d', 'files', 'others', 'activities', 'staging'],
                        metavar="SECTION",
                        help="Only recompute specified section(s). Can be repeated. "
                             "Other sections are preserved from cached metrics. "
                             "Options: ts, assets, equipment, maintenance, annotations, 3d, files, staging")
    
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
    
    # Local runs have no limits by default - remove them from config
    # Only apply limits if explicitly set via CLI
    config["max_timeseries"] = None
    config["max_assets"] = None
    config["max_equipment"] = None
    config["max_annotations"] = None
    config["max_3d_objects"] = None
    config["max_files"] = None
    config["max_maintenance_idi_instances"] = None
    config["max_others_instances"] = None
    config["max_activities"] = None
    
    # Apply CLI limit overrides (only if user wants limits)
    if args.max_ts is not None:
        config["max_timeseries"] = args.max_ts
    if args.max_assets is not None:
        config["max_assets"] = args.max_assets
    if args.max_equipment is not None:
        config["max_equipment"] = args.max_equipment
    if args.max_annotations is not None:
        config["max_annotations"] = args.max_annotations
    if args.max_3d is not None:
        config["max_3d_objects"] = args.max_3d
    if args.max_files is not None:
        config["max_files"] = args.max_files
    if args.max_maintenance_idi is not None:
        config["max_maintenance_idi_instances"] = args.max_maintenance_idi
    if args.max_others is not None:
        config["max_others_instances"] = args.max_others
    if args.max_activities is not None:
        config["max_activities"] = args.max_activities
    if args.chunk_size is not None:
        config["chunk_size"] = args.chunk_size
        logger.info(f"Using chunk_size={args.chunk_size} (smaller pages to reduce graph query timeout risk)")
    
    # Staging configuration (always applied - staging computed unless excluded via --only)
    config["staging_raw_database"] = args.staging_db
    config["staging_dm_space"] = args.staging_space
    config["staging_dm_version"] = args.staging_version
    
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
    
    # Handle selective recompute
    # If --only is used, we automatically load cached metrics and merge
    use_cache = False
    recompute_sections = None
    
    if args.only_sections:
        use_cache = True  # Auto-enable caching when using --only
        recompute_sections = set(args.only_sections)
        logger.info(f"Selective recompute: {', '.join(sorted(recompute_sections))}")
        logger.info("Other sections will be preserved from cached metrics.")
    
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
