"""
File storage utilities for metrics persistence.

Supports both single-run mode and batch processing mode:
- Single-run: save_metrics_to_file() saves final metrics
- Batch mode: save_batch_file() saves intermediate accumulator data
              load_batch_files() loads all batch files for aggregation
              delete_batch_files() cleans up after aggregation
"""

import json
import logging
import tempfile
import os
from typing import List, Optional

from cognite.client import CogniteClient

from .common import BATCH_FILE_PREFIX, CombinedAccumulator


logger = logging.getLogger(__name__)


def save_metrics_to_file(
    client: CogniteClient,
    metrics: dict,
    file_external_id: str,
    file_name: str
):
    """Save metrics to Cognite Files as JSON, overwriting if exists."""
    # Create temp file
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    
    with open(temp_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    
    # Upload with overwrite
    client.files.upload(
        path=temp_path,
        external_id=file_external_id,
        name=file_name,
        mime_type="application/json",
        overwrite=True
    )
    
    # Clean up temp file
    try:
        os.remove(temp_path)
    except Exception:
        pass
    
    logger.info(f"📁 Saved metrics to Cognite Files: {file_external_id}")


# ----------------------------------------------------
# BATCH PROCESSING FUNCTIONS
# ----------------------------------------------------

def get_batch_file_external_id(batch_index: int) -> str:
    """Get the external ID for a batch file."""
    return f"{BATCH_FILE_PREFIX}{batch_index}"


def save_batch_file(
    client: CogniteClient,
    accumulator: CombinedAccumulator,
    batch_index: int,
    batch_metadata: Optional[dict] = None
):
    """
    Save accumulator data to a batch file for later aggregation.
    
    Args:
        client: CogniteClient instance
        accumulator: CombinedAccumulator with collected data
        batch_index: Index of this batch (0, 1, 2, ...)
        batch_metadata: Optional metadata about the batch
    """
    file_external_id = get_batch_file_external_id(batch_index)
    file_name = f"{file_external_id}.json"
    
    # Combine accumulator data with metadata
    batch_data = {
        "batch_index": batch_index,
        "batch_metadata": batch_metadata or {},
        "accumulator": accumulator.to_dict(),
    }
    
    # Create temp file
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    
    with open(temp_path, "w") as f:
        json.dump(batch_data, f, indent=2, default=str)
    
    # Upload with overwrite
    client.files.upload(
        path=temp_path,
        external_id=file_external_id,
        name=file_name,
        mime_type="application/json",
        overwrite=True
    )
    
    # Clean up temp file
    try:
        os.remove(temp_path)
    except Exception:
        pass
    
    logger.info(f"📁 Saved batch file: {file_external_id}")


def list_batch_files(client: CogniteClient, max_batches: int = 20) -> List[str]:
    """
    List all batch files in CDF by checking for expected external IDs.
    
    This approach is more robust than listing all files and filtering,
    as it directly checks for batch files by their expected external IDs.
    
    Args:
        client: CogniteClient instance
        max_batches: Maximum number of batches to check for (default 20)
    
    Returns:
        List of batch file external IDs that exist, sorted by batch index
    """
    batch_files = []
    
    # Check for each expected batch file by external ID
    for i in range(max_batches):
        ext_id = get_batch_file_external_id(i)
        try:
            # Try to retrieve file metadata - if it exists, add to list
            file_metadata = client.files.retrieve(external_id=ext_id)
            if file_metadata:
                batch_files.append(ext_id)
                logger.info(f"Found batch file: {ext_id}")
        except Exception:
            # File doesn't exist - stop checking (batches are sequential)
            # But continue checking a few more in case of gaps
            continue
    
    logger.info(f"Found {len(batch_files)} batch files")
    return batch_files


def load_batch_file(client: CogniteClient, file_external_id: str) -> Optional[dict]:
    """
    Load a single batch file.
    
    Args:
        client: CogniteClient instance
        file_external_id: External ID of the batch file
        
    Returns:
        Batch data dict or None if failed
    """
    try:
        file_bytes = client.files.download_bytes(external_id=file_external_id)
        return json.loads(file_bytes.decode("utf-8"))
    except Exception as e:
        logger.error(f"Failed to load batch file {file_external_id}: {e}")
        return None


def load_and_merge_all_batches(client: CogniteClient) -> Optional[CombinedAccumulator]:
    """
    Load all batch files and merge them into a single accumulator.
    
    Returns:
        Merged CombinedAccumulator or None if no batches found
    """
    batch_files = list_batch_files(client)
    
    if not batch_files:
        logger.warning("No batch files found")
        return None
    
    logger.info(f"Found {len(batch_files)} batch files to merge")
    
    merged_acc = None
    
    for file_ext_id in batch_files:
        logger.info(f"Loading batch file: {file_ext_id}")
        batch_data = load_batch_file(client, file_ext_id)
        
        if not batch_data:
            logger.warning(f"Skipping empty/invalid batch file: {file_ext_id}")
            continue
        
        acc_data = batch_data.get("accumulator", {})
        batch_acc = CombinedAccumulator.from_dict(acc_data)
        
        if merged_acc is None:
            merged_acc = batch_acc
        else:
            merged_acc.merge_from(batch_acc)
        
        logger.info(f"Merged batch {batch_data.get('batch_index', '?')}: "
                   f"assets={batch_acc.total_assets:,}, ts={batch_acc.total_ts:,}")
    
    if merged_acc:
        logger.info(f"✅ Merged all batches: "
                   f"total_assets={merged_acc.total_assets:,}, "
                   f"total_ts={merged_acc.total_ts:,}, "
                   f"total_equipment={merged_acc.total_equipment:,}")
    
    return merged_acc


def delete_batch_files(client: CogniteClient) -> int:
    """
    Delete all batch files after successful aggregation.
    
    Returns:
        Number of files deleted
    """
    batch_files = list_batch_files(client)
    
    if not batch_files:
        return 0
    
    deleted = 0
    for file_ext_id in batch_files:
        try:
            client.files.delete(external_id=file_ext_id)
            deleted += 1
            logger.info(f"🗑️ Deleted batch file: {file_ext_id}")
        except Exception as e:
            logger.warning(f"Failed to delete batch file {file_ext_id}: {e}")
    
    return deleted
