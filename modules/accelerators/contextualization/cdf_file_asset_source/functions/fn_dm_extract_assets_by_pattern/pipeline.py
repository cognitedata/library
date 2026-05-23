"""
CDF Pipeline for Extract Annotation Tags

This module provides the main pipeline function that processes files
and extracts annotation tags using pattern-based diagram detection.
"""

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.data_classes.contextualization import (
    DiagramDetectConfig,
    DiagramDetectResults,
    FileReference,
)
from cognite.client.exceptions import CogniteAPIError

# Try to import RawUploadQueue, but make it optional for local testing
try:
    from cognite.extractorutils.uploader import RawUploadQueue

    EXTRACTOR_UTILS_AVAILABLE = True
except ImportError:
    EXTRACTOR_UTILS_AVAILABLE = False
    RawUploadQueue = None

try:
    from .logger import CogniteFunctionLogger
    from .utils.file_utils import chunk_file_into_page_blocks, get_cognite_files
except ImportError:
    from logger import CogniteFunctionLogger
    from utils.file_utils import chunk_file_into_page_blocks, get_cognite_files

logger = None  # Use CogniteFunctionLogger directly


def _normalize_uploaded_time(uploaded_time: Any) -> Optional[str]:
    """Normalize uploadedTime to a readable UTC ISO datetime string.

    Handles datetime objects, timestamp numbers, and string formats.
    Returns None if input is None or empty.
    """
    if not uploaded_time:
        return None

    # If it's already a datetime object
    if isinstance(uploaded_time, datetime):
        # Ensure datetime is timezone-aware and convert to UTC
        if uploaded_time.tzinfo is None:
            uploaded_time = uploaded_time.replace(tzinfo=timezone.utc)
        else:
            uploaded_time = uploaded_time.astimezone(timezone.utc)
        return uploaded_time.isoformat()

    # If it's a string, try to parse it
    if isinstance(uploaded_time, str):
        # Check if it's already in ISO format (contains 'T' or 'Z' or has timezone)
        if (
            "T" in uploaded_time
            or "Z" in uploaded_time
            or "+" in uploaded_time
            or uploaded_time.count("-") >= 2
        ):
            # Try to parse and re-format to ensure UTC
            try:
                # Parse the string
                if uploaded_time.endswith("Z"):
                    # Remove Z and parse as UTC
                    dt = datetime.fromisoformat(uploaded_time.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(uploaded_time)
                # Ensure UTC and return ISO format
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            except (ValueError, AttributeError):
                # If parsing fails, check if it's a timestamp string
                try:
                    # Try to parse as Unix timestamp (seconds or milliseconds)
                    timestamp = float(uploaded_time)
                    if timestamp > 1e12:  # Likely milliseconds
                        timestamp = timestamp / 1000
                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    return dt.isoformat()
                except (ValueError, OSError):
                    # If all parsing fails, return as-is (might already be readable)
                    return uploaded_time
        else:
            # Might be a timestamp string, try to parse
            try:
                timestamp = float(uploaded_time)
                if timestamp > 1e12:  # Likely milliseconds
                    timestamp = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                return dt.isoformat()
            except (ValueError, OSError):
                # Return as-is if not a number
                return uploaded_time

    # If it's a number (timestamp)
    if isinstance(uploaded_time, (int, float)):
        if uploaded_time > 1e12:  # Likely milliseconds
            uploaded_time = uploaded_time / 1000
        dt = datetime.fromtimestamp(uploaded_time, tz=timezone.utc)
        return dt.isoformat()

    # If it has isoformat method (other datetime-like objects)
    if hasattr(uploaded_time, "isoformat"):
        try:
            result = uploaded_time.isoformat()
            # Try to parse and normalize to UTC
            try:
                dt = datetime.fromisoformat(result)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            except (ValueError, AttributeError):
                return result
        except Exception:
            pass

    # Fallback: convert to string
    return str(uploaded_time)


def _create_table_if_not_exists(
    client: CogniteClient,
    raw_db: str,
    tbl: str,
    logger: Optional[CogniteFunctionLogger] = None,
) -> None:
    """Create RAW database and table if they don't exist."""
    log = logger or CogniteFunctionLogger()
    try:
        if raw_db not in client.raw.databases.list(limit=-1).as_names():
            client.raw.databases.create(raw_db)
            log.debug(f"Created RAW database: {raw_db}")
    except Exception as e:
        log.warning(f"Error creating RAW database {raw_db}: {e}")

    try:
        if tbl not in client.raw.tables.list(raw_db, limit=-1).as_names():
            client.raw.tables.create(raw_db, tbl)
            log.debug(f"Created RAW table: {raw_db}.{tbl}")
    except Exception as e:
        log.warning(f"Error creating RAW table {raw_db}.{tbl}: {e}")


def _load_state_from_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_state: str,
    results_field: str = "results",
    logger: Optional[CogniteFunctionLogger] = None,
) -> Dict[int, Dict[str, Any]]:
    """Load state from RAW table (includes results field with configurable name)."""
    log = logger or CogniteFunctionLogger()
    state_store = {}

    # Check if state table exists first
    try:
        tables = client.raw.tables.list(raw_db, limit=-1).as_names()
        if raw_table_state not in tables:
            log.warning(
                f"State table '{raw_table_state}' does not exist in database '{raw_db}'. Starting with empty state."
            )
            return state_store
    except Exception as e:
        log.warning(
            f"Error checking if state table exists: {e}. Attempting to load anyway..."
        )

    try:
        # Note: table name should be passed as string, not list
        rows = client.raw.rows.list(raw_db, raw_table_state).to_pandas()
        if not rows.empty:
            for key, row in rows.iterrows():
                try:
                    file_id = int(key)
                    # Parse state column (stored as JSON string)
                    state_json = row.get("state", "{}")
                    if isinstance(state_json, str):
                        state_data = json.loads(state_json)
                    else:
                        state_data = state_json if isinstance(state_json, dict) else {}

                    # Ensure file_info exists and has uploadedTime from RAW column if available
                    if "file_info" not in state_data:
                        state_data["file_info"] = {}

                    # Use uploaded_time from RAW column if state doesn't have it
                    uploaded_time = row.get("uploaded_time")
                    if uploaded_time and not state_data["file_info"].get(
                        "uploadedTime"
                    ):
                        state_data["file_info"]["uploadedTime"] = uploaded_time

                    # Use status from RAW column if available (prefer top-level over state JSON)
                    status = row.get("status")
                    if status:
                        state_data["status"] = status
                    elif "status" not in state_data:
                        # If no status in top-level column and not in state, set to pending
                        state_data["status"] = "pending"

                    # Use attempts from RAW column if available (prefer top-level over state JSON)
                    attempts = row.get("attempts")
                    if attempts is not None:
                        try:
                            state_data["attempts"] = int(attempts)
                        except (ValueError, TypeError):
                            state_data["attempts"] = 0
                    elif "attempts" not in state_data:
                        # If no attempts in top-level column and not in state, set to 0
                        state_data["attempts"] = 0

                    # Use results from RAW column if available (prefer top-level over state JSON)
                    # Results are now a first-class property
                    results_json = row.get("results")
                    if results_json:
                        try:
                            if isinstance(results_json, str):
                                results_data = json.loads(results_json)
                            else:
                                results_data = (
                                    results_json
                                    if isinstance(results_json, dict)
                                    else {}
                                )
                            # Store results at top level in state_data
                            state_data[results_field] = results_data
                        except (ValueError, json.JSONDecodeError) as e:
                            log.warning(
                                f"Error parsing results for file_id {file_id}: {e}"
                            )
                            # Fall back to results in state JSON if top-level parsing fails
                            if results_field not in state_data:
                                state_data[results_field] = None
                    elif results_field not in state_data:
                        # If no results in top-level column and not in state, set to None
                        state_data[results_field] = None

                    # Add metadata from other columns
                    state_data["file_id"] = file_id
                    if "updated_at" in row:
                        state_data["updated_at"] = row["updated_at"]

                    state_store[file_id] = state_data
                except (ValueError, json.JSONDecodeError) as e:
                    log.warning(f"Error parsing state for key {key}: {e}")
                    continue
            log.info(f"Loaded state for {len(state_store)} file(s) from RAW")
        else:
            log.info(
                f"State table '{raw_table_state}' exists but is empty. Starting with empty state."
            )
    except Exception as e:
        log.warning(f"Error loading state from RAW table '{raw_table_state}': {e}")
        log.warning(
            "This may reset status for existing files. If the table should exist, check database/table names."
        )

    return state_store


def _save_state_to_raw(
    raw_uploader: RawUploadQueue,
    raw_db: str,
    raw_table_state: str,
    file_id: int,
    state_data: Dict[str, Any],
    results_field: str = "results",
) -> None:
    """Save state for a file to RAW table."""
    try:
        # Extract uploadedTime from file_info if available
        file_info = state_data.get("file_info", {})
        uploaded_time = file_info.get("uploadedTime")

        # Normalize uploadedTime to readable UTC ISO string
        uploaded_time_str = _normalize_uploaded_time(uploaded_time) or ""

        # Extract status and attempts from state_data for top-level storage
        status = state_data.get("status", "pending")
        attempts = state_data.get("attempts", 0)

        # Extract results from state_data for top-level storage (promoted to first-class property)
        results = state_data.get(results_field)
        results_json_str = ""
        if results is not None:
            try:
                results_json_str = json.dumps(results, default=str)
            except (TypeError, ValueError) as e:
                # If results can't be serialized, log warning but continue
                pass

        # Convert state to JSON string for storage (results are also stored separately)
        columns = {
            "state": json.dumps(state_data, default=str),
            "file_id": str(file_id),
            "status": str(status) if status else "",
            "attempts": str(attempts) if attempts is not None else "0",
            "results": results_json_str,  # Results as first-class property
            "uploaded_time": uploaded_time_str,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        row = Row(key=str(file_id), columns=columns)
        raw_uploader.add_to_upload_queue(
            database=raw_db, table=raw_table_state, raw_row=row
        )
    except Exception as e:
        # Log but don't fail the pipeline
        pass


def _save_state_to_raw_direct(
    client: CogniteClient,
    raw_db: str,
    raw_table_state: str,
    file_id: int,
    state_data: Dict[str, Any],
    results_field: str = "results",
    logger: Optional[CogniteFunctionLogger] = None,
) -> None:
    """Save state for a file to RAW table directly using client (no RawUploadQueue)."""
    try:
        # Extract uploadedTime from file_info if available
        file_info = state_data.get("file_info", {})
        uploaded_time = file_info.get("uploadedTime")

        # Normalize uploadedTime to readable UTC ISO string
        uploaded_time_str = _normalize_uploaded_time(uploaded_time) or ""

        # Extract status and attempts from state_data for top-level storage
        status = state_data.get("status", "pending")
        attempts = state_data.get("attempts", 0)

        # Extract results from state_data for top-level storage (promoted to first-class property)
        results = state_data.get(results_field)
        results_json_str = ""
        if results is not None:
            try:
                results_json_str = json.dumps(results, default=str)
            except (TypeError, ValueError) as e:
                # If results can't be serialized, log warning but continue
                pass

        # Convert state to JSON string for storage (results are also stored separately)
        columns = {
            "state": json.dumps(state_data, default=str),
            "file_id": str(file_id),
            "status": str(status) if status else "",
            "attempts": str(attempts) if attempts is not None else "0",
            "results": results_json_str,  # Results as first-class property
            "uploaded_time": uploaded_time_str,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        row = Row(key=str(file_id), columns=columns)
        # Delete existing row first to ensure update works, then insert
        try:
            client.raw.rows.delete(
                db_name=raw_db, table_name=raw_table_state, row_keys=[str(file_id)]
            )
        except Exception:
            # Row might not exist, which is fine
            pass
        client.raw.rows.insert(db_name=raw_db, table_name=raw_table_state, row=[row])
    except Exception as e:
        # Log but don't fail the pipeline
        if logger:
            logger.warning(f"Error saving state for file {file_id}: {e}")
        pass


def run_pattern_diagram_detect(
    client: CogniteClient,
    file_refs: List[FileReference],
    patterns: List[Dict[str, Any]],
    partial_match: bool = True,
    min_tokens: int = 1,
    diagram_detect_config: Optional[Dict[str, Any]] = None,
    logger: Optional[CogniteFunctionLogger] = None,
) -> int:
    """Run diagram detect in pattern mode for a batch of files."""
    log = logger or CogniteFunctionLogger()
    try:
        # Create DiagramDetectConfig from dictionary if provided, otherwise use API defaults
        detect_config = None
        if diagram_detect_config:
            detect_config = DiagramDetectConfig(**diagram_detect_config)

        detect_kwargs = {
            "file_references": file_refs,
            "entities": patterns,
            "partial_match": partial_match,
            "min_tokens": min_tokens,
            "search_field": "sample",
            "pattern_mode": True,
        }
        if detect_config is not None:
            detect_kwargs["configuration"] = detect_config

        detect_job: DiagramDetectResults = client.diagrams.detect(**detect_kwargs)

        if detect_job.job_id:
            return detect_job.job_id
        else:
            raise Exception(
                "API call to diagram/detect in pattern mode did not return a job ID"
            )

    except CogniteAPIError as e:
        log.error(f"Error running diagram detect: {e}")
        raise


def wait_for_job_completion(
    client: CogniteClient,
    job_id: int,
    timeout: int = 3600,
    logger: Optional[CogniteFunctionLogger] = None,
) -> Dict[str, Any]:
    """Wait for diagram detect job to complete and return results."""
    log = logger or CogniteFunctionLogger()
    log.info(f"Waiting for job {job_id} to complete...")

    start_time = time.time()
    poll_interval = 5  # seconds

    # Get project for API call
    project = client.config.project

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(f"Job {job_id} timed out after {timeout} seconds")

        try:
            # Check job status using direct API call
            job_api = f"/api/v1/projects/{project}/context/diagram/detect/{job_id}"

            # Use client's get method which handles authentication
            response = client.get(job_api)

            if response.status_code == 200:
                job_results = response.json()
                status = job_results.get("status", "unknown")

                if status == "Completed":
                    log.info(f"Job {job_id} completed")
                    return job_results
                elif status == "Failed":
                    error_msg = job_results.get("error", "Unknown error")
                    raise Exception(f"Job {job_id} failed: {error_msg}")
                else:
                    # Still processing
                    log.debug(
                        f"Job {job_id} status: {status} (elapsed: {int(elapsed)}s)"
                    )
                    time.sleep(poll_interval)
            elif response.status_code == 404:
                # Job might not be ready yet
                log.debug(f"Job {job_id} not found yet, waiting...")
                time.sleep(poll_interval)
            else:
                raise Exception(
                    f"Unexpected status code {response.status_code}: {response.text}"
                )

        except requests.RequestException:
            # Handle connection errors
            log.warning(f"Connection error checking job {job_id}, retrying...")
            time.sleep(poll_interval)
        except Exception as e:
            # Handle other exceptions
            if "404" in str(e) or "not found" in str(e).lower():
                log.debug(f"Job {job_id} not found yet, waiting...")
                time.sleep(poll_interval)
            else:
                raise


def process_batch(
    client: CogniteClient,
    file_batch: List[Dict[str, Any]],
    patterns: List[Dict[str, Any]],
    partial_match: bool = True,
    min_tokens: int = 1,
    diagram_detect_config: Optional[Dict[str, Any]] = None,
    state_store: Optional[Dict[int, Dict[str, Any]]] = None,
    file_refs: Optional[List[FileReference]] = None,
    max_pages_per_chunk: int = 50,
    results_field: str = "results",
    logger: Optional[CogniteFunctionLogger] = None,
) -> Dict[int, Dict[str, Any]]:
    """
    Process a batch of files through diagram detect.

    Args:
        file_batch: List of file info dictionaries
        patterns: List of pattern dictionaries for diagram detection
        partial_match: Whether to enable partial matching
        min_tokens: Minimum number of tokens required for pattern matching
        state_store: Optional state store dictionary to update with results
        file_refs: Optional list of FileReference objects. If provided, these will be used
                   instead of creating default references. Must match file_batch in order.
        logger: Optional logger instance

    Returns a dictionary mapping file_id to result status:
    {
        file_id: {
            'status': 'success' | 'failed',
            'error': str (if failed)
        }
    }

    Results are stored directly in state_store under each file_id's entry.
    """
    log = logger or CogniteFunctionLogger()
    file_results = {}

    # Create file references for the batch
    # If file_refs not provided, chunk files that have >max_pages_per_chunk pages
    if file_refs is None:
        file_refs = []
        for file_info in file_batch:
            file_id = file_info["id"]
            page_count = file_info.get("page_count", 1)

            # Use chunk_file_into_page_blocks to handle files with >max_pages_per_chunk pages
            if page_count > max_pages_per_chunk:
                # File has more pages than max_pages_per_chunk - chunk it
                chunked_refs = chunk_file_into_page_blocks(
                    file_info, max_pages_per_chunk
                )
                file_refs.extend(chunked_refs)
                log.debug(
                    f"File {file_id} has {page_count} pages, chunked into {len(chunked_refs)} chunks"
                )
            else:
                # File has <= max_pages_per_chunk pages - single reference
                file_ref = FileReference(
                    file_id=file_id, first_page=1, last_page=page_count
                )
                file_refs.append(file_ref)

    file_info_map = {}  # Map file_id to file_info for later use

    for file_info in file_batch:
        file_id = file_info["id"]
        file_info_map[file_id] = file_info
        # Initialize result as failed (will be updated on success)
        file_results[file_id] = {
            "status": "failed",
            "error": "Unknown error",
            "result_file": None,
        }

    # Display batch info
    file_names = [
        f.get("name") or f.get("external_id") or f"file_{f['id']}" for f in file_batch
    ]
    log.info(f"Processing batch of {len(file_batch)} file(s):")
    for name in file_names[:5]:  # Show first 5
        log.debug(f"   - {name}")
    if len(file_names) > 5:
        log.debug(f"   ... and {len(file_names) - 5} more")

    try:
        # Run diagram detect for the batch
        job_id = run_pattern_diagram_detect(
            client=client,
            file_refs=file_refs,
            patterns=patterns,
            partial_match=partial_match,
            min_tokens=min_tokens,
            diagram_detect_config=diagram_detect_config,
            logger=log,
        )

        log.info(f"Job ID: {job_id}")

        # Wait for completion
        job_results = wait_for_job_completion(client, job_id, logger=log)

        # Extract results for each file from the batch results
        # The results.items should contain results for each file
        if "items" in job_results:
            # Create a mapping of file_id to results (aggregate results from multiple page chunks)
            file_results_map = {}
            for item in job_results.get("items", []):
                file_id = item.get("fileId")
                if file_id:
                    if file_id not in file_results_map:
                        file_results_map[file_id] = []
                    file_results_map[file_id].append(item)

            # Save results for each file
            for file_id, file_info in file_info_map.items():
                file_name = (
                    file_info["name"]
                    or file_info.get("external_id")
                    or f"file_{file_id}"
                )

                # Get results for this specific file (aggregated from all page chunks)
                file_specific_results = {
                    "createdTime": job_results.get("createdTime"),
                    "items": file_results_map.get(file_id, []),
                    "status": job_results.get("status"),
                }

                # Store results in state_store instead of writing to file
                # If file already has results in state, merge them
                existing_items = []
                if state_store and file_id in state_store:
                    existing_results = state_store[file_id].get(results_field)
                    if existing_results and "items" in existing_results:
                        existing_items = existing_results["items"]

                # Merge existing items with new items
                all_items = existing_items + file_specific_results["items"]

                # Store results in state_store
                result_data = {
                    "file_info": file_info,
                    "job_id": job_id,
                    results_field: {
                        "createdTime": job_results.get("createdTime"),
                        "items": all_items,
                        "status": job_results.get("status"),
                    },
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }

                if state_store and file_id in state_store:
                    state_store[file_id][results_field] = result_data[results_field]
                    state_store[file_id]["job_id"] = job_id
                    state_store[file_id]["processed_at"] = result_data["processed_at"]
                    state_store[file_id]["status"] = "success"
                    state_store[file_id]["last_error"] = None
                    # On success, ensure attempts is set (but don't increment - attempts only count failures)
                    if "attempts" not in state_store[file_id]:
                        state_store[file_id]["attempts"] = 0

                log.info(f"Results saved for {file_name} (stored in state)")
                file_results[file_id] = {
                    "status": "success",
                    "error": None,
                    "result_file": None,  # No longer using file-based storage
                }
        else:
            # Fallback: save combined results if structure is different
            log.warning("Unexpected results structure, saving combined results")
            for file_info in file_batch:
                file_id = file_info["id"]
                file_name = (
                    file_info["name"]
                    or file_info.get("external_id")
                    or f"file_{file_id}"
                )

                # Store results in state_store
                result_data = {
                    "file_info": file_info,
                    "job_id": job_id,
                    results_field: job_results,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }

                if state_store and file_id in state_store:
                    state_store[file_id][results_field] = result_data[results_field]
                    state_store[file_id]["job_id"] = job_id
                    state_store[file_id]["processed_at"] = result_data["processed_at"]
                    state_store[file_id]["status"] = "success"
                    state_store[file_id]["last_error"] = None
                    # On success, ensure attempts is set (but don't increment - attempts only count failures)
                    if "attempts" not in state_store[file_id]:
                        state_store[file_id]["attempts"] = 0

                file_results[file_id] = {
                    "status": "success",
                    "error": None,
                    "result_file": None,  # No longer using file-based storage
                }

        return file_results

    except Exception as e:
        error_msg = str(e)
        log.error(f"Error processing batch: {error_msg}")
        # Mark all files in batch as failed with the error
        for file_id in file_info_map.keys():
            file_results[file_id] = {
                "status": "failed",
                "error": error_msg,
                "result_file": None,
            }
            # Update state_store if available
            if state_store and file_id in state_store:
                state_store[file_id]["status"] = "failed"
                state_store[file_id]["last_error"] = error_msg
                # Increment attempts
                state_store[file_id]["attempts"] = (
                    state_store[file_id].get("attempts", 0) + 1
                )
        return file_results


def extract_assets_by_pattern(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Main pipeline function for extract assets by pattern in CDF format.

    This function processes files and extracts assets using
    pattern-based diagram detection.

    Args:
        client: CogniteClient instance (optional if not using CDF)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing pipeline parameters and results
    """
    pipeline_ext_id = data.get("ExtractionPipelineExtId", "unknown")
    status = "failure"
    pipeline_run_id = None
    use_cdf_format = False
    raw_uploader = None

    try:
        logger.info("Starting Extract Annotation Tags Pipeline")

        # Check if using CDF format
        cdf_config = data.get("_cdf_config")
        use_cdf_format = cdf_config is not None and client is not None

        if use_cdf_format:
            # Extract parameters from CDF config
            raw_db = cdf_config.parameters.raw_db
            raw_table_state = cdf_config.parameters.raw_table_state
            raw_table_results = (
                cdf_config.parameters.raw_table_results or raw_table_state
            )  # Default to state table if not specified
            results_field = cdf_config.parameters.results_field
            overwrite = cdf_config.parameters.overwrite
            run_all = cdf_config.parameters.run_all
            initialize_state = cdf_config.parameters.initialize_state

            logger.debug(
                f"Using CDF format: raw_db={raw_db}, raw_table_state={raw_table_state}, raw_table_results={raw_table_results}, results_field={results_field}"
            )

            # Check for initialize_state mode
            if initialize_state:
                logger.info(
                    "Update state mode: Only querying for new files and updating state, skipping processing"
                )

            # Initialize RAW upload queue (optional if RawUploadQueue not available)
            raw_uploader = None
            if EXTRACTOR_UTILS_AVAILABLE:
                raw_uploader = RawUploadQueue(
                    cdf_client=client, max_queue_size=500000, trigger_log_level="INFO"
                )
            else:
                logger.info(
                    "RawUploadQueue not available, will use direct client calls for state updates"
                )

            # Ensure state table exists
            _create_table_if_not_exists(client, raw_db, raw_table_state, logger)

            # Load state from RAW if not overwriting (results are included in state)
            if not overwrite:
                state_store = _load_state_from_raw(
                    client, raw_db, raw_table_state, results_field, logger
                )
                logger.info(f"Loaded state for {len(state_store)} file(s) from RAW")
            else:
                state_store = {}
                logger.info("Overwrite mode: starting with empty state")
        else:
            # Standalone mode - use provided state_store or empty dict
            state_store = data.get("state_store", {})
            logger.info(
                f"Using standalone mode with {len(state_store)} file(s) in state"
            )

        # Extract parameters from data
        files = data.get("files", [])
        patterns = data.get("patterns")
        partial_match = data.get("partial_match", True)
        min_tokens = data.get("min_tokens", 2)
        batch_size = data.get("batch_size", 20)
        max_attempts = data.get("max_attempts", 3)  # Default to 3 if not specified
        max_pages_per_chunk = data.get(
            "max_pages_per_chunk", 50
        )  # Default to 50 if not specified
        diagram_detect_config = data.get("diagram_detect_config", {})

        # Patterns must be provided directly in data
        if patterns is None:
            raise ValueError(
                "patterns must be provided in data (either directly or via ExtractionPipelineExtId config)"
            )
        logger.info(f"Using {len(patterns)} pattern groups")

        # Get files if not provided
        if not files:
            if client is None:
                raise ValueError(
                    "Client is required when files are not provided in data"
                )
            limit = data.get("limit")
            mime_type = data.get("mime_type")
            instance_space = data.get("instance_space")
            logger.info(
                f"Querying CDF for files (limit={limit}, mime_type={mime_type}, instance_space={instance_space})"
            )
            try:
                files = get_cognite_files(
                    client,
                    limit=limit,
                    mime_type=mime_type,
                    instance_space=instance_space,
                    skip_page_count=False,
                )
                logger.info(f"Retrieved {len(files)} file(s) from CDF")
            except Exception as e:
                logger.error(f"Failed to retrieve files from CDF: {e}")
                raise

        if not files:
            logger.warning("No files to process")
            status = "success"
            return

        # Initialize tracking lists for file filtering
        files_reset = []  # Track files that were reset due to re-upload

        # Filter files based on state if not run_all (or in update_state mode)
        if use_cdf_format and ((not run_all and not overwrite) or update_state):
            # Process files that haven't been processed yet OR have been re-uploaded
            # Skip files that have exceeded max_attempts (unless in update_state mode)
            files_to_process = []
            files_skipped_max_attempts = []

            for file_info in files:
                file_id = file_info["id"]
                file_uploaded_time = file_info.get("uploadedTime")

                if file_id not in state_store:
                    # New file - add to process list (or state update list in initialize_state mode)
                    files_to_process.append(file_info)
                elif file_uploaded_time:
                    # Check if file has been re-uploaded (newer uploadedTime)
                    stored_state = state_store[file_id]
                    stored_file_info = stored_state.get("file_info", {})
                    stored_uploaded_time = stored_file_info.get("uploadedTime")

                    # Compare uploadedTime to detect re-uploads
                    if stored_uploaded_time and file_uploaded_time:
                        # Parse timestamps for comparison
                        try:
                            # Try ISO format parsing first (most common)
                            if isinstance(file_uploaded_time, str) and isinstance(
                                stored_uploaded_time, str
                            ):
                                # Simple string comparison for ISO format timestamps
                                if file_uploaded_time > stored_uploaded_time:
                                    # File has been re-uploaded - reset state and reprocess
                                    logger.info(
                                        f"File {file_id} has been re-uploaded (new uploadedTime: {file_uploaded_time} > {stored_uploaded_time}), resetting state"
                                    )
                                    state_store[file_id] = {
                                        "file_info": file_info,
                                        results_field: None,
                                        "job_id": None,
                                        "processed_at": None,
                                        "status": "pending",  # Reset status for fresh start
                                        "attempts": 0,  # Reset attempts counter for re-uploaded file
                                        "last_error": None,  # Clear previous error
                                    }
                                    files_to_process.append(file_info)
                                    files_reset.append(file_id)
                                    continue
                            else:
                                # Non-string comparison (datetime objects)
                                if file_uploaded_time > stored_uploaded_time:
                                    logger.info(
                                        f"File {file_id} has been re-uploaded, resetting state"
                                    )
                                    state_store[file_id] = {
                                        "file_info": file_info,
                                        results_field: None,
                                        "job_id": None,
                                        "processed_at": None,
                                        "status": "pending",  # Reset status for fresh start
                                        "attempts": 0,  # Reset attempts counter for re-uploaded file
                                        "last_error": None,  # Clear previous error
                                    }
                                    files_to_process.append(file_info)
                                    files_reset.append(file_id)
                                    continue
                        except Exception as e:
                            logger.warning(
                                f"Error comparing uploadedTime for file {file_id}: {e}"
                            )
                            # If comparison fails, assume file needs processing
                            files_to_process.append(file_info)
                            continue
                    elif not stored_uploaded_time and file_uploaded_time:
                        # Stored state doesn't have uploadedTime but current file does - reset
                        logger.info(
                            f"File {file_id} now has uploadedTime, resetting state"
                        )
                        state_store[file_id] = {
                            "file_info": file_info,
                            results_field: None,
                            "job_id": None,
                            "processed_at": None,
                            "status": "pending",  # Reset status for fresh start
                            "attempts": 0,  # Reset attempts counter for re-uploaded file
                            "last_error": None,  # Clear previous error
                        }
                        files_to_process.append(file_info)
                        files_reset.append(file_id)
                        continue
                    # File exists in state and uploadedTime hasn't changed
                    # In initialize_state mode, skip status checks (just update state for existing files)
                    if initialize_state:
                        # In initialize_state mode, don't add to files_to_process (already in state)
                        continue

                    # Check if file failed and exceeded max_attempts
                    stored_state = state_store[file_id]
                    stored_status = stored_state.get("status")
                    stored_attempts = stored_state.get("attempts", 0)

                    if stored_status == "failed" and stored_attempts >= max_attempts:
                        # File has failed and exceeded max attempts - skip
                        files_skipped_max_attempts.append(file_id)
                        logger.debug(
                            f"Skipping file {file_id}: failed {stored_attempts} times (max_attempts={max_attempts})"
                        )
                        continue
                    elif stored_status == "success":
                        # File already successfully processed - skip
                        continue
                    elif stored_status == "failed" and stored_attempts < max_attempts:
                        # File failed but hasn't exceeded max attempts - retry
                        logger.info(
                            f"Retrying file {file_id}: failed {stored_attempts} times (max_attempts={max_attempts})"
                        )
                        files_to_process.append(file_info)
                        continue
                    # If status is None or other, process it
                    files_to_process.append(file_info)
                else:
                    # File exists in state but no uploadedTime available
                    # In initialize_state mode, skip status checks (just update state for existing files)
                    if initialize_state:
                        # In initialize_state mode, don't add to files_to_process (already in state)
                        continue

                    # Check retry logic
                    stored_state = state_store.get(file_id, {})
                    stored_status = stored_state.get("status")
                    stored_attempts = stored_state.get("attempts", 0)

                    if stored_status == "failed" and stored_attempts >= max_attempts:
                        # File has failed and exceeded max attempts - skip
                        files_skipped_max_attempts.append(file_id)
                        logger.debug(
                            f"Skipping file {file_id}: failed {stored_attempts} times (max_attempts={max_attempts})"
                        )
                        continue
                    elif stored_status == "success":
                        # File already successfully processed - skip
                        continue
                    elif stored_status == "failed" and stored_attempts < max_attempts:
                        # File failed but hasn't exceeded max attempts - retry
                        logger.info(
                            f"Retrying file {file_id}: failed {stored_attempts} times (max_attempts={max_attempts})"
                        )
                        files_to_process.append(file_info)
                        continue
                    # If status is None or other, process it
                    files_to_process.append(file_info)

            if len(files_to_process) < len(files):
                skipped = len(files) - len(files_to_process)
                logger.info(
                    f"Filtered to {len(files_to_process)} file(s) to process (skipping {skipped} already processed)"
                )
            if files_reset:
                logger.info(f"Reset state for {len(files_reset)} re-uploaded file(s)")
            if files_skipped_max_attempts:
                logger.warning(
                    f"Skipped {len(files_skipped_max_attempts)} file(s) that exceeded max_attempts ({max_attempts})"
                )
            files = files_to_process

        if not files:
            logger.info("No files to process (all already processed)")
            status = "success"
            return

        # Initialize state_store entries for all files
        new_files_added = []
        for file_info in files:
            file_id = file_info["id"]
            if file_id not in state_store:
                state_store[file_id] = {
                    "file_info": file_info,
                    results_field: None,
                    "job_id": None,
                    "processed_at": None,
                    "status": "pending",  # Will be set to 'success' or 'failed' after processing
                    "attempts": 0,  # Track number of processing attempts
                    "last_error": None,  # Track last error if processing failed
                }
                new_files_added.append(file_id)

        # Save initial state to RAW for newly added files
        if new_files_added and use_cdf_format:
            logger.info(
                f"Saving initial state for {len(new_files_added)} newly added file(s) to RAW"
            )
            if raw_uploader:
                for file_id in new_files_added:
                    file_info = state_store[file_id]["file_info"]
                    file_name = (
                        file_info.get("name")
                        or file_info.get("external_id")
                        or f"file_{file_id}"
                    )
                    _save_state_to_raw(
                        raw_uploader,
                        cdf_config.parameters.raw_db,
                        cdf_config.parameters.raw_table_state,
                        file_id,
                        state_store[file_id],
                        results_field=results_field,
                    )
                try:
                    raw_uploader.upload()
                    logger.info(
                        f"Successfully saved initial state for {len(new_files_added)} file(s) to RAW"
                    )
                except Exception as e:
                    logger.warning(f"Error uploading initial state to RAW: {e}")
            else:
                # Use direct client calls
                for file_id in new_files_added:
                    try:
                        _save_state_to_raw_direct(
                            client,
                            cdf_config.parameters.raw_db,
                            cdf_config.parameters.raw_table_state,
                            file_id,
                            state_store[file_id],
                            results_field=results_field,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error saving state for file {file_id} to RAW: {e}"
                        )
                logger.info(
                    f"Successfully saved initial state for {len(new_files_added)} file(s) to RAW (direct)"
                )

        # Save reset state to RAW for re-uploaded files
        if files_reset and use_cdf_format:
            logger.info(
                f"Saving reset state for {len(files_reset)} re-uploaded file(s) to RAW"
            )
            if raw_uploader:
                for file_id in files_reset:
                    file_info = state_store[file_id]["file_info"]
                    file_name = (
                        file_info.get("name")
                        or file_info.get("external_id")
                        or f"file_{file_id}"
                    )
                    _save_state_to_raw(
                        raw_uploader,
                        cdf_config.parameters.raw_db,
                        cdf_config.parameters.raw_table_state,
                        file_id,
                        state_store[file_id],
                        results_field=results_field,
                    )
                try:
                    raw_uploader.upload()
                    logger.info(
                        f"Successfully saved reset state for {len(files_reset)} file(s) to RAW"
                    )
                except Exception as e:
                    logger.warning(f"Error uploading reset state to RAW: {e}")
            else:
                # Use direct client calls
                for file_id in files_reset:
                    try:
                        _save_state_to_raw_direct(
                            client,
                            cdf_config.parameters.raw_db,
                            cdf_config.parameters.raw_table_state,
                            file_id,
                            state_store[file_id],
                            results_field=results_field,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error saving reset state for file {file_id} to RAW: {e}"
                        )
                logger.info(
                    f"Successfully saved reset state for {len(files_reset)} file(s) to RAW (direct)"
                )

        # If in initialize_state mode, skip processing and return
        if use_cdf_format and initialize_state:
            logger.info(
                f"Update state mode complete: Added {len(new_files_added)} new file(s) to state, reset {len(files_reset)} re-uploaded file(s)"
            )
            status = "success"
            return

        # Process files in batches
        total_batches = (len(files) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(files))
            file_batch = files[start_idx:end_idx]

            logger.info(
                f"Processing batch {batch_num + 1}/{total_batches} ({len(file_batch)} file(s))"
            )

            # Process batch
            batch_results = process_batch(
                client=client,
                file_batch=file_batch,
                patterns=patterns,
                partial_match=partial_match,
                min_tokens=min_tokens,
                diagram_detect_config=diagram_detect_config,
                state_store=state_store,
                max_pages_per_chunk=max_pages_per_chunk,
                results_field=results_field,
                logger=logger,
            )

            # Update state_store with status from batch_results
            for file_id, result in batch_results.items():
                if file_id in state_store:
                    # Update status, attempts, and last_error from batch results
                    if result.get("status") == "failed":
                        state_store[file_id]["status"] = "failed"
                        state_store[file_id]["last_error"] = result.get("error")
                        # Increment attempts
                        state_store[file_id]["attempts"] = (
                            state_store[file_id].get("attempts", 0) + 1
                        )
                    elif (
                        result.get("status") == "success"
                        and state_store[file_id].get("status") != "success"
                    ):
                        # Status already set in process_batch for success, but ensure it's there
                        if (
                            "status" not in state_store[file_id]
                            or state_store[file_id]["status"] != "success"
                        ):
                            state_store[file_id]["status"] = "success"
                            state_store[file_id]["last_error"] = None

            # Save state (including results) to RAW after each batch
            if use_cdf_format:
                logger.info(
                    f"Saving state for batch {batch_num + 1}/{total_batches} to RAW"
                )
                for file_info in file_batch:
                    file_id = file_info["id"]

                    if file_id in state_store:
                        state_data = state_store[file_id]
                        # Save state (results are included in state_data)
                        if raw_uploader:
                            _save_state_to_raw(
                                raw_uploader,
                                cdf_config.parameters.raw_db,
                                cdf_config.parameters.raw_table_state,
                                file_id,
                                state_data,
                                results_field=results_field,
                            )
                        else:
                            _save_state_to_raw_direct(
                                client,
                                cdf_config.parameters.raw_db,
                                cdf_config.parameters.raw_table_state,
                                file_id,
                                state_data,
                                results_field=results_field,
                            )

                # Upload batch to RAW (only if using RawUploadQueue)
                if raw_uploader:
                    try:
                        raw_uploader.upload()
                        logger.info(
                            f"Successfully uploaded batch {batch_num + 1}/{total_batches} state to RAW ({len(file_batch)} file(s))"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error uploading batch {batch_num + 1} to RAW: {e}"
                        )
                else:
                    logger.info(
                        f"Successfully saved batch {batch_num + 1}/{total_batches} state to RAW (direct) ({len(file_batch)} file(s))"
                    )

            # Update data with results
            if "results" not in data:
                data["results"] = {}
            data["results"].update(batch_results)

        # Final upload to RAW
        if use_cdf_format and raw_uploader:
            try:
                raw_uploader.upload()
                logger.info("Successfully uploaded all state to RAW")
            except Exception as e:
                logger.error(f"Failed to upload final batch to RAW: {e}")
                raise

        status = "success"
        logger.info("Extract Annotation Tags Pipeline completed successfully")

        # Update pipeline run status
        if use_cdf_format and client:
            from cognite.client.data_classes import ExtractionPipelineRun
            from cognite.client.utils._text import shorten

            files_processed = sum(
                1 for s in state_store.values() if s.get(results_field) is not None
            )
            message = f"Successfully processed {files_processed} file(s)"

            try:
                pipeline_run = client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=pipeline_ext_id,
                        status=status,
                        message=shorten(message, 1000),
                    )
                )
                pipeline_run_id = pipeline_run.id
                logger.info(f"Pipeline run ID: {pipeline_run_id}")
            except Exception as e:
                logger.warning(f"Failed to create pipeline run: {e}")

        # Store results in data dict for return
        data["status"] = status
        data["pipeline_run_id"] = pipeline_run_id
        data["state_store"] = state_store

    except Exception as e:
        error_msg = f"Extract annotation tags pipeline failed: {str(e) if str(e) else type(e).__name__}"
        logger.error(error_msg)
        import traceback

        logger.debug(f"Traceback: {traceback.format_exc()}")

        # Update pipeline run with failure
        if use_cdf_format and client and pipeline_ext_id:
            try:
                from cognite.client.data_classes import ExtractionPipelineRun
                from cognite.client.utils._text import shorten

                client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=pipeline_ext_id,
                        status="failure",
                        message=shorten(error_msg, 1000),
                    )
                )
            except Exception:
                pass

        raise
