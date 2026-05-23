"""
CDF Pipeline for Create Asset Hierarchy

This module provides the main pipeline function that creates asset hierarchy
from locations and extracted tags.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    from cognite.client import CogniteClient
    from cognite.client.data_classes import Row

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False
    CogniteClient = None
    Row = None

from .logger import CogniteFunctionLogger
from .utils.location_utils import load_extracted_assets

# Import asset tag classifier
try:
    import sys
    from pathlib import Path

    # Add module root to path to import asset_tag_classifier
    # Path structure: .../create_asset_hierarchy_from_files/functions/fn_dm_create_asset_hierarchy/pipeline.py
    # We need: .../create_asset_hierarchy_from_files/ (3 levels up)
    module_root = Path(__file__).parent.parent.parent
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))
    from asset_tag_classifier import AssetTagClassifier

    CLASSIFIER_AVAILABLE = True
    _classifier_import_error = None
except ImportError as e:
    CLASSIFIER_AVAILABLE = False
    AssetTagClassifier = None
    # Store error for debugging
    _classifier_import_error = str(e)

logger = None  # Use CogniteFunctionLogger directly


def _load_results_from_state_table(
    client: CogniteClient,
    raw_db: str,
    raw_table_state: str,
    results_field: str = "results",
    logger: Optional[CogniteFunctionLogger] = None,
    limit: Optional[int] = -1,
    batch_size: Optional[int] = None,
) -> Dict[int, Dict[str, Any]]:
    """
    Load results from state table's results field.

    Args:
        client: CogniteClient instance
        raw_db: RAW database name
        raw_table_state: RAW state table name
        results_field: Field name for results in state table
        logger: Logger instance
        limit: Maximum number of files to load (-1 for no limit)
        batch_size: Number of files to process per batch (None for no batching)

    Returns:
        Dictionary mapping file_id to results data
    """
    from cognite.client.exceptions import CogniteAPIError

    log = logger or CogniteFunctionLogger()
    results_store = {}

    # Check if state table exists first
    try:
        tables = client.raw.tables.list(raw_db, limit=-1).as_names()
        if raw_table_state not in tables:
            log.warning(
                f"State table '{raw_table_state}' not found in database '{raw_db}'"
            )
            return results_store
    except Exception as e:
        log.warning(
            f"Error checking if state table exists: {e}. Attempting to load anyway..."
        )

    # Try to query the table
    try:
        # Always use limit=-1 to get all rows, then apply limit/batch_size in processing
        log.info(
            f"Loading results from state table (limit: {limit if limit > 0 else 'unlimited'}, batch_size: {batch_size or 'none'})..."
        )
        rows = client.raw.rows.list(raw_db, raw_table_state, limit=-1).to_pandas()

        if not rows.empty:
            total_loaded = 0
            batch_count = 0

            for key, row in rows.iterrows():
                # Check limit
                if limit > 0 and total_loaded >= limit:
                    log.info(f"Reached limit of {limit} files. Stopping.")
                    break

                try:
                    file_id = int(key)

                    # Parse state column (stored as JSON string)
                    state_json = row.get("state", "{}")
                    if isinstance(state_json, str):
                        state_data = json.loads(state_json)
                    else:
                        state_data = state_json if isinstance(state_json, dict) else {}

                    # Get file info from state
                    file_info = state_data.get("file_info", {})
                    file_name = file_info.get("name", f"file_{file_id}")

                    # Get results from top-level column first, then from state JSON
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
                        except (ValueError, json.JSONDecodeError) as e:
                            log.warning(
                                f"Error parsing top-level results for file_id {file_id}: {e}"
                            )
                            # Fall back to results in state JSON
                            results_data = state_data.get(results_field, {})
                    else:
                        # Get results from state JSON
                        results_data = state_data.get(results_field, {})

                    # Only include files that have results
                    if (
                        results_data
                        and isinstance(results_data, dict)
                        and results_data.get("items")
                    ):
                        # Store results with file info
                        results_store[file_id] = {
                            "file_id": file_id,
                            "file_name": file_name,
                            "results": results_data,
                            "processed_at": state_data.get("updated_at", ""),
                        }
                        total_loaded += 1
                        batch_count += 1

                        # Log batch progress if batch_size is specified
                        if batch_size and batch_size > 0 and batch_count >= batch_size:
                            log.debug(
                                f"Processed batch: {batch_count} file(s) (total: {total_loaded})"
                            )
                            batch_count = 0

                except (ValueError, json.JSONDecodeError) as e:
                    log.warning(f"Error parsing state for key {key}: {e}")
                    continue

            log.info(
                f"Loaded results for {len(results_store)} file(s) from state table"
            )
        else:
            log.info(
                f"State table '{raw_table_state}' exists but is empty. Run the extraction pipeline to populate it."
            )
    except CogniteAPIError as e:
        # Handle 404 or other API errors - table might be empty
        if "404" in str(e) or "not found" in str(e).lower():
            log.info(
                f"State table '{raw_table_state}' appears to be empty or not accessible. This is normal if the extraction pipeline hasn't been run yet."
            )
        else:
            log.warning(f"Error loading results from state table: {e}")
    except Exception as e:
        log.warning(f"Error loading results from state table: {e}")

    return results_store


def _build_asset_list_from_state(state_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build asset list from a state entry."""
    assets = []
    file_id = state_entry.get("file_id", "unknown")

    try:
        file_info = state_entry.get("file_info", {})
        file_id = file_info.get("id") or file_id
        file_name = file_info.get("name")
        file_external_id = file_info.get("external_id")

        if not file_id or not file_name:
            return assets

        results = state_entry.get("results", {})
        items = results.get("items", [])

        for item in items:
            annotations = item.get("annotations", [])
            for annotation in annotations:
                confidence = annotation.get("confidence")
                text = annotation.get("text")
                entities = annotation.get("entities", [])

                for entity in entities:
                    # Extract sample as text
                    sample = entity.get("sample", text)

                    # Extract all other fields excluding sample
                    asset_data = {
                        "file_id": file_id,
                        "file_name": file_name,
                        "confidence": confidence,
                        "text": sample,
                    }

                    # Add file_external_id if available
                    if file_external_id:
                        asset_data["file_external_id"] = file_external_id

                    # Add all other entity fields
                    for key, value in entity.items():
                        if key != "sample":
                            asset_data[key] = value

                    assets.append(asset_data)

    except Exception as e:
        raise Exception(f"Error processing state entry for file_id {file_id}: {e}")

    return assets


def _remove_duplicates(assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate assets within the same file (same file_id, file_name, and text)."""
    seen = set()
    unique_assets = []

    for asset in assets:
        # Create a unique key based on file_id, file_name, and text
        key = (asset["file_id"], asset["file_name"], asset["text"])

        if key not in seen:
            seen.add(key)
            unique_assets.append(asset)

    return unique_assets


def get_asset_list(
    client: Optional[CogniteClient],
    raw_db: str,
    raw_table_state: str,
    results_field: str = "results",
    logger: Optional[CogniteFunctionLogger] = None,
    limit: Optional[int] = -1,
    batch_size: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get asset list from state table's results field.

    Args:
        client: CogniteClient instance (required)
        raw_db: RAW database name
        raw_table_state: RAW state table name
        results_field: Field name for results in state table
        logger: Logger instance
        limit: Maximum number of files to process (-1 for no limit)
        batch_size: Number of files to process per batch (None for no batching)

    Returns:
        List of unique asset dictionaries
    """
    log = logger or CogniteFunctionLogger()

    if not client:
        raise ValueError("Client is required to load results from RAW")

    # Load results from state table
    results_store = _load_results_from_state_table(
        client,
        raw_db,
        raw_table_state,
        results_field,
        log,
        limit=limit,
        batch_size=batch_size,
    )

    if not results_store:
        log.warning("No results found in state table")
        return []

    # Build asset list from all results entries
    all_assets = []
    files_with_results = 0
    for file_id, result_entry in results_store.items():
        results = result_entry.get("results", {})
        if results and "items" in results:
            # Convert results entry to state-like format for build_asset_list_from_state
            state_like_entry = {
                "file_id": file_id,
                "file_info": {
                    "id": file_id,
                    "name": result_entry.get("file_name", f"file_{file_id}"),
                },
                "results": results,
            }
            assets = _build_asset_list_from_state(state_like_entry)
            all_assets.extend(assets)
            if assets:
                files_with_results += 1
                file_name = result_entry.get("file_name", f"file_{file_id}")
                log.debug(f"{file_name}: {len(assets)} asset(s)")

    if files_with_results == 0:
        log.warning("No files with results found in results store")
        return []

    log.info(f"Total assets built: {len(all_assets)}")
    log.info(f"Files with results: {files_with_results}")

    # Remove duplicates
    unique_assets = _remove_duplicates(all_assets)
    log.info(f"Unique assets (after deduplication): {len(unique_assets)}")

    return unique_assets


def _save_assets_to_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_assets: str,
    assets: List[Dict[str, Any]],
    logger: Optional[CogniteFunctionLogger] = None,
) -> None:
    """Save assets to RAW table."""
    from cognite.client.exceptions import CogniteAPIError

    log = logger or CogniteFunctionLogger()

    # Ensure table exists
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
        table_names = [tbl.name for tbl in tables]
        if raw_table_assets not in table_names:
            client.raw.tables.create(raw_db, raw_table_assets)
            log.info(f"Created RAW table: {raw_db}.{raw_table_assets}")
    except Exception as e:
        log.warning(f"Error ensuring table exists: {e}")

    # Save each asset as a row (using externalId as key)
    rows = []
    for asset in assets:
        try:
            external_id = asset.get("externalId", "")
            if not external_id:
                continue

            columns = {
                "asset_data": json.dumps(asset, default=str),
                "external_id": external_id,
                "space": asset.get("space", ""),
                "name": asset.get("properties", {}).get("name", ""),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            row = Row(key=external_id, columns=columns)
            rows.append(row)
        except Exception as e:
            log.warning(
                f"Error preparing asset {asset.get('externalId', 'unknown')} for RAW: {e}"
            )

    # Insert in batches
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            client.raw.rows.insert(
                db_name=raw_db, table_name=raw_table_assets, row=batch
            )
            log.debug(f"Saved batch of {len(batch)} asset(s) to RAW")
        except CogniteAPIError as e:
            log.error(f"Error saving batch to RAW: {e}")
            raise
        except Exception as e:
            log.error(f"Error saving batch to RAW: {e}")
            raise

    log.info(f"Saved {len(rows)} asset(s) to RAW table {raw_db}.{raw_table_assets}")


def create_asset_hierarchy(
    client: Optional[CogniteClient],
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Main pipeline function for create asset hierarchy.

    This function creates asset hierarchy from locations and extracted assets.

    Args:
        client: CogniteClient instance (required if loading assets from RAW)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing pipeline parameters and results
    """
    try:
        logger.info("Starting Create Asset Hierarchy Pipeline")

        # Extract parameters from data
        tags_file = data.get("tags_file")
        output_file = data.get("output_file")
        space = data.get("space", "inst_enterprise_file_assets")
        include_resource_type = data.get("include_resource_type", False)
        include_resource_subtype = data.get("include_resource_subtype", False)
        include_resource_subsubtype = data.get("include_resource_subsubtype", False)
        include_resource_variant = data.get("include_resource_variant", False)
        locations = data.get("locations")
        tags = data.get("tags")
        hierarchy_levels = data.get("hierarchy_levels")

        if locations is None:
            raise ValueError(
                "locations must be provided in data (handler converts config scope tree to a flat list)"
            )

        # Load tags/assets if not provided
        if tags is None:
            # Check if we should load from RAW results table
            cdf_config = data.get("_cdf_config")
            use_cdf_format = cdf_config is not None and client is not None

            if use_cdf_format:
                # Load assets from state table's results field
                raw_db = cdf_config.parameters.raw_db
                raw_table_state = cdf_config.parameters.raw_table_state
                results_field = cdf_config.parameters.results_field
                limit = (
                    cdf_config.parameters.limit
                    if hasattr(cdf_config.parameters, "limit")
                    else -1
                )
                batch_size = (
                    cdf_config.parameters.batch_size
                    if hasattr(cdf_config.parameters, "batch_size")
                    else None
                )
                logger.info(
                    f"Loading assets from state table {raw_db}.{raw_table_state} (results_field: {results_field}, limit: {limit if limit > 0 else 'unlimited'}, batch_size: {batch_size or 'none'})"
                )
                tags = get_asset_list(
                    client,
                    raw_db,
                    raw_table_state,
                    results_field,
                    logger,
                    limit=limit,
                    batch_size=batch_size,
                )
                logger.info(f"Loaded {len(tags)} asset(s) from state table")
            elif tags_file:
                # Fallback to CSV file
                tags_path = Path(tags_file)
                if not tags_path.exists():
                    raise FileNotFoundError(
                        f"Extracted tags file not found: {tags_file}"
                    )
                tags = load_extracted_assets(tags_path)
                logger.info(f"Loaded {len(tags)} tag(s) from {tags_file}")
            else:
                raise ValueError(
                    "Either tags, tags_file, or CDF config (for RAW loading) must be provided in data"
                )

        # Classify assets if classifier is available and pattern config is provided
        pattern_config_path = data.get("pattern_config_path")
        if pattern_config_path and CLASSIFIER_AVAILABLE and tags:
            try:
                pattern_path = Path(pattern_config_path)
                if not pattern_path.is_absolute():
                    # Config path is relative to module root, but we need to resolve it from project root
                    # Try relative to module root first, then try from project root
                    module_root = Path(__file__).parent.parent.parent
                    pattern_path_module = module_root / pattern_config_path
                    # Also try from project root (one level up from module_root)
                    project_root = module_root.parent
                    pattern_path_project = project_root / pattern_config_path

                    if pattern_path_module.exists():
                        pattern_path = pattern_path_module
                    elif pattern_path_project.exists():
                        pattern_path = pattern_path_project
                    else:
                        pattern_path = pattern_path_module  # Use module root path for error message

                if pattern_path.exists():
                    logger.info(
                        f"Classifying {len(tags)} asset(s) using pattern config: {pattern_path}"
                    )
                    # Find document patterns file (same directory as asset patterns)
                    document_patterns_path = (
                        pattern_path.parent / "document_patterns.yaml"
                    )
                    if not document_patterns_path.exists():
                        document_patterns_path = None
                    classifier = AssetTagClassifier(
                        pattern_path, document_patterns_path=document_patterns_path
                    )
                    # Classify using 'text' field as the tag field, skip already classified items
                    tags = classifier.classify_assets(
                        tags, tag_field="text", skip_classified=True
                    )
                    logger.info(
                        f"Classification complete. {len(tags)} asset(s) processed."
                    )
                else:
                    logger.warning(
                        f"Pattern config file not found: {pattern_path}. Skipping classification."
                    )
            except Exception as e:
                logger.warning(
                    f"Error during asset classification: {e}. Continuing without classification."
                )
        elif pattern_config_path and not CLASSIFIER_AVAILABLE:
            logger.warning(
                f"Asset tag classifier not available. Skipping classification. Error: {_classifier_import_error if '_classifier_import_error' in globals() else 'Unknown'}"
            )
        elif pattern_config_path and not tags:
            logger.warning("No tags to classify. Skipping classification.")

        # Generate hierarchy
        from .utils.hierarchy_utils import generate_hierarchy

        logger.info(
            f"Generating asset hierarchy (space: {space}, include_resource_type: {include_resource_type}, include_resource_subtype: {include_resource_subtype}, include_resource_subsubtype: {include_resource_subsubtype}, include_resource_variant: {include_resource_variant}, hierarchy_levels: {hierarchy_levels})..."
        )
        assets = generate_hierarchy(
            locations,
            tags,
            space=space,
            include_resource_subtype=include_resource_subtype,
            include_resource_type=include_resource_type,
            include_resource_subsubtype=include_resource_subsubtype,
            include_resource_variant=include_resource_variant,
            hierarchy_levels=hierarchy_levels,
        )
        logger.info(f"Generated {len(assets)} asset instance(s)")

        # Store results in data
        data["assets"] = assets

        # Always save to RAW table (both CDF and local mode)
        cdf_config = data.get("_cdf_config")
        is_local_mode = data.get(
            "_local_mode", False
        )  # Flag to indicate local execution

        # Get RAW table info from config or data
        if cdf_config is not None:
            raw_db = cdf_config.parameters.raw_db
            raw_table_assets = cdf_config.parameters.raw_table_assets
        else:
            # Fallback to data if no config (shouldn't happen in normal flow)
            raw_db = data.get("raw_db")
            raw_table_assets = data.get("raw_table_assets")

        if client and raw_db and raw_table_assets:
            # Always save to RAW table
            logger.info(
                f"Saving {len(assets)} asset(s) to RAW table {raw_db}.{raw_table_assets}"
            )
            _save_assets_to_raw(client, raw_db, raw_table_assets, assets, logger)
            logger.info(f"Assets saved to RAW table")
        else:
            logger.warning(
                "Cannot save to RAW: missing client, raw_db, or raw_table_assets"
            )

        # Additionally write YAML file when running locally (for ease of review)
        if is_local_mode and output_file:
            output_path = Path(output_file)
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Create YAML structure for CDF data modeling
            output_data = {"items": assets}

            # Write to YAML file
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(
                    output_data,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    allow_unicode=True,
                )

            logger.info(f"Asset hierarchy also saved to YAML for review: {output_file}")

        logger.info(f"Total assets: {len(assets)}")

        # Print summary
        site_count = sum(1 for a in assets if "parent" not in a.get("properties", {}))
        logger.info(f"Root assets (sites): {site_count}")

        logger.info("Create Asset Hierarchy Pipeline completed successfully")

    except Exception as e:
        error_msg = f"Create asset hierarchy pipeline failed: {str(e)}"
        logger.error(error_msg)
        raise
