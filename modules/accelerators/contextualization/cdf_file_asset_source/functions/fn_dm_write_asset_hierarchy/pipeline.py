"""
CDF Pipeline for Write Asset Hierarchy

This module provides the main pipeline function that writes asset hierarchy
to CDF data modeling.
"""

import json
from typing import Any, Dict, List, Optional

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError

from .logger import CogniteFunctionLogger
from .utils.asset_utils import convert_to_cognite_asset, load_asset_hierarchy

logger = None  # Use CogniteFunctionLogger directly


def _load_assets_from_raw(
    client: CogniteClient,
    raw_db: str,
    raw_table_assets: str,
    logger: Optional[CogniteFunctionLogger] = None,
) -> List[Dict[str, Any]]:
    """Load assets from RAW table."""
    log = logger or CogniteFunctionLogger()
    assets = []

    # First verify table exists
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
        table_names = [tbl.name for tbl in tables]
        if raw_table_assets not in table_names:
            log.warning(f"Table '{raw_table_assets}' not found in database '{raw_db}'")
            return assets
    except Exception as e:
        log.warning(f"Error checking if table exists: {e}")
        return assets

    # Try to query the table
    try:
        # Note: table name should be passed as string, not list
        # Use limit=-1 to retrieve all rows
        rows = client.raw.rows.list(raw_db, raw_table_assets, limit=-1).to_pandas()
        if not rows.empty:
            for key, row in rows.iterrows():
                try:
                    # Parse asset_data column (stored as JSON string)
                    asset_data_json = row.get("asset_data", "{}")
                    if isinstance(asset_data_json, str):
                        asset_data = json.loads(asset_data_json)
                    else:
                        asset_data = (
                            asset_data_json if isinstance(asset_data_json, dict) else {}
                        )

                    if asset_data:
                        assets.append(asset_data)
                except (ValueError, json.JSONDecodeError) as e:
                    log.warning(f"Error parsing asset data for key {key}: {e}")
                    continue
            log.info(
                f"Loaded {len(assets)} asset(s) from RAW table {raw_db}.{raw_table_assets}"
            )
        else:
            log.info(f"Table '{raw_table_assets}' exists but is empty.")
    except CogniteAPIError as e:
        # Handle 404 or other API errors - table might be empty
        if "404" in str(e) or "not found" in str(e).lower():
            log.info(
                f"Table '{raw_table_assets}' appears to be empty or not accessible."
            )
        else:
            log.warning(f"Error loading assets from RAW: {e}")
    except Exception as e:
        log.warning(f"Error loading assets from RAW: {e}")

    return assets


def write_assets_to_cdf(
    client: CogniteClient,
    assets: List,
    batch_size: int = 100,
    dry_run: bool = False,
    logger: Any = None,
) -> Dict[str, int]:
    """Write assets to CDF in batches."""
    log = logger or CogniteFunctionLogger()
    stats = {
        "total": len(assets),
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "failed": 0,
    }

    if dry_run:
        log.info(f"DRY RUN MODE - Would write {len(assets)} asset(s)")
        return stats

    # Process in batches
    for i in range(0, len(assets), batch_size):
        batch = assets[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(assets) + batch_size - 1) // batch_size

        log.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch)} asset(s))..."
        )

        try:
            result = client.data_modeling.instances.apply(nodes=batch)

            # Count results
            for node_result in result.nodes:
                if node_result.was_modified:
                    if node_result.created_time == node_result.last_updated_time:
                        stats["created"] += 1
                    else:
                        stats["updated"] += 1
                else:
                    stats["unchanged"] += 1

            log.info(f"Batch {batch_num} completed")

        except CogniteAPIError as e:
            stats["failed"] += len(batch)
            log.error(f"API Error in batch {batch_num}: {e}")
            # Try to process individually to see which ones fail
            for asset in batch:
                try:
                    client.data_modeling.instances.apply(nodes=[asset])
                    stats["failed"] -= 1
                    stats["created"] += 1
                except Exception as individual_error:
                    log.error(f"Failed: {asset.external_id} - {individual_error}")
        except Exception as e:
            stats["failed"] += len(batch)
            log.error(f"Error in batch {batch_num}: {e}")

    return stats


def write_asset_hierarchy(
    client: CogniteClient,
    logger: Any,
    data: Dict[str, Any],
) -> None:
    """
    Main pipeline function for write asset hierarchy.

    This function writes asset hierarchy to CDF data modeling.

    Args:
        client: CogniteClient instance (required)
        logger: Logger instance (CogniteFunctionLogger or standard logger)
        data: Dictionary containing pipeline parameters and results
    """
    try:
        logger.info("Starting Write Asset Hierarchy Pipeline")

        # Extract parameters from data
        hierarchy_file = data.get("hierarchy_file")
        assets = data.get("assets")
        batch_size = data.get("batch_size", 100)
        dry_run = data.get("dry_run", False)
        view_space = data.get("view_space", "cdf_cdm")
        view_external_id = data.get("view_external_id", "CogniteAsset")
        view_version = data.get("view_version", "v1")

        # Always load from RAW table (both CDF and local mode)
        cdf_config = data.get("_cdf_config")
        is_local_mode = data.get(
            "_local_mode", False
        )  # Flag to indicate local execution

        # Get RAW table info from config or data
        raw_db = data.get("raw_db")
        raw_table_assets = data.get("raw_table_assets")
        if cdf_config is not None:
            raw_db = raw_db or cdf_config.parameters.raw_db
            raw_table_assets = (
                raw_table_assets or cdf_config.parameters.raw_table_assets
            )

        # Load assets if not provided
        if assets is None:
            # Always try to load from RAW first
            if client and raw_db and raw_table_assets:
                logger.info(
                    f"Loading assets from RAW table {raw_db}.{raw_table_assets}"
                )
                asset_data_list = _load_assets_from_raw(
                    client, raw_db, raw_table_assets, logger
                )
                if asset_data_list:
                    logger.info(f"Loaded {len(asset_data_list)} asset(s) from RAW")
                elif is_local_mode and hierarchy_file:
                    # Fallback to YAML file only when running locally and RAW is empty
                    from pathlib import Path

                    hierarchy_path = Path(hierarchy_file)
                    if hierarchy_path.exists():
                        logger.info(
                            f"RAW table is empty, falling back to YAML file: {hierarchy_file}"
                        )
                        asset_data_list = load_asset_hierarchy(hierarchy_path)
                        logger.info(
                            f"Loaded {len(asset_data_list)} asset(s) from {hierarchy_file}"
                        )
                    else:
                        raise ValueError(
                            f"No assets found in RAW table {raw_db}.{raw_table_assets} and YAML file not found: {hierarchy_file}"
                        )
                else:
                    raise ValueError(
                        f"No assets found in RAW table {raw_db}.{raw_table_assets}"
                    )
            elif is_local_mode and hierarchy_file:
                # Fallback to YAML file when running locally and RAW is not available
                from pathlib import Path

                hierarchy_path = Path(hierarchy_file)
                if not hierarchy_path.exists():
                    raise FileNotFoundError(
                        f"Hierarchy file not found: {hierarchy_file}"
                    )

                asset_data_list = load_asset_hierarchy(hierarchy_path)
                logger.info(
                    f"Loaded {len(asset_data_list)} asset(s) from {hierarchy_file}"
                )
            else:
                raise ValueError(
                    "Either assets, RAW table (raw_db + raw_table_assets), or hierarchy_file (local mode only) must be provided in data"
                )
        else:
            asset_data_list = assets

        # Convert to CogniteAssetApply instances
        logger.info("Converting to CogniteAsset instances...")
        cognite_assets = []
        skipped_properties = set()
        for asset_data in asset_data_list:
            try:
                asset = convert_to_cognite_asset(
                    asset_data,
                    view_space=view_space,
                    view_external_id=view_external_id,
                    view_version=view_version,
                )
                cognite_assets.append(asset)

                # Track skipped custom properties for reporting
                properties = asset_data.get("properties", {})
                for key in properties.keys():
                    if key not in [
                        "name",
                        "description",
                        "parent",
                        "tags",
                        "aliases",
                        "source_id",
                        "sourceId",
                        "source_context",
                        "sourceContext",
                        "source",
                        "asset_class",
                        "asset_type",
                        "type",
                        "object_3d",
                    ]:
                        skipped_properties.add(key)
            except Exception as e:
                logger.warning(
                    f"Error converting asset {asset_data.get('externalId', 'unknown')}: {e}"
                )

        logger.info(f"Converted {len(cognite_assets)} asset(s)")
        if skipped_properties:
            logger.info(
                f"Note: Custom properties were skipped (not in standard CogniteAsset schema): {', '.join(sorted(skipped_properties))}"
            )

        if dry_run:
            logger.info(f"DRY RUN MODE - No assets will be written to CDF")
            logger.info(f"Would write {len(cognite_assets)} asset(s)")
            data["stats"] = {
                "total": len(cognite_assets),
                "created": 0,
                "updated": 0,
                "unchanged": 0,
                "failed": 0,
            }
            return

        # Write assets to CDF
        logger.info(f"Writing {len(cognite_assets)} asset(s) to CDF...")
        logger.info(f"View: {view_space}/{view_external_id}/{view_version}")
        logger.info(f"Batch size: {batch_size}")

        stats = write_assets_to_cdf(
            client,
            cognite_assets,
            batch_size=batch_size,
            dry_run=dry_run,
            logger=logger,
        )

        # Store stats in data
        data["stats"] = stats

        # Print summary
        logger.info("Summary:")
        logger.info(f"Total assets: {stats['total']}")
        logger.info(f"Created: {stats['created']}")
        logger.info(f"Updated: {stats['updated']}")
        logger.info(f"Unchanged: {stats['unchanged']}")
        logger.info(f"Failed: {stats['failed']}")

        logger.info("Write Asset Hierarchy Pipeline completed successfully")

    except Exception as e:
        error_msg = f"Write asset hierarchy pipeline failed: {str(e)}"
        logger.error(error_msg)
        raise
