#!/usr/bin/env python3
"""
Script to delete assets from CDF data modeling.
Supports filtering by root externalId and/or space.
"""

import sys
from pathlib import Path
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from cognite.client.exceptions import CogniteAPIError

from modules.create_asset_hierarchy_from_files.functions.fn_dm_extract_assets_by_pattern.dependencies import (
    create_client,
    get_env_variables,
)
from modules.create_asset_hierarchy_from_files.functions.fn_dm_extract_assets_by_pattern.logger import (
    CogniteFunctionLogger,
)


def delete_assets(
    root_external_id: Optional[str] = None,
    space: str = "sp_enterprise_schema",
    view_space: str = "cdf_cdm",
    view_external_id: str = "CogniteAsset",
    view_version: str = "v1",
    dry_run: bool = True,
) -> None:
    """
    Delete assets from CDF data modeling.

    By default, this runs in dry-run mode (safe mode) and only shows what would be deleted.
    Use --force to actually perform the deletion.

    Args:
        root_external_id: Optional root asset externalId to filter by (e.g., "site_BLO_PID")
        space: Instance space to filter by (required, e.g., "sp_enterprise_schema")
        view_space: View space for data modeling
        view_external_id: View external ID
        view_version: View version
        dry_run: If True, only show what would be deleted without actually deleting (default: True)
    """
    logger = CogniteFunctionLogger(log_level="INFO")
    env_config = get_env_variables()
    client = create_client(env_config)

    logger.info("=" * 80)
    if root_external_id:
        logger.info(f"Deleting assets with root: {root_external_id}")
    if space:
        logger.info(f"Deleting assets in space: {space}")
    logger.info("=" * 80)
    logger.info(f"View: {view_space}.{view_external_id}/{view_version}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("")

    from cognite.client import data_modeling as dm
    from cognite.client.data_classes.data_modeling.ids import ViewId

    # Build view ID
    view_id = ViewId(
        space=view_space, external_id=view_external_id, version=view_version
    )

    try:
        # Build filter expression based on provided filters
        filter_expressions = []

        if root_external_id:
            # Filter by root property
            root_property_ref = view_id.as_property_ref("root")
            filter_expressions.append(
                dm.filters.Equals(
                    property=root_property_ref,
                    value={"space": space, "externalId": root_external_id},
                )
            )
            logger.info(f"Filter: root.externalId == {root_external_id}")

        # Combine filters with AND if multiple
        if len(filter_expressions) > 1:
            filter_expr = dm.filters.And(filter_expressions)
        elif len(filter_expressions) == 1:
            filter_expr = filter_expressions[0]
        else:
            filter_expr = None

        # Query instances (space is always required)
        logger.info("Querying for assets...")
        query_kwargs = {
            "instance_type": "node",
            "space": [space],
            "sources": [view_id],
            "limit": None,
        }

        if filter_expr:
            query_kwargs["filter"] = filter_expr

        all_instances = list(client.data_modeling.instances.list(**query_kwargs))

        # If filtering by root, also include the root asset itself if not already in the list
        assets_to_delete = []
        root_found = False

        if root_external_id:
            for instance in all_instances:
                if instance.external_id == root_external_id:
                    root_found = True
                assets_to_delete.append(
                    {
                        "space": instance.space,
                        "externalId": instance.external_id,
                    }
                )

            # Add root if not found in filtered results
            if not root_found:
                assets_to_delete.append(
                    {
                        "space": space,
                        "externalId": root_external_id,
                    }
                )
                logger.info("Added root asset to deletion list")

            logger.info(
                f"Found {len(all_instances)} asset(s) with root '{root_external_id}'"
            )
            logger.info(
                f"Total {len(assets_to_delete)} asset(s) to delete (root + all assets with matching root property)"
            )
        else:
            # Filtering by space only
            for instance in all_instances:
                assets_to_delete.append(
                    {
                        "space": instance.space,
                        "externalId": instance.external_id,
                    }
                )

            logger.info(f"Found {len(assets_to_delete)} asset(s) in space '{space}'")

        if not assets_to_delete:
            logger.info("No assets found to delete.")
            return

        # Show what will be deleted
        logger.info("\nAssets to delete:")
        for i, asset in enumerate(assets_to_delete[:30], 1):  # Show first 30
            logger.info(f"  {i}. [{asset['space']}] {asset['externalId']}")
        if len(assets_to_delete) > 30:
            logger.info(f"  ... and {len(assets_to_delete) - 30} more")

        if dry_run:
            logger.info(
                f"\n🔍 DRY RUN MODE - Would delete {len(assets_to_delete)} asset(s)"
            )
            logger.info("⚠️  No assets were actually deleted.")
            logger.info("💡 To actually delete, run with --force flag")
            return

        # Confirm deletion
        logger.info(f"\n⚠️  WARNING: About to delete {len(assets_to_delete)} asset(s)")
        logger.info("⚠️  This action cannot be undone!")
        logger.info("⚠️  Deletion will proceed in 3 seconds...")
        import time

        time.sleep(3)

        # Delete in batches
        batch_size = 100
        deleted_count = 0
        failed_count = 0

        for i in range(0, len(assets_to_delete), batch_size):
            batch = assets_to_delete[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(assets_to_delete) + batch_size - 1) // batch_size

            try:
                logger.info(
                    f"Deleting batch {batch_num}/{total_batches} ({len(batch)} asset(s))..."
                )

                # Delete using data modeling API
                # Use NodeId objects as expected by the SDK
                from cognite.client.data_classes.data_modeling import NodeId

                nodes_to_delete = [
                    NodeId(space=asset["space"], external_id=asset["externalId"])
                    for asset in batch
                ]

                # Try the delete call and check the response
                result = client.data_modeling.instances.delete(nodes=nodes_to_delete)

                # Log result details - check what the result actually contains
                logger.debug(f"Delete result type: {type(result)}")
                logger.debug(f"Delete result: {result}")

                # The delete API may not return confirmation, so we'll verify after all deletions
                logger.info(f"Batch {batch_num} delete API call completed")

                deleted_count += len(batch)
                logger.info(f"Batch {batch_num} deleted successfully")

            except CogniteAPIError as e:
                failed_count += len(batch)
                logger.error(f"Error deleting batch {batch_num}: {e}")
                logger.error(
                    f"Error details: {e.message if hasattr(e, 'message') else str(e)}"
                )
                # Try to continue with next batch
                continue
            except Exception as e:
                failed_count += len(batch)
                logger.error(f"Error deleting batch {batch_num}: {e}")
                import traceback

                logger.error(traceback.format_exc())
                # Try to continue with next batch
                continue

        logger.info("")
        logger.info("=" * 80)
        logger.info("SUMMARY:")
        logger.info(f"  Total assets found: {len(assets_to_delete)}")
        logger.info(f"  Successfully deleted: {deleted_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info("=" * 80)

        # Verify deletion by querying again
        if not dry_run and deleted_count > 0:
            logger.info("")
            logger.info("Verifying deletion...")
            remaining_instances = list(
                client.data_modeling.instances.list(
                    instance_type="node",
                    space=[space],
                    sources=[view_id],
                    filter=filter_expr,
                    limit=10,  # Just check a few
                )
            )
            if remaining_instances:
                logger.warning(
                    f"⚠️  WARNING: {len(remaining_instances)} asset(s) still found after deletion!"
                )
                logger.warning(
                    "This may indicate the deletion did not complete successfully."
                )
            else:
                logger.info(
                    "✓ Verification: No assets found with matching filters (deletion successful)"
                )

    except Exception as e:
        logger.error(f"Error during deletion: {e}")
        import traceback

        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Delete assets from CDF. Filter by root externalId and/or space.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete all assets in a specific space (dry-run by default)
  %(prog)s --space sp_enterprise_schema

  # Delete all assets with a specific root in a space (dry-run by default)
  %(prog)s --root site_BLO_PID --space sp_enterprise_schema

  # Actually delete (requires --force)
  %(prog)s --root site_BLO_PID --space sp_enterprise_schema --force
        """,
    )
    parser.add_argument(
        "--root",
        type=str,
        dest="root_external_id",
        help="Root asset externalId to filter by (e.g., 'site_BLO_PID')",
    )
    parser.add_argument(
        "--space",
        type=str,
        required=True,
        help="Instance space to filter by (required, e.g., 'sp_enterprise_schema')",
    )
    parser.add_argument(
        "--view-space",
        type=str,
        default="cdf_cdm",
        dest="view_space",
        help="View space (default: cdf_cdm)",
    )
    parser.add_argument(
        "--view-external-id",
        type=str,
        default="CogniteAsset",
        dest="view_external_id",
        help="View external ID (default: CogniteAsset)",
    )
    parser.add_argument(
        "--view-version",
        type=str,
        default="v1",
        dest="view_version",
        help="View version (default: v1)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Actually delete the assets (default is dry-run mode for safety)",
    )

    args = parser.parse_args()

    # Default to dry-run unless --force is provided
    dry_run = not args.force

    try:
        delete_assets(
            root_external_id=args.root_external_id,
            space=args.space,
            view_space=args.view_space,
            view_external_id=args.view_external_id,
            view_version=args.view_version,
            dry_run=dry_run,
        )
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
