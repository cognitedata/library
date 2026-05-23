"""
Shared hierarchy utility functions.

This module provides common hierarchy utilities used across multiple functions
to avoid code duplication.
"""

from typing import Any, Dict, Optional


def create_asset_instance(
    external_id: str,
    name: str,
    description: Optional[str] = None,
    parent_external_id: Optional[str] = None,
    space: str = "sp_enterprise_schema",
    level: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create an asset instance with NodeReference for parent.

    This is a shared utility function used by multiple pipeline functions
    to create consistent asset instances.

    Args:
        external_id: Unique external ID for the asset
        name: Asset name
        description: Optional asset description
        parent_external_id: Optional parent asset external ID
        space: Instance space for the asset
        level: Optional hierarchy level name (used as tag)
        **kwargs: Additional properties to add to the asset

    Returns:
        Dictionary representing the asset instance
    """
    asset = {"externalId": external_id, "space": space, "properties": {"name": name}}

    if description:
        asset["properties"]["description"] = description

    # Add parent using NodeReference format
    if parent_external_id:
        asset["properties"]["parent"] = {
            "space": space,
            "externalId": parent_external_id,
        }

    # Add tags property (hierarchy level)
    if level:
        asset["properties"]["tags"] = [level]  # CogniteAsset tags is a list

    # Add any additional properties
    for key, value in kwargs.items():
        if value is not None and value != "":
            asset["properties"][key] = value

    return asset
