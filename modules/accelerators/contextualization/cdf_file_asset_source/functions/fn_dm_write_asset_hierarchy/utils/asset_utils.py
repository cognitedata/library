"""
Asset utility functions for write asset hierarchy.

This module provides utilities for converting and writing assets to CDF.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAssetApply


def load_asset_hierarchy(yaml_file: Path) -> List[Dict[str, Any]]:
    """Load asset hierarchy from YAML file."""
    with open(yaml_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data.get("items", [])


def convert_to_cognite_asset(
    asset_data: Dict[str, Any],
    view_space: str = "cdf_cdm",
    view_external_id: str = "CogniteAsset",
    view_version: str = "v1",
) -> CogniteAssetApply:
    """Convert asset data from YAML to CogniteAssetApply instance."""
    external_id = asset_data["externalId"]
    space = asset_data["space"]
    properties = asset_data.get("properties", {})

    # Extract name (required)
    name = properties.get("name", external_id)

    # Extract description
    description = properties.get("description")

    # Extract parent if present
    parent_ref = None
    if "parent" in properties:
        parent_data = properties["parent"]
        parent_ref = DirectRelationReference(
            space=parent_data["space"], external_id=parent_data["externalId"]
        )

    # Extract standard CogniteAsset properties
    standard_properties = {}

    # Map custom properties to standard ones if applicable
    if "tags" in properties:
        standard_properties["tags"] = (
            properties["tags"]
            if isinstance(properties["tags"], list)
            else [properties["tags"]]
        )
    if "aliases" in properties:
        standard_properties["aliases"] = (
            properties["aliases"]
            if isinstance(properties["aliases"], list)
            else [properties["aliases"]]
        )
    if "source_id" in properties:
        standard_properties["source_id"] = properties["source_id"]
    if "sourceId" in properties:
        standard_properties["source_id"] = properties["sourceId"]
    if "source_context" in properties:
        standard_properties["source_context"] = properties["source_context"]
    if "sourceContext" in properties:
        standard_properties["source_context"] = properties["sourceContext"]
    if "source" in properties:
        source_data = properties["source"]
        # Source can be either a string (file name) or a DirectRelationReference dict
        if isinstance(source_data, dict):
            standard_properties["source"] = DirectRelationReference(
                space=source_data["space"], external_id=source_data["externalId"]
            )
    if "asset_class" in properties:
        asset_class_data = properties["asset_class"]
        if isinstance(asset_class_data, dict):
            standard_properties["asset_class"] = DirectRelationReference(
                space=asset_class_data["space"],
                external_id=asset_class_data["externalId"],
            )
    if "asset_type" in properties:
        asset_type_data = properties["asset_type"]
        if isinstance(asset_type_data, dict):
            standard_properties["asset_type"] = DirectRelationReference(
                space=asset_type_data["space"],
                external_id=asset_type_data["externalId"],
            )
    if "type" in properties:
        type_data = properties["type"]
        if isinstance(type_data, dict):
            standard_properties["type"] = DirectRelationReference(
                space=type_data["space"], external_id=type_data["externalId"]
            )

    # Create CogniteAssetApply instance
    asset = CogniteAssetApply(
        space=space,
        external_id=external_id,
        name=name,
        description=description,
        parent=parent_ref,
        **standard_properties,
    )

    return asset
