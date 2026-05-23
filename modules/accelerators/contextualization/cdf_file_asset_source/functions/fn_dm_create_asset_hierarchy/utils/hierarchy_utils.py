"""
Hierarchy utility functions for create asset hierarchy.

This module provides utilities for generating asset hierarchies.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional

# Import shared utilities to avoid duplication
try:
    from ...shared.utils.hierarchy_utils import create_asset_instance
except ImportError:
    # Fallback if shared module not available
    from typing import Any, Dict, Optional

    def create_asset_instance(
        external_id: str,
        name: str,
        description: Optional[str] = None,
        parent_external_id: Optional[str] = None,
        space: str = "inst_enterprise_file_assets",
        level: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Create an asset instance with NodeReference for parent."""
        asset = {
            "externalId": external_id,
            "space": space,
            "properties": {"name": name},
        }

        if description:
            asset["properties"]["description"] = description

        if parent_external_id:
            asset["properties"]["parent"] = {
                "space": space,
                "externalId": parent_external_id,
            }

        if level:
            asset["properties"]["tags"] = [level]

        for key, value in kwargs.items():
            if value is not None and value != "":
                asset["properties"][key] = value

        return asset


from .location_utils import match_file_to_system


def build_tag_description(
    resource_sub_type_formatted: str,
    hierarchy_levels: List[str],
    level_external_ids: Dict[str, str],
    base_descriptions_by_external_id: Dict[str, str],
    level_codes: Dict[str, str],
) -> str:
    """Build tag description using dynamic hierarchy levels.

    Args:
        resource_sub_type_formatted: Formatted resource subtype and text
        hierarchy_levels: List of hierarchy level names in order
        level_external_ids: Dictionary mapping level names to external_ids
        base_descriptions_by_external_id: Dictionary mapping external_ids to base descriptions
        level_codes: Dictionary mapping level names to codes

    Returns:
        Formatted tag description string
    """
    # Build description parts in reverse order (from last level to first)
    description_parts = []
    for level in reversed(hierarchy_levels):
        level_external_id = level_external_ids.get(level)
        if level_external_id:
            level_desc = base_descriptions_by_external_id.get(
                level_external_id, level_codes.get(level, "")
            )
            description_parts.append(level_desc)

    # Format: "{resource_sub_type} for {last_level} in {second_to_last} of {third_to_last} at {first_level}"
    if len(description_parts) >= 4:
        return f"{resource_sub_type_formatted} for {description_parts[0]} in {description_parts[1]} of {description_parts[2]} at {description_parts[3]}"
    elif len(description_parts) == 3:
        return f"{resource_sub_type_formatted} for {description_parts[0]} in {description_parts[1]} at {description_parts[2]}"
    elif len(description_parts) == 2:
        return f"{resource_sub_type_formatted} for {description_parts[0]} at {description_parts[1]}"
    else:
        return f"{resource_sub_type_formatted} for {description_parts[0] if description_parts else 'unknown'}"


def generate_hierarchy(
    locations: List[Dict[str, str]],
    tags: List[Dict[str, Any]],
    space: str = "inst_enterprise_file_assets",
    include_resource_subtype: bool = False,
    include_resource_type: bool = False,
    include_resource_subsubtype: bool = False,
    include_resource_variant: bool = False,
    hierarchy_levels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Generate the complete asset hierarchy.

    Args:
        locations: List of location dictionaries
        tags: List of tag dictionaries
        space: Instance space for assets
        include_resource_subtype: Include resourceSubType (equipment_class_name) as intermediate level
        include_resource_type: Include resourceType (tag_class_name) as intermediate level
        include_resource_subsubtype: Include resourceSubSubType (equipment_subclass_name) as intermediate level
        include_resource_variant: Include resourceVariant (equipment_variant_name) as intermediate level
        hierarchy_levels: List of hierarchy level names in order (e.g., ['site', 'plant', 'area', 'system'])
                         If None, defaults to ['site', 'plant', 'area', 'system']
    """
    # Default hierarchy levels when not configured
    if hierarchy_levels is None:
        hierarchy_levels = ["site", "unit", "area", "system"]

    if not hierarchy_levels:
        return []

    assets = []
    created_assets = set()  # Track created external_ids to avoid duplicates

    # Track which files belong to which system
    file_to_location = {}
    for tag in tags:
        file_name = tag.get("file_name", "")
        if file_name and file_name not in file_to_location:
            location = match_file_to_system(file_name, locations)
            if location:
                file_to_location[file_name] = location

    # Build system key from all hierarchy levels
    def get_location_key(location: Dict[str, str]) -> tuple:
        """Get a tuple key from location using all hierarchy levels."""
        return tuple(location.get(f"{level}_code", "") for level in hierarchy_levels)

    # Group tags by file_name and location key
    tags_by_file_and_location = defaultdict(lambda: defaultdict(list))
    for tag in tags:
        file_name = tag.get("file_name", "")
        if file_name and file_name in file_to_location:
            location = file_to_location[file_name]
            location_key = get_location_key(location)
            tags_by_file_and_location[location_key][file_name].append(tag)

    # Create hierarchy for each unique location
    processed_locations = set()
    # Track cumulative descriptions by external_id for all hierarchy levels
    descriptions_by_external_id = {}
    # Track base descriptions (non-cumulative) for use in asset_tag descriptions
    base_descriptions_by_external_id = {}

    for location in locations:
        # Create unique key for location
        loc_key = get_location_key(location)

        # Skip if location key is already processed
        # Note: We allow partial hierarchy levels (some codes may be empty) for files at intermediate levels
        if loc_key in processed_locations:
            continue

        # Find the deepest non-empty level (for files at intermediate levels)
        deepest_level_index = -1
        for i, code in enumerate(loc_key):
            if code:
                deepest_level_index = i

        # Skip if no levels have codes
        if deepest_level_index == -1:
            continue

        processed_locations.add(loc_key)

        # Build hierarchy levels dynamically
        level_external_ids = {}  # Track external_id for each level
        level_codes = {}  # Track code for each level

        # Extract codes and names for each level
        for level in hierarchy_levels:
            level_codes[level] = location.get(f"{level}_code", "")
            # Handle case where 'System' might be capitalized in location dict
            level_name = location.get(
                level, location.get(level.capitalize(), level_codes[level])
            )

        # Create assets for each hierarchy level up to the deepest non-empty level
        parent_external_id = None
        for level_index, level in enumerate(hierarchy_levels):
            level_code = level_codes[level]

            # Skip creating assets for levels beyond the deepest non-empty level
            if level_index > deepest_level_index:
                break

            # Skip creating asset if this level has no code (shouldn't happen due to deepest_level_index, but safety check)
            if not level_code:
                continue

            level_name = location.get(
                level, location.get(level.capitalize(), level_code)
            )

            # Build external_id from all parent levels
            external_id_parts = [level]
            for prev_level in hierarchy_levels[:level_index]:
                prev_code = level_codes[prev_level]
                if prev_code:  # Only include non-empty parent codes
                    external_id_parts.append(prev_code)
            external_id_parts.append(level_code)
            level_external_id = "_".join(external_id_parts)

            if level_external_id not in created_assets:
                if level_index == 0:
                    # Root level
                    description = level_name
                    descriptions_by_external_id[level_external_id] = description
                    base_descriptions_by_external_id[level_external_id] = description
                    assets.append(
                        create_asset_instance(
                            external_id=level_external_id,
                            name=level_code,
                            description=description,
                            space=space,
                            level=level,
                        )
                    )
                else:
                    # Child level
                    parent_description = descriptions_by_external_id.get(
                        parent_external_id, ""
                    )
                    description = f"{parent_description} > {level_name}"
                    descriptions_by_external_id[level_external_id] = description
                    base_descriptions_by_external_id[level_external_id] = level_name
                    assets.append(
                        create_asset_instance(
                            external_id=level_external_id,
                            name=level_code,
                            description=description,
                            parent_external_id=parent_external_id,
                            space=space,
                            level=level,
                        )
                    )
                created_assets.add(level_external_id)

            level_external_ids[level] = level_external_id
            parent_external_id = level_external_id

        # Get the deepest level's external_id for tag processing (may not be the last level if files are at intermediate levels)
        deepest_level = hierarchy_levels[deepest_level_index]
        deepest_external_id = level_external_ids.get(deepest_level)
        deepest_level_code = level_codes.get(deepest_level, "")

        # Extract tags for files in this location
        # Get tags for this location using the location key
        location_key = loc_key
        system_file_tags = tags_by_file_and_location.get(location_key, {})

        # Only process tags if we have a valid external_id for the deepest level
        if not deepest_external_id:
            continue

        # Build prefix to remove from deepest_external_id when creating child external_ids
        # This removes the hierarchy level prefix (e.g., "system_", "plant_", etc.) based on config
        deepest_level_prefix = f"{deepest_level}_"
        deepest_external_id_without_prefix = (
            deepest_external_id.replace(deepest_level_prefix, "", 1)
            if deepest_external_id.startswith(deepest_level_prefix)
            else deepest_external_id
        )

        if include_resource_type and include_resource_subtype:
            # Hierarchy: system -> resource_type -> resource_subtype -> asset_tag
            # Group tags by resourceType first, then resourceSubType, then by text (across all files)
            tags_by_resource_type = defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(lambda: {"files": set(), "tag_data": None})
                )
            )
            for file_name, file_tags in system_file_tags.items():
                for tag in file_tags:
                    # Skip documents
                    if tag.get("category", "").lower() == "document":
                        continue

                    text = tag.get("text", "").strip()
                    if not text:
                        continue

                    resource_type = tag.get("resourceType", "") or "Unclassified"
                    resource_sub_type = tag.get("resourceSubType", "") or "Unclassified"

                    # Collect all files where this tag appears
                    if (
                        text
                        not in tags_by_resource_type[resource_type][resource_sub_type]
                    ):
                        tags_by_resource_type[resource_type][resource_sub_type][
                            text
                        ] = {
                            "files": set(),
                            "tag_data": {
                                "text": text,
                                "confidence": tag.get("confidence"),
                                "category": tag.get("category"),
                                "resourceSubType": tag.get("resourceSubType"),
                                "resourceType": tag.get("resourceType"),
                                "standard": tag.get("standard"),
                            },
                        }
                    tags_by_resource_type[resource_type][resource_sub_type][text][
                        "files"
                    ].add(file_name)

            # Create resourceType nodes, then resourceSubType nodes, then asset_tag nodes
            for resource_type, subtypes_dict in tags_by_resource_type.items():
                # Create resourceType node
                safe_resource_type = (
                    resource_type.replace(" ", "_")
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace("&", "_")
                )
                resource_type_external_id = f"resource_type_{deepest_external_id_without_prefix}_{safe_resource_type}"

                if resource_type_external_id not in created_assets:
                    resource_type_formatted = (
                        resource_type.replace("_", " ")
                        if resource_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        deepest_external_id, deepest_level_code
                    )
                    resource_type_description = (
                        f"{system_description_cumulative} > {resource_type_formatted}"
                    )
                    descriptions_by_external_id[
                        resource_type_external_id
                    ] = resource_type_description
                    base_descriptions_by_external_id[
                        resource_type_external_id
                    ] = resource_type_formatted

                    assets.append(
                        create_asset_instance(
                            external_id=resource_type_external_id,
                            name=resource_type_formatted,
                            description=resource_type_description,
                            parent_external_id=deepest_external_id,
                            space=space,
                            level="resource_type",
                        )
                    )
                    created_assets.add(resource_type_external_id)

                # Process resourceSubType nodes under this resourceType
                for resource_sub_type, tags_by_text_dict in subtypes_dict.items():
                    # Create resourceSubType node
                    safe_resource_sub_type = (
                        resource_sub_type.replace(" ", "_")
                        .replace("-", "_")
                        .replace("/", "_")
                        .replace("&", "_")
                    )
                    resource_subtype_external_id = f"resource_subtype_{resource_type_external_id.replace('resource_type_', '')}_{safe_resource_sub_type}"

                    if resource_subtype_external_id not in created_assets:
                        resource_sub_type_formatted = (
                            resource_sub_type.replace("_", " ")
                            if resource_sub_type != "Unclassified"
                            else "Unclassified"
                        )
                        resource_type_description_cumulative = (
                            descriptions_by_external_id.get(
                                resource_type_external_id, resource_type
                            )
                        )
                        resource_subtype_description = f"{resource_type_description_cumulative} > {resource_sub_type_formatted}"
                        descriptions_by_external_id[
                            resource_subtype_external_id
                        ] = resource_subtype_description
                        base_descriptions_by_external_id[
                            resource_subtype_external_id
                        ] = resource_sub_type_formatted

                        assets.append(
                            create_asset_instance(
                                external_id=resource_subtype_external_id,
                                name=resource_sub_type_formatted,
                                description=resource_subtype_description,
                                parent_external_id=resource_type_external_id,
                                space=space,
                                level="resource_subtype",
                            )
                        )
                        created_assets.add(resource_subtype_external_id)

                    # Process tags across all files for this resourceSubType
                    for text, tag_info in tags_by_text_dict.items():
                        tag_data = tag_info["tag_data"]
                        source_files = sorted(tag_info["files"])  # Sort for consistency
                        source_files_str = ", ".join(
                            source_files
                        )  # Comma-separated list of all files

                        safe_text = (
                            text.replace(" ", "_")
                            .replace("-", "_")
                            .replace("/", "_")
                            .replace("&", "_")
                        )
                        tag_external_id = f"asset_tag_{resource_subtype_external_id.replace('resource_subtype_', '')}_{safe_text}"

                        if tag_external_id not in created_assets:
                            resource_sub_type_formatted = tag_data.get(
                                "resourceSubType", ""
                            )
                            if (
                                resource_sub_type_formatted
                                and resource_sub_type_formatted != "Unclassified"
                            ):
                                resource_sub_type_formatted = f"{resource_sub_type_formatted.replace('_', ' ')} {tag_data['text']}"
                            else:
                                resource_sub_type_formatted = tag_data["text"]

                            tag_description = build_tag_description(
                                resource_sub_type_formatted,
                                hierarchy_levels,
                                level_external_ids,
                                base_descriptions_by_external_id,
                                level_codes,
                            )

                            asset = create_asset_instance(
                                external_id=tag_external_id,
                                name=tag_data["text"],
                                description=tag_description,
                                parent_external_id=resource_subtype_external_id,
                                space=space,
                                level="asset_tag",
                                confidence=tag_data.get("confidence"),
                                category=tag_data.get("category"),
                                resourceSubType=tag_data.get("resourceSubType"),
                                resourceType=tag_data.get("resourceType"),
                                standard=tag_data.get("standard"),
                                sourceFile=source_files_str,
                                sourceContext=source_files_str,
                            )
                            assets.append(asset)
                            created_assets.add(tag_external_id)

        elif include_resource_type:
            # Hierarchy: system -> resource_type -> asset_tag
            # Group tags by resourceType first, then by text (across all files)
            tags_by_resource_type = defaultdict(
                lambda: defaultdict(lambda: {"files": set(), "tag_data": None})
            )
            for file_name, file_tags in system_file_tags.items():
                for tag in file_tags:
                    # Skip documents
                    if tag.get("category", "").lower() == "document":
                        continue

                    text = tag.get("text", "").strip()
                    if not text:
                        continue

                    resource_type = tag.get("resourceType", "") or "Unclassified"

                    # Collect all files where this tag appears
                    if text not in tags_by_resource_type[resource_type]:
                        tags_by_resource_type[resource_type][text] = {
                            "files": set(),
                            "tag_data": {
                                "text": text,
                                "confidence": tag.get("confidence"),
                                "category": tag.get("category"),
                                "resourceSubType": tag.get("resourceSubType"),
                                "resourceType": tag.get("resourceType"),
                                "standard": tag.get("standard"),
                            },
                        }
                    tags_by_resource_type[resource_type][text]["files"].add(file_name)

            # Create resourceType nodes and asset_tag nodes under them
            for resource_type, file_tags_dict in tags_by_resource_type.items():
                # Create resourceType node
                safe_resource_type = (
                    resource_type.replace(" ", "_")
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace("&", "_")
                )
                resource_type_external_id = f"resource_type_{deepest_external_id_without_prefix}_{safe_resource_type}"

                if resource_type_external_id not in created_assets:
                    resource_type_formatted = (
                        resource_type.replace("_", " ")
                        if resource_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        deepest_external_id, deepest_level_code
                    )
                    resource_type_description = (
                        f"{system_description_cumulative} > {resource_type_formatted}"
                    )
                    descriptions_by_external_id[
                        resource_type_external_id
                    ] = resource_type_description
                    base_descriptions_by_external_id[
                        resource_type_external_id
                    ] = resource_type_formatted

                    assets.append(
                        create_asset_instance(
                            external_id=resource_type_external_id,
                            name=resource_type_formatted,
                            description=resource_type_description,
                            parent_external_id=deepest_external_id,
                            space=space,
                            level="resource_type",
                        )
                    )
                    created_assets.add(resource_type_external_id)

                # Process tags across all files for this resourceType
                for text, tag_info in file_tags_dict.items():
                    tag_data = tag_info["tag_data"]
                    source_files = sorted(tag_info["files"])  # Sort for consistency
                    source_files_str = ", ".join(
                        source_files
                    )  # Comma-separated list of all files

                    safe_text = (
                        text.replace(" ", "_")
                        .replace("-", "_")
                        .replace("/", "_")
                        .replace("&", "_")
                    )
                    tag_external_id = f"asset_tag_{resource_type_external_id.replace('resource_type_', '')}_{safe_text}"

                    if tag_external_id not in created_assets:
                        resource_sub_type = tag_data.get("resourceSubType", "")
                        if resource_sub_type and resource_sub_type != "Unclassified":
                            resource_sub_type_formatted = f"{resource_sub_type.replace('_', ' ')} {tag_data['text']}"
                        else:
                            resource_sub_type_formatted = tag_data["text"]

                        tag_description = build_tag_description(
                            resource_sub_type_formatted,
                            hierarchy_levels,
                            level_external_ids,
                            base_descriptions_by_external_id,
                            level_codes,
                        )

                        asset = create_asset_instance(
                            external_id=tag_external_id,
                            name=tag_data["text"],
                            description=tag_description,
                            parent_external_id=resource_type_external_id,
                            space=space,
                            level="asset_tag",
                            confidence=tag_data.get("confidence"),
                            category=tag_data.get("category"),
                            resourceSubType=tag_data.get("resourceSubType"),
                            resourceType=tag_data.get("resourceType"),
                            standard=tag_data.get("standard"),
                            sourceFile=source_files_str,
                            sourceContext=source_files_str,
                        )
                        assets.append(asset)
                        created_assets.add(tag_external_id)

        elif include_resource_subtype:
            # Group tags by resourceSubType first, then by text (across all files)
            tags_by_resource_subtype = defaultdict(
                lambda: defaultdict(lambda: {"files": set(), "tag_data": None})
            )
            for file_name, file_tags in system_file_tags.items():
                for tag in file_tags:
                    # Skip documents
                    if tag.get("category", "").lower() == "document":
                        continue

                    text = tag.get("text", "").strip()
                    if not text:
                        continue

                    resource_sub_type = tag.get("resourceSubType", "") or "Unclassified"

                    # Collect all files where this tag appears
                    if text not in tags_by_resource_subtype[resource_sub_type]:
                        tags_by_resource_subtype[resource_sub_type][text] = {
                            "files": set(),
                            "tag_data": {
                                "text": text,
                                "confidence": tag.get("confidence"),
                                "category": tag.get("category"),
                                "resourceSubType": tag.get("resourceSubType"),
                                "resourceType": tag.get("resourceType"),
                                "standard": tag.get("standard"),
                            },
                        }
                    tags_by_resource_subtype[resource_sub_type][text]["files"].add(
                        file_name
                    )

            # Create resourceSubType nodes and asset_tag nodes under them
            for resource_sub_type, file_tags_dict in tags_by_resource_subtype.items():
                # Create resourceSubType node
                safe_resource_sub_type = (
                    resource_sub_type.replace(" ", "_")
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace("&", "_")
                )
                resource_subtype_external_id = f"resource_subtype_{deepest_external_id_without_prefix}_{safe_resource_sub_type}"

                if resource_subtype_external_id not in created_assets:
                    resource_sub_type_formatted = (
                        resource_sub_type.replace("_", " ")
                        if resource_sub_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        deepest_external_id, deepest_level_code
                    )
                    resource_subtype_description = f"{system_description_cumulative} > {resource_sub_type_formatted}"
                    descriptions_by_external_id[
                        resource_subtype_external_id
                    ] = resource_subtype_description
                    base_descriptions_by_external_id[
                        resource_subtype_external_id
                    ] = resource_sub_type_formatted

                    assets.append(
                        create_asset_instance(
                            external_id=resource_subtype_external_id,
                            name=resource_sub_type_formatted,
                            description=resource_subtype_description,
                            parent_external_id=deepest_external_id,
                            space=space,
                            level="resource_subtype",
                        )
                    )
                    created_assets.add(resource_subtype_external_id)

                # Process tags across all files for this resourceSubType
                for text, tag_info in file_tags_dict.items():
                    tag_data = tag_info["tag_data"]
                    source_files = sorted(tag_info["files"])  # Sort for consistency
                    source_files_str = ", ".join(
                        source_files
                    )  # Comma-separated list of all files

                    safe_text = (
                        text.replace(" ", "_")
                        .replace("-", "_")
                        .replace("/", "_")
                        .replace("&", "_")
                    )
                    tag_external_id = f"asset_tag_{resource_subtype_external_id.replace('resource_subtype_', '')}_{safe_text}"

                    if tag_external_id not in created_assets:
                        resource_sub_type_formatted = tag_data.get(
                            "resourceSubType", ""
                        )
                        if (
                            resource_sub_type_formatted
                            and resource_sub_type_formatted != "Unclassified"
                        ):
                            resource_sub_type_formatted = f"{resource_sub_type_formatted.replace('_', ' ')} {tag_data['text']}"
                        else:
                            resource_sub_type_formatted = tag_data["text"]

                        tag_description = build_tag_description(
                            resource_sub_type_formatted,
                            hierarchy_levels,
                            level_external_ids,
                            base_descriptions_by_external_id,
                            level_codes,
                        )

                        asset = create_asset_instance(
                            external_id=tag_external_id,
                            name=tag_data["text"],
                            description=tag_description,
                            parent_external_id=resource_subtype_external_id,
                            space=space,
                            level="asset_tag",
                            confidence=tag_data.get("confidence"),
                            category=tag_data.get("category"),
                            resourceSubType=tag_data.get("resourceSubType"),
                            resourceType=tag_data.get("resourceType"),
                            standard=tag_data.get("standard"),
                            sourceFile=source_files_str,
                            sourceContext=source_files_str,
                        )
                        assets.append(asset)
                        created_assets.add(tag_external_id)
        else:
            # Original behavior: asset_tag nodes directly under system
            # Group tags by text across all files in this system
            tags_by_text = {}
            for file_name, file_tags in system_file_tags.items():
                for tag in file_tags:
                    # Skip documents
                    if tag.get("category", "").lower() == "document":
                        continue

                    text = tag.get("text", "").strip()
                    if not text:
                        continue

                    # Collect all files where this tag appears
                    if text not in tags_by_text:
                        tags_by_text[text] = {
                            "files": set(),
                            "tag_data": {
                                "text": text,
                                "confidence": tag.get("confidence"),
                                "category": tag.get("category"),
                                "resourceSubType": tag.get("resourceSubType"),
                                "resourceType": tag.get("resourceType"),
                                "standard": tag.get("standard"),
                            },
                        }
                    tags_by_text[text]["files"].add(file_name)

            # Create assets for each unique tag
            for text, tag_info in tags_by_text.items():
                tag_data = tag_info["tag_data"]
                source_files = sorted(tag_info["files"])  # Sort for consistency
                source_files_str = ", ".join(
                    source_files
                )  # Comma-separated list of all files

                # Create a unique external ID for this tag under the deepest level
                # Sanitize text for use in external ID (replace special chars)
                safe_text = (
                    text.replace(" ", "_")
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace("&", "_")
                )
                tag_external_id = (
                    f"asset_tag_{deepest_external_id_without_prefix}_{safe_text}"
                )

                if tag_external_id not in created_assets:
                    # Create description from resourceSubType in camel case with spaces
                    resource_sub_type = tag_data.get("resourceSubType", "")
                    if resource_sub_type and resource_sub_type != "Unclassified":
                        # Convert from snake_case/underscore to camel case with spaces
                        # e.g., "Level_Monitor" -> "Level Monitor"
                        resource_sub_type_formatted = (
                            f"{resource_sub_type.replace('_', ' ')} {tag_data['text']}"
                        )
                    else:
                        # Fallback to text if no valid resourceSubType
                        resource_sub_type_formatted = tag_data["text"]

                    # Build description using format: "{resourceSubType} for {system} in {plant} {area} at {site}"
                    tag_description = build_tag_description(
                        resource_sub_type_formatted,
                        hierarchy_levels,
                        level_external_ids,
                        base_descriptions_by_external_id,
                        level_codes,
                    )

                    # Set sourceFile and sourceContext to comma-separated list of all files where tag appears
                    asset = create_asset_instance(
                        external_id=tag_external_id,
                        name=tag_data["text"],
                        description=tag_description,
                        parent_external_id=deepest_external_id,
                        space=space,
                        level="asset_tag",
                        confidence=tag_data.get("confidence"),
                        category=tag_data.get("category"),
                        resourceSubType=tag_data.get("resourceSubType"),
                        resourceType=tag_data.get("resourceType"),
                        standard=tag_data.get("standard"),
                        sourceFile=source_files_str,
                        sourceContext=source_files_str,
                    )
                    assets.append(asset)
                    created_assets.add(tag_external_id)

    return assets
