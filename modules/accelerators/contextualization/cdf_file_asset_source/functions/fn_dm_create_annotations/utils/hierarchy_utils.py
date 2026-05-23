"""
Hierarchy utility functions for create annotations.

This module provides utilities for generating asset hierarchies in annotations.
Note: This module uses a simplified version of hierarchy generation.
For full hierarchy generation, see fn_dm_create_asset_hierarchy.
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
        space: str = "sp_enterprise_schema",
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


def generate_hierarchy(
    locations: List[Dict[str, str]],
    tags: List[Dict[str, Any]],
    space: str = "sp_enterprise_schema",
    include_resource_subtype: bool = False,
    include_resource_type: bool = False,
) -> List[Dict[str, Any]]:
    """Generate the complete asset hierarchy."""
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

    # Group tags by file_name and system
    tags_by_file_and_system = defaultdict(lambda: defaultdict(list))
    for tag in tags:
        file_name = tag.get("file_name", "")
        if file_name and file_name in file_to_location:
            location = file_to_location[file_name]
            system_key = (
                location.get("site_code", ""),
                location.get("plant_code", ""),
                location.get("area_code", ""),
                location.get("system_code", ""),
            )
            tags_by_file_and_system[system_key][file_name].append(tag)

    # Create hierarchy for each unique location
    processed_locations = set()
    # Track cumulative descriptions by external_id for site/plant/area/system levels
    descriptions_by_external_id = {}
    # Track base descriptions (non-cumulative) for use in asset_tag descriptions
    base_descriptions_by_external_id = {}

    for location in locations:
        # Create unique key for location
        loc_key = (
            location.get("site_code", ""),
            location.get("plant_code", ""),
            location.get("area_code", ""),
            location.get("system_code", ""),
        )

        if not all(loc_key) or loc_key in processed_locations:
            continue

        processed_locations.add(loc_key)

        site_code = location["site_code"]
        plant_code = location["plant_code"]
        area_code = location["area_code"]
        system_code = location["system_code"]

        # Level 1: Site (root)
        site_external_id = f"site_{site_code}"
        if site_external_id not in created_assets:
            site_description = location.get("site", site_code)
            descriptions_by_external_id[site_external_id] = site_description
            base_descriptions_by_external_id[site_external_id] = site_description
            assets.append(
                create_asset_instance(
                    external_id=site_external_id,
                    name=site_code,
                    description=site_description,
                    space=space,
                    level="site",
                )
            )
            created_assets.add(site_external_id)

        # Level 2: Plant (child of site)
        plant_external_id = f"plant_{site_code}_{plant_code}"
        if plant_external_id not in created_assets:
            plant_description_base = location.get("plant", plant_code)
            site_description = descriptions_by_external_id.get(
                site_external_id, site_code
            )
            plant_description = f"{site_description} > {plant_description_base}"
            descriptions_by_external_id[plant_external_id] = plant_description
            base_descriptions_by_external_id[plant_external_id] = plant_description_base
            assets.append(
                create_asset_instance(
                    external_id=plant_external_id,
                    name=plant_code,
                    description=plant_description,
                    parent_external_id=site_external_id,
                    space=space,
                    level="plant",
                )
            )
            created_assets.add(plant_external_id)

        # Level 3: Area (child of plant)
        area_external_id = f"area_{site_code}_{plant_code}_{area_code}"
        if area_external_id not in created_assets:
            area_description_base = location.get("area", area_code)
            plant_description = descriptions_by_external_id.get(
                plant_external_id, plant_code
            )
            area_description = f"{plant_description} > {area_description_base}"
            descriptions_by_external_id[area_external_id] = area_description
            base_descriptions_by_external_id[area_external_id] = area_description_base
            assets.append(
                create_asset_instance(
                    external_id=area_external_id,
                    name=area_code,
                    description=area_description,
                    parent_external_id=plant_external_id,
                    space=space,
                    level="area",
                )
            )
            created_assets.add(area_external_id)

        # Level 4: System (child of area)
        system_external_id = (
            f"system_{site_code}_{plant_code}_{area_code}_{system_code}"
        )
        if system_external_id not in created_assets:
            system_description_base = location.get("System", system_code)
            area_description = descriptions_by_external_id.get(
                area_external_id, area_code
            )
            system_description = f"{area_description} > {system_description_base}"
            descriptions_by_external_id[system_external_id] = system_description
            base_descriptions_by_external_id[
                system_external_id
            ] = system_description_base
            assets.append(
                create_asset_instance(
                    external_id=system_external_id,
                    name=system_code,
                    description=system_description,
                    parent_external_id=area_external_id,
                    space=space,
                    level="system",
                )
            )
            created_assets.add(system_external_id)

        # Level 5: Extract tags for files in this system
        # Get tags for this system using the system key
        system_key = (site_code, plant_code, area_code, system_code)
        system_file_tags = tags_by_file_and_system.get(system_key, {})

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
                resource_type_external_id = f"resource_type_{system_external_id.replace('system_', '')}_{safe_resource_type}"

                if resource_type_external_id not in created_assets:
                    resource_type_formatted = (
                        resource_type.replace("_", " ")
                        if resource_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        system_external_id, system_code
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
                            parent_external_id=system_external_id,
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

                            site_description_base = (
                                base_descriptions_by_external_id.get(
                                    site_external_id, site_code
                                )
                            )
                            plant_description_base = (
                                base_descriptions_by_external_id.get(
                                    plant_external_id, plant_code
                                )
                            )
                            area_description_base = (
                                base_descriptions_by_external_id.get(
                                    area_external_id, area_code
                                )
                            )
                            system_description_base = (
                                base_descriptions_by_external_id.get(
                                    system_external_id, system_code
                                )
                            )

                            tag_description = f"{resource_sub_type_formatted} for {system_description_base} in {area_description_base} of {plant_description_base} at {site_description_base}"

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
                resource_type_external_id = f"resource_type_{system_external_id.replace('system_', '')}_{safe_resource_type}"

                if resource_type_external_id not in created_assets:
                    resource_type_formatted = (
                        resource_type.replace("_", " ")
                        if resource_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        system_external_id, system_code
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
                            parent_external_id=system_external_id,
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

                        site_description_base = base_descriptions_by_external_id.get(
                            site_external_id, site_code
                        )
                        plant_description_base = base_descriptions_by_external_id.get(
                            plant_external_id, plant_code
                        )
                        area_description_base = base_descriptions_by_external_id.get(
                            area_external_id, area_code
                        )
                        system_description_base = base_descriptions_by_external_id.get(
                            system_external_id, system_code
                        )

                        tag_description = f"{resource_sub_type_formatted} for {system_description_base} in {area_description_base} of {plant_description_base} at {site_description_base}"

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
                resource_subtype_external_id = f"resource_subtype_{system_external_id.replace('system_', '')}_{safe_resource_sub_type}"

                if resource_subtype_external_id not in created_assets:
                    resource_sub_type_formatted = (
                        resource_sub_type.replace("_", " ")
                        if resource_sub_type != "Unclassified"
                        else "Unclassified"
                    )
                    system_description_cumulative = descriptions_by_external_id.get(
                        system_external_id, system_code
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
                            parent_external_id=system_external_id,
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

                        site_description_base = base_descriptions_by_external_id.get(
                            site_external_id, site_code
                        )
                        plant_description_base = base_descriptions_by_external_id.get(
                            plant_external_id, plant_code
                        )
                        area_description_base = base_descriptions_by_external_id.get(
                            area_external_id, area_code
                        )
                        system_description_base = base_descriptions_by_external_id.get(
                            system_external_id, system_code
                        )

                        tag_description = f"{resource_sub_type_formatted} for {system_description_base} in {area_description_base} of {plant_description_base} at {site_description_base}"

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

                # Create a unique external ID for this tag under this system
                # Sanitize text for use in external ID (replace special chars)
                safe_text = (
                    text.replace(" ", "_")
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace("&", "_")
                )
                tag_external_id = (
                    f"asset_tag_{system_external_id.replace('system_', '')}_{safe_text}"
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
                    site_description_base = base_descriptions_by_external_id.get(
                        site_external_id, site_code
                    )
                    plant_description_base = base_descriptions_by_external_id.get(
                        plant_external_id, plant_code
                    )
                    area_description_base = base_descriptions_by_external_id.get(
                        area_external_id, area_code
                    )
                    system_description_base = base_descriptions_by_external_id.get(
                        system_external_id, system_code
                    )

                    tag_description = f"{resource_sub_type_formatted} for {system_description_base} in {area_description_base} of {plant_description_base} at {site_description_base}"

                    # Set sourceFile and sourceContext to comma-separated list of all files where tag appears
                    asset = create_asset_instance(
                        external_id=tag_external_id,
                        name=tag_data["text"],
                        description=tag_description,
                        parent_external_id=system_external_id,
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
