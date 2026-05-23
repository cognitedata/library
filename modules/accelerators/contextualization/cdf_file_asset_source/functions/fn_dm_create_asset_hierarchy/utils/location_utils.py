"""
Location utility functions for create asset hierarchy.

This module provides utilities for loading and working with location files.
"""

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def load_locations_csv(csv_file: Path) -> List[Dict[str, str]]:
    """Load locations CSV file."""
    locations = []
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            locations.append(row)
    return locations


def convert_locations_dict_to_flat_list(
    data: Dict[str, Any], hierarchy_levels: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """Convert locations from nested dict structure to flat list format.

    Args:
        data: Dictionary containing locations structure
        hierarchy_levels: List of hierarchy level names in order (e.g., ['site', 'plant', 'area', 'system'])
                          If None, defaults to ['site', 'plant', 'area', 'system'] for backward compatibility
    """
    locations = []

    # Default hierarchy levels for backward compatibility
    if hierarchy_levels is None:
        hierarchy_levels = ["site", "plant", "area", "system"]

    if not hierarchy_levels:
        return locations

    if isinstance(data, list):
        top_level_items = data
    else:
        top_level_items = data.get("scope", [])

    def process_level(
        items: List[Dict[str, Any]], level_index: int, parent_values: Dict[str, str]
    ) -> None:
        """Recursively process hierarchy levels.

        Uses "locations" as the generic property name for child nodes.
        """
        if level_index >= len(hierarchy_levels):
            return

        current_level = hierarchy_levels[level_index]
        is_last_level = level_index == len(hierarchy_levels) - 1

        for item in items:
            code = item.get("name", "")
            name = item.get("description", code)

            # Store current level values
            current_values = parent_values.copy()
            current_values[f"{current_level}_code"] = code
            current_values[current_level] = name

            # Check if this level has files (files can be defined at any level, not just leaf nodes)
            files = item.get("files", [])
            if files:
                # Create location entry for this level with files
                # Fill in missing hierarchy levels with empty strings for consistency
                location = current_values.copy()
                for level in hierarchy_levels:
                    if f"{level}_code" not in location:
                        location[f"{level}_code"] = ""
                    if level not in location:
                        location[level] = ""
                location["file_name"] = ", ".join(files) if files else ""
                locations.append(location)

            # Process child levels if they exist (regardless of whether this level has files)
            if not is_last_level:
                next_level_items = item.get("locations", [])
                if next_level_items:
                    process_level(next_level_items, level_index + 1, current_values)

    process_level(top_level_items, 0, {})

    return locations


def load_locations_yaml(
    yaml_file: Path, hierarchy_levels: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """Load locations from YAML config file and convert to flat list format.

    Args:
        yaml_file: Path to YAML file
        hierarchy_levels: List of hierarchy level names in order
                         If None, will try to read from data.get('hierarchy_levels')
    """
    with open(yaml_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Try to get hierarchy_levels from data if not provided
    if hierarchy_levels is None and isinstance(data, dict):
        hierarchy_levels = data.get("hierarchy_levels")

    locations_data = data
    if isinstance(data, dict):
        section = data.get("data", data)
        if isinstance(section, dict):
            locations_data = section.get("scope", [])
        elif isinstance(data.get("scope"), list):
            locations_data = data["scope"]
        else:
            locations_data = []

    return convert_locations_dict_to_flat_list(locations_data, hierarchy_levels)


def load_extracted_assets(csv_file: Path) -> List[Dict[str, Any]]:
    """Load extracted assets CSV file."""
    tags = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tags.append(row)
    return tags


def normalize_file_name_for_matching(file_name: str) -> str:
    """Normalize file name for matching by removing extensions, spaces, and special chars."""
    from pathlib import Path

    # Remove extension
    base = Path(file_name).stem if "." in file_name else file_name
    # Normalize: replace ~ with -, replace spaces with dashes, lowercase
    normalized = base.replace("~", "-").replace(" ", "-").lower().strip()
    # Clean up multiple consecutive dashes
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized


def match_file_to_system(
    file_name: str, locations: List[Dict[str, str]]
) -> Optional[Dict[str, str]]:
    """Match a file name to a system in the locations CSV.

    Matching is done by checking if the file name starts with the value specified
    in the location config. This allows files like "E-208-M-001.dwg.pdf" to match
    location config entries like "E-208-M-001".
    """
    import re
    from pathlib import Path

    # Handle cognitefile_ prefix and timestamp suffix
    # e.g., "cognitefile_E-208-M-001.dwg_20251110_234713" -> "E-208-M-001.dwg"
    file_name_cleaned = file_name
    if file_name.startswith("cognitefile_"):
        # Remove prefix
        file_name_cleaned = file_name.replace("cognitefile_", "", 1)
        # Remove timestamp suffix (format: _YYYYMMDD_HHMMSS or _timestamp)
        # Match pattern like _20251110_234713 or _1762840033548
        file_name_cleaned = re.sub(
            r"_\d{8}_\d{6}$", "", file_name_cleaned
        )  # Remove _YYYYMMDD_HHMMSS
        file_name_cleaned = re.sub(
            r"_\d{13}$", "", file_name_cleaned
        )  # Remove _timestamp (13 digits)
        file_name_cleaned = re.sub(
            r"_\d+$", "", file_name_cleaned
        )  # Remove any trailing _digits

    # Get base file name (without extension) for matching
    file_base = (
        Path(file_name_cleaned).stem if "." in file_name_cleaned else file_name_cleaned
    )

    for location in locations:
        # Try both 'file_name' and 'P&ID' column names
        file_names_str = location.get("file_name", "") or location.get("P&ID", "")
        if not file_names_str:
            continue

        # Split by comma and strip whitespace
        file_list = [f.strip() for f in file_names_str.split(",")]

        # Check if file_name starts with any file in the list
        for loc_file in file_list:
            # Primary matching strategy: file name starts with location config value
            # This handles cases like "E-208-M-001.dwg.pdf" matching "E-208-M-001"
            if file_name_cleaned.startswith(loc_file) or file_base.startswith(loc_file):
                return location

            # Also check normalized versions for flexibility
            file_normalized = normalize_file_name_for_matching(file_name_cleaned)
            loc_normalized = normalize_file_name_for_matching(loc_file)
            if file_normalized.startswith(loc_normalized):
                return location

    return None
