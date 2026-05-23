#!/usr/bin/env python3
"""
Script to identify file names that don't match any location.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import yaml

from modules.create_asset_hierarchy_from_files.functions.fn_dm_create_asset_hierarchy.handler import (
    run_locally,
)
from modules.create_asset_hierarchy_from_files.functions.fn_dm_create_asset_hierarchy.utils.location_utils import (
    match_file_to_system,
)

# Load config to get locations (default.config.yaml → file_asset_source.create)
module_root = Path(__file__).resolve().parent.parent
config_path = module_root / "default.config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

create_block = (config.get("file_asset_source") or {}).get("create") or {}
parameters = create_block.get("parameters", {})
data_section = create_block.get("data", {})

# Get scope tree from data section
locations_data = data_section.get("scope")
if locations_data:
    from modules.create_asset_hierarchy_from_files.functions.fn_dm_create_asset_hierarchy.utils.location_utils import (
        convert_locations_dict_to_flat_list,
    )

    locations = convert_locations_dict_to_flat_list(locations_data)
else:
    locations = []

from modules.create_asset_hierarchy_from_files.functions.fn_dm_create_asset_hierarchy.dependencies import (
    create_client,
    get_env_variables,
)

# Get tags using the handler's approach
from modules.create_asset_hierarchy_from_files.functions.fn_dm_create_asset_hierarchy.pipeline import (
    get_asset_list,
)

# Create client using the same method as run_locally
env_vars = get_env_variables()
client = create_client(env_vars)

# Get tags
print("Loading assets from RAW table...")
tags = get_asset_list(
    client=client,
    raw_db=parameters["raw_db"],
    raw_table_results=parameters["raw_table_results"],
    logger=None,
)

# Find file names that don't match
file_names_in_tags = set()
file_names_with_location = set()
file_name_counts = {}

for tag in tags:
    file_name = tag.get("file_name", "")
    if file_name:
        file_names_in_tags.add(file_name)
        if file_name not in file_name_counts:
            file_name_counts[file_name] = 0
        file_name_counts[file_name] += 1

        location = match_file_to_system(file_name, locations)
        if location:
            file_names_with_location.add(file_name)

missing_files = file_names_in_tags - file_names_with_location

print(f'\n{"="*80}')
print(f"File Name Analysis:")
print(f'{"="*80}')
print(f"Total unique file names in tags: {len(file_names_in_tags)}")
print(f"File names with matching locations: {len(file_names_with_location)}")
print(f"File names missing locations: {len(missing_files)}")
print(f"\nMissing file names ({len(missing_files)}):")
print(f'{"-"*80}')
for fname in sorted(missing_files):
    count = file_name_counts.get(fname, 0)
    print(f"  - {fname} ({count} tag(s))")
