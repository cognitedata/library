#!/usr/bin/env python3
"""
Script to check for duplicate CogniteAsset names for asset_tags under site_BLO_PID root.
"""

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml


def build_asset_tree(assets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Build a tree structure mapping parent external_id to list of child external_ids."""
    tree: Dict[str, List[str]] = defaultdict(list)

    for asset in assets:
        external_id = asset.get("externalId")
        parent = asset.get("properties", {}).get("parent")

        if parent:
            parent_external_id = parent.get("externalId")
            if parent_external_id:
                tree[parent_external_id].append(external_id)

    return tree


def get_all_descendants(
    root_external_id: str, tree: Dict[str, List[str]], visited: Set[str] = None
) -> Set[str]:
    """Get all descendant external_ids starting from root."""
    if visited is None:
        visited = set()

    if root_external_id in visited:
        return visited

    visited.add(root_external_id)

    if root_external_id in tree:
        for child in tree[root_external_id]:
            get_all_descendants(child, tree, visited)

    return visited


def check_duplicate_names(
    yaml_file: Path, root_external_id: str = "site_BLO_PID"
) -> None:
    """Check for duplicate asset names for asset_tags under the specified root."""

    print(f"Loading asset hierarchy from: {yaml_file}")
    with open(yaml_file, "r") as f:
        data = yaml.safe_load(f)

    assets = data.get("items", [])
    print(f"Total assets in file: {len(assets)}")

    # Build asset lookup by external_id
    asset_lookup: Dict[str, Dict[str, Any]] = {}
    for asset in assets:
        external_id = asset.get("externalId")
        if external_id:
            asset_lookup[external_id] = asset

    # Build tree structure
    tree = build_asset_tree(assets)

    # Get all descendants of root
    descendants = get_all_descendants(root_external_id, tree)
    print(f"\nDescendants of {root_external_id}: {len(descendants)} assets")

    # Filter for asset_tag assets
    asset_tag_assets = []
    for external_id in descendants:
        if external_id not in asset_lookup:
            continue

        asset = asset_lookup[external_id]
        properties = asset.get("properties", {})
        tags = properties.get("tags", [])

        # Check if asset_tag is in tags
        if isinstance(tags, list) and "asset_tag" in tags:
            name = properties.get("name")
            if name:
                asset_tag_assets.append(
                    {"external_id": external_id, "name": name, "asset": asset}
                )

    print(f"Asset_tag assets under {root_external_id}: {len(asset_tag_assets)}")

    # Check for duplicate names
    name_to_assets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for asset_info in asset_tag_assets:
        name = asset_info["name"]
        name_to_assets[name].append(asset_info)

    # Find duplicates
    duplicates = {
        name: assets_list
        for name, assets_list in name_to_assets.items()
        if len(assets_list) > 1
    }

    print(f"\n{'='*80}")
    if duplicates:
        print(f"Found {len(duplicates)} duplicate asset names:")
        print(f"{'='*80}\n")

        for name, assets_list in sorted(duplicates.items()):
            print(f"Name: '{name}' ({len(assets_list)} occurrences)")
            for asset_info in assets_list:
                external_id = asset_info["external_id"]
                asset = asset_info["asset"]
                properties = asset.get("properties", {})
                parent = properties.get("parent", {})
                parent_id = parent.get("externalId", "N/A")
                source_file = properties.get("sourceFile", "N/A")
                print(f"  - external_id: {external_id}")
                print(f"    parent: {parent_id}")
                print(f"    source_file: {source_file}")
            print()
    else:
        print("No duplicate asset names found!")
        print(f"{'='*80}\n")

    # Detailed duplication analysis
    print(f"\n{'='*80}")
    print("DUPLICATION SCOPE ANALYSIS")
    print(f"{'='*80}\n")

    # Count total duplicate instances
    total_duplicate_instances = sum(
        len(assets_list) for assets_list in duplicates.values()
    )
    total_unique_duplicate_names = len(duplicates)

    print(f"1. OVERALL STATISTICS:")
    print(f"   Total asset_tag assets: {len(asset_tag_assets)}")
    print(f"   Unique asset_tag names: {len(name_to_assets)}")
    print(f"   Duplicate names: {total_unique_duplicate_names}")
    print(f"   Total duplicate asset instances: {total_duplicate_instances}")
    print(
        f"   Percentage of assets with duplicate names: {(total_duplicate_instances / len(asset_tag_assets) * 100):.1f}%"
    )

    # Distribution of duplicates
    print(f"\n2. DUPLICATION DISTRIBUTION:")
    dup_distribution = defaultdict(int)
    for name, assets_list in duplicates.items():
        dup_distribution[len(assets_list)] += 1

    for count in sorted(dup_distribution.keys(), reverse=True):
        names_count = dup_distribution[count]
        instances = names_count * count
        print(
            f"   {count} occurrences: {names_count} unique name(s) ({instances} total asset instances)"
        )

    # Most problematic duplicates
    print(f"\n3. TOP 10 MOST DUPLICATED NAMES:")
    sorted_dups = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)
    for i, (name, assets_list) in enumerate(sorted_dups[:10], 1):
        print(f"   {i:2d}. '{name}': {len(assets_list)} occurrences")

    # Analyze duplication by system/area
    print(f"\n4. DUPLICATION BY SYSTEM/AREA:")
    system_analysis = defaultdict(lambda: {"names": set(), "instances": 0})

    for name, assets_list in duplicates.items():
        for asset_info in assets_list:
            external_id = asset_info["external_id"]
            # Extract system from external_id (e.g., asset_tag_BLO_PID_BAY_1_SEC_800_GLY_ETH_...)
            parts = external_id.split("_")
            if len(parts) >= 6:
                # Try to identify system/area
                system_key = "_".join(parts[2:6])  # BAY_X_SEC_XXXX
                system_analysis[system_key]["names"].add(name)
                system_analysis[system_key]["instances"] += 1

    # Show systems with most duplicates
    sorted_systems = sorted(
        system_analysis.items(), key=lambda x: x[1]["instances"], reverse=True
    )
    print(f"   Systems/Areas with duplicate instances:")
    for system_key, data in sorted_systems[:10]:
        print(
            f"   - {system_key}: {len(data['names'])} duplicate names, {data['instances']} instances"
        )

    # Cross-system duplication analysis
    print(f"\n5. CROSS-SYSTEM DUPLICATION:")
    name_to_systems = defaultdict(set)
    for name, assets_list in duplicates.items():
        for asset_info in assets_list:
            external_id = asset_info["external_id"]
            parts = external_id.split("_")
            if len(parts) >= 6:
                system_key = "_".join(parts[2:6])
                name_to_systems[name].add(system_key)

    cross_system_dups = {
        name: systems for name, systems in name_to_systems.items() if len(systems) > 1
    }
    print(f"   Names duplicated across different systems: {len(cross_system_dups)}")
    print(
        f"   Names duplicated within same system only: {total_unique_duplicate_names - len(cross_system_dups)}"
    )

    if cross_system_dups:
        print(f"\n   Top 10 names duplicated across most systems:")
        sorted_cross = sorted(
            cross_system_dups.items(), key=lambda x: len(x[1]), reverse=True
        )
        for i, (name, systems) in enumerate(sorted_cross[:10], 1):
            print(f"   {i:2d}. '{name}': appears in {len(systems)} different systems")
            print(
                f"       Systems: {', '.join(sorted(list(systems))[:5])}{'...' if len(systems) > 5 else ''}"
            )

    # Summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY:")
    print(f"  Total assets: {len(assets)}")
    print(f"  Assets under {root_external_id}: {len(descendants)}")
    print(f"  Asset_tag assets: {len(asset_tag_assets)}")
    print(f"  Unique asset_tag names: {len(name_to_assets)}")
    print(f"  Duplicate names: {total_unique_duplicate_names}")
    print(f"  Total duplicate instances: {total_duplicate_instances}")
    print(f"  Cross-system duplicates: {len(cross_system_dups)}")


if __name__ == "__main__":
    yaml_file = (
        Path(__file__).parent
        / "modules/create_asset_hierarchy_from_files/results/asset_hierarchy.yaml"
    )

    if not yaml_file.exists():
        print(f"Error: File not found: {yaml_file}")
        exit(1)

    check_duplicate_names(yaml_file)
