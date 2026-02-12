"""
Asset processing and hierarchy metrics computation.
"""

from typing import Dict, Optional
from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
    extract_parent_external_id,
    CombinedAccumulator,
)


def process_asset_batch(
    asset_batch,
    asset_view: ViewId,
    acc: CombinedAccumulator
):
    """
    Process asset batch - collect data for:
    - TS metrics (critical asset coverage)
    - Hierarchy metrics (parent-child relationships)
    - Equipment metrics (asset type mapping)
    """
    for node in asset_batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_asset_instances += 1
        
        # Skip if already processed (duplicate)
        if node_id in acc.asset_ids_seen:
            acc.asset_duplicate_ids.append(node_id)
            continue
        acc.asset_ids_seen.add(node_id)
        
        props = get_props(node, asset_view)
        
        # For TS metrics: critical asset tracking
        if props.get("criticality") == "critical":
            acc.critical_assets_total += 1
            if node_id in acc.assets_with_ts:
                acc.critical_assets_with_ts += 1
        
        # For hierarchy metrics: parent-child relationships
        parent_id = extract_parent_external_id(node, asset_view)
        acc.parent_of[node_id] = parent_id
        if parent_id:
            acc.children_count_map[parent_id] = acc.children_count_map.get(parent_id, 0) + 1
        
        # For equipment metrics: asset type mapping
        asset_type = props.get("type") or props.get("assetClass")
        acc.asset_type_map[node_id] = asset_type


def compute_depth_map(parent_map: Dict[str, Optional[str]]) -> Dict[str, int]:
    """Compute depth for each node in the hierarchy."""
    depth_cache: Dict[str, int] = {}

    def get_depth(nid: str) -> int:
        if nid in depth_cache:
            return depth_cache[nid]
        parent = parent_map.get(nid)
        if not parent:
            depth_cache[nid] = 0
            return 0
        visited = set()
        cur = nid
        d = 0
        while True:
            if cur in visited:
                break
            visited.add(cur)
            parent = parent_map.get(cur)
            if not parent:
                break
            d += 1
            cur = parent
        depth_cache[nid] = d
        return d

    for node_id in parent_map.keys():
        get_depth(node_id)
    return depth_cache


def compute_asset_hierarchy_metrics(acc: CombinedAccumulator) -> dict:
    """Compute all asset hierarchy metrics."""
    total_assets = len(acc.parent_of)
    assets_with_parents = sum(1 for v in acc.parent_of.values() if v)
    root_assets = total_assets - assets_with_parents
    
    # Orphans: no parent AND no children
    orphan_ids = [
        nid for nid, parent in acc.parent_of.items()
        if parent is None and nid not in acc.children_count_map
    ]
    orphan_count = len(orphan_ids)
    orphan_rate = (orphan_count / total_assets * 100) if total_assets else 0.0
    
    # Hierarchy completion rate
    non_root = total_assets - root_assets
    completion_rate = (assets_with_parents / non_root * 100) if non_root > 0 else 100.0
    
    # Depth metrics
    depths_map = compute_depth_map(acc.parent_of)
    depth_values = list(depths_map.values()) if depths_map else [0]
    avg_depth = sum(depth_values) / len(depth_values) if depth_values else 0.0
    max_depth = max(depth_values) if depth_values else 0
    
    # Breadth metrics
    children_values = list(acc.children_count_map.values()) if acc.children_count_map else [0]
    avg_children = sum(children_values) / len(children_values) if children_values else 0.0
    if len(children_values) > 0:
        variance = sum((x - avg_children) ** 2 for x in children_values) / len(children_values)
        std_children = variance ** 0.5
    else:
        std_children = 0.0
    max_children = max(children_values) if children_values else 0
    
    # Distributions
    depth_dist: Dict[int, int] = {}
    for d in depths_map.values():
        depth_dist[d] = depth_dist.get(d, 0) + 1
    
    breadth_dist: Dict[int, int] = {}
    for c in acc.children_count_map.values():
        breadth_dist[c] = breadth_dist.get(c, 0) + 1
    
    return {
        "hierarchy_total_assets": total_assets,
        "hierarchy_root_assets": root_assets,
        "hierarchy_assets_with_parents": assets_with_parents,
        "hierarchy_orphan_count": orphan_count,
        "hierarchy_orphan_rate": round(orphan_rate, 2),
        "hierarchy_completion_rate": round(completion_rate, 2),
        "hierarchy_avg_depth": round(avg_depth, 2),
        "hierarchy_max_depth": max_depth,
        "hierarchy_avg_children": round(avg_children, 2),
        "hierarchy_std_children": round(std_children, 2),
        "hierarchy_max_children": max_children,
        "hierarchy_parents_count": len(acc.children_count_map),
        "hierarchy_depth_distribution": dict(sorted(depth_dist.items())),
        "hierarchy_breadth_distribution": dict(sorted(breadth_dist.items())),
    }
