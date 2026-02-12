# -*- coding: utf-8 -*-
"""
IDI Maintenance metrics module.

Computes metrics for maintenance-related IDI views:
- IDI_MaintenancePlan (linked to: asset, work order)
- IDI_MaintenancePlanItem (linked to: asset, maintenance plan, work order)
- IDI_Maintenance_Order (linked to: asset, maintenance plan, maintenance plan item, operations)
- IDI_Operations (linked to: work order)
- IDI_MaximoWorkorder (linked to: asset)
- IDI_Notifications (linked to: asset, work order)

These replace the previous RMDM v1 maintenance metrics with customer-specific IDI views.
"""

import logging
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
)

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_MAINTENANCE_IDI_CONFIG = {
    "maintenance_idi_dm_space": "rmdm",
    "maintenance_idi_dm_version": "v1",
    
    # Feature flag
    "enable_maintenance_idi_metrics": True,
    
    # Max instances per view
    "max_maintenance_idi_instances": 150000,
}

# View definitions with their relation properties
# Format: {view_id: {"display_name": str, "relations": {prop_name: relation_type}}}
MAINTENANCE_IDI_VIEW_DEFINITIONS = {
    "IDI_MaintenancePlan": {
        "display_name": "Maintenance Plans",
        "relations": {
            "asset": "single",        # asset relation
            "workOrder": "single",    # work order relation
        },
    },
    "IDI_MaintenancePlanItem": {
        "display_name": "Maintenance Plan Items",
        "relations": {
            "asset": "single",
            "maintenancePlan": "single",
            "workOrder": "single",
        },
    },
    "IDI_Maintenance_Order": {
        "display_name": "Maintenance Orders",
        "relations": {
            "asset": "single",
            "maintenancePlan": "single",
            "maintenancePlanItem": "single",
            "operations": "multi",
        },
    },
    "IDI_Operations": {
        "display_name": "Operations",
        "relations": {
            "workOrder": "single",
        },
    },
    "IDI_MaximoWorkorder": {
        "display_name": "Maximo Work Orders",
        "relations": {
            "asset": "single",
        },
    },
    "IDI_Notifications": {
        "display_name": "Notifications",
        "relations": {
            "asset": "single",
            "workOrder": "single",
        },
    },
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class MaintenanceIDIViewData:
    """Data collected for a single maintenance IDI view."""
    view_id: str
    display_name: str
    total_instances: int = 0
    unique_instances: int = 0
    
    # Per-relation counts
    relation_counts: Dict[str, int] = field(default_factory=dict)
    
    # Unique linked entities per relation
    relation_unique_links: Dict[str, Set[str]] = field(default_factory=dict)
    
    # Duplicate tracking
    duplicate_ids: List[str] = field(default_factory=list)
    ids_seen: Set[str] = field(default_factory=set)
    
    # IDs without asset link (for CSV export)
    ids_without_asset: List[str] = field(default_factory=list)
    
    # Error tracking
    error: Optional[str] = None
    
    @property
    def duplicates(self) -> int:
        return self.total_instances - self.unique_instances
    
    def get_link_rate(self, relation: str) -> Optional[float]:
        """Get the link rate for a specific relation."""
        if self.unique_instances == 0:
            return None
        count = self.relation_counts.get(relation, 0)
        return round((count / self.unique_instances) * 100, 2)


@dataclass
class MaintenanceIDIAccumulator:
    """Accumulates data for all maintenance IDI views."""
    view_data: Dict[str, MaintenanceIDIViewData] = field(default_factory=dict)
    
    # Cross-view linkage tracking
    assets_with_maintenance: Set[str] = field(default_factory=set)
    
    def get_or_create(self, view_id: str, display_name: str) -> MaintenanceIDIViewData:
        """Get existing view data or create new one."""
        if view_id not in self.view_data:
            self.view_data[view_id] = MaintenanceIDIViewData(
                view_id=view_id,
                display_name=display_name
            )
        return self.view_data[view_id]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for JSON storage."""
        return {
            "view_data": {
                view_id: {
                    "view_id": data.view_id,
                    "display_name": data.display_name,
                    "total_instances": data.total_instances,
                    "unique_instances": data.unique_instances,
                    "relation_counts": data.relation_counts,
                    "relation_unique_links": {
                        k: len(v) for k, v in data.relation_unique_links.items()
                    },
                    "duplicates": data.duplicates,
                    "error": data.error,
                }
                for view_id, data in self.view_data.items()
            },
            "assets_with_maintenance": list(self.assets_with_maintenance)[:1000],
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MaintenanceIDIAccumulator":
        """Deserialize from JSON."""
        acc = cls()
        acc.assets_with_maintenance = set(data.get("assets_with_maintenance", []))
        for view_id, vdata in data.get("view_data", {}).items():
            view = MaintenanceIDIViewData(
                view_id=vdata["view_id"],
                display_name=vdata["display_name"],
                total_instances=vdata["total_instances"],
                unique_instances=vdata["unique_instances"],
                relation_counts=vdata.get("relation_counts", {}),
                error=vdata.get("error"),
            )
            acc.view_data[view_id] = view
        return acc


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_direct_relation_id(prop) -> Optional[str]:
    """Extract external_id from a direct relation property."""
    if not prop:
        return None
    if isinstance(prop, dict):
        return prop.get("externalId") or prop.get("external_id")
    if isinstance(prop, list) and prop:
        first = prop[0]
        if isinstance(first, dict):
            return first.get("externalId") or first.get("external_id")
        return getattr(first, "external_id", None)
    return getattr(prop, "external_id", None)


def get_direct_relation_ids(prop) -> List[str]:
    """Extract list of external_ids from a multi-relation property."""
    if not prop:
        return []
    if isinstance(prop, list):
        result = []
        for item in prop:
            if isinstance(item, dict):
                eid = item.get("externalId") or item.get("external_id")
            else:
                eid = getattr(item, "external_id", None)
            if eid:
                result.append(eid)
        return result
    # Single item
    eid = get_direct_relation_id(prop)
    return [eid] if eid else []


# ============================================================
# BATCH PROCESSING
# ============================================================

def process_maintenance_idi_view_batch(
    batch,
    view: ViewId,
    view_id: str,
    display_name: str,
    relations: Dict[str, str],
    acc: MaintenanceIDIAccumulator
):
    """
    Process a batch of instances from a maintenance IDI view.
    
    Args:
        batch: Batch of DM instances
        view: ViewId for property access
        view_id: External ID of the view
        display_name: Human-readable name
        relations: Dict of relation property names -> type (single/multi)
        acc: MaintenanceIDIAccumulator to update
    """
    view_data = acc.get_or_create(view_id, display_name)
    
    # Initialize relation tracking if needed
    for rel_name in relations.keys():
        if rel_name not in view_data.relation_counts:
            view_data.relation_counts[rel_name] = 0
        if rel_name not in view_data.relation_unique_links:
            view_data.relation_unique_links[rel_name] = set()
    
    for node in batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        view_data.total_instances += 1
        
        # Check for duplicates
        if node_id in view_data.ids_seen:
            view_data.duplicate_ids.append(node_id)
            continue
        view_data.ids_seen.add(node_id)
        view_data.unique_instances += 1
        
        # Extract properties
        props = get_props(node, view)
        
        # Process each relation
        has_asset = False
        for rel_name, rel_type in relations.items():
            if rel_type == "multi":
                rel_ids = get_direct_relation_ids(props.get(rel_name))
            else:
                rel_id = get_direct_relation_id(props.get(rel_name))
                rel_ids = [rel_id] if rel_id else []
            
            if rel_ids:
                view_data.relation_counts[rel_name] += 1
                view_data.relation_unique_links[rel_name].update(rel_ids)
                if rel_name == "asset":
                    has_asset = True
                    acc.assets_with_maintenance.update(rel_ids)
        
        # Track IDs without asset link for CSV export (only for views that have asset relation)
        if "asset" in relations and not has_asset:
            view_data.ids_without_asset.append(node_id)


# ============================================================
# COLLECTION FUNCTION
# ============================================================

def collect_maintenance_idi_metrics(
    client: CogniteClient,
    config: Dict[str, Any] = None,
    view_definitions: Dict[str, Dict[str, Any]] = None
) -> MaintenanceIDIAccumulator:
    """
    Collect metrics for all maintenance IDI views.
    
    Args:
        client: CogniteClient instance
        config: Configuration overrides
        view_definitions: Custom view definitions (optional)
    
    Returns:
        MaintenanceIDIAccumulator with collected data
    """
    cfg = {**DEFAULT_MAINTENANCE_IDI_CONFIG, **(config or {})}
    
    space = cfg.get("maintenance_idi_dm_space", "rmdm")
    version = cfg.get("maintenance_idi_dm_version", "v1")
    max_instances = cfg.get("max_maintenance_idi_instances", 150000)
    chunk_size = cfg.get("chunk_size", 500)
    
    definitions = view_definitions or MAINTENANCE_IDI_VIEW_DEFINITIONS
    
    acc = MaintenanceIDIAccumulator()
    
    logger.info(f"Collecting Maintenance IDI metrics: space={space}, version={version}")
    logger.info(f"Processing {len(definitions)} views")
    
    for view_id, view_def in definitions.items():
        display_name = view_def.get("display_name", view_id)
        relations = view_def.get("relations", {})
        
        logger.info(f"[MaintenanceIDI] Processing view: {view_id}")
        
        try:
            view = ViewId(space=space, external_id=view_id, version=version)
            view_data = acc.get_or_create(view_id, display_name)
            
            for batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=view,
            ):
                process_maintenance_idi_view_batch(
                    batch, view, view_id, display_name, relations, acc
                )
                
                if max_instances is not None and view_data.unique_instances >= max_instances:
                    logger.info(f"[MaintenanceIDI] {view_id}: Reached limit ({max_instances:,})")
                    break
            
            # Log summary
            rel_summary = ", ".join(
                f"{k}={view_data.relation_counts.get(k, 0)}"
                for k in relations.keys()
            )
            logger.info(
                f"[MaintenanceIDI] {view_id}: total={view_data.unique_instances:,}, "
                f"relations: {rel_summary}"
            )
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                logger.warning(f"[MaintenanceIDI] {view_id}: View not found")
                view_data = acc.get_or_create(view_id, display_name)
                view_data.error = "not_found"
            else:
                logger.warning(f"[MaintenanceIDI] {view_id}: Error - {error_msg[:100]}")
                view_data = acc.get_or_create(view_id, display_name)
                view_data.error = error_msg[:200]
    
    return acc


# ============================================================
# METRICS COMPUTATION
# ============================================================

def compute_maintenance_idi_metrics(
    acc: MaintenanceIDIAccumulator,
    total_assets: int = 0
) -> Dict[str, Any]:
    """
    Compute metrics for all maintenance IDI views.
    
    Args:
        acc: MaintenanceIDIAccumulator with collected data
        total_assets: Total number of assets (for coverage calculation)
    
    Returns:
        Dictionary containing metrics for all views
    """
    views_metrics = []
    total_instances = 0
    total_with_asset = 0
    views_with_data = 0
    views_not_found = 0
    views_with_errors = 0
    
    for view_id, data in acc.view_data.items():
        if data.error == "not_found":
            views_not_found += 1
            status = "not_found"
        elif data.error:
            views_with_errors += 1
            status = "error"
        elif data.unique_instances > 0:
            views_with_data += 1
            status = "ok"
        else:
            status = "empty"
        
        # Build relation metrics
        relation_metrics = {}
        for rel_name in data.relation_counts.keys():
            count = data.relation_counts.get(rel_name, 0)
            unique_links = len(data.relation_unique_links.get(rel_name, set()))
            rate = data.get_link_rate(rel_name)
            
            relation_metrics[rel_name] = {
                "count": count,
                "rate": rate,
                "unique_links": unique_links,
                "without": data.unique_instances - count,
            }
        
        view_metrics = {
            "view_id": view_id,
            "display_name": data.display_name,
            "status": status,
            "total_instances": data.total_instances,
            "unique_instances": data.unique_instances,
            "duplicates": data.duplicates,
            "relations": relation_metrics,
            "unlinked_asset_ids": data.ids_without_asset,
            "error": data.error,
        }
        views_metrics.append(view_metrics)
        
        if status == "ok":
            total_instances += data.unique_instances
            # Sum asset links from views that have asset relation
            if "asset" in data.relation_counts:
                total_with_asset += data.relation_counts["asset"]
    
    # Asset maintenance coverage
    assets_with_maintenance = len(acc.assets_with_maintenance)
    asset_coverage_rate = (
        round((assets_with_maintenance / total_assets) * 100, 2)
        if total_assets > 0 else None
    )
    
    return {
        "maint_idi_has_data": views_with_data > 0,
        
        # Summary
        "maint_idi_total_views": len(acc.view_data),
        "maint_idi_views_with_data": views_with_data,
        "maint_idi_views_not_found": views_not_found,
        "maint_idi_views_with_errors": views_with_errors,
        
        # Totals
        "maint_idi_total_instances": total_instances,
        "maint_idi_total_with_asset": total_with_asset,
        
        # Asset coverage
        "maint_idi_assets_with_maintenance": assets_with_maintenance,
        "maint_idi_asset_coverage_rate": asset_coverage_rate,
        
        # Per-view metrics
        "maint_idi_views": views_metrics,
    }
