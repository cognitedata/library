# -*- coding: utf-8 -*-
"""
"Others" metrics module for miscellaneous IDI_* views.

Computes asset linkage metrics for views that don't fit into
the main dashboard categories (maintenance, equipment, files, etc.).

Views covered:
- IDI_LIMS (asset)
- IDI_Dynamo (asset)
- IDI_Runlog (asset)
- IDI_ELOG_Val (asset)
- IDI_DownGradeSituation (asset)
- IDI_MOC (asset)
- IDI_BOM (asset)
- IDI_Permits (asset)
- IDI_InspectionTask (asset)
- IDI_InspectionRec (asset)
- IDI_InspectionRepo (asset)
- IDI_PRD_ANAL_RBI (asset)
- IDI_RBI_PIPI (asset)
- IDI_RCA_Analysis (asset)
- IDI_RCA_Legacy (asset)
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

# Default view configurations for "Others" views
# All views are in the same DM space (configurable)
DEFAULT_OTHERS_CONFIG = {
    "others_dm_space": "rmdm",
    "others_dm_version": "v1",
    
    # Feature flag
    "enable_others_metrics": True,
    
    # Max instances per view
    "max_others_instances": 150000,
}

# Views to process with their asset relation property name
# Format: {view_external_id: {"asset_prop": "property_name", "display_name": "Human Name"}}
OTHERS_VIEW_DEFINITIONS = {
    "IDI_LIMS": {
        "asset_prop": "asset",
        "display_name": "LIMS (Lab Data)",
    },
    "IDI_Dynamo": {
        "asset_prop": "asset",
        "display_name": "Dynamo (Alarms)",
    },
    "IDI_Runlog": {
        "asset_prop": "asset",
        "display_name": "Runlog",
    },
    "IDI_ELOG_Val": {
        "asset_prop": "asset",
        "display_name": "ELOG Values",
    },
    "IDI_DownGradeSituation": {
        "asset_prop": "asset",
        "display_name": "Downgrade Situations",
    },
    "IDI_MOC": {
        "asset_prop": "asset",
        "display_name": "Management of Change",
    },
    "IDI_BOM": {
        "asset_prop": "asset",
        "display_name": "Bill of Materials",
    },
    "IDI_Permits": {
        "asset_prop": "asset",
        "display_name": "Permits",
    },
    "IDI_InspectionTask": {
        "asset_prop": "asset",
        "display_name": "Inspection Tasks",
    },
    "IDI_InspectionRec": {
        "asset_prop": "asset",
        "display_name": "Inspection Records",
    },
    "IDI_InspectionRepo": {
        "asset_prop": "asset",
        "display_name": "Inspection Reports",
    },
    "IDI_PRD_ANAL_RBI": {
        "asset_prop": "asset",
        "display_name": "PRD Analysis RBI",
    },
    "IDI_RBI_PIPI": {
        "asset_prop": "asset",
        "display_name": "RBI PIPI",
    },
    "IDI_RCA_Analysis": {
        "asset_prop": "asset",
        "display_name": "RCA Analysis",
    },
    "IDI_RCA_Legacy": {
        "asset_prop": "asset",
        "display_name": "RCA Legacy",
    },
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class OthersViewData:
    """Data collected for a single "others" view."""
    view_id: str
    display_name: str
    total_instances: int = 0
    unique_instances: int = 0
    instances_with_asset: int = 0
    assets_linked: Set[str] = field(default_factory=set)
    duplicate_ids: List[str] = field(default_factory=list)
    ids_seen: Set[str] = field(default_factory=set)
    ids_without_asset: List[str] = field(default_factory=list)
    error: Optional[str] = None
    
    @property
    def duplicates(self) -> int:
        return self.total_instances - self.unique_instances
    
    @property
    def asset_link_rate(self) -> Optional[float]:
        if self.unique_instances == 0:
            return None
        return round((self.instances_with_asset / self.unique_instances) * 100, 2)


@dataclass
class OthersAccumulator:
    """Accumulates data for all 'Others' views."""
    view_data: Dict[str, OthersViewData] = field(default_factory=dict)
    
    def get_or_create(self, view_id: str, display_name: str) -> OthersViewData:
        """Get existing view data or create new one."""
        if view_id not in self.view_data:
            self.view_data[view_id] = OthersViewData(
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
                    "instances_with_asset": data.instances_with_asset,
                    "assets_linked": list(data.assets_linked),
                    "duplicate_ids": data.duplicate_ids[:100],  # Limit for storage
                    "ids_without_asset": getattr(data, "ids_without_asset", [])[:50000],
                    "error": data.error,
                }
                for view_id, data in self.view_data.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OthersAccumulator":
        """Deserialize from JSON."""
        acc = cls()
        for view_id, vdata in data.get("view_data", {}).items():
            view = OthersViewData(
                view_id=vdata["view_id"],
                display_name=vdata["display_name"],
                total_instances=vdata["total_instances"],
                unique_instances=vdata["unique_instances"],
                instances_with_asset=vdata["instances_with_asset"],
                assets_linked=set(vdata.get("assets_linked", [])),
                duplicate_ids=vdata.get("duplicate_ids", []),
                ids_without_asset=vdata.get("ids_without_asset", []),
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

def process_others_view_batch(
    batch,
    view: ViewId,
    view_id: str,
    asset_prop: str,
    display_name: str,
    acc: OthersAccumulator
):
    """
    Process a batch of instances from an 'Others' view.
    
    Args:
        batch: Batch of DM instances
        view: ViewId for property access
        view_id: External ID of the view
        asset_prop: Property name for asset relation
        display_name: Human-readable name for the view
        acc: OthersAccumulator to update
    """
    view_data = acc.get_or_create(view_id, display_name)
    
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
        
        # Extract asset relation
        props = get_props(node, view)
        asset_ids = get_direct_relation_ids(props.get(asset_prop))
        
        if asset_ids:
            view_data.instances_with_asset += 1
            view_data.assets_linked.update(asset_ids)
        else:
            view_data.ids_without_asset.append(node_id)


# ============================================================
# COLLECTION FUNCTION
# ============================================================

def collect_others_metrics(
    client: CogniteClient,
    config: Dict[str, Any] = None,
    view_definitions: Dict[str, Dict[str, str]] = None
) -> OthersAccumulator:
    """
    Collect metrics for all 'Others' views.
    
    Args:
        client: CogniteClient instance
        config: Configuration overrides
        view_definitions: Custom view definitions (optional)
    
    Returns:
        OthersAccumulator with collected data
    """
    cfg = {**DEFAULT_OTHERS_CONFIG, **(config or {})}
    
    space = cfg.get("others_dm_space", "rmdm")
    version = cfg.get("others_dm_version", "v1")
    max_instances = cfg.get("max_others_instances", 150000)
    chunk_size = cfg.get("chunk_size", 500)
    
    definitions = view_definitions or OTHERS_VIEW_DEFINITIONS
    
    acc = OthersAccumulator()
    
    logger.info(f"Collecting 'Others' metrics: space={space}, version={version}")
    logger.info(f"Processing {len(definitions)} views")
    
    for view_id, view_def in definitions.items():
        asset_prop = view_def.get("asset_prop", "asset")
        display_name = view_def.get("display_name", view_id)
        
        logger.info(f"[Others] Processing view: {view_id}")
        
        try:
            view = ViewId(space=space, external_id=view_id, version=version)
            view_data = acc.get_or_create(view_id, display_name)
            
            for batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=view,
            ):
                process_others_view_batch(
                    batch, view, view_id, asset_prop, display_name, acc
                )
                
                # Guard against None: avoid ">=" between int and NoneType
                u = view_data.unique_instances if view_data.unique_instances is not None else 0
                if max_instances is not None and u >= max_instances:
                    logger.info(f"[Others] {view_id}: Reached limit ({max_instances:,})")
                    break
            
            rate = view_data.asset_link_rate
            rate_str = f"{rate}%" if rate is not None else "N/A"
            logger.info(
                f"[Others] {view_id}: total={view_data.unique_instances or 0:,}, "
                f"with_asset={view_data.instances_with_asset:,} ({rate_str})"
            )
            
        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                logger.warning(f"[Others] {view_id}: View not found")
                view_data = acc.get_or_create(view_id, display_name)
                view_data.error = "not_found"
            else:
                logger.warning(f"[Others] {view_id}: Error - {error_msg[:100]}")
                view_data = acc.get_or_create(view_id, display_name)
                view_data.error = error_msg[:200]
    
    return acc


# ============================================================
# METRICS COMPUTATION
# ============================================================

def compute_others_metrics(acc: OthersAccumulator) -> Dict[str, Any]:
    """
    Compute metrics for all 'Others' views.
    
    Args:
        acc: OthersAccumulator with collected data
    
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
        elif (data.unique_instances or 0) > 0:
            views_with_data += 1
            status = "ok"
        else:
            status = "empty"
        
        u = data.unique_instances if data.unique_instances is not None else 0
        with_asset = data.instances_with_asset if data.instances_with_asset is not None else 0
        view_metrics = {
            "view_id": view_id,
            "display_name": data.display_name,
            "status": status,
            "total_instances": data.total_instances,
            "unique_instances": data.unique_instances,
            "duplicates": data.duplicates,
            "instances_with_asset": data.instances_with_asset,
            "instances_without_asset": u - with_asset,
            "unlinked_asset_ids": data.ids_without_asset,
            "asset_link_rate": data.asset_link_rate,
            "unique_assets_linked": len(data.assets_linked),
            "error": data.error,
        }
        views_metrics.append(view_metrics)
        
        if status == "ok":
            total_instances += u
            total_with_asset += with_asset
    
    # Overall rate
    overall_asset_rate = (
        round((total_with_asset / total_instances) * 100, 2)
        if total_instances > 0 else None
    )
    
    return {
        "others_has_data": views_with_data > 0,
        
        # Summary
        "others_total_views": len(acc.view_data),
        "others_views_with_data": views_with_data,
        "others_views_not_found": views_not_found,
        "others_views_with_errors": views_with_errors,
        
        # Totals
        "others_total_instances": total_instances,
        "others_total_with_asset": total_with_asset,
        "others_overall_asset_rate": overall_asset_rate,
        
        # Per-view metrics
        "others_views": views_metrics,
    }
