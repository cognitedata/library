"""
3D Model Contextualization metrics processing and computation.

Metrics:
1. Overall Asset 3D Coverage - % of assets with 3D object associations
2. Critical Asset 3D Association - % of critical assets with 3D links
3. Bounding Box Completeness - % of 3D objects with complete bounding boxes
"""

from typing import Optional, Set
from dataclasses import dataclass, field
from cognite.client.data_classes.data_modeling import ViewId

from .common import get_props, get_external_id


# ----------------------------------------------------
# 3D ACCUMULATOR
# ----------------------------------------------------

@dataclass
class Model3DAccumulator:
    """
    Accumulates data for 3D model contextualization metrics.
    """
    # Asset-3D relationship tracking
    total_assets_checked: int = 0
    assets_with_3d: int = 0
    asset_ids_with_3d: Set[str] = field(default_factory=set)
    
    # Critical asset tracking
    critical_assets_total: int = 0
    critical_assets_with_3d: int = 0
    
    # 3D Object tracking
    total_3d_objects: int = 0
    objects_3d_ids_seen: Set[str] = field(default_factory=set)
    objects_3d_duplicate_ids: list = field(default_factory=list)  # Duplicate external IDs
    
    # 3D → Asset contextualization (MOST IMPORTANT)
    objects_with_asset_link: int = 0
    
    # Bounding box completeness
    objects_with_complete_bbox: int = 0
    objects_with_partial_bbox: int = 0
    objects_with_no_bbox: int = 0
    
    # Model type coverage (CAD, 360, PointCloud)
    objects_with_cad_nodes: int = 0
    objects_with_360_images: int = 0
    objects_with_point_cloud: int = 0
    objects_with_multiple_models: int = 0
    
    def to_dict(self) -> dict:
        """Serialize for batch storage."""
        return {
            "total_assets_checked": self.total_assets_checked,
            "assets_with_3d": self.assets_with_3d,
            "asset_ids_with_3d": list(self.asset_ids_with_3d),
            "critical_assets_total": self.critical_assets_total,
            "critical_assets_with_3d": self.critical_assets_with_3d,
            "total_3d_objects": self.total_3d_objects,
            "objects_3d_ids_seen": list(self.objects_3d_ids_seen),
            "objects_3d_duplicate_ids": self.objects_3d_duplicate_ids,
            "objects_with_asset_link": self.objects_with_asset_link,
            "objects_with_complete_bbox": self.objects_with_complete_bbox,
            "objects_with_partial_bbox": self.objects_with_partial_bbox,
            "objects_with_no_bbox": self.objects_with_no_bbox,
            "objects_with_cad_nodes": self.objects_with_cad_nodes,
            "objects_with_360_images": self.objects_with_360_images,
            "objects_with_point_cloud": self.objects_with_point_cloud,
            "objects_with_multiple_models": self.objects_with_multiple_models,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Model3DAccumulator":
        """Deserialize from batch storage."""
        acc = cls()
        acc.total_assets_checked = data.get("total_assets_checked", 0)
        acc.assets_with_3d = data.get("assets_with_3d", 0)
        acc.asset_ids_with_3d = set(data.get("asset_ids_with_3d", []))
        acc.critical_assets_total = data.get("critical_assets_total", 0)
        acc.critical_assets_with_3d = data.get("critical_assets_with_3d", 0)
        acc.total_3d_objects = data.get("total_3d_objects", 0)
        acc.objects_3d_ids_seen = set(data.get("objects_3d_ids_seen", []))
        acc.objects_3d_duplicate_ids = data.get("objects_3d_duplicate_ids", [])
        acc.objects_with_asset_link = data.get("objects_with_asset_link", 0)
        acc.objects_with_complete_bbox = data.get("objects_with_complete_bbox", 0)
        acc.objects_with_partial_bbox = data.get("objects_with_partial_bbox", 0)
        acc.objects_with_no_bbox = data.get("objects_with_no_bbox", 0)
        acc.objects_with_cad_nodes = data.get("objects_with_cad_nodes", 0)
        acc.objects_with_360_images = data.get("objects_with_360_images", 0)
        acc.objects_with_point_cloud = data.get("objects_with_point_cloud", 0)
        acc.objects_with_multiple_models = data.get("objects_with_multiple_models", 0)
        return acc
    
    def merge_from(self, other: "Model3DAccumulator"):
        """Merge another accumulator (for batch aggregation)."""
        self.total_assets_checked += other.total_assets_checked
        self.assets_with_3d += other.assets_with_3d
        self.asset_ids_with_3d.update(other.asset_ids_with_3d)
        self.critical_assets_total += other.critical_assets_total
        self.critical_assets_with_3d += other.critical_assets_with_3d
        self.total_3d_objects += other.total_3d_objects
        self.objects_3d_ids_seen.update(other.objects_3d_ids_seen)
        self.objects_3d_duplicate_ids.extend(other.objects_3d_duplicate_ids)
        self.objects_with_asset_link += other.objects_with_asset_link
        self.objects_with_complete_bbox += other.objects_with_complete_bbox
        self.objects_with_partial_bbox += other.objects_with_partial_bbox
        self.objects_with_no_bbox += other.objects_with_no_bbox
        self.objects_with_cad_nodes += other.objects_with_cad_nodes
        self.objects_with_360_images += other.objects_with_360_images
        self.objects_with_point_cloud += other.objects_with_point_cloud
        self.objects_with_multiple_models += other.objects_with_multiple_models


# ----------------------------------------------------
# BATCH PROCESSORS
# ----------------------------------------------------

def get_direct_relation_id(prop) -> Optional[str]:
    """Extract external ID from a direct relation property."""
    if not prop:
        return None
    if isinstance(prop, dict):
        return prop.get("externalId")
    return getattr(prop, "external_id", None)


def process_asset_3d_batch(
    asset_batch,
    asset_view: ViewId,
    acc: Model3DAccumulator
):
    """
    Process assets to check for 3D object associations.
    
    Checks:
    - object3D property for 3D link
    - criticality/technicalObjectAbcIndicator for critical asset tracking
    """
    for node in asset_batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_assets_checked += 1
        props = get_props(node, asset_view)
        
        # Check for 3D object link
        object_3d = props.get("object3D")
        object_3d_id = get_direct_relation_id(object_3d)
        
        has_3d = object_3d_id is not None
        
        if has_3d:
            acc.assets_with_3d += 1
            acc.asset_ids_with_3d.add(node_id)
        
        # Check criticality - support multiple property names
        criticality = (
            props.get("criticality") or 
            props.get("technicalObjectAbcIndicator") or
            ""
        )
        
        # Treat "A" or "critical" as critical
        is_critical = (
            criticality.upper() == "A" or 
            criticality.lower() == "critical"
        )
        
        if is_critical:
            acc.critical_assets_total += 1
            if has_3d:
                acc.critical_assets_with_3d += 1


def process_3d_object_batch(
    object_batch,
    object_view: ViewId,
    acc: Model3DAccumulator
):
    """
    Process 3D objects for contextualization, bounding box, and model type coverage.
    
    Checks:
    - asset property for 3D→Asset contextualization (MOST IMPORTANT)
    - xMin, xMax, yMin, yMax, zMin, zMax for bounding box completeness
    - cadNodes, images360, pointCloudVolumes for model type coverage
    """
    bbox_props = ["xMin", "xMax", "yMin", "yMax", "zMin", "zMax"]
    
    for node in object_batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        # Skip duplicates
        if node_id in acc.objects_3d_ids_seen:
            acc.objects_3d_duplicate_ids.append(node_id)
            continue
        acc.objects_3d_ids_seen.add(node_id)
        acc.total_3d_objects += 1
        
        props = get_props(node, object_view)
        
        # Check 3D → Asset contextualization (MOST IMPORTANT METRIC)
        # The 'asset' property is a reverse relation from CogniteAsset.object3D
        asset_link = props.get("asset")
        if asset_link is not None:
            # Asset link can be a dict, object, or list
            has_asset = True
            if isinstance(asset_link, list):
                has_asset = len(asset_link) > 0
            elif isinstance(asset_link, dict):
                has_asset = asset_link.get("externalId") is not None
        else:
            has_asset = False
        
        if has_asset:
            acc.objects_with_asset_link += 1
        
        # Check bounding box completeness
        bbox_values = [props.get(p) for p in bbox_props]
        bbox_count = sum(1 for v in bbox_values if v is not None)
        
        if bbox_count == 6:
            acc.objects_with_complete_bbox += 1
        elif bbox_count > 0:
            acc.objects_with_partial_bbox += 1
        else:
            acc.objects_with_no_bbox += 1
        
        # Check model type coverage (these are reverse relations, may be empty)
        # For now, we track if properties are present
        has_cad = props.get("cadNodes") is not None
        has_360 = props.get("images360") is not None
        has_pc = props.get("pointCloudVolumes") is not None
        
        model_count = sum([has_cad, has_360, has_pc])
        
        if has_cad:
            acc.objects_with_cad_nodes += 1
        if has_360:
            acc.objects_with_360_images += 1
        if has_pc:
            acc.objects_with_point_cloud += 1
        if model_count > 1:
            acc.objects_with_multiple_models += 1


# ----------------------------------------------------
# METRIC COMPUTATION
# ----------------------------------------------------

def compute_3d_metrics(acc: Model3DAccumulator) -> dict:
    """
    Compute 3D model contextualization metrics.
    
    Returns dict with:
    - 3D Contextualization Rate (3D → Asset) - MOST IMPORTANT
    - Asset 3D coverage metrics (Asset → 3D)
    - Critical asset 3D association
    - Bounding box completeness
    - Model type distribution
    """
    total_assets = acc.total_assets_checked
    total_objects = acc.total_3d_objects
    
    # PRIMARY METRIC: 3D → Asset Contextualization Rate
    # This is the MOST IMPORTANT metric - shows how many 3D objects are linked to assets
    object_contextualization_rate = round(
        (acc.objects_with_asset_link / total_objects * 100) if total_objects > 0 else 0, 1
    )
    
    # Secondary metrics
    asset_3d_coverage = round(
        (acc.assets_with_3d / total_assets * 100) if total_assets > 0 else 0, 1
    )
    
    critical_3d_rate = round(
        (acc.critical_assets_with_3d / acc.critical_assets_total * 100)
        if acc.critical_assets_total > 0 else 0, 1
    )
    
    bbox_completeness = round(
        (acc.objects_with_complete_bbox / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    
    # Bounding box distribution
    bbox_complete_pct = round(
        (acc.objects_with_complete_bbox / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    bbox_partial_pct = round(
        (acc.objects_with_partial_bbox / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    bbox_none_pct = round(
        (acc.objects_with_no_bbox / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    
    # Model type distribution
    cad_pct = round(
        (acc.objects_with_cad_nodes / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    img360_pct = round(
        (acc.objects_with_360_images / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    pointcloud_pct = round(
        (acc.objects_with_point_cloud / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    multi_model_pct = round(
        (acc.objects_with_multiple_models / total_objects * 100)
        if total_objects > 0 else 0, 1
    )
    
    return {
        # Primary metric (MOST IMPORTANT)
        "model3d_contextualization_rate": object_contextualization_rate,
        "model3d_objects_with_asset": acc.objects_with_asset_link,
        
        # Secondary metrics
        "model3d_asset_coverage": asset_3d_coverage,
        "model3d_critical_asset_rate": critical_3d_rate,
        "model3d_bbox_completeness": bbox_completeness,
        
        # Counts
        "model3d_total_assets": total_assets,
        "model3d_assets_with_3d": acc.assets_with_3d,
        "model3d_critical_total": acc.critical_assets_total,
        "model3d_critical_with_3d": acc.critical_assets_with_3d,
        "model3d_total_objects": total_objects,
        
        # Bounding box distribution
        "model3d_bbox_complete_count": acc.objects_with_complete_bbox,
        "model3d_bbox_partial_count": acc.objects_with_partial_bbox,
        "model3d_bbox_none_count": acc.objects_with_no_bbox,
        "model3d_bbox_complete_pct": bbox_complete_pct,
        "model3d_bbox_partial_pct": bbox_partial_pct,
        "model3d_bbox_none_pct": bbox_none_pct,
        
        # Model type distribution
        "model3d_cad_count": acc.objects_with_cad_nodes,
        "model3d_360_count": acc.objects_with_360_images,
        "model3d_pointcloud_count": acc.objects_with_point_cloud,
        "model3d_multi_model_count": acc.objects_with_multiple_models,
        "model3d_cad_pct": cad_pct,
        "model3d_360_pct": img360_pct,
        "model3d_pointcloud_pct": pointcloud_pct,
        "model3d_multi_model_pct": multi_model_pct,
        
        # Duplicate tracking
        "model3d_duplicates": len(acc.objects_3d_duplicate_ids),
        "model3d_duplicate_ids": acc.objects_3d_duplicate_ids,
    }
