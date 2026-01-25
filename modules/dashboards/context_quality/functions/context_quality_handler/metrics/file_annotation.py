"""
File Annotation Metrics (CDM CogniteDiagramAnnotation).

Computes metrics for file/diagram annotation contextualization.
Uses CogniteDiagramAnnotation edges from the Core Data Model.
"""

import logging
from typing import Optional, Set
from dataclasses import dataclass, field

from cognite.client.data_classes.data_modeling import ViewId


logger = logging.getLogger(__name__)


# ----------------------------------------------------
# DATA CLASSES
# ----------------------------------------------------

@dataclass
class AnnotationData:
    """Data collected for a single diagram annotation edge."""
    annotation_id: str
    start_node_space: Optional[str]
    start_node_external_id: Optional[str]  # Source file
    end_node_space: Optional[str]
    end_node_external_id: Optional[str]    # Target (asset, file, etc.)
    status: Optional[str]                  # approved, suggested, rejected
    confidence: Optional[float]
    annotation_type: Optional[str]         # Derived from end node type


@dataclass
class FileAnnotationAccumulator:
    """
    Accumulates file annotation data for metrics computation.
    """
    # Annotation counts
    total_annotations: int = 0
    annotation_ids_seen: Set[str] = field(default_factory=set)
    
    # By target type
    asset_annotations: int = 0           # Annotations pointing to assets (asset tags)
    file_annotations: int = 0            # Annotations pointing to files (file links)
    equipment_annotations: int = 0       # Annotations pointing to equipment
    other_annotations: int = 0           # Other annotation types
    
    # By status
    status_approved: int = 0
    status_suggested: int = 0
    status_rejected: int = 0
    status_other: int = 0
    
    # Confidence distribution
    confidence_high: int = 0    # >= 0.9
    confidence_medium: int = 0  # >= 0.5 and < 0.9
    confidence_low: int = 0     # < 0.5
    confidence_missing: int = 0 # No confidence value
    confidence_sum: float = 0.0
    confidence_count: int = 0
    
    # Unique entities
    unique_source_files: Set[str] = field(default_factory=set)  # Files with annotations
    unique_target_assets: Set[str] = field(default_factory=set)  # Assets linked
    unique_target_files: Set[str] = field(default_factory=set)   # Files linked to other files
    
    # Page tracking
    annotations_with_page: int = 0
    unique_pages_annotated: Set[tuple] = field(default_factory=set)  # (file_id, page_number)
    
    @property
    def unique_annotations(self) -> int:
        """Unique annotation count."""
        return len(self.annotation_ids_seen)


def get_edge_props(edge, view: ViewId) -> dict:
    """Safely retrieves properties from an edge instance for a given view."""
    return edge.properties.get(view, {}) or {}


def get_node_ref(node_ref) -> tuple:
    """Extract space and external_id from a node reference."""
    if node_ref is None:
        return (None, None)
    if isinstance(node_ref, dict):
        return (node_ref.get("space"), node_ref.get("externalId"))
    # NodeId object
    return (getattr(node_ref, "space", None), getattr(node_ref, "external_id", None))


def infer_annotation_type(end_node_external_id: Optional[str]) -> str:
    """
    Infer annotation type from the end node external ID.
    This is a heuristic - adjust based on your naming conventions.
    """
    if not end_node_external_id:
        return "unknown"
    
    ext_id_lower = end_node_external_id.lower()
    
    # Common asset indicators
    if any(x in ext_id_lower for x in ["asset", "equip", "pump", "valve", "tag"]):
        return "asset"
    
    # Common file indicators
    if any(x in ext_id_lower for x in ["file", "doc", "pdf", "dwg", "drawing"]):
        return "file"
    
    return "other"


def process_annotation_batch(
    batch,
    view: ViewId,
    acc: FileAnnotationAccumulator
):
    """
    Process a batch of CogniteDiagramAnnotation edges.
    
    Args:
        batch: Batch of edge instances
        view: The annotation view ID
        acc: Accumulator for annotation data
    """
    for edge in batch:
        acc.total_annotations += 1
        
        # Get edge ID
        edge_id = getattr(edge, "external_id", None) or str(edge)
        
        # Skip duplicates
        if edge_id in acc.annotation_ids_seen:
            continue
        acc.annotation_ids_seen.add(edge_id)
        
        # Get properties
        props = get_edge_props(edge, view)
        
        # Extract source and target nodes
        start_node = getattr(edge, "start_node", None)
        end_node = getattr(edge, "end_node", None)
        
        start_space, start_ext_id = get_node_ref(start_node)
        end_space, end_ext_id = get_node_ref(end_node)
        
        # Track unique source files
        if start_ext_id:
            acc.unique_source_files.add(start_ext_id)
        
        # Infer annotation type and track targets
        annotation_type = infer_annotation_type(end_ext_id)
        
        if annotation_type == "asset":
            acc.asset_annotations += 1
            if end_ext_id:
                acc.unique_target_assets.add(end_ext_id)
        elif annotation_type == "file":
            acc.file_annotations += 1
            if end_ext_id:
                acc.unique_target_files.add(end_ext_id)
        else:
            acc.other_annotations += 1
        
        # Process status
        status = props.get("status")
        if status:
            status_lower = str(status).lower()
            if "approved" in status_lower:
                acc.status_approved += 1
            elif "suggested" in status_lower:
                acc.status_suggested += 1
            elif "rejected" in status_lower:
                acc.status_rejected += 1
            else:
                acc.status_other += 1
        else:
            acc.status_other += 1
        
        # Process confidence
        confidence = props.get("confidence")
        if confidence is not None:
            try:
                conf_val = float(confidence)
                acc.confidence_sum += conf_val
                acc.confidence_count += 1
                
                if conf_val >= 0.9:
                    acc.confidence_high += 1
                elif conf_val >= 0.5:
                    acc.confidence_medium += 1
                else:
                    acc.confidence_low += 1
            except (ValueError, TypeError):
                acc.confidence_missing += 1
        else:
            acc.confidence_missing += 1
        
        # Track page information
        page_number = props.get("startNodePageNumber")
        if page_number is not None:
            acc.annotations_with_page += 1
            if start_ext_id:
                acc.unique_pages_annotated.add((start_ext_id, page_number))


def compute_file_annotation_metrics(acc: FileAnnotationAccumulator) -> dict:
    """
    Compute file annotation metrics from accumulated data.
    
    Args:
        acc: FileAnnotationAccumulator with collected data
        
    Returns:
        Dictionary of computed metrics
    """
    total = acc.unique_annotations
    
    # Average confidence
    avg_confidence = round(acc.confidence_sum / acc.confidence_count * 100, 1) if acc.confidence_count > 0 else None
    
    # Confidence distribution percentages
    conf_total = acc.confidence_high + acc.confidence_medium + acc.confidence_low
    high_pct = round(acc.confidence_high / conf_total * 100, 1) if conf_total > 0 else 0
    medium_pct = round(acc.confidence_medium / conf_total * 100, 1) if conf_total > 0 else 0
    low_pct = round(acc.confidence_low / conf_total * 100, 1) if conf_total > 0 else 0
    
    # Status distribution percentages
    status_total = acc.status_approved + acc.status_suggested + acc.status_rejected + acc.status_other
    approved_pct = round(acc.status_approved / status_total * 100, 1) if status_total > 0 else 0
    suggested_pct = round(acc.status_suggested / status_total * 100, 1) if status_total > 0 else 0
    rejected_pct = round(acc.status_rejected / status_total * 100, 1) if status_total > 0 else 0
    
    # Annotation type distribution
    type_total = acc.asset_annotations + acc.file_annotations + acc.other_annotations
    asset_pct = round(acc.asset_annotations / type_total * 100, 1) if type_total > 0 else 0
    file_pct = round(acc.file_annotations / type_total * 100, 1) if type_total > 0 else 0
    
    return {
        # Flags
        "annot_has_data": total > 0,
        
        # Counts
        "annot_total": total,
        "annot_unique_files_with_annotations": len(acc.unique_source_files),
        "annot_unique_assets_linked": len(acc.unique_target_assets),
        "annot_unique_files_linked": len(acc.unique_target_files),
        "annot_unique_pages": len(acc.unique_pages_annotated),
        
        # By type
        "annot_asset_tags": acc.asset_annotations,
        "annot_file_links": acc.file_annotations,
        "annot_other": acc.other_annotations,
        "annot_asset_tag_pct": asset_pct,
        "annot_file_link_pct": file_pct,
        
        # By status
        "annot_approved": acc.status_approved,
        "annot_suggested": acc.status_suggested,
        "annot_rejected": acc.status_rejected,
        "annot_approved_pct": approved_pct,
        "annot_suggested_pct": suggested_pct,
        "annot_rejected_pct": rejected_pct,
        
        # Confidence metrics
        "annot_avg_confidence": avg_confidence,
        "annot_confidence_high": acc.confidence_high,
        "annot_confidence_medium": acc.confidence_medium,
        "annot_confidence_low": acc.confidence_low,
        "annot_confidence_missing": acc.confidence_missing,
        "annot_confidence_high_pct": high_pct,
        "annot_confidence_medium_pct": medium_pct,
        "annot_confidence_low_pct": low_pct,
    }
