"""
File processing and metrics computation.

Measures file contextualization quality based on the CogniteFile view from CDM.
"""

from typing import Optional, List
from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
    FileData,
    CombinedAccumulator,
)


def get_direct_relation_ids(prop) -> List[str]:
    """Extract list of external_ids from a multi-relation property (e.g., assets)."""
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
    # Single item case
    if isinstance(prop, dict):
        eid = prop.get("externalId") or prop.get("external_id")
        return [eid] if eid else []
    eid = getattr(prop, "external_id", None)
    return [eid] if eid else []


def get_direct_relation_id(prop) -> Optional[str]:
    """Extract external_id from a single direct relation property."""
    if not prop:
        return None
    if isinstance(prop, dict):
        return prop.get("externalId") or prop.get("external_id")
    return getattr(prop, "external_id", None)


def process_file_batch(
    file_batch,
    file_view: ViewId,
    acc: CombinedAccumulator
):
    """Process file batch - collect file-asset relationship data."""
    for file_node in file_batch:
        file_id = get_external_id(file_node)
        if not file_id:
            continue
        
        acc.total_file_instances += 1
        
        # Skip if already processed (duplicate)
        if file_id in acc.file_ids_seen:
            acc.file_duplicate_ids.append(file_id)
            continue
        acc.file_ids_seen.add(file_id)
        
        props = get_props(file_node, file_view)
        
        # Extract asset links (direct relation, can be multiple)
        asset_ids = get_direct_relation_ids(props.get("assets"))
        
        # Extract category (direct relation to CogniteFileCategory)
        category_id = get_direct_relation_id(props.get("category"))
        
        # Extract other properties
        mime_type = props.get("mimeType")
        directory = props.get("directory")
        is_uploaded = props.get("isUploaded", False)
        name = props.get("name")
        description = props.get("description")
        source_id = props.get("sourceId")
        
        file_data = FileData(
            file_id=file_id,
            asset_ids=asset_ids,
            category_id=category_id,
            mime_type=mime_type,
            directory=directory,
            is_uploaded=is_uploaded,
            name=name,
            description=description,
            source_id=source_id,
        )
        
        acc.file_list.append(file_data)
        
        # Track file-asset relationships
        if asset_ids:
            acc.files_with_assets += 1
            for asset_id in asset_ids:
                acc.assets_with_files.add(asset_id)
        
        # Track category usage
        if category_id:
            acc.files_with_category += 1
            acc.file_category_counts[category_id] = acc.file_category_counts.get(category_id, 0) + 1
        
        # Track upload status
        if is_uploaded:
            acc.files_uploaded += 1
        
        # Track metadata completeness
        if name and str(name).strip():
            acc.files_with_name += 1
        if description and str(description).strip():
            acc.files_with_description += 1
        if source_id and str(source_id).strip():
            acc.files_with_source_id += 1
        
        # Track MIME type distribution
        if mime_type:
            acc.file_mime_type_counts[mime_type] = acc.file_mime_type_counts.get(mime_type, 0) + 1


def compute_file_metrics(acc: CombinedAccumulator) -> dict:
    """
    Compute all file contextualization metrics.
    
    Primary metric: Files to Asset Contextualization (% of files linked to at least one asset)
    """
    total_files = acc.total_files
    
    # PRIMARY: File to Asset Contextualization
    # This is the main metric - files should be linked to assets for context
    file_to_asset_rate = (
        (acc.files_with_assets / total_files * 100)
        if total_files > 0 else 0.0
    )
    
    # Asset File Coverage (% of assets that have at least one file)
    asset_file_coverage = (
        (len(acc.assets_with_files) / acc.total_assets * 100)
        if acc.total_assets > 0 else 0.0
    )
    
    # Category Completeness (% of files with a category assigned)
    category_rate = (
        (acc.files_with_category / total_files * 100)
        if total_files > 0 else 0.0
    )
    
    # Upload Completeness (% of files that have content uploaded)
    upload_rate = (
        (acc.files_uploaded / total_files * 100)
        if total_files > 0 else 0.0
    )
    
    # Metadata Completeness
    name_rate = (
        (acc.files_with_name / total_files * 100)
        if total_files > 0 else 0.0
    )
    description_rate = (
        (acc.files_with_description / total_files * 100)
        if total_files > 0 else 0.0
    )
    source_id_rate = (
        (acc.files_with_source_id / total_files * 100)
        if total_files > 0 else 0.0
    )
    
    # Files per asset (for assets that have files)
    if acc.assets_with_files:
        # Count files per asset
        asset_file_counts = {}
        for f in acc.file_list:
            for asset_id in f.asset_ids:
                asset_file_counts[asset_id] = asset_file_counts.get(asset_id, 0) + 1
        
        if asset_file_counts:
            avg_files_per_asset = sum(asset_file_counts.values()) / len(asset_file_counts)
            max_files_per_asset = max(asset_file_counts.values())
        else:
            avg_files_per_asset = 0.0
            max_files_per_asset = 0
    else:
        avg_files_per_asset = 0.0
        max_files_per_asset = 0
    
    # Top MIME types (for diagnostics)
    top_mime_types = dict(
        sorted(acc.file_mime_type_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )
    
    # Top categories
    top_categories = dict(
        sorted(acc.file_category_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )
    
    return {
        # Primary metric
        "file_to_asset_rate": round(file_to_asset_rate, 2),
        "files_with_assets": acc.files_with_assets,
        "files_without_assets": total_files - acc.files_with_assets,
        
        # Coverage metrics
        "file_asset_coverage": round(asset_file_coverage, 2),
        "assets_with_files": len(acc.assets_with_files),
        
        # Category metrics
        "file_category_rate": round(category_rate, 2),
        "files_with_category": acc.files_with_category,
        "unique_categories": len(acc.file_category_counts),
        "top_categories": top_categories,
        
        # Upload status
        "file_upload_rate": round(upload_rate, 2),
        "files_uploaded": acc.files_uploaded,
        "files_not_uploaded": total_files - acc.files_uploaded,
        
        # Metadata completeness
        "file_name_rate": round(name_rate, 2),
        "files_with_name": acc.files_with_name,
        "file_description_rate": round(description_rate, 2),
        "files_with_description": acc.files_with_description,
        "file_source_id_rate": round(source_id_rate, 2),
        "files_with_source_id": acc.files_with_source_id,
        
        # Distribution stats
        "avg_files_per_asset": round(avg_files_per_asset, 2),
        "max_files_per_asset": max_files_per_asset,
        "unique_mime_types": len(acc.file_mime_type_counts),
        "top_mime_types": top_mime_types,
        
        # Totals
        "file_total": total_files,
        
        # Feature flag
        "file_has_data": total_files > 0,
    }
