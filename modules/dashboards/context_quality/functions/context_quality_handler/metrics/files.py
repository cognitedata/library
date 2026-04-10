"""
File processing and metrics computation.

Measures file contextualization quality based on the CogniteFile view from CDM.
"""

from typing import Dict, List, Optional, Tuple
from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
    FileData,
    CombinedAccumulator,
)


def _unique_files_by_id(file_list: List[FileData]) -> List[FileData]:
    """One row per file_id (last wins). Needed after batch merge may duplicate IDs."""
    by_id: Dict[str, FileData] = {}
    for f in file_list:
        by_id[f.file_id] = f
    return list(by_id.values())


def _file_counts_from_unique_files(unique: List[FileData]) -> Tuple[
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    Dict[str, int],
    Dict[str, int],
]:
    """Derive all per-file numerators and histograms from deduplicated FileData rows."""
    n = len(unique)
    files_with_assets = 0
    files_with_category = 0
    files_uploaded = 0
    files_with_name = 0
    files_with_description = 0
    files_with_source_id = 0
    file_category_counts: Dict[str, int] = {}
    file_mime_type_counts: Dict[str, int] = {}
    for f in unique:
        if f.asset_ids:
            files_with_assets += 1
        if f.category_id:
            files_with_category += 1
            file_category_counts[f.category_id] = file_category_counts.get(f.category_id, 0) + 1
        if f.is_uploaded:
            files_uploaded += 1
        if f.name and str(f.name).strip():
            files_with_name += 1
        if f.description and str(f.description).strip():
            files_with_description += 1
        if f.source_id and str(f.source_id).strip():
            files_with_source_id += 1
        if f.mime_type:
            file_mime_type_counts[f.mime_type] = file_mime_type_counts.get(f.mime_type, 0) + 1
    return (
        n,
        files_with_assets,
        files_with_category,
        files_uploaded,
        files_with_name,
        files_with_description,
        files_with_source_id,
        file_category_counts,
        file_mime_type_counts,
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

    When ``acc.file_list`` is populated (quick run and batch runs with per-file rows), all
    per-file numerators and histograms are recomputed from **deduplicated** ``file_id`` rows
    so batch aggregation cannot yield numerators > unique file count (e.g. 151% upload rate).

    Legacy accumulators without ``file_list`` fall back to summed counters, clamped to
    ``total_files`` so rates stay in [0, 100].
    """
    unique_files: List[FileData] = []
    if acc.file_list:
        unique_files = _unique_files_by_id(acc.file_list)
        (
            total_files,
            files_with_assets,
            files_with_category,
            files_uploaded,
            files_with_name,
            files_with_description,
            files_with_source_id,
            file_category_counts,
            file_mime_type_counts,
        ) = _file_counts_from_unique_files(unique_files)
    else:
        total_files = acc.total_files
        # Summed counters across batches can exceed unique file count; clamp for legacy JSON
        def _clamp(c: int) -> int:
            return min(c, total_files) if total_files > 0 else c

        files_with_assets = _clamp(acc.files_with_assets)
        files_with_category = _clamp(acc.files_with_category)
        files_uploaded = _clamp(acc.files_uploaded)
        files_with_name = _clamp(acc.files_with_name)
        files_with_description = _clamp(acc.files_with_description)
        files_with_source_id = _clamp(acc.files_with_source_id)
        file_category_counts = dict(acc.file_category_counts)
        file_mime_type_counts = dict(acc.file_mime_type_counts)

    # PRIMARY: File to Asset Contextualization
    file_to_asset_rate = (
        (files_with_assets / total_files * 100) if total_files > 0 else 0.0
    )

    # Asset File Coverage (% of assets that have at least one file)
    asset_file_coverage = (
        (len(acc.assets_with_files) / acc.total_assets * 100)
        if acc.total_assets > 0 else 0.0
    )

    category_rate = (
        (files_with_category / total_files * 100) if total_files > 0 else 0.0
    )
    upload_rate = (
        (files_uploaded / total_files * 100) if total_files > 0 else 0.0
    )
    name_rate = (
        (files_with_name / total_files * 100) if total_files > 0 else 0.0
    )
    description_rate = (
        (files_with_description / total_files * 100) if total_files > 0 else 0.0
    )
    source_id_rate = (
        (files_with_source_id / total_files * 100) if total_files > 0 else 0.0
    )

    # Files per asset — same deduplicated rows as numerators (avoids double-counting links)
    if acc.assets_with_files and unique_files:
        asset_file_counts: Dict[str, int] = {}
        for f in unique_files:
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

    top_mime_types = dict(
        sorted(file_mime_type_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )
    top_categories = dict(
        sorted(file_category_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    )

    return {
        "file_to_asset_rate": round(file_to_asset_rate, 2),
        "files_with_assets": files_with_assets,
        "files_without_assets": total_files - files_with_assets,
        "file_asset_coverage": round(asset_file_coverage, 2),
        "assets_with_files": len(acc.assets_with_files),
        "file_category_rate": round(category_rate, 2),
        "files_with_category": files_with_category,
        "unique_categories": len(file_category_counts),
        "top_categories": top_categories,
        "file_upload_rate": round(upload_rate, 2),
        "files_uploaded": files_uploaded,
        "files_not_uploaded": total_files - files_uploaded,
        "file_name_rate": round(name_rate, 2),
        "files_with_name": files_with_name,
        "file_description_rate": round(description_rate, 2),
        "files_with_description": files_with_description,
        "file_source_id_rate": round(source_id_rate, 2),
        "files_with_source_id": files_with_source_id,
        "avg_files_per_asset": round(avg_files_per_asset, 2),
        "max_files_per_asset": max_files_per_asset,
        "unique_mime_types": len(file_mime_type_counts),
        "top_mime_types": top_mime_types,
        "file_total": total_files,
        "file_has_data": total_files > 0,
    }
