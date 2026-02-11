"""
Equipment processing and metrics computation.
"""

from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
    get_asset_link,
    is_type_consistent,
    EquipmentData,
    CombinedAccumulator,
)


def process_equipment_batch(
    equipment_batch,
    eq_view: ViewId,
    acc: CombinedAccumulator
):
    """Process equipment batch - collect equipment-asset relationship data."""
    for eq in equipment_batch:
        eq_id = get_external_id(eq)
        if not eq_id:
            continue
        
        acc.total_equipment_instances += 1
        
        # Skip if already processed (duplicate)
        if eq_id in acc.equipment_ids_seen:
            acc.equipment_duplicate_ids.append(eq_id)
            continue
        acc.equipment_ids_seen.add(eq_id)
        
        props = get_props(eq, eq_view)
        
        eq_data = EquipmentData(
            equipment_id=eq_id,
            equipment_type=props.get("equipmentType"),
            asset_id=get_asset_link(eq, eq_view),
            serial_number=props.get("serialNumber"),
            manufacturer=props.get("manufacturer"),
            criticality=props.get("criticality"),
        )
        
        acc.equipment_list.append(eq_data)
        acc.equipment_to_asset[eq_id] = eq_data.asset_id
        
        if eq_data.asset_id:
            acc.assets_with_equipment.setdefault(eq_data.asset_id, []).append(eq_id)


def compute_equipment_metrics(acc: CombinedAccumulator) -> dict:
    """Compute all equipment-asset contextualization metrics."""
    total_eq = acc.total_equipment
    
    # Association rate
    linked_eq = sum(1 for aid in acc.equipment_to_asset.values() if aid)
    association_rate = (linked_eq / total_eq * 100) if total_eq else 0.0
    
    # Asset equipment coverage
    assets_with_eq = len(acc.assets_with_equipment)
    coverage_rate = (assets_with_eq / acc.total_assets * 100) if acc.total_assets else 0.0
    
    # Serial number completeness
    serial_valid = sum(1 for eq in acc.equipment_list 
                       if eq.serial_number and str(eq.serial_number).strip())
    serial_rate = (serial_valid / total_eq * 100) if total_eq else 0.0
    
    # Manufacturer completeness
    manu_valid = sum(1 for eq in acc.equipment_list 
                     if eq.manufacturer and str(eq.manufacturer).strip())
    manu_rate = (manu_valid / total_eq * 100) if total_eq else 0.0
    
    # Type consistency
    consistent = 0
    for eq in acc.equipment_list:
        asset_type = acc.asset_type_map.get(eq.asset_id)
        if is_type_consistent(eq.equipment_type, asset_type):
            consistent += 1
    type_rate = (consistent / total_eq * 100) if total_eq else 0.0
    
    # Critical equipment contextualization
    critical_eq = [eq for eq in acc.equipment_list if eq.criticality == "critical"]
    total_critical = len(critical_eq)
    linked_critical = sum(1 for eq in critical_eq if eq.asset_id)
    # Return None if no critical equipment exists (N/A case)
    critical_rate = (linked_critical / total_critical * 100) if total_critical > 0 else None
    
    # Equipment per asset stats
    if acc.assets_with_equipment:
        eq_counts = [len(eq_list) for eq_list in acc.assets_with_equipment.values()]
        avg_eq_per_asset = sum(eq_counts) / len(eq_counts)
        max_eq_per_asset = max(eq_counts)
    else:
        avg_eq_per_asset = 0.0
        max_eq_per_asset = 0
    
    return {
        "eq_total": total_eq,
        "eq_association_rate": round(association_rate, 2),
        "eq_linked": linked_eq,
        "eq_unlinked": total_eq - linked_eq,
        "eq_asset_coverage": round(coverage_rate, 2),
        "eq_assets_with_equipment": assets_with_eq,
        "eq_serial_completeness": round(serial_rate, 2),
        "eq_with_serial": serial_valid,
        "eq_manufacturer_completeness": round(manu_rate, 2),
        "eq_with_manufacturer": manu_valid,
        "eq_type_consistency_rate": round(type_rate, 2),
        "eq_consistent_relationships": consistent,
        "eq_critical_contextualization": round(critical_rate, 2) if critical_rate is not None else None,
        "eq_critical_total": total_critical,
        "eq_critical_linked": linked_critical,
        "eq_has_critical_equipment": total_critical > 0,  # Flag to indicate if critical equipment exists
        "eq_avg_per_asset": round(avg_eq_per_asset, 2),
        "eq_max_per_asset": max_eq_per_asset,
    }
