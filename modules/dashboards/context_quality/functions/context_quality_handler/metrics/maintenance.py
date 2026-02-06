"""
Maintenance workflow processing and metrics computation (RMDM v1).
"""

from typing import Optional, List
from cognite.client.data_classes.data_modeling import ViewId

from .common import (
    get_props,
    get_external_id,
    normalize_timestamp,
    NotificationData,
    MaintenanceOrderData,
    FailureNotificationData,
    CombinedAccumulator,
)


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


def process_notification_batch(
    batch,
    view: ViewId,
    acc: CombinedAccumulator
):
    """Process notification batch - collect notification-to-order/asset/equipment linkage."""
    for node in batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_notification_instances += 1
        
        if node_id in acc.notification_ids_seen:
            acc.notification_duplicate_ids.append(node_id)
            continue
        acc.notification_ids_seen.add(node_id)
        
        props = get_props(node, view)
        
        # Extract relations
        asset_id = get_direct_relation_id(props.get("asset"))
        equipment_ids = get_direct_relation_ids(props.get("equipment"))
        order_id = get_direct_relation_id(props.get("maintenanceOrder"))
        status = props.get("status") or props.get("statusDescription")
        
        notif_data = NotificationData(
            notification_id=node_id,
            asset_id=asset_id,
            equipment_ids=equipment_ids,
            maintenance_order_id=order_id,
            status=status,
        )
        acc.notification_list.append(notif_data)
        
        # Track linkage metrics
        if order_id:
            acc.notifications_with_order += 1
            acc.orders_with_notification.add(order_id)  # Track WO→Notification reverse lookup
        if asset_id:
            acc.notifications_with_asset += 1
            acc.assets_with_notifications.add(asset_id)
        if equipment_ids:
            acc.notifications_with_equipment += 1
            acc.equipment_with_notifications.update(equipment_ids)


def process_maintenance_order_batch(
    batch,
    view: ViewId,
    acc: CombinedAccumulator
):
    """Process maintenance order batch - collect order-to-asset/equipment coverage."""
    for node in batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_order_instances += 1
        
        if node_id in acc.order_ids_seen:
            acc.order_duplicate_ids.append(node_id)
            continue
        acc.order_ids_seen.add(node_id)
        
        props = get_props(node, view)
        
        # Extract relations - orders can have multiple assets/equipment
        asset_ids = get_direct_relation_ids(props.get("assets"))
        main_asset = get_direct_relation_id(props.get("mainAsset"))
        if main_asset and main_asset not in asset_ids:
            asset_ids.append(main_asset)
        
        equipment_ids = get_direct_relation_ids(props.get("equipment"))
        status = props.get("status") or props.get("statusDescription")
        
        # Check completion - actualEndTime indicates completion
        end_time = props.get("actualEndTime")
        actual_end = normalize_timestamp(end_time)
        
        order_data = MaintenanceOrderData(
            order_id=node_id,
            asset_ids=asset_ids,
            equipment_ids=equipment_ids,
            status=status,
            actual_end_time=actual_end,
        )
        acc.order_list.append(order_data)
        
        # Track linkage metrics
        if asset_ids:
            acc.orders_with_asset += 1
            acc.assets_with_orders.update(asset_ids)
        if equipment_ids:
            acc.orders_with_equipment += 1
            acc.equipment_with_orders.update(equipment_ids)
        if actual_end:
            acc.orders_completed += 1


def process_failure_notification_batch(
    batch,
    view: ViewId,
    acc: CombinedAccumulator
):
    """Process failure notification batch - collect failure analysis documentation."""
    for node in batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_failure_notification_instances += 1
        
        if node_id in acc.failure_notification_ids_seen:
            continue
        acc.failure_notification_ids_seen.add(node_id)
        
        props = get_props(node, view)
        
        # Extract failure analysis fields
        failure_mode_id = get_direct_relation_id(props.get("failureMode"))
        failure_mechanism_id = get_direct_relation_id(props.get("failureMechanism"))
        failure_cause = props.get("failureCause")  # String field
        
        failure_data = FailureNotificationData(
            notification_id=node_id,
            failure_mode_id=failure_mode_id,
            failure_mechanism_id=failure_mechanism_id,
            failure_cause=failure_cause,
        )
        acc.failure_notification_list.append(failure_data)
        
        # Track documentation metrics
        if failure_mode_id:
            acc.failure_notif_with_mode += 1
        if failure_mechanism_id:
            acc.failure_notif_with_mechanism += 1
        if failure_cause and str(failure_cause).strip():
            acc.failure_notif_with_cause += 1


def compute_maintenance_metrics(acc: CombinedAccumulator) -> dict:
    """
    Compute all 12 maintenance workflow quality metrics.
    
    These metrics measure the quality and completeness of maintenance data
    based on the RMDM v1 data model.
    
    IMPORTANT - Metric Direction (per Jan Inge's guidance):
    - Many notifications will NOT have a WO - that's OK (informational metric)
    - All WOs SHOULD have a notification - this is critical!
    - All WOs SHOULD be linked to an asset
    - Not all assets/equipment need maintenance records - low values are OK
    """
    total_notif = acc.total_notifications
    total_orders = acc.total_orders
    total_failure_notif = acc.total_failure_notifications
    
    # 1. Notification → Work Order Linkage (INFORMATIONAL)
    # % of notifications linked to a maintenance order
    # NOTE: Low values are acceptable - not all notifications require a WO
    notif_to_order_rate = (
        (acc.notifications_with_order / total_notif * 100)
        if total_notif > 0 else None
    )
    
    # 2. Work Order → Notification Linkage (CRITICAL - should be ~100%)
    # % of work orders that have at least one notification referencing them
    # NOTE: All work orders should originate from a notification
    orders_with_notif_count = len(acc.orders_with_notification.intersection(acc.order_ids_seen))
    order_to_notif_rate = (
        (orders_with_notif_count / total_orders * 100)
        if total_orders > 0 else None
    )
    
    # 3. Notification → Asset Linkage
    # % of notifications linked to an asset
    notif_to_asset_rate = (
        (acc.notifications_with_asset / total_notif * 100)
        if total_notif > 0 else None
    )
    
    # 4. Notification → Equipment Linkage
    # % of notifications linked to at least one equipment
    notif_to_equipment_rate = (
        (acc.notifications_with_equipment / total_notif * 100)
        if total_notif > 0 else None
    )
    
    # 5. Work Order → Asset Coverage (CRITICAL - should be ~100%)
    # % of work orders linked to at least one asset
    # NOTE: All work orders should be linked to an asset
    order_to_asset_rate = (
        (acc.orders_with_asset / total_orders * 100)
        if total_orders > 0 else None
    )
    
    # 6. Work Order → Equipment Coverage
    # % of work orders linked to at least one equipment
    order_to_equipment_rate = (
        (acc.orders_with_equipment / total_orders * 100)
        if total_orders > 0 else None
    )
    
    # 7. Work Order Completion Rate
    # % of work orders that have been completed (have actualEndTime)
    order_completion_rate = (
        (acc.orders_completed / total_orders * 100)
        if total_orders > 0 else None
    )
    
    # 8. Failure Mode Documentation Rate
    # % of failure notifications with failure mode documented
    failure_mode_rate = (
        (acc.failure_notif_with_mode / total_failure_notif * 100)
        if total_failure_notif > 0 else None
    )
    
    # 9. Failure Mechanism Documentation Rate
    # % of failure notifications with failure mechanism documented
    failure_mechanism_rate = (
        (acc.failure_notif_with_mechanism / total_failure_notif * 100)
        if total_failure_notif > 0 else None
    )
    
    # 10. Failure Cause Documentation Rate
    # % of failure notifications with failure cause documented
    failure_cause_rate = (
        (acc.failure_notif_with_cause / total_failure_notif * 100)
        if total_failure_notif > 0 else None
    )
    
    # 11. Asset Maintenance Coverage (INFORMATIONAL)
    # % of assets that have at least one notification or work order
    # NOTE: Low values are OK - not all assets require maintenance records
    assets_with_maintenance = acc.assets_with_notifications.union(acc.assets_with_orders)
    asset_maintenance_coverage = (
        (len(assets_with_maintenance) / acc.total_assets * 100)
        if acc.total_assets > 0 else None
    )
    
    # 12. Equipment Maintenance Coverage (INFORMATIONAL)
    # % of equipment that have at least one notification or work order
    # NOTE: Low values are OK - not all equipment requires maintenance records
    equipment_with_maintenance = acc.equipment_with_notifications.union(acc.equipment_with_orders)
    equipment_maintenance_coverage = (
        (len(equipment_with_maintenance) / acc.total_equipment * 100)
        if acc.total_equipment > 0 else None
    )
    
    return {
        # Instance counts
        "maint_total_notifications": total_notif,
        "maint_total_orders": total_orders,
        "maint_total_failure_notifications": total_failure_notif,
        
        # 1. Notification → Work Order Linkage (INFORMATIONAL - low values OK)
        "maint_notif_to_order_rate": round(notif_to_order_rate, 2) if notif_to_order_rate is not None else None,
        "maint_notif_with_order": acc.notifications_with_order,
        "maint_notif_without_order": total_notif - acc.notifications_with_order if total_notif > 0 else 0,
        
        # 2. Work Order → Notification Linkage (CRITICAL - should be ~100%)
        "maint_order_to_notif_rate": round(order_to_notif_rate, 2) if order_to_notif_rate is not None else None,
        "maint_orders_with_notification": orders_with_notif_count,
        "maint_orders_without_notification": total_orders - orders_with_notif_count if total_orders > 0 else 0,
        
        # 3. Notification → Asset Linkage
        "maint_notif_to_asset_rate": round(notif_to_asset_rate, 2) if notif_to_asset_rate is not None else None,
        "maint_notif_with_asset": acc.notifications_with_asset,
        
        # 4. Notification → Equipment Linkage
        "maint_notif_to_equipment_rate": round(notif_to_equipment_rate, 2) if notif_to_equipment_rate is not None else None,
        "maint_notif_with_equipment": acc.notifications_with_equipment,
        
        # 5. Work Order → Asset Coverage (CRITICAL - should be ~100%)
        "maint_order_to_asset_rate": round(order_to_asset_rate, 2) if order_to_asset_rate is not None else None,
        "maint_order_with_asset": acc.orders_with_asset,
        
        # 6. Work Order → Equipment Coverage
        "maint_order_to_equipment_rate": round(order_to_equipment_rate, 2) if order_to_equipment_rate is not None else None,
        "maint_order_with_equipment": acc.orders_with_equipment,
        
        # 7. Work Order Completion Rate
        "maint_order_completion_rate": round(order_completion_rate, 2) if order_completion_rate is not None else None,
        "maint_orders_completed": acc.orders_completed,
        
        # 8. Failure Mode Documentation Rate
        "maint_failure_mode_rate": round(failure_mode_rate, 2) if failure_mode_rate is not None else None,
        "maint_failure_notif_with_mode": acc.failure_notif_with_mode,
        
        # 9. Failure Mechanism Documentation Rate
        "maint_failure_mechanism_rate": round(failure_mechanism_rate, 2) if failure_mechanism_rate is not None else None,
        "maint_failure_notif_with_mechanism": acc.failure_notif_with_mechanism,
        
        # 10. Failure Cause Documentation Rate
        "maint_failure_cause_rate": round(failure_cause_rate, 2) if failure_cause_rate is not None else None,
        "maint_failure_notif_with_cause": acc.failure_notif_with_cause,
        
        # 11. Asset Maintenance Coverage (INFORMATIONAL - low values OK)
        "maint_asset_coverage_rate": round(asset_maintenance_coverage, 2) if asset_maintenance_coverage is not None else None,
        "maint_assets_with_maintenance": len(assets_with_maintenance),
        
        # 12. Equipment Maintenance Coverage (INFORMATIONAL - low values OK)
        "maint_equipment_coverage_rate": round(equipment_maintenance_coverage, 2) if equipment_maintenance_coverage is not None else None,
        "maint_equipment_with_maintenance": len(equipment_with_maintenance),
        
        # Additional diagnostics
        "maint_unique_assets_in_notifications": len(acc.assets_with_notifications),
        "maint_unique_assets_in_orders": len(acc.assets_with_orders),
        "maint_unique_equipment_in_notifications": len(acc.equipment_with_notifications),
        "maint_unique_equipment_in_orders": len(acc.equipment_with_orders),
        
        # Feature availability flags
        "maint_has_notifications": total_notif > 0,
        "maint_has_orders": total_orders > 0,
        "maint_has_failure_notifications": total_failure_notif > 0,
    }
