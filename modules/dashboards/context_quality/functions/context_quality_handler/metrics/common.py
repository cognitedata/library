"""
Common utilities, data classes, and accumulator for metrics computation.
"""

from typing import Optional, Dict, Set, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from cognite.client.data_classes.data_modeling import ViewId


# ----------------------------------------------------
# CONSTANTS
# ----------------------------------------------------
LOG_EVERY_N_BATCHES = 10
TIMEOUT_WARNING_SECONDS = 540
TIMEOUT_SECONDS = 600

# File storage config
METRICS_FILE_EXTERNAL_ID = "contextualization_quality_metrics"
METRICS_FILE_NAME = "contextualization_quality_metrics.json"

# Batch processing config
BATCH_FILE_PREFIX = "cq_batch_"  # Batch files: cq_batch_0.json, cq_batch_1.json, etc.


# ----------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------

DEFAULT_CONFIG = {
    "chunk_size": 500,
    # View configurations - CDM
    "asset_view_space": "cdf_cdm",
    "asset_view_external_id": "CogniteAsset",
    "asset_view_version": "v1",
    "ts_view_space": "cdf_cdm",
    "ts_view_external_id": "CogniteTimeSeries",
    "ts_view_version": "v1",
    "equipment_view_space": "cdf_cdm",
    "equipment_view_external_id": "CogniteEquipment",
    "equipment_view_version": "v1",
    # View configurations - RMDM v1 (Maintenance Workflow)
    "notification_view_space": "rmdm",
    "notification_view_external_id": "Notification",
    "notification_view_version": "v1",
    "maintenance_order_view_space": "rmdm",
    "maintenance_order_view_external_id": "MaintenanceOrder",
    "maintenance_order_view_version": "v1",
    "failure_notification_view_space": "rmdm",
    "failure_notification_view_external_id": "FailureNotification",
    "failure_notification_view_version": "v1",
    # View configurations - CDM File Annotations
    "annotation_view_space": "cdf_cdm",
    "annotation_view_external_id": "CogniteDiagramAnnotation",
    "annotation_view_version": "v1",
    # View configurations - CDM 3D Objects
    "object3d_view_space": "cdf_cdm",
    "object3d_view_external_id": "Cognite3DObject",
    "object3d_view_version": "v1",
    # Limits
    "max_timeseries": 150000,
    "max_assets": 150000,
    "max_equipment": 150000,
    "max_notifications": 150000,
    "max_maintenance_orders": 150000,
    "max_annotations": 200000,
    "max_3d_objects": 150000,
    # Feature flags
    "enable_maintenance_metrics": True,  # Enable RMDM maintenance workflow metrics
    "enable_file_annotation_metrics": True,  # Enable CDM file annotation metrics
    "enable_3d_metrics": True,  # Enable 3D model contextualization metrics
    # TS specific
    "freshness_days": 30,
    "enable_historical_gaps": True,  # Enabled: analyzes time series for data gaps
    "gap_sample_rate": 20,  # Analyze every Nth batch (balances accuracy vs performance)
    "gap_threshold_days": 7,  # Gaps longer than 7 days are considered significant
    "gap_lookback": "1000d-ago",  # Look back ~2.7 years for historical data
    # File storage
    "file_external_id": METRICS_FILE_EXTERNAL_ID,
    "file_name": METRICS_FILE_NAME,
    # Batch processing mode (for large datasets 200k+)
    "batch_mode": False,  # Enable batch processing
    "batch_index": 0,     # Current batch index (0, 1, 2, ...)
    "batch_size": 200000, # Instances per batch
    "total_batches": None,  # Total number of batches (optional, for progress tracking)
    "is_aggregation": False,  # True for final aggregation run
}

# Equipment-Asset type mappings for consistency check
TYPE_MAPPINGS = {
    "iso14224_va_di_diaphragm": ["VALVE", "CONTROL_VALVE"],
    "iso14224_va_ball_valve": ["VALVE", "CONTROL_VALVE"],
    "iso14224_pu_centrifugal_pump": ["PUMP", "PUMPING_EQUIPMENT"],
    "iso14224_hx_shell_tube": ["HEAT_EXCHANGER", "HX"],
}


# ----------------------------------------------------
# SHARED UTILITIES
# ----------------------------------------------------

def get_props(node, view: ViewId) -> dict:
    """Safely retrieves properties from a node instance for a given view."""
    return node.properties.get(view, {}) or {}


def get_external_id(node) -> Optional[str]:
    """Safely retrieves external ID from a Cognite object."""
    return getattr(node, "external_id", None) or getattr(node, "externalId", None)


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as MM:SS."""
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"


def normalize_timestamp(ts) -> Optional[datetime]:
    """Converts various timestamp formats to UTC datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def extract_parent_external_id(node, view: ViewId) -> Optional[str]:
    """Extract parent external ID from node properties."""
    props = get_props(node, view)
    parent = props.get("parent")
    if not parent:
        return None
    if isinstance(parent, dict):
        return parent.get("externalId")
    return getattr(parent, "external_id", None)


def get_asset_link(node, view: ViewId) -> Optional[str]:
    """Extract asset link from equipment node."""
    props = get_props(node, view)
    asset = props.get("asset")
    if isinstance(asset, dict):
        return asset.get("externalId")
    if isinstance(asset, list) and asset:
        if isinstance(asset[0], dict):
            return asset[0].get("externalId")
        return asset[0]
    return None


def is_type_consistent(equipment_type: Optional[str], asset_type: Optional[str]) -> bool:
    """Check if equipment_type is consistent with asset_type."""
    if not equipment_type or not asset_type:
        return False
    if equipment_type in TYPE_MAPPINGS:
        return asset_type in TYPE_MAPPINGS[equipment_type]
    return equipment_type == asset_type


# ----------------------------------------------------
# DATA CLASSES
# ----------------------------------------------------

@dataclass
class EquipmentData:
    """Data collected for a single equipment node."""
    equipment_id: str
    equipment_type: Optional[str]
    asset_id: Optional[str]
    serial_number: Optional[str]
    manufacturer: Optional[str]
    criticality: Optional[str]


@dataclass
class NotificationData:
    """Data collected for a single notification node."""
    notification_id: str
    asset_id: Optional[str]
    equipment_ids: List[str]
    maintenance_order_id: Optional[str]
    status: Optional[str]


@dataclass
class MaintenanceOrderData:
    """Data collected for a single maintenance order node."""
    order_id: str
    asset_ids: List[str]
    equipment_ids: List[str]
    status: Optional[str]
    actual_end_time: Optional[datetime]


@dataclass
class FailureNotificationData:
    """Data collected for a failure notification with failure analysis."""
    notification_id: str
    failure_mode_id: Optional[str]
    failure_mechanism_id: Optional[str]
    failure_cause: Optional[str]  # String field in RMDM


# ----------------------------------------------------
# COMBINED ACCUMULATOR
# ----------------------------------------------------

@dataclass
class CombinedAccumulator:
    """
    Accumulates all data needed for TS, Asset Hierarchy, Equipment, and Maintenance metrics.
    Processes assets once and reuses data across all metric computations.
    """
    # Time reference
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    freshness_days: int = 30
    
    # ----- TIME SERIES DATA -----
    assets_with_ts: Set[str] = field(default_factory=set)
    ts_with_asset_link: int = 0  # TS that have at least one asset link
    total_ts_instances: int = 0  # Total instances (including duplicates)
    ts_ids_seen: Set[str] = field(default_factory=set)  # For tracking unique TS
    # Unit metrics
    unit_checks: int = 0  # Total TS checked for units
    has_source_unit: int = 0  # TS with sourceUnit defined
    has_target_unit: int = 0  # TS with unit (standardized) defined
    has_any_unit: int = 0  # TS with either unit defined
    units_match: int = 0  # TS where unit == sourceUnit (when both present)
    source_units_seen: Set[str] = field(default_factory=set)  # Track unique source units
    # Freshness and lag
    fresh_count: int = 0
    lag_sum: float = 0.0
    lag_count: int = 0
    # Historical data completeness
    ts_with_data: int = 0  # TS that have actual datapoints
    ts_analyzed_for_gaps: int = 0  # TS analyzed for gap detection
    total_time_span_days: float = 0.0  # Sum of all TS time spans
    total_gap_duration_days: float = 0.0  # Sum of all gap durations
    gap_count: int = 0  # Number of significant gaps found
    longest_gap_days: float = 0.0  # Longest single gap
    
    # ----- ASSET DATA (shared) -----
    total_asset_instances: int = 0  # Total instances (including duplicates)
    asset_ids_seen: Set[str] = field(default_factory=set)  # For tracking unique assets
    # For TS metrics
    critical_assets_total: int = 0
    critical_assets_with_ts: int = 0
    # For hierarchy metrics
    parent_of: Dict[str, Optional[str]] = field(default_factory=dict)
    children_count_map: Dict[str, int] = field(default_factory=dict)
    # For equipment type consistency
    asset_type_map: Dict[str, Optional[str]] = field(default_factory=dict)
    
    # ----- EQUIPMENT DATA -----
    equipment_list: List[EquipmentData] = field(default_factory=list)
    equipment_to_asset: Dict[str, Optional[str]] = field(default_factory=dict)
    assets_with_equipment: Dict[str, List[str]] = field(default_factory=dict)
    total_equipment_instances: int = 0  # Total instances (including duplicates)
    equipment_ids_seen: Set[str] = field(default_factory=set)  # For tracking unique equipment
    
    # ----- MAINTENANCE WORKFLOW DATA (RMDM v1) -----
    # Notifications
    notification_list: List[NotificationData] = field(default_factory=list)
    notification_ids_seen: Set[str] = field(default_factory=set)
    total_notification_instances: int = 0
    notifications_with_order: int = 0
    notifications_with_asset: int = 0
    notifications_with_equipment: int = 0
    assets_with_notifications: Set[str] = field(default_factory=set)
    equipment_with_notifications: Set[str] = field(default_factory=set)
    
    # Maintenance Orders
    order_list: List[MaintenanceOrderData] = field(default_factory=list)
    order_ids_seen: Set[str] = field(default_factory=set)
    total_order_instances: int = 0
    orders_with_asset: int = 0
    orders_with_equipment: int = 0
    orders_completed: int = 0
    assets_with_orders: Set[str] = field(default_factory=set)
    equipment_with_orders: Set[str] = field(default_factory=set)
    orders_with_notification: Set[str] = field(default_factory=set)  # WOs referenced by notifications
    
    # Failure Notifications
    failure_notification_list: List[FailureNotificationData] = field(default_factory=list)
    failure_notification_ids_seen: Set[str] = field(default_factory=set)
    total_failure_notification_instances: int = 0
    failure_notif_with_mode: int = 0
    failure_notif_with_mechanism: int = 0
    failure_notif_with_cause: int = 0
    
    def __post_init__(self):
        self.fresh_limit = self.now - timedelta(days=self.freshness_days)
    
    @property
    def total_ts(self) -> int:
        """Unique time series count."""
        return len(self.ts_ids_seen)
    
    @property
    def total_assets(self) -> int:
        """Unique asset count."""
        return len(self.asset_ids_seen)
    
    @property
    def total_equipment(self) -> int:
        """Unique equipment count."""
        return len(self.equipment_ids_seen)
    
    @property
    def ts_duplicates(self) -> int:
        """Number of duplicate TS instances."""
        return self.total_ts_instances - self.total_ts
    
    @property
    def asset_duplicates(self) -> int:
        """Number of duplicate asset instances."""
        return self.total_asset_instances - self.total_assets
    
    @property
    def equipment_duplicates(self) -> int:
        """Number of duplicate equipment instances."""
        return self.total_equipment_instances - self.total_equipment
    
    @property
    def total_notifications(self) -> int:
        """Unique notification count."""
        return len(self.notification_ids_seen)
    
    @property
    def total_orders(self) -> int:
        """Unique maintenance order count."""
        return len(self.order_ids_seen)
    
    @property
    def total_failure_notifications(self) -> int:
        """Unique failure notification count."""
        return len(self.failure_notification_ids_seen)
    
    @property
    def notification_duplicates(self) -> int:
        """Number of duplicate notification instances."""
        return self.total_notification_instances - self.total_notifications
    
    @property
    def order_duplicates(self) -> int:
        """Number of duplicate order instances."""
        return self.total_order_instances - self.total_orders
    
    def to_dict(self) -> dict:
        """
        Serialize accumulator to dict for JSON storage (batch mode).
        Sets are converted to lists for JSON compatibility.
        """
        return {
            # Metadata
            "now": self.now.isoformat(),
            "freshness_days": self.freshness_days,
            
            # Time Series Data
            "assets_with_ts": list(self.assets_with_ts),
            "ts_with_asset_link": self.ts_with_asset_link,
            "total_ts_instances": self.total_ts_instances,
            "ts_ids_seen": list(self.ts_ids_seen),
            "unit_checks": self.unit_checks,
            "has_source_unit": self.has_source_unit,
            "has_target_unit": self.has_target_unit,
            "has_any_unit": self.has_any_unit,
            "units_match": self.units_match,
            "source_units_seen": list(self.source_units_seen),
            "fresh_count": self.fresh_count,
            "lag_sum": self.lag_sum,
            "lag_count": self.lag_count,
            "ts_with_data": self.ts_with_data,
            "ts_analyzed_for_gaps": self.ts_analyzed_for_gaps,
            "total_time_span_days": self.total_time_span_days,
            "total_gap_duration_days": self.total_gap_duration_days,
            "gap_count": self.gap_count,
            "longest_gap_days": self.longest_gap_days,
            
            # Asset Data
            "total_asset_instances": self.total_asset_instances,
            "asset_ids_seen": list(self.asset_ids_seen),
            "critical_assets_total": self.critical_assets_total,
            "critical_assets_with_ts": self.critical_assets_with_ts,
            "parent_of": self.parent_of,
            "children_count_map": self.children_count_map,
            "asset_type_map": self.asset_type_map,
            
            # Equipment Data
            "equipment_list": [
                {
                    "equipment_id": e.equipment_id,
                    "equipment_type": e.equipment_type,
                    "asset_id": e.asset_id,
                    "serial_number": e.serial_number,
                    "manufacturer": e.manufacturer,
                    "criticality": e.criticality,
                }
                for e in self.equipment_list
            ],
            "equipment_to_asset": self.equipment_to_asset,
            "assets_with_equipment": self.assets_with_equipment,
            "total_equipment_instances": self.total_equipment_instances,
            "equipment_ids_seen": list(self.equipment_ids_seen),
            
            # Maintenance Data
            "notification_ids_seen": list(self.notification_ids_seen),
            "total_notification_instances": self.total_notification_instances,
            "notifications_with_order": self.notifications_with_order,
            "notifications_with_asset": self.notifications_with_asset,
            "notifications_with_equipment": self.notifications_with_equipment,
            "assets_with_notifications": list(self.assets_with_notifications),
            "equipment_with_notifications": list(self.equipment_with_notifications),
            
            "order_ids_seen": list(self.order_ids_seen),
            "total_order_instances": self.total_order_instances,
            "orders_with_asset": self.orders_with_asset,
            "orders_with_equipment": self.orders_with_equipment,
            "orders_completed": self.orders_completed,
            "assets_with_orders": list(self.assets_with_orders),
            "equipment_with_orders": list(self.equipment_with_orders),
            "orders_with_notification": list(self.orders_with_notification),
            
            "failure_notification_ids_seen": list(self.failure_notification_ids_seen),
            "total_failure_notification_instances": self.total_failure_notification_instances,
            "failure_notif_with_mode": self.failure_notif_with_mode,
            "failure_notif_with_mechanism": self.failure_notif_with_mechanism,
            "failure_notif_with_cause": self.failure_notif_with_cause,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "CombinedAccumulator":
        """
        Deserialize accumulator from dict (batch mode).
        Lists are converted back to sets.
        """
        acc = cls(freshness_days=data.get("freshness_days", 30))
        
        # Parse timestamp
        now_str = data.get("now")
        if now_str:
            try:
                acc.now = datetime.fromisoformat(now_str)
            except:
                pass
        
        # Time Series Data
        acc.assets_with_ts = set(data.get("assets_with_ts", []))
        acc.ts_with_asset_link = data.get("ts_with_asset_link", 0)
        acc.total_ts_instances = data.get("total_ts_instances", 0)
        acc.ts_ids_seen = set(data.get("ts_ids_seen", []))
        acc.unit_checks = data.get("unit_checks", 0)
        acc.has_source_unit = data.get("has_source_unit", 0)
        acc.has_target_unit = data.get("has_target_unit", 0)
        acc.has_any_unit = data.get("has_any_unit", 0)
        acc.units_match = data.get("units_match", 0)
        acc.source_units_seen = set(data.get("source_units_seen", []))
        acc.fresh_count = data.get("fresh_count", 0)
        acc.lag_sum = data.get("lag_sum", 0.0)
        acc.lag_count = data.get("lag_count", 0)
        acc.ts_with_data = data.get("ts_with_data", 0)
        acc.ts_analyzed_for_gaps = data.get("ts_analyzed_for_gaps", 0)
        acc.total_time_span_days = data.get("total_time_span_days", 0.0)
        acc.total_gap_duration_days = data.get("total_gap_duration_days", 0.0)
        acc.gap_count = data.get("gap_count", 0)
        acc.longest_gap_days = data.get("longest_gap_days", 0.0)
        
        # Asset Data
        acc.total_asset_instances = data.get("total_asset_instances", 0)
        acc.asset_ids_seen = set(data.get("asset_ids_seen", []))
        acc.critical_assets_total = data.get("critical_assets_total", 0)
        acc.critical_assets_with_ts = data.get("critical_assets_with_ts", 0)
        acc.parent_of = data.get("parent_of", {})
        acc.children_count_map = data.get("children_count_map", {})
        acc.asset_type_map = data.get("asset_type_map", {})
        
        # Equipment Data
        acc.equipment_list = [
            EquipmentData(
                equipment_id=e["equipment_id"],
                equipment_type=e.get("equipment_type"),
                asset_id=e.get("asset_id"),
                serial_number=e.get("serial_number"),
                manufacturer=e.get("manufacturer"),
                criticality=e.get("criticality"),
            )
            for e in data.get("equipment_list", [])
        ]
        acc.equipment_to_asset = data.get("equipment_to_asset", {})
        acc.assets_with_equipment = data.get("assets_with_equipment", {})
        acc.total_equipment_instances = data.get("total_equipment_instances", 0)
        acc.equipment_ids_seen = set(data.get("equipment_ids_seen", []))
        
        # Maintenance Data
        acc.notification_ids_seen = set(data.get("notification_ids_seen", []))
        acc.total_notification_instances = data.get("total_notification_instances", 0)
        acc.notifications_with_order = data.get("notifications_with_order", 0)
        acc.notifications_with_asset = data.get("notifications_with_asset", 0)
        acc.notifications_with_equipment = data.get("notifications_with_equipment", 0)
        acc.assets_with_notifications = set(data.get("assets_with_notifications", []))
        acc.equipment_with_notifications = set(data.get("equipment_with_notifications", []))
        
        acc.order_ids_seen = set(data.get("order_ids_seen", []))
        acc.total_order_instances = data.get("total_order_instances", 0)
        acc.orders_with_asset = data.get("orders_with_asset", 0)
        acc.orders_with_equipment = data.get("orders_with_equipment", 0)
        acc.orders_completed = data.get("orders_completed", 0)
        acc.assets_with_orders = set(data.get("assets_with_orders", []))
        acc.equipment_with_orders = set(data.get("equipment_with_orders", []))
        acc.orders_with_notification = set(data.get("orders_with_notification", []))
        
        acc.failure_notification_ids_seen = set(data.get("failure_notification_ids_seen", []))
        acc.total_failure_notification_instances = data.get("total_failure_notification_instances", 0)
        acc.failure_notif_with_mode = data.get("failure_notif_with_mode", 0)
        acc.failure_notif_with_mechanism = data.get("failure_notif_with_mechanism", 0)
        acc.failure_notif_with_cause = data.get("failure_notif_with_cause", 0)
        
        return acc
    
    def merge_from(self, other: "CombinedAccumulator"):
        """
        Merge another accumulator into this one (for batch aggregation).
        Counts are summed, sets are unioned, maps are merged.
        """
        # Time Series Data
        self.assets_with_ts.update(other.assets_with_ts)
        self.ts_with_asset_link += other.ts_with_asset_link
        self.total_ts_instances += other.total_ts_instances
        self.ts_ids_seen.update(other.ts_ids_seen)
        self.unit_checks += other.unit_checks
        self.has_source_unit += other.has_source_unit
        self.has_target_unit += other.has_target_unit
        self.has_any_unit += other.has_any_unit
        self.units_match += other.units_match
        self.source_units_seen.update(other.source_units_seen)
        self.fresh_count += other.fresh_count
        self.lag_sum += other.lag_sum
        self.lag_count += other.lag_count
        self.ts_with_data += other.ts_with_data
        self.ts_analyzed_for_gaps += other.ts_analyzed_for_gaps
        self.total_time_span_days += other.total_time_span_days
        self.total_gap_duration_days += other.total_gap_duration_days
        self.gap_count += other.gap_count
        self.longest_gap_days = max(self.longest_gap_days, other.longest_gap_days)
        
        # Asset Data
        self.total_asset_instances += other.total_asset_instances
        self.asset_ids_seen.update(other.asset_ids_seen)
        self.critical_assets_total += other.critical_assets_total
        self.critical_assets_with_ts += other.critical_assets_with_ts
        self.parent_of.update(other.parent_of)
        # Merge children_count_map (sum counts for same asset)
        for asset_id, count in other.children_count_map.items():
            self.children_count_map[asset_id] = self.children_count_map.get(asset_id, 0) + count
        self.asset_type_map.update(other.asset_type_map)
        
        # Equipment Data
        self.equipment_list.extend(other.equipment_list)
        self.equipment_to_asset.update(other.equipment_to_asset)
        # Merge assets_with_equipment (extend lists for same asset)
        for asset_id, eq_list in other.assets_with_equipment.items():
            if asset_id in self.assets_with_equipment:
                self.assets_with_equipment[asset_id].extend(eq_list)
            else:
                self.assets_with_equipment[asset_id] = eq_list
        self.total_equipment_instances += other.total_equipment_instances
        self.equipment_ids_seen.update(other.equipment_ids_seen)
        
        # Maintenance Data
        self.notification_ids_seen.update(other.notification_ids_seen)
        self.total_notification_instances += other.total_notification_instances
        self.notifications_with_order += other.notifications_with_order
        self.notifications_with_asset += other.notifications_with_asset
        self.notifications_with_equipment += other.notifications_with_equipment
        self.assets_with_notifications.update(other.assets_with_notifications)
        self.equipment_with_notifications.update(other.equipment_with_notifications)
        
        self.order_ids_seen.update(other.order_ids_seen)
        self.total_order_instances += other.total_order_instances
        self.orders_with_asset += other.orders_with_asset
        self.orders_with_equipment += other.orders_with_equipment
        self.orders_completed += other.orders_completed
        self.assets_with_orders.update(other.assets_with_orders)
        self.equipment_with_orders.update(other.equipment_with_orders)
        self.orders_with_notification.update(other.orders_with_notification)
        
        self.failure_notification_ids_seen.update(other.failure_notification_ids_seen)
        self.total_failure_notification_instances += other.total_failure_notification_instances
        self.failure_notif_with_mode += other.failure_notif_with_mode
        self.failure_notif_with_mechanism += other.failure_notif_with_mechanism
        self.failure_notif_with_cause += other.failure_notif_with_cause
