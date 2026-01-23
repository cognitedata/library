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
    # Limits
    "max_timeseries": 150000,
    "max_assets": 150000,
    "max_equipment": 150000,
    "max_notifications": 150000,
    "max_maintenance_orders": 150000,
    "max_annotations": 200000,
    # Feature flags
    "enable_maintenance_metrics": True,  # Enable RMDM maintenance workflow metrics
    "enable_file_annotation_metrics": True,  # Enable CDM file annotation metrics
    # TS specific
    "freshness_days": 30,
    "enable_historical_gaps": True,  # Enabled: analyzes time series for data gaps
    "gap_sample_rate": 20,  # Analyze every Nth batch (balances accuracy vs performance)
    "gap_threshold_days": 7,  # Gaps longer than 7 days are considered significant
    "gap_lookback": "1000d-ago",  # Look back ~2.7 years for historical data
    # File storage
    "file_external_id": METRICS_FILE_EXTERNAL_ID,
    "file_name": METRICS_FILE_NAME,
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
