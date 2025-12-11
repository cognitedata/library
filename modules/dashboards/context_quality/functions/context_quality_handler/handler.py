"""
Cognite Function: Contextualization Quality Metrics

Computes all quality metrics in a single function:

1. TIME SERIES METRICS:
   - Asset TS Association Rate
   - Critical Asset Coverage  
   - Unit Consistency
   - Data Freshness
   - Processing Lag

2. ASSET HIERARCHY METRICS:
   - Hierarchy Completion Rate
   - Orphan Count/Rate
   - Depth Statistics (Avg, Max)
   - Breadth Statistics (Avg Children, Std Dev)
   - Depth/Breadth Distributions

3. EQUIPMENT-ASSET METRICS:
   - Equipment Association Rate
   - Asset Equipment Coverage
   - Serial Number Completeness
   - Manufacturer Completeness
   - Type Consistency
   - Critical Equipment Contextualization

Results are saved to a Cognite File as JSON for persistence.
"""

import json
import logging
import time
from typing import Optional, Dict, Set, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId, NodeId


# ----------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S'
    ))
    logger.addHandler(handler)

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
    # View configurations
    "asset_view_space": "cdf_cdm",
    "asset_view_external_id": "CogniteAsset",
    "asset_view_version": "v1",
    "ts_view_space": "cdf_cdm",
    "ts_view_external_id": "CogniteTimeSeries",
    "ts_view_version": "v1",
    "equipment_view_space": "cdf_cdm",
    "equipment_view_external_id": "CogniteEquipment",
    "equipment_view_version": "v1",
    # Limits
    "max_timeseries": 150000,
    "max_assets": 150000,
    "max_equipment": 150000,
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


# ----------------------------------------------------
# COMBINED ACCUMULATOR
# ----------------------------------------------------

@dataclass
class CombinedAccumulator:
    """
    Accumulates all data needed for TS, Asset Hierarchy, and Equipment metrics.
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


# ----------------------------------------------------
# BATCH PROCESSORS
# ----------------------------------------------------

def process_timeseries_batch(
    ts_batch,
    ts_view: ViewId,
    acc: CombinedAccumulator
):
    """Process time series batch - collect TS metrics data."""
    for ts in ts_batch:
        ts_id = get_external_id(ts)
        if not ts_id:
            continue
        
        acc.total_ts_instances += 1
        
        # Skip if already processed (duplicate)
        if ts_id in acc.ts_ids_seen:
            continue
        acc.ts_ids_seen.add(ts_id)
        
        # Track assets with TS and TS with asset links
        props = get_props(ts, ts_view)
        assets_ref = props.get("assets") or []
        if isinstance(assets_ref, dict):
            assets_ref = [assets_ref]
        
        has_asset_link = False
        for a in assets_ref:
            aid = a.get("externalId")
            if aid:
                acc.assets_with_ts.add(aid)
                has_asset_link = True
        
        if has_asset_link:
            acc.ts_with_asset_link += 1
        
        # Unit Metrics - get from view properties first, fallback to dump
        unit = props.get("unit")
        src_unit = props.get("sourceUnit")
        
        # If not in view props, try the dump (some SDKs expose it differently)
        if unit is None or src_unit is None:
            try:
                props_dump = ts.dump() if hasattr(ts, 'dump') else {}
                # Check nested properties structure
                if "properties" in props_dump:
                    for view_key, view_props in props_dump.get("properties", {}).items():
                        if isinstance(view_props, dict):
                            if unit is None:
                                unit = view_props.get("unit")
                            if src_unit is None:
                                src_unit = view_props.get("sourceUnit")
                else:
                    if unit is None:
                        unit = props_dump.get("unit")
                    if src_unit is None:
                        src_unit = props_dump.get("sourceUnit")
            except Exception:
                pass
        
        acc.unit_checks += 1
        
        # Clean up unit strings
        unit_str = unit.strip() if isinstance(unit, str) and unit.strip() else None
        src_unit_str = src_unit.strip() if isinstance(src_unit, str) and src_unit.strip() else None
        
        # Track source unit completeness
        if src_unit_str:
            acc.has_source_unit += 1
            acc.source_units_seen.add(src_unit_str)
        
        # Track target (standardized) unit completeness
        if unit_str:
            acc.has_target_unit += 1
        
        # Track if any unit is defined
        if unit_str or src_unit_str:
            acc.has_any_unit += 1
        
        # Track if units match (when both present)
        if unit_str and src_unit_str and unit_str == src_unit_str:
            acc.units_match += 1
        
        # Data Freshness
        raw_ts = getattr(ts, "last_updated_time", None)
        ts_last_update = normalize_timestamp(raw_ts)
        if ts_last_update and ts_last_update >= acc.fresh_limit:
            acc.fresh_count += 1
        
        # Processing Lag
        if ts_last_update:
            lag_seconds = (acc.now - ts_last_update).total_seconds()
            lag_hours = lag_seconds / 3600
            if 0 < lag_hours < 8760:
                acc.lag_sum += lag_hours
                acc.lag_count += 1


def process_asset_batch(
    asset_batch,
    asset_view: ViewId,
    acc: CombinedAccumulator
):
    """
    Process asset batch - collect data for:
    - TS metrics (critical asset coverage)
    - Hierarchy metrics (parent-child relationships)
    - Equipment metrics (asset type mapping)
    """
    for node in asset_batch:
        node_id = get_external_id(node)
        if not node_id:
            continue
        
        acc.total_asset_instances += 1
        
        # Skip if already processed (duplicate)
        if node_id in acc.asset_ids_seen:
            continue
        acc.asset_ids_seen.add(node_id)
        
        props = get_props(node, asset_view)
        
        # For TS metrics: critical asset tracking
        if props.get("criticality") == "critical":
            acc.critical_assets_total += 1
            if node_id in acc.assets_with_ts:
                acc.critical_assets_with_ts += 1
        
        # For hierarchy metrics: parent-child relationships
        parent_id = extract_parent_external_id(node, asset_view)
        acc.parent_of[node_id] = parent_id
        if parent_id:
            acc.children_count_map[parent_id] = acc.children_count_map.get(parent_id, 0) + 1
        
        # For equipment metrics: asset type mapping
        asset_type = props.get("type") or props.get("assetClass")
        acc.asset_type_map[node_id] = asset_type


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


def compute_historical_gaps_batch(
    ts_batch,
    client: CogniteClient,
    acc: CombinedAccumulator,
    gap_threshold_days: int = 7,
    lookback: str = "1000d-ago",
):
    """
    Compute historical data completeness for a batch of timeseries.
    
    For each time series:
    1. Calculate total time span (first to last datapoint)
    2. Find gaps larger than threshold (e.g., 7 days without data)
    3. Sum gap durations to calculate data completeness
    
    Example: 1 year of data with a 1-month gap = (365-30)/365 = 91.8% complete
    """
    ts_instance_ids = []
    for n in ts_batch:
        if getattr(n, "space", None) and getattr(n, "external_id", None):
            ts_instance_ids.append(NodeId(n.space, n.external_id))

    if not ts_instance_ids:
        return

    try:
        df = client.time_series.data.retrieve_dataframe(
            instance_id=ts_instance_ids,
            start=lookback,
            end="now",
            column_names="external_id",
            ignore_unknown_ids=True,
        )
    except Exception:
        return

    if df.empty:
        return

    gap_threshold_ms = gap_threshold_days * 24 * 3600 * 1000
    ms_per_day = 24 * 3600 * 1000

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 2:
            continue
        
        acc.ts_analyzed_for_gaps += 1
        acc.ts_with_data += 1
        
        timestamps = series.index.astype("int64") // 1_000_000  # Convert to ms
        
        # Calculate total time span (first to last datapoint)
        first_ts = timestamps.min()
        last_ts = timestamps.max()
        time_span_ms = last_ts - first_ts
        time_span_days = time_span_ms / ms_per_day
        acc.total_time_span_days += time_span_days
        
        # Find gaps between consecutive datapoints
        diffs = timestamps[1:] - timestamps[:-1]
        
        # Identify significant gaps (longer than threshold)
        significant_gaps = diffs[diffs > gap_threshold_ms]
        
        if len(significant_gaps) > 0:
            # Sum total gap duration for this TS
            gap_duration_ms = significant_gaps.sum()
            gap_duration_days = gap_duration_ms / ms_per_day
            acc.total_gap_duration_days += gap_duration_days
            acc.gap_count += len(significant_gaps)
            
            # Track longest gap
            longest_gap_ms = significant_gaps.max()
            longest_gap_days = longest_gap_ms / ms_per_day
            if longest_gap_days > acc.longest_gap_days:
                acc.longest_gap_days = longest_gap_days


# ----------------------------------------------------
# METRIC COMPUTATION - TIME SERIES
# ----------------------------------------------------

def compute_ts_metrics(acc: CombinedAccumulator) -> dict:
    """Compute all time series contextualization metrics."""
    associated_assets = len(acc.assets_with_ts)
    
    # TS to Asset Rate: % of time series that are linked to at least one asset
    # This is the PRIMARY metric - orphaned TS (not linked to asset) are a problem
    ts_to_asset_rate = (
        (acc.ts_with_asset_link / acc.total_ts * 100)
        if acc.total_ts else 0.0
    )
    
    # Asset Monitoring Coverage: % of assets that have at least one TS linked
    # This is SECONDARY - it's OK for some assets to not have TS
    asset_monitoring_coverage = (
        (associated_assets / acc.total_assets * 100)
        if acc.total_assets else 0.0
    )
    
    # Return None if no critical assets exist (N/A case)
    critical_coverage = (
        (acc.critical_assets_with_ts / acc.critical_assets_total * 100)
        if acc.critical_assets_total > 0 else None
    )
    
    # Unit metrics - multiple perspectives on unit quality
    source_unit_rate = (
        (acc.has_source_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    target_unit_rate = (
        (acc.has_target_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    any_unit_rate = (
        (acc.has_any_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    # Unit mapping rate: when both are present, do they match?
    both_units_present = min(acc.has_source_unit, acc.has_target_unit)
    unit_mapping_rate = (
        (acc.units_match / both_units_present * 100)
        if both_units_present > 0 else None
    )
    
    # Historical Data Completeness
    # = (total_time_span - gap_duration) / total_time_span × 100%
    # Example: 1 year span with 1 month gap = (365-30)/365 = 91.8%
    if acc.total_time_span_days > 0:
        data_with_gaps = acc.total_time_span_days - acc.total_gap_duration_days
        historical_data_completeness = (data_with_gaps / acc.total_time_span_days) * 100
    else:
        historical_data_completeness = None  # No data analyzed
    
    # Average gap duration
    avg_gap_days = (
        acc.total_gap_duration_days / acc.gap_count 
        if acc.gap_count > 0 else 0.0
    )
    
    data_freshness = (
        (acc.fresh_count / acc.total_ts * 100)
        if acc.total_ts else 0.0
    )
    
    processing_lag_hours = (
        (acc.lag_sum / acc.lag_count)
        if acc.lag_count else None
    )
    
    return {
        # PRIMARY: TS to Asset Contextualization (orphaned TS are a problem)
        "ts_to_asset_rate": round(ts_to_asset_rate, 2),
        "ts_with_asset_link": acc.ts_with_asset_link,
        "ts_without_asset_link": acc.total_ts - acc.ts_with_asset_link,
        # SECONDARY: Asset Monitoring Coverage (OK for some assets to lack TS)
        "ts_asset_monitoring_coverage": round(asset_monitoring_coverage, 2),
        "ts_associated_assets": associated_assets,
        "ts_critical_coverage": round(critical_coverage, 2) if critical_coverage is not None else None,
        "ts_critical_with_ts": acc.critical_assets_with_ts,
        "ts_critical_total": acc.critical_assets_total,
        "ts_has_critical_assets": acc.critical_assets_total > 0,
        # Unit metrics
        "ts_source_unit_completeness": round(source_unit_rate, 2),
        "ts_target_unit_completeness": round(target_unit_rate, 2),
        "ts_any_unit_completeness": round(any_unit_rate, 2),
        "ts_unit_mapping_rate": round(unit_mapping_rate, 2) if unit_mapping_rate is not None else None,
        "ts_has_source_unit": acc.has_source_unit,
        "ts_has_target_unit": acc.has_target_unit,
        "ts_has_any_unit": acc.has_any_unit,
        "ts_units_match": acc.units_match,
        "ts_unit_checks": acc.unit_checks,
        "ts_unique_source_units": len(acc.source_units_seen),
        # Historical Data Completeness - time-based coverage
        "ts_historical_data_completeness": round(historical_data_completeness, 2) if historical_data_completeness is not None else None,
        "ts_analyzed_for_gaps": acc.ts_analyzed_for_gaps,
        "ts_total_time_span_days": round(acc.total_time_span_days, 1),
        "ts_total_gap_duration_days": round(acc.total_gap_duration_days, 1),
        "ts_gap_count": acc.gap_count,
        "ts_longest_gap_days": round(acc.longest_gap_days, 1),
        "ts_avg_gap_days": round(avg_gap_days, 1),
        "ts_data_freshness": round(data_freshness, 2),
        "ts_fresh_count": acc.fresh_count,
        "ts_processing_lag_hours": round(processing_lag_hours, 2) if processing_lag_hours else None,
        "ts_total": acc.total_ts,
    }


# ----------------------------------------------------
# METRIC COMPUTATION - ASSET HIERARCHY
# ----------------------------------------------------

def compute_depth_map(parent_map: Dict[str, Optional[str]]) -> Dict[str, int]:
    """Compute depth for each node in the hierarchy."""
    depth_cache: Dict[str, int] = {}

    def get_depth(nid: str) -> int:
        if nid in depth_cache:
            return depth_cache[nid]
        parent = parent_map.get(nid)
        if not parent:
            depth_cache[nid] = 0
            return 0
        visited = set()
        cur = nid
        d = 0
        while True:
            if cur in visited:
                break
            visited.add(cur)
            parent = parent_map.get(cur)
            if not parent:
                break
            d += 1
            cur = parent
        depth_cache[nid] = d
        return d

    for node_id in parent_map.keys():
        get_depth(node_id)
    return depth_cache


def compute_asset_hierarchy_metrics(acc: CombinedAccumulator) -> dict:
    """Compute all asset hierarchy metrics."""
    total_assets = len(acc.parent_of)
    assets_with_parents = sum(1 for v in acc.parent_of.values() if v)
    root_assets = total_assets - assets_with_parents
    
    # Orphans: no parent AND no children
    orphan_ids = [
        nid for nid, parent in acc.parent_of.items()
        if parent is None and nid not in acc.children_count_map
    ]
    orphan_count = len(orphan_ids)
    orphan_rate = (orphan_count / total_assets * 100) if total_assets else 0.0
    
    # Hierarchy completion rate
    non_root = total_assets - root_assets
    completion_rate = (assets_with_parents / non_root * 100) if non_root > 0 else 100.0
    
    # Depth metrics
    depths_map = compute_depth_map(acc.parent_of)
    depth_values = list(depths_map.values()) if depths_map else [0]
    avg_depth = sum(depth_values) / len(depth_values) if depth_values else 0.0
    max_depth = max(depth_values) if depth_values else 0
    
    # Breadth metrics
    children_values = list(acc.children_count_map.values()) if acc.children_count_map else [0]
    avg_children = sum(children_values) / len(children_values) if children_values else 0.0
    if len(children_values) > 0:
        variance = sum((x - avg_children) ** 2 for x in children_values) / len(children_values)
        std_children = variance ** 0.5
    else:
        std_children = 0.0
    max_children = max(children_values) if children_values else 0
    
    # Distributions
    depth_dist: Dict[int, int] = {}
    for d in depths_map.values():
        depth_dist[d] = depth_dist.get(d, 0) + 1
    
    breadth_dist: Dict[int, int] = {}
    for c in acc.children_count_map.values():
        breadth_dist[c] = breadth_dist.get(c, 0) + 1
    
    return {
        "hierarchy_total_assets": total_assets,
        "hierarchy_root_assets": root_assets,
        "hierarchy_assets_with_parents": assets_with_parents,
        "hierarchy_orphan_count": orphan_count,
        "hierarchy_orphan_rate": round(orphan_rate, 2),
        "hierarchy_completion_rate": round(completion_rate, 2),
        "hierarchy_avg_depth": round(avg_depth, 2),
        "hierarchy_max_depth": max_depth,
        "hierarchy_avg_children": round(avg_children, 2),
        "hierarchy_std_children": round(std_children, 2),
        "hierarchy_max_children": max_children,
        "hierarchy_parents_count": len(acc.children_count_map),
        "hierarchy_depth_distribution": dict(sorted(depth_dist.items())),
        "hierarchy_breadth_distribution": dict(sorted(breadth_dist.items())),
    }


# ----------------------------------------------------
# METRIC COMPUTATION - EQUIPMENT
# ----------------------------------------------------

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


# ----------------------------------------------------
# FILE STORAGE
# ----------------------------------------------------

def save_metrics_to_file(
    client: CogniteClient,
    metrics: dict,
    file_external_id: str,
    file_name: str
):
    """Save metrics to Cognite Files as JSON, overwriting if exists."""
    import tempfile
    import os
    
    # Create temp file
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    
    with open(temp_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    
    # Upload with overwrite
    client.files.upload(
        path=temp_path,
        external_id=file_external_id,
        name=file_name,
        mime_type="application/json",
        overwrite=True
    )
    
    # Clean up temp file
    try:
        os.remove(temp_path)
    except Exception:
        pass
    
    logger.info(f"📁 Saved metrics to Cognite Files: {file_external_id}")


# ----------------------------------------------------
# MAIN HANDLER
# ----------------------------------------------------

def handle(data: dict, client: CogniteClient) -> dict:
    """
    Cognite Function entry point - Combined metrics computation.

    Computes TS, Asset Hierarchy, and Equipment metrics in a single run.
    Saves results to Cognite Files as JSON.

    Args:
        data: Configuration overrides (optional)
        client: CogniteClient instance

    Returns:
        dict: All computed metrics
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("STARTING Contextualization Quality Metrics Function")
    logger.info("=" * 70)
    
    # Merge config
    config = {**DEFAULT_CONFIG, **(data or {})}
    
    chunk_size = config["chunk_size"]
    max_ts = config["max_timeseries"]
    max_assets = config["max_assets"]
    max_eq = config["max_equipment"]
    freshness_days = config["freshness_days"]
    enable_gaps = config["enable_historical_gaps"]
    gap_sample_rate = config["gap_sample_rate"]
    gap_threshold_days = config["gap_threshold_days"]
    gap_lookback = config["gap_lookback"]
    file_external_id = config["file_external_id"]
    file_name = config["file_name"]
    
    logger.info(f"Limits: TS={max_ts:,}, Assets={max_assets:,}, Equipment={max_eq:,}")
    
    # Build view IDs
    ts_view = ViewId(
        config["ts_view_space"],
        config["ts_view_external_id"],
        config["ts_view_version"]
    )
    asset_view = ViewId(
        config["asset_view_space"],
        config["asset_view_external_id"],
        config["asset_view_version"]
    )
    equipment_view = ViewId(
        config["equipment_view_space"],
        config["equipment_view_external_id"],
        config["equipment_view_version"]
    )
    
    # Initialize accumulator
    acc = CombinedAccumulator(freshness_days=freshness_days)
    
    batch_counts = {"ts": 0, "assets": 0, "equipment": 0}
    
    # ============================================================
    # PHASE 1: Process Time Series
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 1: Processing Time Series")
    logger.info("-" * 50)
    
    phase1_start = time.time()
    
    for ts_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=ts_view,
    ):
        batch_counts["ts"] += 1
        process_timeseries_batch(ts_batch, ts_view, acc)
        
        # Debug: Log unit info after first batch
        if batch_counts["ts"] == 1:
            logger.info(f"[DEBUG] After 1st batch: sourceUnit={acc.has_source_unit}, targetUnit={acc.has_target_unit}, checked={acc.unit_checks}")
        
        # Historical gap analysis: always analyze first batch + every Nth batch
        if enable_gaps and (batch_counts["ts"] == 1 or batch_counts["ts"] % gap_sample_rate == 0):
            logger.info(f"[Gaps] Analyzing batch {batch_counts['ts']} for historical data completeness...")
            compute_historical_gaps_batch(
                ts_batch, client, acc,
                gap_threshold_days=gap_threshold_days,
                lookback=gap_lookback
            )
            logger.info(f"[Gaps] Analyzed: {acc.ts_analyzed_for_gaps} TS, gaps found: {acc.gap_count}")
        
        if batch_counts["ts"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[TS] Batch {batch_counts['ts']:,} | "
                f"Total: {acc.total_ts:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_ts >= max_ts:
            logger.info(f"🛑 Reached TS limit ({max_ts:,})")
            break
    
    logger.info(f"✅ PHASE 1: {acc.total_ts:,} TS in {format_elapsed(time.time() - phase1_start)}")
    
    # ============================================================
    # PHASE 2: Process Assets (shared data for TS, Hierarchy, EQ)
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 2: Processing Assets (shared data)")
    logger.info("-" * 50)
    
    phase2_start = time.time()
    
    for asset_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=asset_view,
    ):
        batch_counts["assets"] += 1
        process_asset_batch(asset_batch, asset_view, acc)
        
        if batch_counts["assets"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[Assets] Batch {batch_counts['assets']:,} | "
                f"Total: {acc.total_assets:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_assets >= max_assets:
            logger.info(f"🛑 Reached Asset limit ({max_assets:,})")
            break
    
    logger.info(f"✅ PHASE 2: {acc.total_assets:,} Assets in {format_elapsed(time.time() - phase2_start)}")
    
    # ============================================================
    # PHASE 3: Process Equipment
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 3: Processing Equipment")
    logger.info("-" * 50)
    
    phase3_start = time.time()
    
    for eq_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=equipment_view,
    ):
        batch_counts["equipment"] += 1
        process_equipment_batch(eq_batch, equipment_view, acc)
        
        if batch_counts["equipment"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[Equipment] Batch {batch_counts['equipment']:,} | "
                f"Total: {acc.total_equipment:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_equipment >= max_eq:
            logger.info(f"🛑 Reached Equipment limit ({max_eq:,})")
            break
    
    logger.info(f"✅ PHASE 3: {acc.total_equipment:,} Equipment in {format_elapsed(time.time() - phase3_start)}")
    
    # ============================================================
    # PHASE 4: Compute All Metrics
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 4: Computing All Metrics")
    logger.info("-" * 50)
    
    phase4_start = time.time()
    
    ts_metrics = compute_ts_metrics(acc)
    hierarchy_metrics = compute_asset_hierarchy_metrics(acc)
    equipment_metrics = compute_equipment_metrics(acc)
    
    logger.info(f"✅ PHASE 4: Metrics computed in {format_elapsed(time.time() - phase4_start)}")
    
    # ============================================================
    # PHASE 5: Compile and Save Results
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 5: Saving Results to Cognite Files")
    logger.info("-" * 50)
    
    total_elapsed = time.time() - start_time
    
    all_metrics = {
        "metadata": {
            "computed_at": acc.now.isoformat(),
            "execution_time_seconds": round(total_elapsed, 2),
            "batches_processed": batch_counts,
            "instance_counts": {
                "timeseries": {
                    "total_instances": acc.total_ts_instances,
                    "unique": acc.total_ts,
                    "duplicates": acc.ts_duplicates,
                },
                "assets": {
                    "total_instances": acc.total_asset_instances,
                    "unique": acc.total_assets,
                    "duplicates": acc.asset_duplicates,
                },
                "equipment": {
                    "total_instances": acc.total_equipment_instances,
                    "unique": acc.total_equipment,
                    "duplicates": acc.equipment_duplicates,
                },
            },
            "limits_reached": {
                "timeseries": acc.total_ts >= max_ts,
                "assets": acc.total_assets >= max_assets,
                "equipment": acc.total_equipment >= max_eq,
            },
            "config": {
                "chunk_size": chunk_size,
                "max_timeseries": max_ts,
                "max_assets": max_assets,
                "max_equipment": max_eq,
                "freshness_days": freshness_days,
                "enable_historical_gaps": enable_gaps,
            },
        },
        "timeseries_metrics": ts_metrics,
        "hierarchy_metrics": hierarchy_metrics,
        "equipment_metrics": equipment_metrics,
    }
    
    # Save to Cognite Files
    save_metrics_to_file(client, all_metrics, file_external_id, file_name)
    
    # Final summary
    logger.info("=" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info("Instance Counts (Total / Unique / Duplicates):")
    logger.info(f"  Time Series:  {acc.total_ts_instances:,} / {acc.total_ts:,} / {acc.ts_duplicates:,}")
    logger.info(f"  Assets:       {acc.total_asset_instances:,} / {acc.total_assets:,} / {acc.asset_duplicates:,}")
    logger.info(f"  Equipment:    {acc.total_equipment_instances:,} / {acc.total_equipment:,} / {acc.equipment_duplicates:,}")
    logger.info("-" * 50)
    logger.info(f"TS to Asset Rate:        {ts_metrics['ts_to_asset_rate']}%")
    logger.info(f"Asset Monitoring:        {ts_metrics['ts_asset_monitoring_coverage']}%")
    logger.info(f"TS Critical Coverage:    {ts_metrics['ts_critical_coverage']}%")
    logger.info(f"Hierarchy Completion:    {hierarchy_metrics['hierarchy_completion_rate']}%")
    logger.info(f"Hierarchy Orphan Rate:   {hierarchy_metrics['hierarchy_orphan_rate']}%")
    logger.info(f"EQ Association:          {equipment_metrics['eq_association_rate']}%")
    logger.info(f"EQ Type Consistency:     {equipment_metrics['eq_type_consistency_rate']}%")
    logger.info("-" * 50)
    logger.info(f"Total Execution Time:    {format_elapsed(total_elapsed)}")
    logger.info(f"File saved:              {file_external_id}")
    logger.info("=" * 70)
    
    return all_metrics