"""
Metrics Modules for Contextualization Quality Function.

Exports all processing functions and metric computations organized by domain:
- common: Shared utilities, data classes, and CombinedAccumulator
- timeseries: TS processing and metrics
- asset_hierarchy: Asset processing and hierarchy metrics
- equipment: Equipment processing and metrics
- maintenance: Maintenance workflow processing and metrics (RMDM v1)
- files: File contextualization processing and metrics (CogniteFile)
- storage: File storage utilities
"""

from .asset_hierarchy import (
    compute_asset_hierarchy_metrics,
    compute_depth_map,
    process_asset_batch,
)
from .common import (
    BATCH_FILE_PREFIX,
    # Config
    DEFAULT_CONFIG,
    LOG_EVERY_N_BATCHES,
    METRICS_FILE_EXTERNAL_ID,
    METRICS_FILE_NAME,
    TIMEOUT_SECONDS,
    TIMEOUT_WARNING_SECONDS,
    TYPE_MAPPINGS,
    # Accumulator
    CombinedAccumulator,
    # Data classes
    EquipmentData,
    FailureNotificationData,
    FileData,
    MaintenanceOrderData,
    NotificationData,
    extract_parent_external_id,
    format_elapsed,
    get_asset_link,
    get_external_id,
    # Utilities
    get_props,
    is_type_consistent,
    normalize_timestamp,
)
from .equipment import (
    compute_equipment_metrics,
    process_equipment_batch,
)
from .file_annotation import (
    AnnotationData,
    FileAnnotationAccumulator,
    compute_file_annotation_metrics,
    process_annotation_batch,
)
from .files import (
    compute_file_metrics,
    process_file_batch,
)
from .maintenance import (
    compute_maintenance_metrics,
    get_direct_relation_id,
    get_direct_relation_ids,
    process_failure_notification_batch,
    process_maintenance_order_batch,
    process_notification_batch,
)
from .model_3d import (
    Model3DAccumulator,
    compute_3d_metrics,
    process_3d_object_batch,
    process_asset_3d_batch,
)
from .storage import (
    delete_batch_files,
    # Batch processing functions
    get_batch_file_external_id,
    list_batch_files,
    load_and_merge_all_batches,
    load_batch_file,
    save_batch_file,
    save_metrics_to_file,
)
from .timeseries import (
    compute_historical_gaps_batch,
    compute_ts_metrics,
    process_timeseries_batch,
)
