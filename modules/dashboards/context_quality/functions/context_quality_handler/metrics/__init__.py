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

from .common import (
    # Utilities
    get_props,
    get_external_id,
    format_elapsed,
    normalize_timestamp,
    extract_parent_external_id,
    get_asset_link,
    is_type_consistent,
    # Data classes
    EquipmentData,
    NotificationData,
    MaintenanceOrderData,
    FailureNotificationData,
    FileData,
    # Accumulator
    CombinedAccumulator,
    # Config
    DEFAULT_CONFIG,
    TYPE_MAPPINGS,
    LOG_EVERY_N_BATCHES,
    TIMEOUT_WARNING_SECONDS,
    TIMEOUT_SECONDS,
    METRICS_FILE_EXTERNAL_ID,
    METRICS_FILE_NAME,
    BATCH_FILE_PREFIX,
)

from .timeseries import (
    process_timeseries_batch,
    compute_historical_gaps_batch,
    compute_ts_metrics,
)

from .asset_hierarchy import (
    process_asset_batch,
    compute_depth_map,
    compute_asset_hierarchy_metrics,
)

from .equipment import (
    process_equipment_batch,
    compute_equipment_metrics,
)

from .maintenance import (
    get_direct_relation_id,
    get_direct_relation_ids,
    process_notification_batch,
    process_maintenance_order_batch,
    process_failure_notification_batch,
    compute_maintenance_metrics,
)

from .file_annotation import (
    AnnotationData,
    FileAnnotationAccumulator,
    process_annotation_batch,
    compute_file_annotation_metrics,
)

from .model_3d import (
    Model3DAccumulator,
    process_asset_3d_batch,
    process_3d_object_batch,
    compute_3d_metrics,
)

from .files import (
    process_file_batch,
    compute_file_metrics,
)

from .storage import (
    save_metrics_to_file,
    # Batch processing functions
    get_batch_file_external_id,
    save_batch_file,
    list_batch_files,
    load_batch_file,
    load_and_merge_all_batches,
    delete_batch_files,
)
