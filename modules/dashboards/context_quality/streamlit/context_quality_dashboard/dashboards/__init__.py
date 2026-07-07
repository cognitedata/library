"""
Dashboard Modules for Contextualization Quality Dashboard.

Exports all rendering functions and UI components organized by domain:
- common: Shared UI components (gauge, metric_card, color functions)
- asset_hierarchy: Asset Hierarchy Quality dashboard
- equipment: Equipment-Asset Quality dashboard
- timeseries: Time Series Contextualization dashboard
- maintenance: Maintenance Workflow Quality dashboard (RMDM v1)
- file_annotation: File Annotation Quality dashboard (CDM CogniteDiagramAnnotation)
- files: File Contextualization dashboard (CDM CogniteFile)
- configuration: Data model configuration panel
"""

from .asset_hierarchy import render_asset_hierarchy_dashboard
from .common import (
    gauge,
    gauge_na,
    get_status_color_equipment,
    # Color functions
    get_status_color_hierarchy,
    get_status_color_maintenance,
    get_status_color_ts,
    # UI components
    metric_card,
)
from .configuration import render_configuration_panel
from .equipment import render_equipment_dashboard
from .file_annotation import render_file_annotation_dashboard
from .files import render_files_dashboard
from .maintenance import render_maintenance_dashboard
from .model_3d import render_3d_model_dashboard
from .sidebar import render_metadata_sidebar
from .timeseries import render_time_series_dashboard
