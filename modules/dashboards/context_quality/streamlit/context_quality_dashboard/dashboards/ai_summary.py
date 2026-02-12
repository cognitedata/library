# -*- coding: utf-8 -*-
"""
AI Summary Generator for Context Quality Dashboards.

Uses Cognite AI Chat Completions API to generate contextual insights
based on the metrics data.
"""

import streamlit as st
from cognite.client import CogniteClient


def get_client() -> CogniteClient:
    """Get the Cognite client from session state or create a new one."""
    if 'cognite_client' not in st.session_state:
        st.session_state['cognite_client'] = CogniteClient()
    return st.session_state['cognite_client']


def generate_ai_summary(
    dashboard_type: str,
    metrics_data: dict,
    system_prompt: str,
    model: str = "azure/gpt-4.1"
) -> str:
    """
    Generate an AI summary for a dashboard.
    
    Args:
        dashboard_type: Type of dashboard (hierarchy, equipment, timeseries, maintenance)
        metrics_data: Dictionary containing the metrics to summarize
        system_prompt: System prompt with context about the metrics
        model: AI model to use (default: azure/gpt-4.1)
    
    Returns:
        Generated summary text
    """
    client = get_client()
    
    # Build the user prompt with metrics data
    user_prompt = f"""Based on the following {dashboard_type} metrics data, provide a brief, actionable summary:

{metrics_data}

Provide a 3-5 sentence summary that:
1. Highlights the most critical issues (if any)
2. Acknowledges what's working well
3. Suggests 1-2 priority actions if needed

Keep the tone professional but conversational. Use specific numbers from the data."""

    try:
        response = client.post(
            url=f"/api/v1/projects/{client.config.project}/ai/chat/completions",
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "model": model,
                "maxTokens": 400,
                "temperature": 0.7
            },
            headers={'cdf-version': 'alpha'}
        )
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"Unable to generate AI summary: {str(e)}"


def render_ai_summary_section(
    dashboard_type: str,
    metrics_data: dict,
    system_prompt: str,
    key_prefix: str
):
    """
    Render an AI summary section with a generate button.
    
    Args:
        dashboard_type: Type of dashboard for display
        metrics_data: Dictionary containing the metrics to summarize
        system_prompt: System prompt with context
        key_prefix: Unique prefix for session state keys
    """
    st.markdown("---")
    st.subheader("AI Insights")
    
    summary_key = f"{key_prefix}_ai_summary"
    loading_key = f"{key_prefix}_loading"
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        if st.button("Generate Insights", key=f"{key_prefix}_generate_btn"):
            st.session_state[loading_key] = True
            st.session_state[summary_key] = None
    
    # Check if we need to generate
    if st.session_state.get(loading_key, False):
        with st.spinner("Analyzing metrics..."):
            summary = generate_ai_summary(
                dashboard_type=dashboard_type,
                metrics_data=metrics_data,
                system_prompt=system_prompt
            )
            st.session_state[summary_key] = summary
            st.session_state[loading_key] = False
            st.rerun()
    
    # Display the summary if available
    if summary_key in st.session_state and st.session_state[summary_key]:
        st.info(st.session_state[summary_key])


# =============================================================================
# DASHBOARD-SPECIFIC PROMPTS AND DATA FORMATTERS
# =============================================================================

def get_hierarchy_prompt() -> str:
    """System prompt for Asset Hierarchy dashboard."""
    return """You are an industrial data quality expert analyzing asset hierarchy metrics for Cognite Data Fusion.

CRITICAL RULES - Follow these exactly:

1. Hierarchy Completion Rate (CRITICAL - HIGH IS GOOD)
   - Target: >98%. This measures % of non-root assets with a valid parent link.
   - If <95%: Major issue - broken hierarchy links.
   - If 95-98%: Minor issue - some missing parent links.
   - If >98%: Good.

2. Orphan Rate (CRITICAL - LOW IS GOOD)
   - Target: 0%. Orphans are assets with no parent AND no children.
   - Any orphans indicate disconnected assets that won't appear in hierarchy navigation.

3. Depth Metrics (INFORMATIONAL)
   - Max Depth >8 levels MAY indicate over-nesting, but is not inherently bad.
   - Average Depth is informational only - varies by industry.

4. Breadth Metrics (INFORMATIONAL)
   - High max children may indicate flat hierarchies - not necessarily bad.
   - Standard deviation shows distribution evenness.

DO NOT:
- Flag depth metrics as critical issues
- Say low asset counts are problems (they may be correct)
- Suggest specific fixes without knowing the data source

FOCUS ON: Hierarchy completion and orphan issues that break asset navigation."""


def get_equipment_prompt() -> str:
    """System prompt for Equipment-Asset dashboard."""
    return """You are an industrial data quality expert analyzing equipment contextualization metrics.

CRITICAL RULES - Follow these exactly:

1. Equipment Association Rate (CRITICAL - HIGH IS GOOD)
   - Target: >90%. ALL equipment items SHOULD be linked to an asset.
   - Equipment without asset links cannot be found via asset navigation.
   - If <70%: Major issue. If 70-90%: Moderate issue. If >90%: Good.

2. Asset Equipment Coverage (INFORMATIONAL - LOW VALUES ARE OK)
   - This shows % of assets that have equipment attached.
   - LOW VALUES ARE ACCEPTABLE - not all assets have physical equipment.
   - Example: A "Building" asset won't have equipment; a "Pump Station" will.
   - DO NOT flag low values as problems.

3. Critical Equipment Contextualization (CRITICAL - MUST BE 100%)
   - ALL critical equipment must be linked to assets.
   - Any value <100% is a problem for critical equipment.

4. Serial Number/Manufacturer Completeness (INFORMATIONAL)
   - Data quality indicators, not critical for contextualization.
   - Nice to have for maintenance and procurement.

DO NOT:
- Flag Asset Equipment Coverage as a problem if low
- Assume all assets should have equipment
- Suggest increasing equipment-asset coverage as a priority

FOCUS ON: Equipment Association Rate (equipment->asset) and Critical Equipment only."""


def get_timeseries_prompt() -> str:
    """System prompt for Time Series dashboard."""
    return """You are an industrial data quality expert analyzing time series contextualization metrics.

CRITICAL RULES - Follow these exactly:

1. TS to Asset Contextualization (CRITICAL - HIGH IS GOOD)
   - Target: >95%. ALL time series SHOULD be linked to an asset.
   - Orphaned TS (no asset link) cannot be found via asset navigation.
   - Orphaned TS break dashboards and analytics that filter by asset.
   - If <90%: Major issue. If 90-95%: Moderate issue. If >95%: Good.

2. Asset Monitoring Coverage (INFORMATIONAL - LOW VALUES ARE OK)
   - This shows % of assets that have time series attached.
   - LOW VALUES ARE ACCEPTABLE - not all assets need sensor monitoring.
   - Example: A "Building" asset won't have TS; a "Compressor" will.
   - DO NOT flag low values as problems.

3. Critical Asset Coverage (CRITICAL - MUST BE 100%)
   - ALL critical assets must have time series monitoring.
   - Any value <100% means critical assets lack monitoring data.

4. Data Freshness (INFORMATIONAL)
   - % of TS with recent data. Low values may indicate stale sensors.

5. Historical Data Completeness (INFORMATIONAL)
   - % of time with actual data. Gaps may indicate sensor outages.

DO NOT:
- Flag Asset Monitoring Coverage as a problem if low
- Assume all assets should have time series
- Confuse "TS to Asset" (critical) with "Asset Monitoring Coverage" (informational)

FOCUS ON: TS to Asset Contextualization (TS->asset) and Critical Asset Coverage only."""


def get_maintenance_prompt() -> str:
    """System prompt for Maintenance Workflow dashboard."""
    return """You are an industrial data quality expert analyzing maintenance workflow quality metrics from RMDM v1.

CRITICAL RULES - Follow these exactly (based on maintenance domain expert guidance):

1. Work Order -> Notification Rate (CRITICAL - MUST BE ~100%)
   - ALL work orders SHOULD originate from a notification.
   - This is the primary workflow: Notification triggers Work Order.
   - If <80%: Major issue - work orders created without proper notification.
   - If 80-95%: Moderate issue. If >95%: Good.

2. Notification -> Work Order Rate (INFORMATIONAL - LOW VALUES ARE OK)
   - Many notifications do NOT need a work order - that's normal.
   - Example: Informational notifications, minor observations, already resolved issues.
   - DO NOT flag low values as problems.

3. Work Order -> Asset Coverage (CRITICAL - HIGH IS GOOD)
   - Target: >90%. Work orders need asset context for proper assignment.

4. Work Order -> Equipment Coverage (CRITICAL - HIGH IS GOOD)
   - Target: >90%. Work orders need equipment context for technicians.

5. Notification -> Asset/Equipment (CRITICAL - HIGH IS GOOD)
   - Notifications need context for proper routing and assignment.

6. Asset Maintenance Coverage (INFORMATIONAL - LOW VALUES ARE OK)
   - Shows % of assets with maintenance records.
   - LOW VALUES ARE ACCEPTABLE - not all assets require maintenance.
   - Example: Buildings, land, infrastructure may not need maintenance orders.
   - DO NOT flag low values as problems.

7. Equipment Maintenance Coverage (INFORMATIONAL - LOW VALUES ARE OK)
   - Same as above - not all equipment needs maintenance records.
   - DO NOT flag low values as problems.

8. Failure Documentation Rates (INFORMATIONAL)
   - Important for reliability engineering but not all failures require full documentation.

DO NOT:
- Flag Notification -> Work Order as critical (it's informational)
- Flag Asset/Equipment Maintenance Coverage as problems
- Assume all assets/equipment need maintenance records
- Confuse Work Order -> Notification (critical) with Notification -> Work Order (informational)

FOCUS ON: Work Order -> Notification linkage and Work Order -> Asset/Equipment coverage."""


def format_hierarchy_metrics(hierarchy: dict) -> str:
    """Format hierarchy metrics for AI prompt."""
    return f"""Asset Hierarchy Metrics:
- Total Assets: {hierarchy.get('hierarchy_total_assets', 0):,}
- Root Assets: {hierarchy.get('hierarchy_root_count', 0):,}
- Hierarchy Completion Rate: {hierarchy.get('hierarchy_completion_rate', 0):.1f}%
- Orphan Rate: {hierarchy.get('hierarchy_orphan_rate', 0):.1f}%
- Max Depth: {hierarchy.get('hierarchy_max_depth', 0)} levels
- Average Depth: {hierarchy.get('hierarchy_avg_depth', 0):.1f} levels
- Average Children per Parent: {hierarchy.get('hierarchy_avg_children', 0):.1f}
- Assets with Parent Link: {hierarchy.get('hierarchy_with_parent', 0):,}
- Assets Missing Parent: {hierarchy.get('hierarchy_missing_parent', 0):,}"""


def format_equipment_metrics(equipment: dict, hierarchy: dict) -> str:
    """Format equipment metrics for AI prompt."""
    return f"""Equipment-Asset Metrics:
- Total Equipment: {equipment.get('eq_total', 0):,}
- Equipment with Asset Link: {equipment.get('eq_linked', 0):,}
- Equipment without Asset Link: {equipment.get('eq_unlinked', 0):,}
- Equipment Association Rate: {equipment.get('eq_association_rate', 0):.1f}%
- Assets with Equipment: {equipment.get('eq_assets_with_equipment', 0):,}
- Total Assets: {hierarchy.get('hierarchy_total_assets', 0):,}
- Asset Equipment Coverage: {equipment.get('eq_asset_coverage', 0):.1f}%
- Serial Number Completeness: {equipment.get('eq_serial_rate', 0):.1f}%
- Manufacturer Data Quality: {equipment.get('eq_manufacturer_rate', 0):.1f}%
- Type Consistency: {equipment.get('eq_type_consistency', 0):.1f}%
- Critical Equipment Linked: {equipment.get('eq_critical_rate', 'N/A')}%"""


def format_timeseries_metrics(ts_metrics: dict, hierarchy: dict) -> str:
    """Format time series metrics for AI prompt."""
    return f"""Time Series Contextualization Metrics:
- Total Time Series: {ts_metrics.get('ts_total', 0):,}
- TS with Asset Link: {ts_metrics.get('ts_with_asset_link', 0):,}
- Orphaned TS (no asset): {ts_metrics.get('ts_without_asset_link', 0):,}
- TS to Asset Rate: {ts_metrics.get('ts_to_asset_rate', 0):.1f}%
- Assets with Time Series: {ts_metrics.get('ts_associated_assets', 0):,}
- Total Assets: {hierarchy.get('hierarchy_total_assets', 0):,}
- Asset Monitoring Coverage: {ts_metrics.get('ts_asset_monitoring_coverage', 0):.1f}%
- Critical Asset Coverage: {ts_metrics.get('ts_critical_coverage', 'N/A')}%
- Data Freshness (recent data): {ts_metrics.get('ts_data_freshness', 0):.1f}%
- Historical Data Completeness: {ts_metrics.get('ts_gap_rate', 'N/A')}%
- Unit Consistency: {ts_metrics.get('ts_unit_consistency', 0):.1f}%"""


def format_maintenance_metrics(maintenance: dict) -> str:
    """Format maintenance workflow metrics for AI prompt."""
    return f"""Maintenance Workflow Metrics (RMDM v1):
- Total Notifications: {maintenance.get('maint_total_notifications', 0):,}
- Total Work Orders: {maintenance.get('maint_total_orders', 0):,}
- Total Failure Notifications: {maintenance.get('maint_total_failure_notifications', 0):,}

Notification Linkage:
- Notification -> Work Order Rate: {maintenance.get('maint_notif_to_order_rate', 'N/A')}% (INFORMATIONAL - low values OK)
- Notification -> Asset Rate: {maintenance.get('maint_notif_to_asset_rate', 'N/A')}%
- Notification -> Equipment Rate: {maintenance.get('maint_notif_to_equipment_rate', 'N/A')}%

Work Order Quality:
- Work Order -> Notification Rate: {maintenance.get('maint_order_to_notif_rate', 'N/A')}% (CRITICAL - should be ~100%)
- Work Order -> Asset Rate: {maintenance.get('maint_order_to_asset_rate', 'N/A')}%
- Work Order -> Equipment Rate: {maintenance.get('maint_order_to_equipment_rate', 'N/A')}%
- Work Order Completion Rate: {maintenance.get('maint_order_completion_rate', 'N/A')}%

Failure Documentation:
- Failure Mode Documentation: {maintenance.get('maint_failure_mode_rate', 'N/A')}%
- Failure Mechanism Documentation: {maintenance.get('maint_failure_mechanism_rate', 'N/A')}%
- Failure Cause Documentation: {maintenance.get('maint_failure_cause_rate', 'N/A')}%

Maintenance Coverage:
- Asset Maintenance Coverage: {maintenance.get('maint_asset_coverage_rate', 'N/A')}% (INFORMATIONAL - low values OK)
- Equipment Maintenance Coverage: {maintenance.get('maint_equipment_coverage_rate', 'N/A')}% (INFORMATIONAL - low values OK)"""


def get_file_annotation_prompt() -> str:
    """System prompt for P&ID Annotation dashboard."""
    return """You are an industrial data quality expert analyzing P&ID annotation metrics for P&ID diagrams.

CRITICAL RULES - Follow these exactly:

1. Reference Numbers Context
   - The user provides "Files in Scope" and "Expected Asset Tags" manually
   - If these are 0 or not provided, don't calculate rates - just report counts
   - These are user-provided estimates, not ground truth

2. File Processing Rate (CALCULATED - requires user input)
   - Shows % of in-scope files that have been annotated
   - Only meaningful if "Files in Scope" is provided
   - Target: >90% for complete processing

3. Confidence Score (HIGH IS GOOD)
   - Average confidence should be >70% for production quality
   - High confidence (>=90%) annotations are reliable
   - Low confidence (<50%) may need manual review

4. Status Distribution (INFORMATIONAL)
   - Approved annotations are validated
   - Suggested annotations are pending review
   - Rejected annotations were incorrect matches

5. Annotation Types (INFORMATIONAL)
   - Asset Tags: Links from P&ID diagrams to asset/equipment
   - File Links: Cross-references between documents
   - Both are valuable - ratio depends on document type

DO NOT:
- Assume all annotations should be approved (suggested is normal for new files)
- Flag low "File Processing Rate" if user hasn't entered "Files in Scope"
- Assume high confidence is always achievable (depends on document quality)

FOCUS ON: Confidence distribution and review backlog (suggested annotations)."""


def format_file_annotation_metrics(annot: dict, files_in_scope: int = 0, expected_tags: int = 0) -> str:
    """Format P&ID annotation metrics for AI prompt."""
    # Calculate rates if reference numbers provided
    file_rate = "N/A (user did not provide reference)"
    if files_in_scope > 0:
        rate = round(annot.get('annot_unique_files_with_annotations', 0) / files_in_scope * 100, 1)
        file_rate = f"{rate}%"
    
    tag_rate = "N/A (user did not provide reference)"
    if expected_tags > 0:
        rate = round(annot.get('annot_asset_tags', 0) / expected_tags * 100, 1)
        tag_rate = f"{rate}%"
    
    return f"""P&ID Annotation Metrics (CDM CogniteDiagramAnnotation):

Core Counts:
- Total Annotations: {annot.get('annot_total', 0):,}
- Files with Annotations: {annot.get('annot_unique_files_with_annotations', 0):,}
- Unique Assets Linked: {annot.get('annot_unique_assets_linked', 0):,}
- Unique Files Cross-Linked: {annot.get('annot_unique_files_linked', 0):,}

User-Provided Reference Numbers:
- Files in Scope: {files_in_scope if files_in_scope > 0 else 'Not provided'}
- Expected Asset Tags: {expected_tags if expected_tags > 0 else 'Not provided'}

Calculated Rates (based on user input):
- File Processing Rate: {file_rate}
- Asset Tag Detection Rate: {tag_rate}

Confidence Distribution:
- Average Confidence: {annot.get('annot_avg_confidence', 'N/A')}%
- High Confidence (>=90%): {annot.get('annot_confidence_high', 0):,} ({annot.get('annot_confidence_high_pct', 0):.1f}%)
- Medium Confidence (50-90%): {annot.get('annot_confidence_medium', 0):,} ({annot.get('annot_confidence_medium_pct', 0):.1f}%)
- Low Confidence (<50%): {annot.get('annot_confidence_low', 0):,} ({annot.get('annot_confidence_low_pct', 0):.1f}%)

Review Status:
- Approved: {annot.get('annot_approved', 0):,} ({annot.get('annot_approved_pct', 0):.1f}%)
- Suggested (pending review): {annot.get('annot_suggested', 0):,} ({annot.get('annot_suggested_pct', 0):.1f}%)
- Rejected: {annot.get('annot_rejected', 0):,} ({annot.get('annot_rejected_pct', 0):.1f}%)

Annotation Types:
- Asset Tag Annotations: {annot.get('annot_asset_tags', 0):,}
- File Link Annotations: {annot.get('annot_file_links', 0):,}
- Other Annotations: {annot.get('annot_other', 0):,}"""


# =====================================================
# 3D MODEL CONTEXTUALIZATION
# =====================================================

def get_3d_model_prompt() -> str:
    """Get the system prompt for 3D model contextualization analysis."""
    return """You are an expert in 3D model contextualization within industrial asset management systems.
You analyze metrics about the relationship between 3D objects and physical assets.

CRITICAL UNDERSTANDING:
1. **3D -> Asset Contextualization Rate** (MOST IMPORTANT): This shows what percentage of 3D objects 
   are linked to assets. This is the key indicator - orphaned 3D objects (not linked to assets) 
   represent wasted 3D modeling effort and limit the usefulness of digital twins.

2. **Asset -> 3D Coverage**: Shows what percentage of assets have 3D representations. 
   Important for digital twin completeness.

3. **Critical Asset 3D Rate**: Critical assets (marked with technicalObjectAbcIndicator='A' or similar)
   MUST have 3D representations for effective maintenance and operations.

4. **Bounding Box Completeness**: 3D objects need complete spatial definitions (xMin, xMax, yMin, yMax, 
   zMin, zMax) for proper visualization and spatial queries.

INTERPRETATION GUIDELINES:
- 3D Contextualization Rate < 70%: Significant issue - many 3D objects are orphaned
- 3D Contextualization Rate < 50%: Critical issue - most 3D work is not linked to assets
- Critical Asset 3D Rate < 100%: Priority gap - critical assets need 3D for safety/maintenance
- Bounding Box issues indicate data quality problems that affect visualization

Be specific with numbers and provide actionable recommendations."""


def format_3d_model_metrics(model3d: dict) -> str:
    """Format 3D model metrics for AI prompt."""
    total_objects = model3d.get('model3d_total_objects', 0)
    objects_with_asset = model3d.get('model3d_objects_with_asset', 0)
    unlinked = total_objects - objects_with_asset
    
    return f"""3D Model Contextualization Metrics:

KEY METRIC (3D -> Asset):
- Total 3D Objects: {total_objects:,}
- 3D Objects Linked to Assets: {objects_with_asset:,}
- Unlinked (Orphaned) 3D Objects: {unlinked:,}
- 3D Contextualization Rate: {model3d.get('model3d_contextualization_rate', 0)}%

Asset Coverage (Asset -> 3D):
- Total Assets Checked: {model3d.get('model3d_total_assets', 0):,}
- Assets with 3D Representation: {model3d.get('model3d_assets_with_3d', 0):,}
- Asset 3D Coverage Rate: {model3d.get('model3d_asset_coverage', 0)}%

Critical Asset 3D:
- Total Critical Assets: {model3d.get('model3d_critical_total', 0):,}
- Critical Assets with 3D: {model3d.get('model3d_critical_with_3d', 0):,}
- Critical Asset 3D Rate: {model3d.get('model3d_critical_asset_rate', 0)}%

Bounding Box Quality:
- Complete Bounding Box: {model3d.get('model3d_bbox_complete_count', 0):,} ({model3d.get('model3d_bbox_completeness', 0)}%)
- Partial Bounding Box: {model3d.get('model3d_bbox_partial_count', 0):,}
- Missing Bounding Box: {model3d.get('model3d_bbox_none_count', 0):,}

Model Type Distribution:
- CAD Models: {model3d.get('model3d_cad_count', 0):,}
- 360 Images: {model3d.get('model3d_360_count', 0):,}
- Point Clouds: {model3d.get('model3d_pointcloud_count', 0):,}
- Multi-Model Objects: {model3d.get('model3d_multi_model_count', 0):,}"""
