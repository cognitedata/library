# -*- coding: utf-8 -*-
"""
Configuration panel for Data Model View settings.

Allows users to configure which data model views to use for metrics computation
for each dashboard. Includes function execution with status tracking.
"""

import streamlit as st
from cognite.client import CogniteClient


# ----------------------------------------------------
# DEFAULT CONFIGURATION (mirrors handler defaults)
# ----------------------------------------------------
DEFAULT_CONFIG = {
    # Dashboard 1: Asset Hierarchy
    "asset_view_space": "cdf_cdm",
    "asset_view_external_id": "CogniteAsset",
    "asset_view_version": "v1",
    
    # Dashboard 2: Equipment-Asset
    "equipment_view_space": "cdf_cdm",
    "equipment_view_external_id": "CogniteEquipment",
    "equipment_view_version": "v1",
    
    # Dashboard 3: Time Series
    "ts_view_space": "cdf_cdm",
    "ts_view_external_id": "CogniteTimeSeries",
    "ts_view_version": "v1",
    
    # Dashboard 4: Maintenance Workflow (RMDM)
    "notification_view_space": "rmdm",
    "notification_view_external_id": "Notification",
    "notification_view_version": "v1",
    "maintenance_order_view_space": "rmdm",
    "maintenance_order_view_external_id": "MaintenanceOrder",
    "maintenance_order_view_version": "v1",
    "failure_notification_view_space": "rmdm",
    "failure_notification_view_external_id": "FailureNotification",
    "failure_notification_view_version": "v1",
    
    # Dashboard 5: File Annotation
    "annotation_view_space": "cdf_cdm",
    "annotation_view_external_id": "CogniteDiagramAnnotation",
    "annotation_view_version": "v1",
    
    # Dashboard 6: 3D Model
    "object3d_view_space": "cdf_cdm",
    "object3d_view_external_id": "Cognite3DObject",
    "object3d_view_version": "v1",
    
    # Feature Flags
    "enable_maintenance_metrics": True,
    "enable_file_annotation_metrics": True,
    "enable_3d_metrics": True,
    
    # Limits
    "max_assets": 150000,
    "max_equipment": 150000,
    "max_timeseries": 150000,
    "max_notifications": 150000,
    "max_maintenance_orders": 150000,
    "max_annotations": 200000,
    "max_3d_objects": 150000,
}

FUNCTION_EXTERNAL_ID = "context_quality_handler"


def _init_session_state():
    """Initialize session state with default config values."""
    if "config_initialized" not in st.session_state:
        for key, value in DEFAULT_CONFIG.items():
            if key not in st.session_state:
                st.session_state[key] = value
        st.session_state["config_initialized"] = True
    
    # Function run tracking
    if "function_call_id" not in st.session_state:
        st.session_state["function_call_id"] = None
    if "function_status" not in st.session_state:
        st.session_state["function_status"] = None


def _get_current_config() -> dict:
    """Get current configuration from session state."""
    return {key: st.session_state.get(key, DEFAULT_CONFIG[key]) for key in DEFAULT_CONFIG}


def _check_function_status(client: CogniteClient, call_id: int) -> dict:
    """Check the status of a function call."""
    try:
        call = client.functions.calls.retrieve(
            call_id=call_id,
            function_external_id=FUNCTION_EXTERNAL_ID
        )
        return {
            "status": call.status,
            "started_at": call.start_time,
            "ended_at": call.end_time,
            "error": getattr(call, "error", None)
        }
    except Exception as e:
        return {"status": "Error", "error": str(e)}


def _render_view_inputs(label: str, space_key: str, external_id_key: str, version_key: str):
    """Render a row of inputs for a single view configuration."""
    # Ensure session state is initialized for these keys BEFORE widget rendering
    keys_to_init = [space_key, external_id_key, version_key]
    for key in keys_to_init:
        if key not in st.session_state:
            st.session_state[key] = DEFAULT_CONFIG.get(key, "")
    
    # Get current values from session state
    space_value = st.session_state.get(space_key, DEFAULT_CONFIG.get(space_key, ""))
    external_id_value = st.session_state.get(external_id_key, DEFAULT_CONFIG.get(external_id_key, ""))
    version_value = st.session_state.get(version_key, DEFAULT_CONFIG.get(version_key, ""))
    
    col1, col2, col3 = st.columns([1, 2, 0.5])
    with col1:
        new_space = st.text_input(
            "Space",
            value=space_value,
            key=f"{space_key}_widget",
            help=f"Data model space for {label}"
        )
        # Update session state if value changed
        if new_space != st.session_state.get(space_key):
            st.session_state[space_key] = new_space
    with col2:
        new_external_id = st.text_input(
            "View External ID",
            value=external_id_value,
            key=f"{external_id_key}_widget",
            help=f"View external ID for {label}"
        )
        # Update session state if value changed
        if new_external_id != st.session_state.get(external_id_key):
            st.session_state[external_id_key] = new_external_id
    with col3:
        new_version = st.text_input(
            "Version",
            value=version_value,
            key=f"{version_key}_widget",
            help=f"View version for {label}"
        )
        # Update session state if value changed
        if new_version != st.session_state.get(version_key):
            st.session_state[version_key] = new_version


def _render_quick_run_section(client: CogniteClient):
    """Render the quick run section for small datasets (in expander)."""
    
    # Check if there's an active function call
    call_id = st.session_state.get("function_call_id")
    
    if call_id:
        # Show status of existing call
        status_info = _check_function_status(client, call_id)
        status = status_info.get("status", "Unknown")
        
        if status == "Running":
            st.info(f"""
            **Function is running...**
            
            - **Call ID:** `{call_id}`
            - **Status:** {status}
            
            This typically takes 1-5 minutes depending on data volume.
            """)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Check Status", use_container_width=True, key="quick_check"):
                    st.rerun()
            with col2:
                if st.button("Cancel Tracking", use_container_width=True, key="quick_cancel"):
                    st.session_state["function_call_id"] = None
                    st.session_state["function_status"] = None
                    st.rerun()
                    
        elif status == "Completed":
            st.success(f"""
            **Function completed successfully!**
            
            - **Call ID:** `{call_id}`
            
            Click any of the dashboard tabs above to view your metrics.
            """)
            
            if st.button("Run Again", use_container_width=True, key="quick_rerun"):
                st.session_state["function_call_id"] = None
                st.rerun()
                    
        elif status == "Failed":
            error_msg = status_info.get("error", "Unknown error")
            st.error(f"""
            **Function failed**
            
            - **Call ID:** `{call_id}`
            - **Error:** {error_msg}
            """)
            
            if st.button("Try Again", use_container_width=True, key="quick_retry"):
                st.session_state["function_call_id"] = None
                st.rerun()
        else:
            st.warning(f"**Function Status:** {status} (Call ID: `{call_id}`)")
            if st.button("Check Status", use_container_width=True, key="quick_check_other"):
                st.rerun()
    else:
        # No active call - show run button
        run_button = st.button(
            "Run Function (Quick Mode)",
            type="secondary",
            use_container_width=True,
            help="Runs with default limits (150k instances max). For larger datasets, use Batch Processing."
        )
        
        if run_button:
            config = _get_current_config()
            
            with st.spinner("Starting function..."):
                try:
                    call = client.functions.call(
                        external_id=FUNCTION_EXTERNAL_ID,
                        data=config,
                        wait=False
                    )
                    
                    st.session_state["function_call_id"] = call.id
                    st.session_state["function_status"] = "Running"
                    st.rerun()
                    
                except Exception as e:
                    error_str = str(e).lower()
                    
                    if "not found" in error_str or "does not exist" in error_str:
                        st.error(f"""
                        **Function not available yet**
                        
                        The function `{FUNCTION_EXTERNAL_ID}` was not found. 
                        Functions can take 2-5 minutes to deploy after `cdf deploy`.
                        """)
                    else:
                        st.error(f"**Failed to trigger function:** {str(e)}")


def _render_batch_processing_section(client: CogniteClient):
    """Render the batch processing section (primary method for running the function)."""
    st.subheader("Run Metrics Function")
    
    st.markdown("""
    **Batch Processing** is the recommended way to compute metrics for most projects.
    It processes your data in multiple runs to handle large datasets and avoid timeouts.
    """)
    
    # Initialize batch mode state
    if "batch_size" not in st.session_state:
        st.session_state["batch_size"] = 200000
    if "num_batches" not in st.session_state:
        st.session_state["num_batches"] = 3
    if "current_batch" not in st.session_state:
        st.session_state["current_batch"] = 0
    if "batch_call_ids" not in st.session_state:
        st.session_state["batch_call_ids"] = []
    
    # Check for aggregation status (from function_call_id)
    agg_call_id = st.session_state.get("function_call_id")
    if agg_call_id:
        status_info = _check_function_status(client, agg_call_id)
        status = status_info.get("status", "Unknown")
        
        if status == "Running":
            st.info(f"""
            **Aggregation is running...**
            
            - **Call ID:** `{agg_call_id}`
            
            This combines all batch results into final metrics.
            """)
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Check Aggregation Status", use_container_width=True):
                    st.rerun()
            with col2:
                if st.button("Cancel Tracking", use_container_width=True):
                    st.session_state["function_call_id"] = None
                    st.rerun()
            return
        elif status == "Completed":
            st.success(f"""
            **Aggregation completed successfully!**
            
            - **Call ID:** `{agg_call_id}`
            
            Click any of the dashboard tabs above to view your metrics.
            """)
            if st.button("Start New Run", use_container_width=True):
                st.session_state["function_call_id"] = None
                st.session_state["batch_call_ids"] = []
                st.session_state["current_batch"] = 0
                st.rerun()
            return
        elif status == "Failed":
            error_msg = status_info.get("error", "Unknown error")
            st.error(f"""
            **Aggregation failed**
            
            - **Call ID:** `{agg_call_id}`
            - **Error:** {error_msg}
            """)
            if st.button("Try Again", use_container_width=True):
                st.session_state["function_call_id"] = None
                st.rerun()
            return
    
    # Batch configuration
    st.markdown("#### Batch Configuration")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.number_input(
            "Batch Size",
            min_value=50000,
            max_value=500000,
            step=50000,
            key="batch_size",
            help="Number of instances to process per batch run"
        )
    with col2:
        st.number_input(
            "Number of Batches",
            min_value=1,
            max_value=10,
            step=1,
            key="num_batches",
            help="Total number of batch runs. Set based on your data size."
        )
    with col3:
        batch_size = st.session_state.get("batch_size", 200000)
        num_batches = st.session_state.get("num_batches", 3)
        total_capacity = batch_size * num_batches
        st.metric("Total Capacity", f"{total_capacity:,}", help="Maximum instances that can be processed")
    
    st.markdown("---")
    
    # Batch execution
    st.markdown("#### Batch Execution")
    
    batch_call_ids = st.session_state.get("batch_call_ids", [])
    num_batches = st.session_state.get("num_batches", 3)
    
    # Check status of completed batches
    completed_batches = []
    running_batches = []
    failed_batches = []
    
    for i, call_id in enumerate(batch_call_ids):
        if call_id:
            status = _check_function_status(client, call_id)
            status_str = status.get("status", "Unknown")
            if status_str == "Completed":
                completed_batches.append(i)
            elif status_str == "Running":
                running_batches.append(i)
            else:
                failed_batches.append(i)
    
    # Progress display
    progress_pct = len(completed_batches) / num_batches if num_batches > 0 else 0
    st.progress(progress_pct, text=f"**Progress:** {len(completed_batches)} / {num_batches} batches completed")
    
    if running_batches:
        st.info(f"Batch {running_batches[0]} is currently running...")
    
    # Action buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        current_batch = st.session_state.get("current_batch", 0)
        # Can only run next batch if no batch is currently running
        can_run_batch = current_batch < num_batches and len(running_batches) == 0
        
        if can_run_batch:
            if st.button(f"Run Batch {current_batch}", type="primary", use_container_width=True):
                config = _get_current_config()
                config["batch_mode"] = True
                config["batch_index"] = current_batch
                config["batch_size"] = st.session_state.get("batch_size", 200000)
                
                try:
                    call = client.functions.call(
                        external_id=FUNCTION_EXTERNAL_ID,
                        data=config,
                        wait=False
                    )
                    # Track this batch call
                    if len(batch_call_ids) <= current_batch:
                        batch_call_ids.append(call.id)
                    else:
                        batch_call_ids[current_batch] = call.id
                    st.session_state["batch_call_ids"] = batch_call_ids
                    st.session_state["current_batch"] = current_batch + 1
                    st.success(f"Batch {current_batch} started! Call ID: {call.id}")
                    st.rerun()
                except Exception as e:
                    error_str = str(e).lower()
                    if "not found" in error_str or "does not exist" in error_str:
                        st.error(f"""
                        **Function not available yet**
                        
                        The function `{FUNCTION_EXTERNAL_ID}` was not found. 
                        Functions can take 2-5 minutes to deploy after `cdf deploy`.
                        """)
                    else:
                        st.error(f"Failed to start batch: {e}")
        elif current_batch >= num_batches and len(running_batches) == 0:
            st.success("All batches submitted")
        else:
            st.button(f"Run Batch {current_batch}", disabled=True, use_container_width=True,
                     help="Wait for current batch to complete")
    
    with col2:
        if st.button("Check Status", use_container_width=True):
            st.rerun()
    
    with col3:
        all_complete = len(completed_batches) == num_batches and num_batches > 0
        if all_complete:
            if st.button("Run Aggregation", type="primary", use_container_width=True):
                config = _get_current_config()
                config["is_aggregation"] = True
                
                try:
                    call = client.functions.call(
                        external_id=FUNCTION_EXTERNAL_ID,
                        data=config,
                        wait=False
                    )
                    st.session_state["function_call_id"] = call.id
                    st.session_state["function_status"] = "Running"
                    st.success(f"Aggregation started! Call ID: {call.id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to start aggregation: {e}")
        else:
            st.button("Run Aggregation", disabled=True, use_container_width=True,
                     help="Complete all batches first")
    
    with col4:
        if st.button("Reset", use_container_width=True):
            st.session_state["batch_call_ids"] = []
            st.session_state["current_batch"] = 0
            st.session_state["function_call_id"] = None
            st.success("Reset complete")
            st.rerun()
    
    # Show batch status table with retry option
    if batch_call_ids:
        st.markdown("#### Batch Status")
        for i, call_id in enumerate(batch_call_ids):
            if call_id:
                status = _check_function_status(client, call_id)
                status_str = status.get("status", "Unknown")
                
                is_success = status_str == "Completed"
                is_running = status_str == "Running"
                is_failed = not is_success and not is_running
                
                if is_success:
                    icon = "[OK]"
                elif is_running:
                    icon = "[...]"
                else:
                    icon = "[X]"
                
                col_status, col_retry = st.columns([4, 1])
                with col_status:
                    st.write(f"{icon} **Batch {i}:** {status_str} (Call ID: {call_id})")
                with col_retry:
                    if is_failed:
                        if st.button(f"Retry", key=f"retry_batch_{i}", use_container_width=True):
                            config = _get_current_config()
                            config["batch_mode"] = True
                            config["batch_index"] = i
                            config["batch_size"] = st.session_state.get("batch_size", 200000)
                            
                            try:
                                call = client.functions.call(
                                    external_id=FUNCTION_EXTERNAL_ID,
                                    data=config,
                                    wait=False
                                )
                                batch_call_ids[i] = call.id
                                st.session_state["batch_call_ids"] = batch_call_ids
                                st.success(f"Batch {i} restarted! Call ID: {call.id}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to retry batch: {e}")


def render_configuration_panel(client: CogniteClient, show_getting_started: bool = False):
    """
    Render the configuration panel for data model view settings.
    
    Args:
        client: CogniteClient instance for function calls
        show_getting_started: If True, show a prominent getting started message
    """
    _init_session_state()
    
    # Getting started banner for first-time users
    if show_getting_started:
        st.info("""
        **Welcome to the Contextualization Quality Dashboard!**
        
        To get started:
        1. **Configure** your data model views for each dashboard below (or use defaults)
        2. **Run the function** using Batch Processing (recommended for most projects)
        3. **View the dashboard** tabs to see your data quality metrics
        """)
    
    st.header("Configuration & Run")
    
    # ----- BATCH PROCESSING SECTION (PRIMARY - AT TOP) -----
    _render_batch_processing_section(client)
    
    st.markdown("---")
    
    # ----- PER-DASHBOARD CONFIGURATION -----
    st.subheader("Dashboard Configuration")
    st.caption("Configure data model views for each dashboard. Defaults are set for Cognite Core Data Model (CDM).")
    
    # Dashboard 1: Asset Hierarchy
    with st.expander("Asset Hierarchy Dashboard", expanded=True):
        st.caption("Measures structural quality of your asset tree: hierarchy completion, orphans, depth/breadth.")
        st.markdown("**Asset View** - Used to query asset hierarchy data")
        _render_view_inputs(
            "Asset",
            "asset_view_space",
            "asset_view_external_id",
            "asset_view_version"
        )
    
    # Dashboard 2: Equipment-Asset
    with st.expander("Equipment-Asset Dashboard", expanded=True):
        st.caption("Measures equipment-to-asset relationships: association rates, metadata completeness, type consistency.")
        
        st.markdown("**Equipment View** - Used to query equipment data")
        _render_view_inputs(
            "Equipment",
            "equipment_view_space",
            "equipment_view_external_id",
            "equipment_view_version"
        )
        
        st.markdown("---")
        st.caption("*Note: This dashboard also uses the Asset View configured above for relationship analysis.*")
    
    # Dashboard 3: Time Series
    with st.expander("Time Series Dashboard", expanded=True):
        st.caption("Measures time series contextualization: asset linkage, unit completeness, data freshness.")
        
        st.markdown("**Time Series View** - Used to query time series data")
        _render_view_inputs(
            "Time Series",
            "ts_view_space",
            "ts_view_external_id",
            "ts_view_version"
        )
        
        st.markdown("---")
        st.caption("*Note: This dashboard also uses the Asset View for coverage analysis.*")
    
    # Dashboard 4: Maintenance Workflow
    with st.expander("Maintenance Workflow Dashboard", expanded=False):
        st.caption("Measures maintenance data quality from RMDM: notifications, work orders, failure documentation.")
        
        # Enable/Disable toggle
        enable_maint = st.checkbox(
            "Enable Maintenance Metrics",
            value=st.session_state.get("enable_maintenance_metrics", True),
            key="enable_maintenance_metrics",
            help="Uncheck to skip maintenance workflow metrics (useful if RMDM is not deployed)"
        )
        
        if enable_maint:
            st.markdown("**Notification View**")
            _render_view_inputs(
                "Notification",
                "notification_view_space",
                "notification_view_external_id",
                "notification_view_version"
            )
            
            st.markdown("**Maintenance Order View**")
            _render_view_inputs(
                "Maintenance Order",
                "maintenance_order_view_space",
                "maintenance_order_view_external_id",
                "maintenance_order_view_version"
            )
            
            st.markdown("**Failure Notification View**")
            _render_view_inputs(
                "Failure Notification",
                "failure_notification_view_space",
                "failure_notification_view_external_id",
                "failure_notification_view_version"
            )
            
            st.markdown("---")
            st.caption("*Note: This dashboard also uses Asset and Equipment Views for coverage analysis.*")
        else:
            st.warning("Maintenance metrics disabled. Enable to configure views.")
    
    # Dashboard 5: File Annotation
    with st.expander("File Annotation Dashboard", expanded=False):
        st.caption("Measures P&ID diagram annotation quality: confidence scores, status distribution, annotation types.")
        
        # Enable/Disable toggle
        enable_annot = st.checkbox(
            "Enable File Annotation Metrics",
            value=st.session_state.get("enable_file_annotation_metrics", True),
            key="enable_file_annotation_metrics",
            help="Uncheck to skip file annotation metrics"
        )
        
        if enable_annot:
            st.markdown("**Diagram Annotation View**")
            _render_view_inputs(
                "Diagram Annotation",
                "annotation_view_space",
                "annotation_view_external_id",
                "annotation_view_version"
            )
        else:
            st.warning("File annotation metrics disabled. Enable to configure views.")
    
    # Dashboard 6: 3D Model
    with st.expander("3D Model Dashboard", expanded=False):
        st.caption("Measures 3D model contextualization: asset-3D associations, critical asset coverage, bounding box completeness.")
        
        # Enable/Disable toggle
        enable_3d = st.checkbox(
            "Enable 3D Model Metrics",
            value=st.session_state.get("enable_3d_metrics", True),
            key="enable_3d_metrics",
            help="Uncheck to skip 3D model metrics"
        )
        
        if enable_3d:
            st.markdown("**3D Object View**")
            _render_view_inputs(
                "3D Object",
                "object3d_view_space",
                "object3d_view_external_id",
                "object3d_view_version"
            )
            
            st.markdown("---")
            st.caption("*Note: This dashboard also uses the Asset View to check for 3D associations.*")
        else:
            st.warning("3D model metrics disabled. Enable to configure views.")
    
    # ----- ADVANCED SETTINGS -----
    st.markdown("---")
    
    with st.expander("Processing Limits (Advanced)", expanded=False):
        st.caption("Adjust these if you have large datasets or experience timeouts (function max runtime: 10 min).")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.number_input(
                "Max Assets",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_assets", 150000),
                step=10000,
                key="max_assets",
                help="Maximum number of assets to process"
            )
            st.number_input(
                "Max Notifications",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_notifications", 150000),
                step=10000,
                key="max_notifications",
                help="Maximum number of notifications to process"
            )
        with col2:
            st.number_input(
                "Max Equipment",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_equipment", 150000),
                step=10000,
                key="max_equipment",
                help="Maximum number of equipment to process"
            )
            st.number_input(
                "Max Maintenance Orders",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_maintenance_orders", 150000),
                step=10000,
                key="max_maintenance_orders",
                help="Maximum number of maintenance orders to process"
            )
        with col3:
            st.number_input(
                "Max Time Series",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_timeseries", 150000),
                step=10000,
                key="max_timeseries",
                help="Maximum number of time series to process"
            )
            st.number_input(
                "Max Annotations",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_annotations", 200000),
                step=10000,
                key="max_annotations",
                help="Maximum number of annotations to process"
            )
            st.number_input(
                "Max 3D Objects",
                min_value=1000,
                max_value=500000,
                value=st.session_state.get("max_3d_objects", 150000),
                step=10000,
                key="max_3d_objects",
                help="Maximum number of 3D objects to process"
            )
    
    # ----- QUICK RUN MODE (for small datasets) -----
    with st.expander("Quick Run Mode (< 150k instances)", expanded=False):
        st.caption("""
        For smaller datasets (under 150k assets/timeseries), you can run a single function call.
        **Note:** This mode has a 150k instance limit. For larger datasets, use Batch Processing above.
        """)
        _render_quick_run_section(client)
    
    # ----- CURRENT CONFIG DISPLAY -----
    with st.expander("View Current Configuration JSON", expanded=False):
        config = _get_current_config()
        st.json(config)
