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
    # Ensure session state is initialized for these keys
    for key, default_key in [(space_key, space_key), (external_id_key, external_id_key), (version_key, version_key)]:
        if key not in st.session_state:
            st.session_state[key] = DEFAULT_CONFIG.get(key, "")
    
    col1, col2, col3 = st.columns([1, 2, 0.5])
    with col1:
        st.text_input(
            "Space",
            key=space_key,
            help=f"Data model space for {label}"
        )
    with col2:
        st.text_input(
            "View External ID",
            key=external_id_key,
            help=f"View external ID for {label}"
        )
    with col3:
        st.text_input(
            "Version",
            key=version_key,
            help=f"View version for {label}"
        )


def _render_function_status_section(client: CogniteClient):
    """Render the function execution and status section."""
    st.subheader("🚀 Run Metrics Function")
    
    # Check if there's an active function call
    call_id = st.session_state.get("function_call_id")
    
    if call_id:
        # Show status of existing call
        status_info = _check_function_status(client, call_id)
        status = status_info.get("status", "Unknown")
        
        if status == "Running":
            st.info(f"""
            ⏳ **Function is running...**
            
            - **Call ID:** `{call_id}`
            - **Status:** {status}
            
            This typically takes 1-5 minutes depending on data volume.
            """)
            
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button("🔄 Check Status", use_container_width=True):
                    st.rerun()
            with col2:
                if st.button("❌ Cancel Tracking", use_container_width=True):
                    st.session_state["function_call_id"] = None
                    st.session_state["function_status"] = None
                    st.rerun()
                    
        elif status == "Completed":
            st.success(f"""
            ✅ **Function completed successfully!**
            
            - **Call ID:** `{call_id}`
            
            👆 Click any of the dashboard tabs above (🌳 Asset Hierarchy, 🔧 Equipment-Asset, etc.) to view your metrics.
            """)
            
            if st.button("▶️ Run Again", use_container_width=True):
                st.session_state["function_call_id"] = None
                st.rerun()
                    
        elif status == "Failed":
            error_msg = status_info.get("error", "Unknown error")
            st.error(f"""
            ❌ **Function failed**
            
            - **Call ID:** `{call_id}`
            - **Error:** {error_msg}
            
            Check the function logs in CDF for more details.
            """)
            
            if st.button("🔄 Try Again", use_container_width=True):
                st.session_state["function_call_id"] = None
                st.rerun()
        else:
            # Unknown or other status
            st.warning(f"""
            **Function Status:** {status}
            
            - **Call ID:** `{call_id}`
            """)
            if st.button("🔄 Check Status", use_container_width=True):
                st.rerun()
    else:
        # No active call - show run button
        st.write("""
        Run the metrics function to compute quality metrics based on your configuration.
        This will analyze your data and generate the dashboard metrics.
        """)
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            run_button = st.button(
                "▶️ Run Function",
                type="primary",
                use_container_width=True,
                help="Trigger the Contextualization Quality Metrics function"
            )
        
        with col2:
            if st.button("🔄 Reset All Config", use_container_width=True):
                for key, value in DEFAULT_CONFIG.items():
                    st.session_state[key] = value
                st.success("Configuration reset to defaults!")
                st.rerun()
        
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
                    
                    # Check if it's a "function not found" error
                    if "not found" in error_str or "does not exist" in error_str:
                        st.error(f"""
                        ❌ **Function not available yet**
                        
                        The function `{FUNCTION_EXTERNAL_ID}` was not found. This usually means:
                        
                        1. **The function is still deploying** — Cognite Functions can take 2-5 minutes to deploy after `cdf deploy`. Please wait and try again.
                        
                        2. **Deployment failed** — Check your deployment status in CDF:
                           - Go to **Data management** → **Build solutions** → **Functions**
                           - Look for `{FUNCTION_EXTERNAL_ID}` and check its status
                        
                        3. **Function was not deployed** — Run `cdf deploy` again from your project directory.
                        """)
                    else:
                        st.error(f"""
                        ❌ **Failed to trigger function**
                        
                        Error: {str(e)}
                        
                        **Troubleshooting:**
                        - Ensure the function `{FUNCTION_EXTERNAL_ID}` is deployed
                        - Check that you have permission to call functions
                        - Verify deployment in CDF: **Data management** → **Build solutions** → **Functions**
                        """)


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
        👋 **Welcome to the Contextualization Quality Dashboard!**
        
        To get started:
        1. **Configure** your data model views for each dashboard below (or use defaults)
        2. **Run the function** to compute metrics
        3. **View the dashboard** tabs to see your data quality metrics
        """)
    
    st.header("⚙️ Configuration & Run")
    
    # ----- FUNCTION EXECUTION SECTION (AT TOP) -----
    _render_function_status_section(client)
    
    st.markdown("---")
    
    # ----- PER-DASHBOARD CONFIGURATION -----
    st.subheader("📊 Dashboard Configuration")
    st.caption("Configure data model views for each dashboard. Defaults are set for Cognite Core Data Model (CDM).")
    
    # Dashboard 1: Asset Hierarchy
    with st.expander("🌳 Asset Hierarchy Dashboard", expanded=True):
        st.caption("Measures structural quality of your asset tree: hierarchy completion, orphans, depth/breadth.")
        st.markdown("**Asset View** - Used to query asset hierarchy data")
        _render_view_inputs(
            "Asset",
            "asset_view_space",
            "asset_view_external_id",
            "asset_view_version"
        )
    
    # Dashboard 2: Equipment-Asset
    with st.expander("🔧 Equipment-Asset Dashboard", expanded=True):
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
    with st.expander("⏱️ Time Series Dashboard", expanded=True):
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
    with st.expander("🛠️ Maintenance Workflow Dashboard", expanded=False):
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
    with st.expander("📄 File Annotation Dashboard", expanded=False):
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
    with st.expander("🎮 3D Model Dashboard", expanded=False):
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
    
    with st.expander("📊 Processing Limits (Advanced)", expanded=False):
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
    
    # ----- BATCH MODE (for large datasets) -----
    with st.expander("🔄 Batch Processing Mode (200k+ instances)", expanded=False):
        st.caption("""
        For large datasets (200k+ assets), use batch processing to avoid function timeouts.
        Each batch run processes a subset of data, and the final aggregation run merges all batches.
        """)
        
        # Initialize batch mode state
        if "batch_mode_enabled" not in st.session_state:
            st.session_state["batch_mode_enabled"] = False
        if "batch_size" not in st.session_state:
            st.session_state["batch_size"] = 200000
        if "num_batches" not in st.session_state:
            st.session_state["num_batches"] = 3
        if "current_batch" not in st.session_state:
            st.session_state["current_batch"] = 0
        if "batch_call_ids" not in st.session_state:
            st.session_state["batch_call_ids"] = []
        
        enable_batch = st.checkbox(
            "Enable Batch Processing",
            value=st.session_state.get("batch_mode_enabled", False),
            key="batch_mode_enabled",
            help="Process data in multiple runs to handle large datasets"
        )
        
        if enable_batch:
            col1, col2 = st.columns(2)
            with col1:
                st.number_input(
                    "Batch Size (instances per run)",
                    min_value=50000,
                    max_value=500000,
                    value=st.session_state.get("batch_size", 200000),
                    step=50000,
                    key="batch_size",
                    help="Number of instances to process per batch run"
                )
            with col2:
                st.number_input(
                    "Number of Batches",
                    min_value=2,
                    max_value=10,
                    value=st.session_state.get("num_batches", 3),
                    step=1,
                    key="num_batches",
                    help="Total number of batch runs before aggregation"
                )
            
            st.markdown("---")
            st.markdown("**Batch Execution**")
            
            # Show batch status
            batch_call_ids = st.session_state.get("batch_call_ids", [])
            num_batches = st.session_state.get("num_batches", 3)
            
            # Check status of completed batches
            completed_batches = []
            for i, call_id in enumerate(batch_call_ids):
                if call_id:
                    status = _check_function_status(client, call_id)
                    if status.get("status") == "Completed":
                        completed_batches.append(i)
            
            st.write(f"**Progress:** {len(completed_batches)} / {num_batches} batches completed")
            
            # Show batch buttons
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                current_batch = st.session_state.get("current_batch", 0)
                if current_batch < num_batches:
                    if st.button(f"▶️ Run Batch {current_batch}", type="primary", use_container_width=True):
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
                            st.error(f"Failed to start batch: {e}")
                else:
                    st.info("All batches submitted")
            
            with col2:
                # Check Status button - refreshes status without losing state
                if st.button("🔄 Check Status", use_container_width=True, 
                            help="Refresh batch status"):
                    st.rerun()
            
            with col3:
                if len(completed_batches) == num_batches:
                    if st.button("🔗 Run Aggregation", type="primary", use_container_width=True):
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
                            # Reset batch state
                            st.session_state["batch_call_ids"] = []
                            st.session_state["current_batch"] = 0
                            st.success(f"Aggregation started! Call ID: {call.id}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to start aggregation: {e}")
                else:
                    st.button("🔗 Run Aggregation", disabled=True, use_container_width=True,
                             help="Complete all batches first")
            
            with col4:
                if st.button("❌ Reset Batches", use_container_width=True):
                    st.session_state["batch_call_ids"] = []
                    st.session_state["current_batch"] = 0
                    st.success("Batch state reset")
                    st.rerun()
            
            # Show batch status table with retry option for failed batches
            if batch_call_ids:
                st.markdown("**Batch Status:**")
                for i, call_id in enumerate(batch_call_ids):
                    if call_id:
                        status = _check_function_status(client, call_id)
                        status_str = status.get("status", "Unknown")
                        icon = "✅" if status_str == "Completed" else ("⏳" if status_str == "Running" else "❌")
                        
                        col_status, col_retry = st.columns([4, 1])
                        with col_status:
                            st.write(f"{icon} Batch {i}: {status_str} (Call ID: {call_id})")
                        with col_retry:
                            # Show retry button for failed batches
                            if status_str == "Failed":
                                if st.button(f"🔄 Retry", key=f"retry_batch_{i}", use_container_width=True):
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
                                        # Update the call ID for this batch
                                        batch_call_ids[i] = call.id
                                        st.session_state["batch_call_ids"] = batch_call_ids
                                        st.success(f"Batch {i} restarted! Call ID: {call.id}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to retry batch: {e}")
    
    # ----- CURRENT CONFIG DISPLAY -----
    with st.expander("📋 View Current Configuration JSON", expanded=False):
        config = _get_current_config()
        st.json(config)
