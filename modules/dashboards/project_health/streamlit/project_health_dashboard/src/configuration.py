"""
Configuration tab: run the Project Health Cognite Function.

Uses async trigger (wait=False): after clicking Run, shows "Function is running..."
and a "Check Status" button until the call completes, then shows completed or failed.
"""

import streamlit as st
from datetime import datetime, timezone, timedelta, date

PROJECT_HEALTH_FUNCTION_EXTERNAL_ID = "project_health_handler"
TIME_RANGE_OPTIONS = ["12 Hours", "1 Day", "7 Days", "30 Days", "Custom"]
DEFAULT_TIME_RANGE_INDEX = 2  # 7 Days
DEFAULT_UPTIME = 75


def _init_function_session_state():
    """Initialize session state for function run tracking."""
    if "function_call_id" not in st.session_state:
        st.session_state["function_call_id"] = None
    if "function_status" not in st.session_state:
        st.session_state["function_status"] = None


def _check_function_status(client, call_id: int) -> dict:
    """Check the status of a single function call."""
    try:
        call = client.functions.calls.retrieve(
            call_id=call_id,
            function_external_id=PROJECT_HEALTH_FUNCTION_EXTERNAL_ID,
        )
        return {
            "status": call.status,
            "started_at": call.start_time,
            "ended_at": call.end_time,
            "error": getattr(call, "error", None),
        }
    except Exception as e:
        return {"status": "Error", "error": str(e)}


@st.cache_data(ttl=300)
def get_available_datasets(_client):
    """Fetch datasets for dropdown."""
    try:
        datasets = list(_client.data_sets.list(limit=500))
        return [
            {"id": ds.id, "external_id": ds.external_id or f"id:{ds.id}", "name": ds.name or (ds.external_id or f"id:{ds.id}")}
            for ds in datasets
        ]
    except Exception:
        return []


def _custom_range_to_ms(custom_start, custom_end):
    if not custom_start or not custom_end:
        return None, None
    start_dt = datetime.combine(custom_start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(custom_end, datetime.max.time()).replace(tzinfo=timezone.utc)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def _format_ts(ts):
    if ts is None:
        return "-"
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(ts)


def _get_function_run_status(_client):
    """Fetch last few function calls for project_health_handler."""
    try:
        fn = _client.functions.retrieve(external_id=PROJECT_HEALTH_FUNCTION_EXTERNAL_ID)
        if fn is None:
            return None, "Function not found"
        calls = list(_client.functions.calls.list(function_external_id=PROJECT_HEALTH_FUNCTION_EXTERNAL_ID, limit=5))
        return calls, None
    except Exception as e:
        return None, str(e)


def render_configuration_tab(client):
    """Render the Configuration tab: dataset, time range, thresholds, Run + Check status."""
    _init_function_session_state()

    st.subheader("Run Project Health Function")
    st.markdown("""
    Run the **Project Health** Cognite Function to compute metrics. The function writes results to a single Cognite File that the dashboard reads.
    - **Single dataset:** one run, one dataset in the file.
    - **All datasets:** one run, all datasets stored in one file (like context_quality); use the sidebar to pick which dataset to view.
    """)

    datasets = get_available_datasets(client)
    dataset_options = ["All datasets"] + [f"{d['name']} ({d['external_id']})" for d in datasets]
    dataset_external_ids = [None] + [d["external_id"] for d in datasets]  # None = All datasets

    # Dataset selection
    selected_index = st.selectbox(
        "Dataset",
        range(len(dataset_options)),
        format_func=lambda i: dataset_options[i],
        index=0,
        key="config_dataset",
        help="Single dataset = one run for that dataset. All datasets = one run for all, stored in one file.",
    )
    selected_dataset_external_id = dataset_external_ids[selected_index]  # None = All datasets

    # Time range
    time_index = st.selectbox(
        "Time range",
        range(len(TIME_RANGE_OPTIONS)),
        format_func=lambda i: TIME_RANGE_OPTIONS[i],
        index=DEFAULT_TIME_RANGE_INDEX,
        key="config_time_range",
    )
    time_range = TIME_RANGE_OPTIONS[time_index]
    custom_start_ms, custom_end_ms = None, None
    if time_range == "Custom":
        col_start, col_end = st.columns(2)
        with col_start:
            custom_start = st.date_input("Start date", value=date.today() - timedelta(days=7), max_value=date.today(), key="config_custom_start")
        with col_end:
            custom_end = st.date_input("End date", value=date.today(), max_value=date.today(), key="config_custom_end")
        if custom_start and custom_end:
            custom_start_ms, custom_end_ms = _custom_range_to_ms(custom_start, custom_end)

    # Uptime thresholds (optional)
    with st.expander("Uptime thresholds (%)", expanded=False):
        ep_th = st.number_input("Extraction Pipelines", min_value=0, max_value=100, value=DEFAULT_UPTIME, key="config_ep_th")
        wf_th = st.number_input("Workflows", min_value=0, max_value=100, value=DEFAULT_UPTIME, key="config_wf_th")
        tr_th = st.number_input("Transformations", min_value=0, max_value=100, value=DEFAULT_UPTIME, key="config_tr_th")
        fn_th = st.number_input("Functions", min_value=0, max_value=100, value=DEFAULT_UPTIME, key="config_fn_th")
    uptime_thresholds = {
        "extraction_pipelines": ep_th,
        "workflows": wf_th,
        "transformations": tr_th,
        "functions": fn_th,
    }

    call_id = st.session_state.get("function_call_id")

    # --- Active run: show status and "Check Status" until completed/failed ---
    if call_id:
        status_info = _check_function_status(client, call_id)
        status = status_info.get("status", "Unknown")

        if status == "Running":
            st.info("""
**Function is running...**

- **Call ID:** `{call_id}`
- **Status:** {status}

This typically takes 1-5 minutes depending on data volume. Click **Check Status** to refresh.
            """.format(call_id=call_id, status=status))
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Check Status", use_container_width=True, key="config_check_running"):
                    st.rerun()
            with col2:
                if st.button("Cancel Tracking", use_container_width=True, key="config_cancel_tracking"):
                    st.session_state["function_call_id"] = None
                    st.session_state["function_status"] = None
                    st.rerun()
            return

        if status == "Completed":
            st.success("""
**Function completed successfully!**

- **Call ID:** `{call_id}`

Click **Refresh data** in the sidebar or any dashboard tab to view your metrics.
            """.format(call_id=call_id))
            if st.button("Run Again", use_container_width=True, key="config_run_again"):
                st.session_state["function_call_id"] = None
                st.session_state["function_status"] = None
                st.rerun()
            return

        if status == "Failed":
            error_msg = status_info.get("error", "Unknown error")
            st.error("""
**Function failed**

- **Call ID:** `{call_id}`
- **Error:** {error_msg}
            """.format(call_id=call_id, error_msg=error_msg))
            if st.button("Try Again", use_container_width=True, key="config_try_again"):
                st.session_state["function_call_id"] = None
                st.session_state["function_status"] = None
                st.rerun()
            return

        # Other status (e.g. Timeout, Error from API)
        st.warning("**Function Status:** {status} (Call ID: `{call_id}`)".format(status=status, call_id=call_id))
        if st.button("Check Status", use_container_width=True, key="config_check_other"):
            st.rerun()
        return

    # --- No active run: show Run and Check status (last runs) ---
    col_run, col_status = st.columns([1, 1])
    with col_run:
        run_clicked = st.button("Run Project Health Function", type="primary", key="config_run_btn")
    with col_status:
        check_clicked = st.button("Check status", key="config_check_status_btn", help="Show last function run status")

    if check_clicked:
        calls, err = _get_function_run_status(client)
        if err:
            st.error("Could not load status: {0}".format(err))
        elif not calls:
            st.info("No function calls found.")
        else:
            st.subheader("Last function runs")
            for i, c in enumerate(calls):
                status = getattr(c, "status", None) or "-"
                start_ts = getattr(c, "start_time", None)
                end_ts = getattr(c, "end_time", None)
                with st.expander("Run {0}: **{1}** - started {2}".format(i + 1, status, _format_ts(start_ts)), expanded=(i == 0)):
                    st.write("**Status:** {0}".format(status))
                    st.write("**Started:** {0}".format(_format_ts(start_ts)))
                    st.write("**Ended:** {0}".format(_format_ts(end_ts)))
                    if getattr(c, "error", None):
                        st.error(c.error)

    if run_clicked:
        if selected_dataset_external_id is None:
            data = {
                "all_datasets": True,
                "time_range": time_range,
                "uptime_thresholds": uptime_thresholds,
            }
            if time_range == "Custom" and custom_start_ms is not None and custom_end_ms is not None:
                data["custom_start_ms"] = custom_start_ms
                data["custom_end_ms"] = custom_end_ms
        else:
            data = {
                "dataset_external_id": selected_dataset_external_id,
                "time_range": time_range,
                "uptime_thresholds": uptime_thresholds,
            }
            if time_range == "Custom" and custom_start_ms is not None and custom_end_ms is not None:
                data["custom_start_ms"] = custom_start_ms
                data["custom_end_ms"] = custom_end_ms

        with st.spinner("Starting function..."):
            try:
                call = client.functions.call(
                    external_id=PROJECT_HEALTH_FUNCTION_EXTERNAL_ID,
                    data=data,
                    wait=False,
                )
                st.session_state["function_call_id"] = call.id
                st.session_state["function_status"] = "Running"
                st.rerun()
            except Exception as e:
                error_str = str(e).lower()
                if "not found" in error_str or "does not exist" in error_str:
                    st.error("""
**Function not available yet**

The function `{0}` was not found.
Functions may take a few minutes to deploy after `cdf deploy`.
                    """.format(PROJECT_HEALTH_FUNCTION_EXTERNAL_ID))
                else:
                    st.error("**Failed to trigger function:** {0}".format(str(e)))
