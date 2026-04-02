"""
CDF Project Health Dashboard

Displays pre-computed health metrics from the Project Health Cognite Function.
The function writes all metrics to a single Cognite file (overwrites if it exists).
The dashboard loads that file and filters the view by your config: selected dataset
(and time range is shown from the file). Run the function from the Configuration tab, then Refresh data.
"""

import json
import streamlit as st
from datetime import datetime

try:
    import pyodide_http  # type: ignore[import-untyped]
    pyodide_http.patch_all()
except ImportError:
    pass

from cognite.client import CogniteClient

from src.tabs import (
    render_overview_tab,
    render_extraction_pipelines_tab,
    render_workflows_tab,
    render_transformations_tab,
    render_functions_tab,
)
from src.configuration import render_configuration_tab
from src.utils import _base_url_to_origin

# ----------------------------------------------------
# PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(
    page_title="CDF Project Health Dashboard",
    page_icon="🏥",
    layout="wide",
)

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
METRICS_FILE_EXTERNAL_ID = "project_health_metrics"

# ----------------------------------------------------
# CDF CLIENT
# ----------------------------------------------------
try:
    client = CogniteClient()
except Exception as e:
    st.error(f"Failed to initialize CDF client: {e}")
    st.info("Run this app in a CDF Streamlit environment.")
    st.stop()

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:project_health"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(cdf_client) -> None:
    if st.session_state.get("_usage_tracked"):
        return
    try:
        import re
        from mixpanel import Consumer, Mixpanel
        mp = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        cluster = getattr(cdf_client.config, "cdf_cluster", None)
        if not cluster:
            m = re.match(r"https://([^.]+)\.cognitedata\.com", getattr(cdf_client.config, "base_url", "") or "")
            cluster = m.group(1) if m else "unknown"
        distinct_id = f"{cdf_client.config.project}:{cluster}"
        mp.track(distinct_id, "streamlit-session", {
            "source": _SOURCE,
            "tracker_version": _TRACKER_VERSION,
            "dp_version": _DP_VERSION,
            "type": "streamlit",
            "cdf_cluster": cluster,
            "cdf_project": cdf_client.config.project,
        })
        st.session_state["_usage_tracked"] = True
    except Exception:
        pass


_report_usage(client)

# ----------------------------------------------------
# LOAD METRICS FROM COGNITE FILE
# ----------------------------------------------------
@st.cache_data(ttl=300)
def load_metrics_from_file(external_id: str) -> dict:
    """Load pre-computed project health metrics from Cognite Files."""
    try:
        file_bytes = client.files.download_bytes(external_id=external_id)
        return json.loads(file_bytes.decode("utf-8"))
    except Exception as e:
        return {"_error": str(e)}


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------
DEFAULT_PROJECT_URL = "https://cog-velocity.fusion.cognite.com/"


def _aggregate_multi_dataset_payload(datasets_dict: dict) -> dict:
    """Merge all datasets' payloads into one so 'All datasets' shows combined data."""
    if not datasets_dict:
        return {}
    all_pipelines = []
    all_workflows = []
    all_transformations = []
    all_functions = []
    all_errors = []
    ep_sum = {"total": 0, "healthy": 0, "failed": 0, "no_runs": 0}
    wf_sum = {"total": 0, "healthy": 0, "failed": 0, "no_runs": 0, "running": 0}
    tr_sum = {"total": 0, "healthy": 0, "failed": 0, "no_runs": 0, "running": 0}
    fn_sum = {"total": 0, "healthy": 0, "failed": 0, "no_calls": 0}
    uptime_thresholds = {}
    time_range_label = None
    config = {}
    for ds_id, p in datasets_dict.items():
        if p.get("error"):
            continue
        uptime_thresholds = p.get("uptime_thresholds") or uptime_thresholds
        time_range_label = p.get("time_range_label") or time_range_label
        config = p.get("config") or config
        ed = p.get("extraction_data") or {}
        for pipe in (ed.get("pipelines") or []):
            all_pipelines.append({**pipe, "dataset_external_id": ds_id})
        for k, v in (ed.get("summary") or {}).items():
            ep_sum[k] = ep_sum.get(k, 0) + (v or 0)
        wd = p.get("workflow_data") or {}
        for w in (wd.get("workflows") or []):
            all_workflows.append({**w, "dataset_external_id": ds_id})
        for k, v in (wd.get("summary") or {}).items():
            wf_sum[k] = wf_sum.get(k, 0) + (v or 0)
        td = p.get("transformation_data") or {}
        for t in (td.get("transformations") or []):
            all_transformations.append({**t, "dataset_external_id": ds_id})
        for k, v in (td.get("summary") or {}).items():
            tr_sum[k] = tr_sum.get(k, 0) + (v or 0)
        fd = p.get("function_data") or {}
        for f in (fd.get("functions") or []):
            all_functions.append({**f, "dataset_external_id": ds_id})
        for k, v in (fd.get("summary") or {}).items():
            fn_sum[k] = fn_sum.get(k, 0) + (v or 0)
        all_errors.extend(p.get("all_errors") or [])
    return {
        "dataset_external_id": "All datasets",
        "time_range_label": time_range_label or "N/A",
        "config": config,
        "uptime_thresholds": uptime_thresholds,
        "extraction_data": {"pipelines": all_pipelines, "summary": ep_sum, "errors": []},
        "workflow_data": {"workflows": all_workflows, "summary": wf_sum, "errors": []},
        "transformation_data": {"transformations": all_transformations, "summary": tr_sum, "errors": []},
        "function_data": {"functions": all_functions, "summary": fn_sum, "errors": []},
        "all_errors": all_errors,
    }


def main():
    st.title("🏥 CDF Project Health Dashboard")

    metrics = load_metrics_from_file(METRICS_FILE_EXTERNAL_ID)
    if metrics.get("_error"):
        err = metrics["_error"]
        if "Files ids not found" in err or "ids not found" in err:
            st.info(
                "**No metrics file yet.** Run the Project Health function at least once (from the **Configuration** tab) "
                "to generate the metrics file, then click **Refresh data**."
            )
        else:
            st.error(f"Could not load metrics file: {err}")
            st.info("Run the Project Health function from the **Configuration** tab to create the file, then click **Refresh data**.")
    is_multi = "datasets" in metrics and isinstance(metrics.get("datasets"), dict)
    has_metrics = not metrics.get("_error") and (
        is_multi or not (metrics.get("error") and not metrics.get("dataset_id"))
    )

    # Resolve payload: single-dataset file vs multi-dataset file
    if has_metrics and is_multi:
        metadata = metrics.get("metadata") or {}
        config = metadata.get("config") or {}
        datasets_dict = metrics.get("datasets") or {}
        dataset_ids = sorted(datasets_dict.keys())
    elif has_metrics:
        metadata = metrics.get("metadata") or {}
        config = metrics.get("config") or {}
        datasets_dict = None
        dataset_ids = []
    else:
        metadata = {}
        config = {}
        datasets_dict = None
        dataset_ids = []

    # Sidebar: only Project URL (organization base); project and cluster from metrics file (set by function from client config)
    with st.sidebar:
        st.header("⚙️ Configuration")
        project_url = st.text_input(
            "Project URL",
            value=config.get("cdf_base_url") or DEFAULT_PROJECT_URL,
            key="project_url",
            help="CDF Fusion base URL (e.g. https://cog-velocity.fusion.cognite.com/). Project and cluster come from the metrics file (set when the function runs).",
        )
        st.caption("Format: **https://**organization**.fusion.cognite.com**")
        config = {**config, "cdf_base_url": _base_url_to_origin(project_url or DEFAULT_PROJECT_URL) or DEFAULT_PROJECT_URL.rstrip("/")}

        if has_metrics:
            if is_multi and datasets_dict and dataset_ids:
                st.caption("Filter by dataset (file contains all datasets; view is filtered by selection):")
                options = ["All datasets"] + dataset_ids
                idx = st.selectbox(
                    "Dataset",
                    range(len(options)),
                    format_func=lambda i: options[i],
                    key="sidebar_dataset",
                    help="Select which dataset to view. File holds all data; dashboard filters by this selection.",
                )
                selected_label = options[idx]
                if selected_label == "All datasets":
                    payload = _aggregate_multi_dataset_payload(datasets_dict)
                    dataset_external_id = "All datasets"
                    dataset_display = f"All datasets ({len(datasets_dict)} combined)"
                else:
                    dataset_external_id = selected_label
                    dataset_display = dataset_external_id
                    payload = datasets_dict.get(dataset_external_id) or {}
            elif is_multi:
                dataset_external_id = "—"
                dataset_display = "All datasets (no data)"
                payload = {}
            else:
                dataset_external_id = metrics.get("dataset_external_id", "Unknown")
                dataset_display = dataset_external_id
                payload = metrics
            time_range_label = payload.get("time_range_label") or metadata.get("time_range_label") or "N/A"
            st.info(f"**Dataset:** {dataset_display}")
            st.info(f"**Time range:** {time_range_label}")
            if metadata.get("computed_at"):
                st.caption(f"Computed: {metadata['computed_at']}")
            if is_multi and (metadata.get("dataset_count") or (len(datasets_dict) if datasets_dict else 0)):
                st.caption(f"Loaded {metadata.get('dataset_count') or len(datasets_dict)} dataset(s) from file.")
        else:
            dataset_external_id = "—"
            time_range_label = "—"
            payload = {}
            if metrics.get("_error"):
                st.warning("Load failed. Run the function in **Configuration**, then **Refresh data**.")
            else:
                st.warning("No metrics yet. Use the **Configuration** tab to run the function, then **Refresh data**.")
        st.markdown("---")
        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.markdown(f"**Project:** {config.get('cdf_project', '—')}")

    if has_metrics and payload:
        uptime_thresholds = payload.get("uptime_thresholds") or metadata.get("uptime_thresholds") or {}
        extraction_data = payload.get("extraction_data") or {"pipelines": [], "summary": {}, "errors": []}
        workflow_data = payload.get("workflow_data") or {"workflows": [], "summary": {}, "errors": []}
        transformation_data = payload.get("transformation_data") or {"transformations": [], "summary": {}, "errors": []}
        function_data = payload.get("function_data") or {"functions": [], "summary": {}, "errors": []}
        all_errors = payload.get("all_errors") or []
    else:
        dataset_external_id = dataset_external_id if has_metrics else "—"
        time_range_label = time_range_label if has_metrics else "—"
        uptime_thresholds = {}
        extraction_data = {"pipelines": [], "summary": {}, "errors": []}
        workflow_data = {"workflows": [], "summary": {}, "errors": []}
        transformation_data = {"transformations": [], "summary": {}, "errors": []}
        function_data = {"functions": [], "summary": {}, "errors": []}
        all_errors = []

    # Tabs: Configuration first, then dashboard tabs
    tab_config, tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "⚙️ Configuration",
        "📊 Overview",
        "📥 Extraction Pipelines",
        "🔄 Workflows",
        "⚡ Transformations",
        "🔧 Functions",
    ])

    with tab_config:
        render_configuration_tab(client)

    if not has_metrics or not payload:
        with tab1:
            st.info("Run the **Project Health** function from the **Configuration** tab, then refresh to see metrics here.")
        with tab2:
            st.info("Run the **Project Health** function from the **Configuration** tab, then refresh to see metrics here.")
        with tab3:
            st.info("Run the **Project Health** function from the **Configuration** tab, then refresh to see metrics here.")
        with tab4:
            st.info("Run the **Project Health** function from the **Configuration** tab, then refresh to see metrics here.")
        with tab5:
            st.info("Run the **Project Health** function from the **Configuration** tab, then refresh to see metrics here.")
        return

    ep_summary = extraction_data.get("summary", {})
    wf_summary = workflow_data.get("summary", {})
    tr_summary = transformation_data.get("summary", {})
    fn_summary = function_data.get("summary", {})

    with tab1:
        render_overview_tab(
            ep_summary, wf_summary, tr_summary, fn_summary,
            dataset_external_id, time_range_label, config, all_errors,
        )
    with tab2:
        render_extraction_pipelines_tab(
            extraction_data, dataset_external_id, time_range_label,
            config, uptime_thresholds.get("extraction_pipelines", 75),
        )
    with tab3:
        render_workflows_tab(
            workflow_data, dataset_external_id, time_range_label,
            config, uptime_thresholds.get("workflows", 75),
        )
    with tab4:
        render_transformations_tab(
            transformation_data, dataset_external_id, time_range_label,
            config, uptime_thresholds.get("transformations", 75),
        )
    with tab5:
        render_functions_tab(
            function_data, dataset_external_id, time_range_label,
            config, uptime_thresholds.get("functions", 75),
        )


if __name__ == "__main__":
    main()
