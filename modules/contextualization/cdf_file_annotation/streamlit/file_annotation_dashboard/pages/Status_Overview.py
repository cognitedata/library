import streamlit as st
import pandas as pd
from helper import (
    fetch_annotation_states,
    fetch_extraction_pipeline_config,
)

# --- Page Configuration ---
st.set_page_config(
    page_title="Annotation Status Overview",
    page_icon="📄",
    layout="wide",
)

# --- Data Fetching ---
ep_config, annotation_state_view, file_view = fetch_extraction_pipeline_config()
df_raw = fetch_annotation_states(annotation_state_view, file_view)


# --- Main Application ---
st.title("Annotation Status Overview")
st.markdown(
    "This page provides an audit trail and overview of the file annotation process."
)

if not df_raw.empty:
    # --- Sidebar Filters ---
    st.sidebar.title("Filters")

    # Status Filter
    all_statuses = ["All"] + sorted(df_raw["status"].unique().tolist())
    selected_status = st.sidebar.selectbox("Filter by Status", options=all_statuses)

    # Date Range Filter
    min_date = df_raw["lastUpdatedTime"].min().date()
    max_date = df_raw["lastUpdatedTime"].max().date()
    # THE FIX IS HERE: Changed max_date to max_value
    date_range = st.sidebar.date_input(
        "Filter by Last Updated Date",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Dynamic Scope Property Filters
    primary_scope_property = ep_config["launchFunction"].get("primaryScopeProperty")
    secondary_scope_property = ep_config["launchFunction"].get("secondaryScopeProperty")

    selected_primary_scope = "All"
    if (
        primary_scope_property
        and f"file{primary_scope_property.capitalize()}" in df_raw.columns
    ):
        primary_scope_options = ["All"] + df_raw[
            f"file{primary_scope_property.capitalize()}"
        ].unique().tolist()
        selected_primary_scope = st.sidebar.selectbox(
            f"Filter by {primary_scope_property}", options=primary_scope_options
        )

    selected_secondary_scope = "All"
    if (
        secondary_scope_property
        and f"file{secondary_scope_property.capitalize()}" in df_raw.columns
    ):
        secondary_scope_options = ["All"] + df_raw[
            f"file{secondary_scope_property.capitalize()}"
        ].unique().tolist()
        selected_secondary_scope = st.sidebar.selectbox(
            f"Filter by {secondary_scope_property}", options=secondary_scope_options
        )

    # Apply all filters
    df_filtered = df_raw.copy()
    if selected_status != "All":
        df_filtered = df_filtered[df_filtered["status"] == selected_status]

    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered["lastUpdatedTime"].dt.date >= start_date)
            & (df_filtered["lastUpdatedTime"].dt.date <= end_date)
        ]

    if selected_primary_scope != "All":
        df_filtered = df_filtered[
            df_filtered[f"file{primary_scope_property.capitalize()}"]
            == selected_primary_scope
        ]

    if selected_secondary_scope != "All":
        df_filtered = df_filtered[
            df_filtered[f"file{secondary_scope_property.capitalize()}"]
            == selected_secondary_scope
        ]

    # --- Dashboard Metrics ---
    st.subheader("Status Overview")

    status_counts = df_filtered["status"].value_counts()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Files", len(df_filtered))
    with col2:
        st.metric("Annotated", status_counts.get("Annotated", 0))
    with col3:
        st.metric("New", status_counts.get("New", 0))
        st.metric("Processing", status_counts.get("Processing", 0))
    with col4:
        st.metric("Finalizing", status_counts.get("Finalizing", 0))
        st.metric("Failed", status_counts.get("Failed", 0))

    # --- Detailed Data View ---
    default_columns = [
        "fileName",
        "status",
        "jobId",
        "annotationMessage",
        "filePageCount",
        "retries",
        "fileTags",
        "lastUpdatedTime",
    ]

    available_columns = df_filtered.columns.tolist()
    default_selection = [col for col in default_columns if col in available_columns]

    with st.popover("Customize Columns"):
        selected_columns = st.multiselect(
            "Select columns to display:",
            options=available_columns,
            default=default_selection,
            label_visibility="collapsed",
        )

    if selected_columns:
        st.dataframe(df_filtered[selected_columns], use_container_width=True)
    else:
        st.warning("Please select at least one column to display.")

else:
    st.info(
        "No annotation state data returned from Cognite Data Fusion. Please check your settings and data model."
    )
