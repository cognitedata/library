import streamlit as st
import pandas as pd
import altair as alt
from helper import (
    fetch_extraction_pipeline_config,
    fetch_raw_table_data,
    find_pipelines,
    generate_file_canvas,
    fetch_pattern_catalog,
    fetch_manual_patterns,
    fetch_annotation_states,
)
from cognite.client.data_classes.data_modeling import NodeId

# --- Page Configuration ---
st.set_page_config(
    page_title="Annotation Quality",
    page_icon="🎯",
    layout="wide",
)


# --- Callback function to reset selection ---
def reset_selection():
    st.session_state.selected_row_index = None


# --- Initialize Session State ---
if "selected_row_index" not in st.session_state:
    st.session_state.selected_row_index = None

# --- Sidebar for Pipeline Selection ---
st.sidebar.title("Pipeline Selection")
pipeline_ids = find_pipelines()

if not pipeline_ids:
    st.info("No active file annotation pipelines found to monitor.")
    st.stop()

selected_pipeline = st.sidebar.selectbox(
    "Select a pipeline to view quality:", options=pipeline_ids, key="quality_pipeline_selector"
)

# --- Data Fetching & Processing ---
config_result = fetch_extraction_pipeline_config(selected_pipeline)
if not config_result:
    st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
    st.stop()

ep_config, annotation_state_view, file_view = config_result
report_config = ep_config.get("finalizeFunction", {}).get("reportService", {})
cache_config = ep_config.get("launchFunction", {}).get("cacheService", {})
db_name = report_config.get("rawDb")
pattern_table = report_config.get("rawTableDocPattern")
tag_table = report_config.get("rawTableDocTag")
doc_table = report_config.get("rawTableDocDoc")
cache_table = cache_config.get("rawTableCache")
manual_patterns_table = cache_config.get("rawManualPatternsCatalog")


if not all([db_name, pattern_table, tag_table, doc_table, cache_table, manual_patterns_table]):
    st.error("Could not find all required RAW table names in the pipeline configuration.")
    st.stop()

df_patterns = fetch_raw_table_data(db_name, pattern_table)
df_tags = fetch_raw_table_data(db_name, tag_table)
df_docs = fetch_raw_table_data(db_name, doc_table)

# Fetch both auto-generated and manual patterns
df_auto_patterns = fetch_pattern_catalog(db_name, cache_table)
df_manual_patterns = fetch_manual_patterns(db_name, manual_patterns_table)

# --- Main Application ---
st.title("Annotation Quality Dashboard")
st.markdown(
    "This page measures annotation quality by comparing potential tags (from pattern mode) against actual, created annotations."
)

if df_patterns.empty:
    st.info("The pattern catalog is empty. Run the pipeline with patternMode enabled to generate data.")
    st.stop()

# --- Data Processing and Merging ---
df_annotations = pd.concat([df_tags, df_docs], ignore_index=True)

df_patterns_agg = df_patterns.groupby("startNode")["text"].apply(set).reset_index(name="potentialTags")

if not df_annotations.empty:
    df_annotations_agg = (
        df_annotations.groupby("startNode")["startNodeText"].apply(set).reset_index(name="actualAnnotations")
    )
    df_quality = pd.merge(df_patterns_agg, df_annotations_agg, on="startNode", how="outer").fillna(0)
    df_quality["actualAnnotations"] = df_quality["actualAnnotations"].apply(
        lambda x: x if isinstance(x, set) else set()
    )
else:
    df_quality = df_patterns_agg
    df_quality["actualAnnotations"] = [set() for _ in range(len(df_patterns_agg))]

df_quality["potentialTags"] = df_quality["potentialTags"].apply(lambda x: x if isinstance(x, set) else set())

df_quality["matchedTags"] = df_quality.apply(
    lambda row: len(row["potentialTags"].intersection(row["actualAnnotations"])), axis=1
)
df_quality["unmatchedByAnnotation"] = df_quality.apply(
    lambda row: len(row["potentialTags"] - row["actualAnnotations"]), axis=1
)
df_quality["missedByPattern"] = df_quality.apply(
    lambda row: len(row["actualAnnotations"] - row["potentialTags"]), axis=1
)

df_quality["coverageRate"] = (
    df_quality["matchedTags"] / (df_quality["matchedTags"] + df_quality["unmatchedByAnnotation"])
) * 100
df_quality["completenessRate"] = (
    df_quality["matchedTags"] / (df_quality["matchedTags"] + df_quality["missedByPattern"])
) * 100
df_quality.fillna(0, inplace=True)

# --- Dashboard Metrics ---
st.header("Overall Annotation Quality")

# Get a unique, sorted list of resource types for the filter
all_resource_types = ["All"] + sorted(df_patterns["resourceType"].unique().tolist())

selected_resource_type = st.selectbox("Filter by Resource Type:", options=all_resource_types, on_change=reset_selection)

# --- Filter the data based on selection ---
if selected_resource_type == "All":
    df_metrics_input = df_patterns
    df_annotations_input = df_annotations
else:
    df_metrics_input = df_patterns[df_patterns["resourceType"] == selected_resource_type]
    if not df_annotations.empty and "endNodeResourceType" in df_annotations.columns:
        df_annotations_input = df_annotations[df_annotations["endNodeResourceType"] == selected_resource_type]
    else:
        df_annotations_input = pd.DataFrame()


# --- Recalculate metrics based on the filtered data ---
potential_tags_set = set(df_metrics_input["text"])
if not df_annotations_input.empty and "startNodeText" in df_annotations_input.columns:
    actual_annotations_set = set(df_annotations_input["startNodeText"])
else:
    actual_annotations_set = set()


matched_tags_set = potential_tags_set.intersection(actual_annotations_set)
unmatched_by_annotation_set = potential_tags_set - actual_annotations_set
missed_by_pattern_set = actual_annotations_set - potential_tags_set

total_matched = len(matched_tags_set)
total_unmatched = len(unmatched_by_annotation_set)
total_missed = len(missed_by_pattern_set)

# Calculate overall rates
overall_coverage = (
    (total_matched / (total_matched + total_unmatched)) * 100 if (total_matched + total_unmatched) > 0 else 0
)
overall_completeness = (
    (total_matched / (total_missed + total_matched)) * 100 if (total_missed + total_matched) > 0 else 0
)

# Display KPIs
kpi_col1, kpi_col2 = st.columns(2)
kpi_col1.metric(
    "Overall Annotation Coverage",
    f"{overall_coverage:.2f}%",
    help="Of all potential tags found by patterns, this is the percentage that were successfully annotated. Formula: Matched / (Matched + Unmatched)",
)
kpi_col2.metric(
    "Overall Pattern Completeness",
    f"{overall_completeness:.2f}%",
    help="Of all annotations created, this is the percentage that the patterns successfully predicted. Formula: Matched / (Matched + Missed by Pattern)",
)

st.divider()

# --- Annotation Quality by Resource Type ---
st.subheader("Analysis by Resource Type")

# --- Prepare data for charts ---
chart_data = []
# Use all_resource_types[1:] to skip the "All" option
for resource_type in all_resource_types[1:]:
    # Filter data for this specific resource type
    df_patterns_filtered = df_patterns[df_patterns["resourceType"] == resource_type]

    if not df_annotations.empty and "endNodeResourceType" in df_annotations.columns:
        df_annotations_filtered = df_annotations[df_annotations["endNodeResourceType"] == resource_type]
    else:
        df_annotations_filtered = pd.DataFrame()

    # Calculate metrics using global unique tags for THIS resource type
    potential = set(df_patterns_filtered["text"])

    if not df_annotations_filtered.empty and "startNodeText" in df_annotations_filtered.columns:
        actual = set(df_annotations_filtered["startNodeText"])
    else:
        actual = set()

    matched = len(potential.intersection(actual))
    unmatched = len(potential - actual)
    missed = len(actual - potential)

    coverage = (matched / (matched + unmatched)) * 100 if (matched + unmatched) > 0 else 0
    completeness = (matched / (matched + missed)) * 100 if (matched + missed) > 0 else 0

    chart_data.append(
        {
            "resourceType": resource_type,
            "coverageRate": coverage,
            "completenessRate": completeness,
            "matchedTags": matched,
            "unmatchedByAnnotation": unmatched,
            "missedByPattern": missed,
        }
    )

df_chart_data = pd.DataFrame(chart_data)

# --- Filter chart data based on dropdown selection ---
if selected_resource_type != "All":
    df_chart_display = df_chart_data[df_chart_data["resourceType"] == selected_resource_type]
else:
    df_chart_display = df_chart_data

# --- Render Charts ---
if not df_chart_display.empty:
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        coverage_chart = (
            alt.Chart(df_chart_display)
            .mark_bar()
            .encode(
                x=alt.X("resourceType:N", title="Resource Type", sort="-y"),
                y=alt.Y("coverageRate:Q", title="Annotation Coverage (%)", scale=alt.Scale(domain=[0, 100])),
                tooltip=["resourceType", "coverageRate", "matchedTags", "unmatchedByAnnotation"],
            )
            .properties(title="Annotation Coverage by Resource Type")
        )
        st.altair_chart(coverage_chart, use_container_width=True)
    with chart_col2:
        completeness_chart = (
            alt.Chart(df_chart_display)
            .mark_bar()
            .encode(
                x=alt.X("resourceType:N", title="Resource Type", sort="-y"),
                y=alt.Y("completenessRate:Q", title="Pattern Completeness (%)", scale=alt.Scale(domain=[0, 100])),
                tooltip=["resourceType", "completenessRate", "matchedTags", "missedByPattern"],
            )
            .properties(title="Pattern Completeness by Resource Type")
        )
        st.altair_chart(completeness_chart, use_container_width=True)
else:
    st.info("No data available for the selected resource type to generate charts.")

# --- Display Matched, Unmatched and Missed Tags ---
st.subheader("Tag Details")
tag_col1, tag_col2, tag_col3 = st.columns(3)

with tag_col1:
    st.metric("✅ Matched Tags", f"{total_matched}")
    st.dataframe(
        pd.DataFrame(sorted(list(matched_tags_set)), columns=["Tag"]), use_container_width=True, hide_index=True
    )

with tag_col2:
    st.metric("❓ Unmatched by Annotation", f"{total_unmatched}")
    st.dataframe(
        pd.DataFrame(sorted(list(unmatched_by_annotation_set)), columns=["Tag"]),
        use_container_width=True,
        hide_index=True,
    )

with tag_col3:
    st.metric("❗️ Missed by Pattern", f"{total_missed}")
    st.dataframe(
        pd.DataFrame(sorted(list(missed_by_pattern_set)), columns=["Tag"]), use_container_width=True, hide_index=True
    )

# --- Pattern Catalog Expander with Tabs ---
with st.expander("View Full Pattern Catalog"):
    # Standardize column names for merging
    df_auto_patterns.rename(columns={"resourceType": "resource_type", "pattern": "sample"}, inplace=True)

    # Select and combine relevant columns
    df_combined_patterns = (
        pd.concat([df_auto_patterns[["resource_type", "sample"]], df_manual_patterns[["resource_type", "sample"]]])
        .drop_duplicates()
        .sort_values(by=["resource_type", "sample"])
    )

    if df_combined_patterns.empty:
        st.info("Pattern catalog is empty or could not be loaded.")
    else:
        resource_types = sorted(df_combined_patterns["resource_type"].unique())
        tabs = st.tabs(resource_types)

        for i, resource_type in enumerate(resource_types):
            with tabs[i]:
                df_filtered_patterns = df_combined_patterns[df_combined_patterns["resource_type"] == resource_type]
                st.dataframe(
                    df_filtered_patterns[["sample"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={"sample": "Pattern"},
                )

# --- File-Level Table ---
st.header("Per-File Annotation Quality")

# --- Fetch file metadata and merge it with the quality data ---
df_file_meta = fetch_annotation_states(annotation_state_view, file_view)
if not df_file_meta.empty:
    df_display_unfiltered = pd.merge(
        df_quality, df_file_meta, left_on="startNode", right_on="fileExternalId", how="left"
    )
else:
    df_display_unfiltered = df_quality

# --- Advanced Filtering Section ---
with st.expander("Filter Per-File Quality Table"):

    # --- DYNAMIC METADATA FILTER ---
    # Define columns that should not be offered as metadata filters
    excluded_columns = [
        "Select",
        "startNode",
        "potentialTags",
        "actualAnnotations",
        "matchedTags",
        "unmatchedByAnnotation",
        "missedByPattern",
        "coverageRate",
        "completenessRate",
        "fileExternalId",
        "externalId",
        "space",
        "annotatedPageCount",
        "annotationMessage",
        "fileAliases",
        "fileAssets",
        "fileIsuploaded",
        "jobId",
        "linkedFile",
        "pageCount",
        "patternModeJobId",
        "sourceCreatedUser",
        "sourceCreatedTime",
        "sourceUpdatedTime",
        "sourceUpdatedUser",
        "fileSourceupdateduser",
        "fileSourcecreatedUser",
        "fileSourceId",
        "createdTime",
        "fileSourcecreateduser",
        "patternModeMessage",
        "fileSourceupdatedtime",
        "fileSourcecreatedtime",
        "fileUploadedtime",
    ]

    # Get the list of available metadata columns for filtering
    filterable_columns = sorted([col for col in df_display_unfiltered.columns if col not in excluded_columns])

    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:
        selected_column = st.selectbox(
            "Filter by Metadata Property", options=["None"] + filterable_columns, on_change=reset_selection
        )

    selected_values = []
    if selected_column != "None":
        unique_values = sorted(df_display_unfiltered[selected_column].dropna().unique().tolist())
        with filter_col2:
            selected_values = st.multiselect(
                f"Select Value(s) for {selected_column}", options=unique_values, on_change=reset_selection
            )

    coverage_range = st.slider("Filter by Annotation Coverage (%)", 0, 100, (0, 100), on_change=reset_selection)
    completeness_range = st.slider("Filter by Pattern Completeness (%)", 0, 100, (0, 100), on_change=reset_selection)

df_display = df_display_unfiltered.copy()
# Apply filters
if selected_column != "None" and selected_values:
    df_display = df_display[df_display[selected_column].isin(selected_values)]

df_display = df_display[
    (df_display["coverageRate"] >= coverage_range[0]) & (df_display["coverageRate"] <= coverage_range[1])
]
df_display = df_display[
    (df_display["completenessRate"] >= completeness_range[0])
    & (df_display["completenessRate"] <= completeness_range[1])
]

# --- Reset the index after all filtering is complete ---
df_display = df_display.reset_index(drop=True)

df_display.insert(0, "Select", False)

# --- Column configuration for the data editor ---
default_columns = [
    "Select",
    "fileName",
    "fileSourceid",
    "fileMimetype",
    "coverageRate",
    "completenessRate",
    "annotationMessage",
    "patternModeMessage",
    "lastUpdatedTime",
]
all_columns = df_display.columns.tolist()

with st.popover("Customize Table Columns"):
    selected_columns = st.multiselect(
        "Select columns to display:",
        options=all_columns,
        default=[col for col in default_columns if col in all_columns],  # Ensure default is valid
    )

if not selected_columns:
    st.warning("Please select at least one column to display.")
    st.stop()


if st.session_state.get("selected_row_index") is not None:
    if st.session_state.selected_row_index < len(df_display):
        df_display.at[st.session_state.selected_row_index, "Select"] = True

edited_df = st.data_editor(
    df_display[selected_columns],
    key="quality_table_editor",
    column_config={
        "Select": st.column_config.CheckboxColumn(required=True),
        "fileName": "File Name",
        "fileSourceid": "Source ID",
        "fileMimetype": "Mime Type",
        "potentialTags": "Potential Tags",
        "actualAnnotations": "Actual Annotations",
        "coverageRate": st.column_config.ProgressColumn(
            "Annotation Coverage ℹ️",
            help="How many of the potential tags were found? (Matched / Potential)",
            format="%.2f%%",
            min_value=0,
            max_value=100,
        ),
        "completenessRate": st.column_config.ProgressColumn(
            "Pattern Completeness ℹ️",
            help="How many of the final annotations did the patterns find? (Matched / Actual)",
            format="%.2f%%",
            min_value=0,
            max_value=100,
        ),
        "annotationMessage": "Annotation Message",
        "patternModeMessage": "Pattern Mode Message",
        "lastUpdatedTime": "Last Updated Time",
    },
    use_container_width=True,
    hide_index=True,
    disabled=df_display.columns.difference(["Select"]),
)

# --- Logic to enforce single selection ---
selected_indices = edited_df[edited_df.Select].index.tolist()
if len(selected_indices) > 1:
    new_selection = [idx for idx in selected_indices if idx != st.session_state.get("selected_row_index")]
    if new_selection:
        st.session_state.selected_row_index = new_selection[0]
        st.rerun()
elif len(selected_indices) == 1:
    st.session_state.selected_row_index = selected_indices[0]
elif len(selected_indices) == 0 and st.session_state.get("selected_row_index") is not None:
    st.session_state.selected_row_index = None
    st.rerun()

# --- Interactive Drill-Down Section ---
st.subheader("Tag Comparison Drill-Down")

if st.session_state.get("selected_row_index") is not None:
    if st.session_state.selected_row_index < len(df_display):
        selected_file_data = df_display.iloc[st.session_state.selected_row_index]
        selected_file = selected_file_data["startNode"]
        st.markdown(f"Displaying details for file: **{selected_file}**")

        file_space_series = df_patterns[df_patterns["startNode"] == selected_file]["startNodeSpace"]
        if not file_space_series.empty:
            file_space = file_space_series.iloc[0]
            file_node_id = NodeId(space=file_space, external_id=selected_file)

            # --- Three-Column Tag Comparison (prepare dataframes first) ---
            df_potential_tags_details = df_patterns[df_patterns["startNode"] == selected_file][
                ["text", "resourceType", "regions"]
            ]

            if not df_annotations.empty:
                df_actual_annotations_details = df_annotations[df_annotations["startNode"] == selected_file][
                    ["startNodeText", "endNodeResourceType"]
                ].rename(columns={"startNodeText": "text", "endNodeResourceType": "resourceType"})
            else:
                df_actual_annotations_details = pd.DataFrame(columns=["text", "resourceType"])

            potential_tags_set = set(df_potential_tags_details["text"])
            actual_tags_set = set(df_actual_annotations_details["text"])

            matched_tags_set = potential_tags_set.intersection(actual_tags_set)
            unmatched_tags_set = potential_tags_set - actual_tags_set
            missed_tags_set = actual_tags_set - potential_tags_set

            matched_df = df_potential_tags_details[
                df_potential_tags_details["text"].isin(matched_tags_set)
            ].drop_duplicates(subset=["text", "resourceType"])
            unmatched_df = df_potential_tags_details[
                df_potential_tags_details["text"].isin(unmatched_tags_set)
            ].drop_duplicates(subset=["text", "resourceType"])
            missed_df = df_actual_annotations_details[
                df_actual_annotations_details["text"].isin(missed_tags_set)
            ].drop_duplicates()

            if st.button("Create in Canvas", key=f"canvas_btn_{selected_file}"):
                with st.spinner("Generating Industrial Canvas with bounding boxes..."):
                    _, _, file_view_config = fetch_extraction_pipeline_config(selected_pipeline)

                    unmatched_tags_for_canvas = unmatched_df[["text", "regions"]].to_dict("records")

                    canvas_url = generate_file_canvas(
                        file_id=file_node_id,
                        file_view=file_view_config,
                        ep_config=ep_config,
                        unmatched_tags_with_regions=unmatched_tags_for_canvas,
                    )
                    if canvas_url:
                        st.session_state["generated_canvas_url"] = canvas_url
                    else:
                        st.session_state.pop("generated_canvas_url", None)

        if "generated_canvas_url" in st.session_state and st.session_state.generated_canvas_url:
            st.markdown(
                f"**[Open Last Generated Canvas]({st.session_state.generated_canvas_url})**", unsafe_allow_html=True
            )

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("✅ Matched Tags", len(matched_df))
            st.dataframe(
                matched_df[["text", "resourceType"]],
                column_config={"text": "Tag", "resourceType": "Resource Type"},
                use_container_width=True,
                hide_index=True,
            )
        with col2:
            st.metric("❓ Unmatched by Annotation", len(unmatched_df))
            st.dataframe(
                unmatched_df[["text", "resourceType"]],
                column_config={"text": "Tag", "resourceType": "Resource Type"},
                use_container_width=True,
                hide_index=True,
            )
        with col3:
            st.metric("❗️ Missed by Pattern", len(missed_df))
            st.dataframe(
                missed_df,
                column_config={"text": "Tag", "resourceType": "Resource Type"},
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("✔️ Previous selection is not in the filtered view. Please select a new file.")

else:
    st.info("✔️ Select a file in the table above to see a detailed breakdown of its tags.")
