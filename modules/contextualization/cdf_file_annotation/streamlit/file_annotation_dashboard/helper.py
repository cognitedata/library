import os
import re
import yaml
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from data_structures import ViewPropertyConfig

client = CogniteClient()

PIPELINE_EXT_ID = "ep_file_annotation" #NOTE: hard coded since it shouldn't change that often.

@st.cache_data(ttl=3600)  # Cache data for an hour
def fetch_extraction_pipeline_config() -> tuple[dict, ViewPropertyConfig, ViewPropertyConfig]:
    """
    Fetch configurations from the latest extraction 
    """
    ep_configuration: ExtractionPipelineConfig = client.extraction_pipelines.config.retrieve(
        external_id=pipeline_ext_id
    )
    config_dict = yaml.safe_load(ep_configuration.config)

    local_annotation_state_view = config_dict['dataModelViews']['annotationStateView']
    annotation_state_view = ViewPropertyConfig(local_annotation_state_view['schemaSpace'], local_annotation_state_view['externalId'], local_annotation_state_view['version'], local_annotation_state_view['instanceSpace'])
    
    local_file_view = config_dict['dataModelViews']['fileView']
    file_view = ViewPropertyConfig(local_file_view['schemaSpace'], local_file_view['externalId'], local_file_view['version'], local_file_view.get('instanceSpace'))

    return (config_dict, annotation_state_view, file_view)


@st.cache_data(ttl=3600)  # Cache data for an hour
def fetch_annotation_states(annotation_state_view: ViewPropertyConfig):
    """
    Fetches annotation state instances from the specified data model view.
    """
    result = client.data_modeling.instances.list(
        instance_type="node", space=annotation_state_view.instance_space, sources=annotation_state_view.as_view_id(), limit=-1
    )
    if not result:
        st.info("No annotation state instances found in the specified view.")
        return pd.DataFrame()

    # Convert Node/Edge objects to a more readable list of dictionaries
    data = []
    for instance in result:
        node_data = {
            "externalId": instance.external_id,
            "space": instance.space,
            "createdTime": pd.to_datetime(instance.created_time, unit="ms"),
            "lastUpdatedTime": pd.to_datetime(instance.last_updated_time, unit="ms"),
        }
        # Add properties from the view
        for prop_key, prop_value in instance.properties[annotation_state_view.as_view_id()].items():
            if prop_key == "linkedFile":
                node_data["fileExternalId"] = prop_value[
                    "externalId"
                ]  # Need to change to file name and/or source id -> best to gather all files and then do a retrieve call
            node_data[prop_key] = prop_value
        data.append(node_data)

    df = pd.DataFrame(data)
    if "createdTime" in df.columns:
        df["createdTime"] = df["createdTime"].dt.tz_localize("UTC")
    if "lastUpdatedTime" in df.columns:
        df["lastUpdatedTime"] = df["lastUpdatedTime"].dt.tz_localize("UTC")

    df.rename(
        columns={
            "annotationStatus": "status",
            "attemptCount": "retries",
            "diagramDetectJobId": "jobId",
        },
        inplace=True,
    )

    # Ensure essential columns exist
    for col in ["status", "fileExternalId", "retries", "jobId"]:
        if col not in df.columns:
            df[col] = None

    return df


@st.cache_data(ttl=3600)
def fetch_pipeline_run_history():
    """
    Fetches the full run history for a given extraction pipeline.
    """
    runs = client.extraction_pipelines.runs.list(external_id=pipeline_ext_id, limit=-1)
    return runs


def calculate_total_stats_from_runs(runs):
    """Calculates launch and finalize counts from a list of pipeline runs."""
    total_launched = 0
    total_finalized = 0
    processed_pattern = re.compile(r"total files processed: (\d+)")

    for run in runs:
        if run.status != "success" or not run.message:
            continue

        match = processed_pattern.search(run.message)
        if not match:
            continue

        count = int(match.group(1))

        if "(Launch)" in run.message:
            total_launched += count
        elif "(Finalize)" in run.message:
            total_finalized += count

    return total_launched, total_finalized


def process_runs_for_graphing(runs):
    """
    Transforms pipeline run data into a DataFrame for graphing.
    Aggregates finalize runs that are close together in time.
    """
    launch_data = []
    finalize_runs_to_agg = []
    processed_pattern = re.compile(r"total files processed: (\d+)")

    for run in runs:
        if run.status != "success" or not run.message:
            continue

        match = processed_pattern.search(run.message)
        if not match:
            continue

        count = int(match.group(1))
        timestamp = pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC")

        if "(Launch)" in run.message:
            launch_data.append(
                {"timestamp": timestamp, "count": count, "type": "Launch"}
            )
        elif "(Finalize)" in run.message:
            finalize_runs_to_agg.append({"timestamp": timestamp, "count": count})

    # --- Aggregate Finalize Runs ---
    aggregated_finalize_data = []
    if finalize_runs_to_agg:
        # Sort by timestamp to process in order
        finalize_runs_to_agg.sort(key=lambda x: x["timestamp"])

        current_group_start_time = finalize_runs_to_agg[0]["timestamp"]
        current_group_count = 0

        for run in finalize_runs_to_agg:
            # If the current run is within 1 minute of the start of the group, add it to the group
            if run["timestamp"] < current_group_start_time + timedelta(minutes=10):
                current_group_count += run["count"]
            else:
                # Otherwise, finalize the previous group and start a new one
                aggregated_finalize_data.append(
                    {
                        "timestamp": current_group_start_time,
                        "count": current_group_count,
                        "type": "Finalize",
                    }
                )
                current_group_start_time = run["timestamp"]
                current_group_count = run["count"]

        # Add the last group
        if current_group_count > 0:
            aggregated_finalize_data.append(
                {
                    "timestamp": current_group_start_time,
                    "count": current_group_count,
                    "type": "Finalize",
                }
            )

    # Combine launch data and aggregated finalize data
    df_launch = pd.DataFrame(launch_data)
    df_finalize = pd.DataFrame(aggregated_finalize_data)

    return pd.concat([df_launch, df_finalize], ignore_index=True)