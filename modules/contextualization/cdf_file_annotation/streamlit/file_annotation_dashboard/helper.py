import os
import re
import yaml
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.functions import FunctionCallLog
from data_structures import ViewPropertyConfig

client = CogniteClient()

PIPELINE_EXT_ID = "ep_file_annotation"


def parse_run_message(message: str) -> dict:
    """Parses the structured run message and returns a dictionary of its components."""
    if not message:
        return {}

    # Regex to capture all key-value pairs from the new format
    pattern = re.compile(
        r"\(caller:(?P<caller>\w+), function_id:(?P<function_id>[\w\.-]+), call_id:(?P<call_id>[\w\.-]+)\) - "
        r"total files processed: (?P<total>\d+) - "
        r"successful files: (?P<success>\d+) - "
        r"failed files: (?P<failed>\d+)"
    )
    match = pattern.search(message)
    if match:
        data = match.groupdict()
        # Convert numeric strings to integers
        for key in ["total", "success", "failed"]:
            if key in data:
                data[key] = int(data[key])
        return data
    return {}


@st.cache_data(ttl=3600)
def fetch_extraction_pipeline_config() -> (
    tuple[dict, ViewPropertyConfig, ViewPropertyConfig]
):
    """
    Fetch configurations from the latest extraction
    """
    ep_configuration = client.extraction_pipelines.config.retrieve(
        external_id=PIPELINE_EXT_ID
    )
    config_dict = yaml.safe_load(ep_configuration.config)

    local_annotation_state_view = config_dict["dataModelViews"]["annotationStateView"]
    annotation_state_view = ViewPropertyConfig(
        local_annotation_state_view["schemaSpace"],
        local_annotation_state_view["externalId"],
        local_annotation_state_view["version"],
        local_annotation_state_view["instanceSpace"],
    )

    local_file_view = config_dict["dataModelViews"]["fileView"]
    file_view = ViewPropertyConfig(
        local_file_view["schemaSpace"],
        local_file_view["externalId"],
        local_file_view["version"],
        local_file_view.get("instanceSpace"),
    )

    return (config_dict, annotation_state_view, file_view)


@st.cache_data(ttl=3600)
def fetch_annotation_states(annotation_state_view: ViewPropertyConfig):
    """
    Fetches annotation state instances from the specified data model view.
    """
    result = client.data_modeling.instances.list(
        instance_type="node",
        space=annotation_state_view.instance_space,
        sources=annotation_state_view.as_view_id(),
        limit=-1,
    )
    if not result:
        st.info("No annotation state instances found in the specified view.")
        return pd.DataFrame()

    data = []
    for instance in result:
        node_data = {
            "externalId": instance.external_id,
            "space": instance.space,
            "createdTime": pd.to_datetime(instance.created_time, unit="ms"),
            "lastUpdatedTime": pd.to_datetime(instance.last_updated_time, unit="ms"),
        }
        for prop_key, prop_value in instance.properties[
            annotation_state_view.as_view_id()
        ].items():
            if prop_key == "linkedFile":
                node_data["fileExternalId"] = prop_value.get("externalId")
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

    for col in ["status", "fileExternalId", "retries", "jobId"]:
        if col not in df.columns:
            df[col] = None
    return df


@st.cache_data(ttl=3600)
def fetch_pipeline_run_history():
    """Fetches the full run history for a given extraction pipeline."""
    return client.extraction_pipelines.runs.list(external_id=PIPELINE_EXT_ID, limit=-1)


def calculate_success_failure_stats(runs):
    """Calculates success and failure counts from a list of pipeline runs."""
    success_count = sum(1 for run in runs if run.status == "success")
    failure_count = sum(1 for run in runs if run.status == "failure")
    return success_count, failure_count


def get_failed_run_details(runs):
    """Filters for failed runs and extracts their details, including IDs."""
    failed_runs = []
    for run in runs:
        if run.status == "failure":
            parsed_message = parse_run_message(run.message)
            failed_runs.append(
                {
                    "timestamp": pd.to_datetime(
                        run.created_time, unit="ms"
                    ).tz_localize("UTC"),
                    "message": run.message,
                    "status": run.status,
                    "function_id": parsed_message.get("function_id"),
                    "call_id": parsed_message.get("call_id"),
                }
            )
    return sorted(failed_runs, key=lambda x: x["timestamp"], reverse=True)


@st.cache_data(ttl=3600)
def fetch_function_logs(function_id: int, call_id: int):
    """Fetches the logs for a specific function call."""
    try:
        log: FunctionCallLog = client.functions.calls.get_logs(call_id, function_id)
        return log.to_text(with_timestamps=False)
    except Exception as e:
        return [f"Could not retrieve logs: {e}"]


def process_runs_for_graphing(runs):
    """Transforms pipeline run data into a DataFrame for graphing."""
    launch_data = []
    finalize_runs_to_agg = []

    for run in runs:
        if run.status != "success":
            continue

        parsed = parse_run_message(run.message)
        if not parsed:
            continue

        timestamp = pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC")
        count = parsed.get("total", 0)
        caller = parsed.get("caller")

        if caller == "Launch":
            launch_data.append(
                {"timestamp": timestamp, "count": count, "type": "Launch"}
            )
        elif caller == "Finalize":
            finalize_runs_to_agg.append({"timestamp": timestamp, "count": count})

    # --- Aggregate Finalize Runs ---
    aggregated_finalize_data = []
    if finalize_runs_to_agg:
        finalize_runs_to_agg.sort(key=lambda x: x["timestamp"])
        current_group_start_time = finalize_runs_to_agg[0]["timestamp"]
        current_group_count = 0

        for run in finalize_runs_to_agg:
            if run["timestamp"] < current_group_start_time + timedelta(minutes=10):
                current_group_count += run["count"]
            else:
                aggregated_finalize_data.append(
                    {
                        "timestamp": current_group_start_time,
                        "count": current_group_count,
                        "type": "Finalize",
                    }
                )
                current_group_start_time = run["timestamp"]
                current_group_count = run["count"]

        if current_group_count > 0:
            aggregated_finalize_data.append(
                {
                    "timestamp": current_group_start_time,
                    "count": current_group_count,
                    "type": "Finalize",
                }
            )

    df_launch = pd.DataFrame(launch_data)
    df_finalize = pd.DataFrame(aggregated_finalize_data)

    return pd.concat([df_launch, df_finalize], ignore_index=True)
