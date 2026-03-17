"""
Cognite Function: Project Health Metrics

Computes health metrics for CDF resources scoped by dataset:
- Extraction Pipelines
- Workflows
- Transformations
- Functions

Reads config from input (dataset_external_id, time range, uptime thresholds),
runs all fetchers, and writes results to a Cognite File for the dashboard.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from cognite.client import CogniteClient

from fetchers import (
    ExtractionPipelineFetcher,
    TransformationFetcher,
    WorkflowFetcher,
    FunctionFetcher,
    get_dataset_id,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(h)

METRICS_FILE_EXTERNAL_ID = "project_health_metrics"
METRICS_FILE_NAME = "project_health_metrics.json"

TIME_RANGE_HOURS = {
    "12 Hours": 12,
    "1 Day": 24,
    "7 Days": 168,
    "30 Days": 720,
}

DEFAULT_UPTIME_THRESHOLDS = {
    "extraction_pipelines": 75,
    "workflows": 75,
    "transformations": 75,
    "functions": 75,
}


def get_time_range_ms(selection: str, custom_start_ms: Optional[int] = None, custom_end_ms: Optional[int] = None) -> tuple:
    now = datetime.now(timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    if selection == "Custom" and custom_start_ms is not None and custom_end_ms is not None:
        start_ms = custom_start_ms
        end_ms = custom_end_ms
    else:
        hours = TIME_RANGE_HOURS.get(selection, 24)
        start_dt = now - timedelta(hours=hours)
        start_ms = int(start_dt.timestamp() * 1000)
    return start_ms, end_ms


def get_time_range_label(selection: str, custom_start_ms: Optional[int], custom_end_ms: Optional[int]) -> str:
    if selection == "Custom" and custom_start_ms and custom_end_ms:
        start_str = datetime.fromtimestamp(custom_start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        end_str = datetime.fromtimestamp(custom_end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        return f"{start_str} to {end_str}"
    return selection


def run_health_computation(
    client: CogniteClient,
    dataset_external_id: str,
    start_ms: int,
    end_ms: int,
    uptime_thresholds: Optional[dict] = None,
) -> dict:
    """Run all health fetchers and return aggregated payload for the dashboard."""
    dataset_id = get_dataset_id(client, dataset_external_id)
    if not dataset_id:
        return {
            "error": f"Dataset not found: {dataset_external_id}",
            "dataset_external_id": dataset_external_id,
            "dataset_id": None,
        }
    thresholds = uptime_thresholds or DEFAULT_UPTIME_THRESHOLDS
    ep_th = thresholds.get("extraction_pipelines", 75)
    wf_th = thresholds.get("workflows", 75)
    tr_th = thresholds.get("transformations", 75)
    fn_th = thresholds.get("functions", 75)

    extraction_data = ExtractionPipelineFetcher(client, dataset_id, start_ms, end_ms, ep_th).fetch_health()
    workflow_data = WorkflowFetcher(client, dataset_id, start_ms, end_ms, wf_th).fetch_health()
    transformation_data = TransformationFetcher(client, dataset_id, start_ms, end_ms, tr_th).fetch_health()
    function_data = FunctionFetcher(
        client, dataset_id, dataset_external_id, start_ms, end_ms, fn_th
    ).fetch_health()

    all_errors = []
    for data in [extraction_data, workflow_data, transformation_data, function_data]:
        all_errors.extend(data.get("errors", []))

    # Build minimal config for dashboard links (project and cluster from client config)
    config = {
        "cdf_base_url": "https://fusion.cognite.com",
        "cdf_project": getattr(client.config, "project", None) or "",
        "cdf_cluster": getattr(client.config, "cdf_cluster", None) or "",
    }

    return {
        "dataset_external_id": dataset_external_id,
        "dataset_id": dataset_id,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "time_range_label": None,  # Set by caller
        "config": config,
        "uptime_thresholds": thresholds,
        "extraction_data": extraction_data,
        "workflow_data": workflow_data,
        "transformation_data": transformation_data,
        "function_data": function_data,
        "all_errors": all_errors,
        "metadata": {
            "computed_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def handle(data: dict, client: CogniteClient) -> dict:
    """
    Cognite Function entrypoint.

    Input data (dict):
      - dataset_external_id (str): single dataset; omit when all_datasets=true
      - all_datasets (bool): if true, run for all datasets in one call and store in one file
      - time_range (str): e.g. "7 Days", "1 Day", "Custom"
      - custom_start_ms (int, optional): for Custom range
      - custom_end_ms (int, optional): for Custom range
      - uptime_thresholds (dict, optional): per-resource thresholds
      - file_external_id (str, optional): override output file external id
    """
    all_datasets = data.get("all_datasets") is True
    dataset_external_id = (data.get("dataset_external_id") or "").strip()
    if not all_datasets and not dataset_external_id:
        return {"error": "dataset_external_id is required (or set all_datasets=true)"}

    time_range = data.get("time_range") or "7 Days"
    custom_start_ms = data.get("custom_start_ms")
    custom_end_ms = data.get("custom_end_ms")
    start_ms, end_ms = get_time_range_ms(time_range, custom_start_ms, custom_end_ms)
    time_range_label = get_time_range_label(time_range, custom_start_ms, custom_end_ms)
    uptime_thresholds = data.get("uptime_thresholds") or DEFAULT_UPTIME_THRESHOLDS
    file_external_id = data.get("file_external_id") or METRICS_FILE_EXTERNAL_ID

    if all_datasets:
        # Single run: compute for all datasets, write one file with "datasets" dict (like context_quality)
        try:
            datasets_list = list(client.data_sets.list(limit=500))
        except Exception as e:
            logger.exception("Failed to list datasets")
            return {"error": str(e)}
        datasets_by_id = {ds.external_id or f"id:{ds.id}": ds for ds in datasets_list}
        result_datasets = {}
        for ext_id in datasets_by_id:
            logger.info("Running project health for dataset=%s, range=%s", ext_id, time_range_label)
            payload = run_health_computation(client, ext_id, start_ms, end_ms, uptime_thresholds)
            if payload.get("error"):
                logger.warning("Skip dataset %s: %s", ext_id, payload.get("error"))
                continue
            payload["time_range_label"] = time_range_label
            result_datasets[ext_id] = payload
        config = {
            "cdf_base_url": "https://fusion.cognite.com",
            "cdf_project": getattr(client.config, "project", None) or "",
            "cdf_cluster": getattr(client.config, "cdf_cluster", None) or "",
        }
        out = {
            "metadata": {
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "time_range_label": time_range_label,
                "config": config,
                "uptime_thresholds": uptime_thresholds,
                "dataset_count": len(result_datasets),
            },
            "datasets": result_datasets,
        }
        try:
            fd, temp_path = tempfile.mkstemp(suffix=".json")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(out, f, indent=2, default=str)
                # Overwrite if file exists: Cognite file holds all data from this run; dashboard filters by config.
                client.files.upload(
                    path=temp_path,
                    external_id=file_external_id,
                    name=METRICS_FILE_NAME,
                    mime_type="application/json",
                    overwrite=True,
                )
                logger.info("Uploaded metrics file: %s (%d datasets)", file_external_id, len(result_datasets))
                return {"file_external_id": file_external_id, "dataset_count": len(result_datasets)}
            finally:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
        except Exception as e:
            logger.exception("Failed to upload metrics file")
            return {"error": str(e)}

    # Single dataset
    logger.info("Running project health for dataset=%s, range=%s", dataset_external_id, time_range_label)
    payload = run_health_computation(client, dataset_external_id, start_ms, end_ms, uptime_thresholds)
    if payload.get("error"):
        return payload
    payload["time_range_label"] = time_range_label
    payload["config"] = {
        "cdf_base_url": "https://fusion.cognite.com",
        "cdf_project": getattr(client.config, "project", None) or "",
        "cdf_cluster": getattr(client.config, "cdf_cluster", None) or "",
    }

    try:
        fd, temp_path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            # Overwrite if file exists; dashboard reads this file and filters by config.
            client.files.upload(
                path=temp_path,
                external_id=file_external_id,
                name=METRICS_FILE_NAME,
                mime_type="application/json",
                overwrite=True,
            )
            logger.info("Uploaded metrics file: %s", file_external_id)
            return {"file_external_id": file_external_id, "dataset_external_id": dataset_external_id}
        finally:
            try:
                os.remove(temp_path)
            except Exception:
                pass
    except Exception as e:
        logger.exception("Failed to upload metrics file")
        return {"error": str(e), "dataset_external_id": dataset_external_id}
