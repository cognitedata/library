"""
Health data fetchers for Project Health (no Streamlit dependency).

Used by the Cognite Function handler to compute extraction pipelines, workflows,
transformations, and functions health. Logic mirrors the dashboard data_fetchers
but without caching or Streamlit.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional
from cognite.client import CogniteClient

# Shared status sets (must match dashboard config)
SUCCESS_STATUSES = frozenset({"success", "completed", "ready", "seen"})
FAILED_STATUSES = frozenset({"failed", "failure", "error", "timed_out", "timeout"})
API_LIST_LIMIT = 500
API_RUNS_LIMIT = 100
MAX_RECENT_RUNS = 5


def _calculate_uptime_percentage(successful: int, failed: int) -> float:
    total = successful + failed
    return (successful / total) * 100 if total > 0 else 100.0


def _count_by_status(items: list, status_attr: str, target_statuses: frozenset) -> int:
    count = 0
    for item in items:
        status = getattr(item, status_attr, None) if hasattr(item, status_attr) else item.get(status_attr)
        if status and status.lower() in target_statuses:
            count += 1
    return count


def _create_error_entry(resource_name: str, status: str, time_val: Any, message: str) -> dict:
    return {"resource": resource_name, "status": status, "time": time_val, "message": message}


def _sort_runs_failed_first(runs: list, status_key: str = "status") -> list:
    def get_timestamp(run: dict) -> int:
        for key in ("created_time", "finished_time", "end_time", "started_time", "start_time"):
            if run.get(key):
                return run[key]
        return 0

    def is_failed(s: str) -> bool:
        return s and s.lower() in FAILED_STATUSES

    def sort_key(run: dict) -> tuple:
        status = run.get(status_key, "")
        return (0 if is_failed(status) else 1, -(get_timestamp(run) or 0))

    return sorted(runs, key=sort_key)


class ResourceHealthFetcher(ABC):
    resource_type_name: str = "Resource"
    resources_key: str = "resources"
    runs_key: str = "recent_runs"
    runs_count_key: str = "runs_in_window"
    no_runs_key: str = "no_runs"
    status_field: str = "status"

    def __init__(
        self,
        client: CogniteClient,
        dataset_id: int,
        start_ms: int,
        end_ms: int,
        uptime_threshold: int = 75,
    ):
        self.client = client
        self.dataset_id = dataset_id
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.uptime_threshold = uptime_threshold
        self.errors: list = []

    @abstractmethod
    def fetch_resources(self) -> list:
        pass

    @abstractmethod
    def fetch_runs(self, resource: Any) -> list:
        pass

    @abstractmethod
    def build_resource_info(self, resource: Any) -> dict:
        pass

    @abstractmethod
    def get_run_time(self, run: Any) -> Optional[int]:
        pass

    @abstractmethod
    def build_recent_run(self, run: Any) -> dict:
        pass

    def get_success_statuses(self) -> frozenset:
        return SUCCESS_STATUSES

    def get_failed_statuses(self) -> frozenset:
        return FAILED_STATUSES

    def get_error_message(self, run: Any) -> Optional[str]:
        return getattr(run, "message", None) or getattr(run, "error", None)

    def get_resource_name(self, resource: Any) -> str:
        return getattr(resource, "name", None) or getattr(resource, "external_id", "Unknown")

    def post_process_info(self, info: dict, resource: Any, runs: list, runs_in_window: list) -> None:
        pass

    def custom_sort_key(self, resource_info: dict) -> tuple:
        failed = (resource_info.get("last_status") or "").lower() in FAILED_STATUSES
        return (0 if failed else 1, (resource_info.get("name", "") or "").lower())

    def build_summary_extra(self, resources_health: list) -> dict:
        return {}

    def filter_runs_in_window(self, runs: list) -> list:
        filtered = []
        for run in runs:
            t = self.get_run_time(run)
            if t and self.start_ms <= t <= self.end_ms:
                filtered.append(run)
        return filtered

    def calculate_statistics(self, info: dict, runs: list, runs_in_window: list) -> None:
        success_statuses = self.get_success_statuses()
        failed_statuses = self.get_failed_statuses()
        if runs:
            latest = runs[0]
            info["last_run"] = self.get_run_time(latest)
            info["last_status"] = getattr(latest, self.status_field, None)
        recent_runs = [self.build_recent_run(r) for r in runs_in_window]
        info[self.runs_key] = _sort_runs_failed_first(recent_runs)[:MAX_RECENT_RUNS]
        info[self.runs_count_key] = len(runs_in_window)
        info["successful_in_window"] = _count_by_status(runs_in_window, self.status_field, success_statuses)
        info["failed_in_window"] = _count_by_status(runs_in_window, self.status_field, failed_statuses)
        info["uptime_percentage"] = _calculate_uptime_percentage(
            info["successful_in_window"], info["failed_in_window"]
        )

    def collect_run_errors(self, info: dict, runs_in_window: list) -> None:
        failed_statuses = self.get_failed_statuses()
        for run in runs_in_window:
            status = getattr(run, self.status_field, None)
            if status and status.lower() in failed_statuses:
                self.errors.append(
                    _create_error_entry(
                        f"{self.resource_type_name}: {info['name']}",
                        status,
                        self.get_run_time(run),
                        self.get_error_message(run),
                    )
                )

    def build_summary(self, resources_health: list) -> dict:
        resources_with_runs = [r for r in resources_health if r.get(self.runs_count_key, 0) > 0]
        summary = {
            "total": len(resources_health),
            "healthy": sum(1 for r in resources_with_runs if r["uptime_percentage"] >= self.uptime_threshold),
            "failed": sum(1 for r in resources_with_runs if r["uptime_percentage"] < self.uptime_threshold),
            self.no_runs_key: sum(1 for r in resources_health if r.get(self.runs_count_key, 0) == 0),
        }
        summary.update(self.build_summary_extra(resources_health))
        return summary

    def fetch_health(self) -> dict:
        self.errors = []
        try:
            resources = self.fetch_resources()
        except Exception as e:
            return {
                self.resources_key: [],
                "summary": {"total": 0, "healthy": 0, "failed": 0, self.no_runs_key: 0},
                "errors": [_create_error_entry(self.resource_type_name, "Error", None, str(e))],
            }
        resources_health = []
        for resource in resources:
            info = self.build_resource_info(resource)
            try:
                runs = self.fetch_runs(resource)
                runs_in_window = self.filter_runs_in_window(runs)
                self.calculate_statistics(info, runs, runs_in_window)
                self.collect_run_errors(info, runs_in_window)
                self.post_process_info(info, resource, runs, runs_in_window)
            except Exception as e:
                self.errors.append(
                    _create_error_entry(
                        f"{self.resource_type_name}: {info['name']}", "Error", None, str(e)
                    )
                )
            resources_health.append(info)
        resources_health.sort(key=self.custom_sort_key)
        return {
            self.resources_key: resources_health,
            "summary": self.build_summary(resources_health),
            "errors": self.errors,
        }


class ExtractionPipelineFetcher(ResourceHealthFetcher):
    resource_type_name = "Extraction Pipeline"
    resources_key = "pipelines"
    runs_key = "recent_runs"
    runs_count_key = "runs_in_window"

    def get_success_statuses(self) -> frozenset:
        return SUCCESS_STATUSES | {"seen"}

    def fetch_resources(self) -> list:
        try:
            pipelines = list(
                self.client.extraction_pipelines.list(
                    data_set_ids=[self.dataset_id], limit=API_LIST_LIMIT
                )
            )
            if pipelines:
                return pipelines
        except Exception:
            pass
        all_pipelines = list(self.client.extraction_pipelines.list(limit=API_LIST_LIMIT))
        return [p for p in all_pipelines if p.data_set_id == self.dataset_id]

    def fetch_runs(self, resource: Any) -> list:
        return list(
            self.client.extraction_pipelines.runs.list(
                external_id=resource.external_id, limit=API_RUNS_LIMIT
            )
        )

    def build_resource_info(self, resource: Any) -> dict:
        return {
            "id": resource.id,
            "external_id": resource.external_id,
            "name": resource.name or resource.external_id,
            "description": resource.description,
            "schedule": resource.schedule,
            "last_run": None,
            "last_seen": None,
            "last_status": None,
            "recent_runs": [],
            "runs_in_window": 0,
            "successful_in_window": 0,
            "failed_in_window": 0,
            "uptime_percentage": 100.0,
        }

    def get_run_time(self, run: Any) -> Optional[int]:
        return run.created_time

    def build_recent_run(self, run: Any) -> dict:
        return {"status": run.status, "created_time": run.created_time, "message": run.message}

    def calculate_statistics(self, info: dict, runs: list, runs_in_window: list) -> None:
        super().calculate_statistics(info, runs, runs_in_window)
        if runs:
            info["last_seen"] = runs[0].created_time


class TransformationFetcher(ResourceHealthFetcher):
    resource_type_name = "Transformation"
    resources_key = "transformations"
    runs_key = "recent_jobs"
    runs_count_key = "jobs_in_window"

    def fetch_resources(self) -> list:
        try:
            transformations = list(
                self.client.transformations.list(
                    data_set_ids=[self.dataset_id], limit=API_LIST_LIMIT
                )
            )
            if transformations:
                return transformations
        except Exception:
            pass
        all_t = list(self.client.transformations.list(limit=API_LIST_LIMIT))
        return [t for t in all_t if t.data_set_id == self.dataset_id]

    def fetch_runs(self, resource: Any) -> list:
        return list(
            self.client.transformations.jobs.list(
                transformation_id=resource.id, limit=API_RUNS_LIMIT
            )
        )

    def build_resource_info(self, resource: Any) -> dict:
        return {
            "id": resource.id,
            "external_id": resource.external_id,
            "name": resource.name or resource.external_id,
            "schedule": resource.schedule.interval if resource.schedule else None,
            "is_paused": resource.schedule.is_paused if resource.schedule else True,
            "last_run": None,
            "last_status": None,
            "recent_jobs": [],
            "jobs_in_window": 0,
            "successful_in_window": 0,
            "failed_in_window": 0,
            "uptime_percentage": 100.0,
        }

    def get_run_time(self, run: Any) -> Optional[int]:
        return run.finished_time or run.started_time

    def build_recent_run(self, run: Any) -> dict:
        return {
            "status": run.status,
            "started_time": run.started_time,
            "finished_time": run.finished_time,
            "error": run.error,
        }

    def get_error_message(self, run: Any) -> Optional[str]:
        return run.error

    def build_summary_extra(self, resources_health: list) -> dict:
        return {
            "running": sum(
                1
                for r in resources_health
                if r.get("last_status") and r["last_status"].lower() in ("running", "transforming")
            )
        }


class WorkflowFetcher(ResourceHealthFetcher):
    resource_type_name = "Workflow"
    resources_key = "workflows"
    runs_key = "recent_executions"
    runs_count_key = "executions_in_window"

    def fetch_resources(self) -> list:
        all_workflows = list(self.client.workflows.list(limit=API_LIST_LIMIT))
        return [w for w in all_workflows if getattr(w, "data_set_id", None) == self.dataset_id]

    def fetch_runs(self, resource: Any) -> list:
        executions = list(
            self.client.workflows.executions.list(
                resource.external_id,
                created_time_start=self.start_ms,
                created_time_end=self.end_ms,
                limit=API_RUNS_LIMIT,
            )
        )
        executions.sort(
            key=lambda ex: getattr(ex, "start_time", 0) or getattr(ex, "created_time", 0) or 0,
            reverse=True,
        )
        return executions

    def filter_runs_in_window(self, runs: list) -> list:
        return runs

    def build_resource_info(self, resource: Any) -> dict:
        return {
            "id": getattr(resource, "id", None),
            "external_id": resource.external_id,
            "name": resource.external_id,
            "description": resource.description,
            "data_set_id": getattr(resource, "data_set_id", None),
            "last_run": None,
            "last_execution": None,
            "last_status": None,
            "recent_executions": [],
            "executions_in_window": 0,
            "successful_in_window": 0,
            "failed_in_window": 0,
            "uptime_percentage": 100.0,
        }

    def get_run_time(self, run: Any) -> Optional[int]:
        return getattr(run, "end_time", None) or getattr(run, "start_time", None)

    def build_recent_run(self, run: Any) -> dict:
        return {
            "status": getattr(run, "status", None),
            "start_time": getattr(run, "start_time", None),
            "end_time": getattr(run, "end_time", None),
            "reason_for_incompletion": getattr(run, "reason_for_incompletion", None),
        }

    def get_error_message(self, run: Any) -> Optional[str]:
        return getattr(run, "reason_for_incompletion", None)

    def calculate_statistics(self, info: dict, runs: list, runs_in_window: list) -> None:
        super().calculate_statistics(info, runs, runs_in_window)
        if runs:
            info["last_execution"] = self.get_run_time(runs[0])

    def build_summary_extra(self, resources_health: list) -> dict:
        return {
            "running": sum(
                1
                for r in resources_health
                if r.get("last_status") and r["last_status"].lower() == "running"
            )
        }


class FunctionFetcher(ResourceHealthFetcher):
    resource_type_name = "Function"
    resources_key = "functions"
    runs_key = "recent_calls"
    runs_count_key = "calls_in_window"
    no_runs_key = "no_calls"

    def __init__(
        self,
        client: CogniteClient,
        dataset_id: int,
        dataset_external_id: str,
        start_ms: int,
        end_ms: int,
        uptime_threshold: int = 75,
    ):
        super().__init__(client, dataset_id, start_ms, end_ms, uptime_threshold)
        self.dataset_external_id = dataset_external_id
        self._file_dataset_map: dict = {}

    def get_failed_statuses(self) -> frozenset:
        return FAILED_STATUSES | {"timeout"}

    def fetch_resources(self) -> list:
        all_functions = list(self.client.functions.list(limit=API_LIST_LIMIT))
        file_ids = [f.file_id for f in all_functions if getattr(f, "file_id", None)]
        if file_ids:
            try:
                files = self.client.files.retrieve_multiple(ids=file_ids, ignore_unknown_ids=True)
                for f in files:
                    if f:
                        self._file_dataset_map[f.id] = f.data_set_id
            except Exception:
                for file_id in file_ids:
                    try:
                        f = self.client.files.retrieve(id=file_id)
                        if f:
                            self._file_dataset_map[f.id] = f.data_set_id
                    except Exception:
                        pass
        return [
            f
            for f in all_functions
            if getattr(f, "file_id", None) and self._file_dataset_map.get(f.file_id) == self.dataset_id
        ]

    def fetch_runs(self, resource: Any) -> list:
        return list(self.client.functions.calls.list(function_id=resource.id, limit=API_RUNS_LIMIT))

    def build_resource_info(self, resource: Any) -> dict:
        return {
            "id": resource.id,
            "external_id": resource.external_id,
            "name": resource.name or resource.external_id,
            "description": resource.description,
            "status": resource.status,
            "last_run": None,
            "last_call": None,
            "last_call_status": None,
            "last_status": None,
            "recent_calls": [],
            "calls_in_window": 0,
            "successful_in_window": 0,
            "failed_in_window": 0,
            "uptime_percentage": 100.0,
        }

    def get_run_time(self, run: Any) -> Optional[int]:
        return run.end_time or run.start_time

    def build_recent_run(self, run: Any) -> dict:
        return {
            "status": run.status,
            "start_time": run.start_time,
            "end_time": run.end_time,
            "error": getattr(run, "error", None),
        }

    def get_error_message(self, run: Any) -> Optional[str]:
        error = getattr(run, "error", None)
        if isinstance(error, dict):
            return error.get("message", "Unknown error")
        return str(error) if error else None

    def calculate_statistics(self, info: dict, runs: list, runs_in_window: list) -> None:
        super().calculate_statistics(info, runs, runs_in_window)
        if runs:
            info["last_call"] = self.get_run_time(runs[0])
            info["last_call_status"] = runs[0].status

    def post_process_info(self, info: dict, resource: Any, runs: list, runs_in_window: list) -> None:
        if resource.status and resource.status.lower() == "failed":
            self.errors.append(
                _create_error_entry(
                    f"Function: {info['name']}", resource.status, None, "Function deployment failed"
                )
            )

    def custom_sort_key(self, resource_info: dict) -> tuple:
        deployment_failed = (resource_info.get("status") or "").lower() in FAILED_STATUSES
        call_failed = (resource_info.get("last_call_status") or "").lower() in FAILED_STATUSES
        return (
            0 if deployment_failed else 1,
            0 if call_failed else 1,
            (resource_info.get("name", "") or "").lower(),
        )


def get_dataset_id(client: CogniteClient, dataset_external_id: str) -> Optional[int]:
    try:
        dataset = client.data_sets.retrieve(external_id=dataset_external_id)
        return dataset.id if dataset else None
    except Exception:
        return None
