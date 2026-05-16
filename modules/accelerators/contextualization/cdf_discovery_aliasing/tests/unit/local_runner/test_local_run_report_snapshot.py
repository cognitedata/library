"""Tests for slim local run report vs cdf discovery snapshot."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from local_runner.local_run_report import (  # noqa: E402
    cdf_discovery_tasks_path_for_run_report,
    compose_local_run_report_document,
)


def test_compose_local_run_report_includes_raw_results_only_when_present() -> None:
    rr = {"schema_version": 1, "tables": [{"raw_db": "d", "raw_table": "t", "rows": []}]}
    doc = compose_local_run_report_document(
        tasks=[{"task_id": "t1", "status": "succeeded"}],
        wall_t0=0.0,
        dry_run=False,
        paths={"discovery": "/tmp/x_cdf_discovery_tasks.json"},
        raw_results=rr,
    )
    assert set(doc.keys()) == {"schema_version", "paths", "end_of_process", "raw_results"}
    assert doc["raw_results"] is rr


def test_compose_local_run_report_omits_raw_results_when_empty() -> None:
    doc = compose_local_run_report_document(
        tasks=[{"task_id": "t1", "status": "succeeded"}],
        wall_t0=0.0,
        dry_run=False,
        paths={"discovery": "/tmp/x_cdf_discovery_tasks.json"},
        raw_results={"tables": []},
    )
    assert "raw_results" not in doc


def test_cdf_discovery_sidecar_path() -> None:
    p = Path("/r/local_run_results/20260205_121212_local_run_report.json")
    assert cdf_discovery_tasks_path_for_run_report(p) == Path(
        "/r/local_run_results/20260205_121212_cdf_discovery_tasks.json"
    )
