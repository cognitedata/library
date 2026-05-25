"""Tests for ETL predecessor mode resolution."""

from __future__ import annotations

import os

from cdf_fn_common.etl_predecessor_mode import (
    MODE_COHORT,
    MODE_IN_MEMORY,
    resolve_local_predecessor_mode,
    seed_predecessor_mode,
    use_in_memory_predecessors,
)


def test_default_cohort() -> None:
    assert resolve_local_predecessor_mode({}) == MODE_COHORT
    assert use_in_memory_predecessors({}) is False


def test_explicit_cohort_on_shared_data() -> None:
    data = {"local_predecessor_mode": "cohort"}
    assert resolve_local_predecessor_mode(data) == MODE_COHORT
    assert use_in_memory_predecessors(data) is False


def test_pipeline_parameters_override() -> None:
    data = {
        "configuration": {"parameters": {"local_predecessor_mode": "cohort"}},
    }
    assert resolve_local_predecessor_mode(data) == MODE_COHORT


def test_task_config_flags() -> None:
    assert resolve_local_predecessor_mode({}, {"_use_cohort_predecessors": True}) == MODE_COHORT
    assert resolve_local_predecessor_mode({}, {"_use_in_memory_predecessors": False}) == MODE_COHORT


def test_env_etl_transform_in_memory(monkeypatch) -> None:
    monkeypatch.setenv("ETL_TRANSFORM_IN_MEMORY", "0")
    assert resolve_local_predecessor_mode({}) == MODE_COHORT
    monkeypatch.setenv("ETL_TRANSFORM_IN_MEMORY", "1")
    assert resolve_local_predecessor_mode({}) == MODE_IN_MEMORY


def test_seed_predecessor_mode() -> None:
    shared: dict = {}
    seed_predecessor_mode(shared, "cohort")
    assert shared["local_predecessor_mode"] == MODE_COHORT
