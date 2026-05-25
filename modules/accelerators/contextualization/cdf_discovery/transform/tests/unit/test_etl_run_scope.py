"""Tests for All vs Incremental run scope resolution."""

from __future__ import annotations

from cdf_fn_common.etl_run_scope import (
    incremental_change_processing_enabled,
    incremental_listing_narrowed,
    incremental_skip_unchanged,
    pipeline_parameters,
    resolve_effective_incremental_change_processing,
    resolve_workflow_scope,
)


def test_pipeline_parameters_from_configuration() -> None:
    data = {"configuration": {"parameters": {"incremental": True}}}
    assert pipeline_parameters(data) == {"incremental": True}


def test_pipeline_parameters_top_level_fallback() -> None:
    data = {"parameters": {"incremental_change_processing": True}}
    assert pipeline_parameters(data) == {"incremental_change_processing": True}


def test_incremental_change_processing_enabled() -> None:
    assert incremental_change_processing_enabled({"parameters": {"incremental": True}}) is True
    assert incremental_change_processing_enabled(
        {"parameters": {"incremental_change_processing": True}}
    ) is True
    assert incremental_change_processing_enabled({}, {"incremental_change_processing": True}) is True
    assert incremental_change_processing_enabled({"incremental_change_processing": True}) is True
    assert incremental_change_processing_enabled({"incremental": True}) is True
    assert incremental_change_processing_enabled({}) is False


def test_incremental_listing_narrowed_from_run_payload() -> None:
    data = {"incremental_change_processing": True}
    assert incremental_listing_narrowed(data) is True


def test_resolve_effective_incremental_change_processing_from_flag() -> None:
    assert resolve_effective_incremental_change_processing({"incremental_change_processing": True}) is True
    assert resolve_effective_incremental_change_processing({"incremental_change_processing": False}) is False


def test_resolve_effective_incremental_change_processing_node_override() -> None:
    data = {"incremental_change_processing": True, "parameters": {"incremental": True}}
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "all"}) is False
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "incremental"}) is True
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "inherit"}) is True


def test_resolve_effective_incremental_change_processing_legacy_scope_mode_key() -> None:
    data = {"incremental_change_processing": False}
    assert resolve_effective_incremental_change_processing(data, {"scope_mode": "incremental"}) is True


def test_incremental_listing_narrowed() -> None:
    data = {"incremental_change_processing": True, "parameters": {"incremental_change_processing": True}}
    assert incremental_listing_narrowed(data) is True
    assert incremental_listing_narrowed({**data, "incremental_change_processing": False}) is False
    assert incremental_listing_narrowed(data, {"query_scope_mode": "all"}) is False
    assert incremental_listing_narrowed({"incremental_change_processing": False}) is False


def test_incremental_skip_unchanged() -> None:
    data = {"parameters": {"incremental_change_processing": True}}
    cfg: dict = {}
    assert incremental_skip_unchanged(data, cfg, listing_narrowed=False) is False
    assert incremental_skip_unchanged(data, cfg, listing_narrowed=True) is True
    assert (
        incremental_skip_unchanged(
            data,
            {"incremental_skip_unchanged": False},
            listing_narrowed=True,
        )
        is False
    )
    assert (
        incremental_skip_unchanged(
            {"parameters": {"incremental_skip_unchanged": False}},
            {},
            listing_narrowed=True,
        )
        is False
    )


def test_resolve_workflow_scope() -> None:
    assert resolve_workflow_scope({"parameters": {"workflow_scope": "site_a"}}) == "site_a"
    assert resolve_workflow_scope({"configuration": {"id": "etl_default"}}) == "etl_default"
    assert resolve_workflow_scope({}) == ""
