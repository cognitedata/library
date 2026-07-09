"""Tests for All vs Incremental run scope resolution."""

from __future__ import annotations

from cdf_fn_common.etl_run_scope import (
    incremental_change_processing_enabled,
    incremental_listing_narrowed,
    incremental_skip_unchanged,
    pipeline_parameters,
    resolve_query_scope_mode,
    resolve_effective_incremental_change_processing,
    resolve_workflow_scope,
)


def test_pipeline_parameters_from_configuration() -> None:
    data = {"configuration": {"parameters": {"incremental": True}}}
    assert pipeline_parameters(data) == {"incremental": True}


def test_pipeline_parameters_ignores_top_level_parameters() -> None:
    data = {"parameters": {"incremental_change_processing": True}}
    assert pipeline_parameters(data) == {}


def test_incremental_change_processing_enabled() -> None:
    assert incremental_change_processing_enabled({"parameters": {"incremental": True}}) is False
    assert incremental_change_processing_enabled(
        {"configuration": {"parameters": {"incremental_change_processing": True}}}
    ) is True
    assert incremental_change_processing_enabled({}, {"incremental_change_processing": True}) is True
    assert incremental_change_processing_enabled({"incremental_change_processing": True}) is False
    assert incremental_change_processing_enabled({"incremental": True}) is False
    assert incremental_change_processing_enabled({}) is False


def test_incremental_change_processing_enabled_parses_string_bool_flags() -> None:
    assert (
        incremental_change_processing_enabled(
            {"configuration": {"parameters": {"incremental_change_processing": "false"}}}
        )
        is False
    )
    assert incremental_change_processing_enabled(
        {}, {"incremental_change_processing": "0"}
    ) is False


def test_incremental_listing_narrowed_from_run_payload() -> None:
    data = {"configuration": {"parameters": {"incremental_change_processing": True}}}
    assert incremental_listing_narrowed(data) is True


def test_resolve_effective_incremental_change_processing_from_flag() -> None:
    assert resolve_effective_incremental_change_processing({"incremental_change_processing": True}) is True
    assert resolve_effective_incremental_change_processing({"incremental_change_processing": False}) is False


def test_resolve_effective_incremental_change_processing_parses_string_bool_flags() -> None:
    assert (
        resolve_effective_incremental_change_processing({"incremental_change_processing": "false"})
        is False
    )
    assert (
        resolve_effective_incremental_change_processing(
            {"configuration": {"parameters": {"incremental_change_processing": "off"}}}
        )
        is False
    )


def test_resolve_effective_incremental_change_processing_node_override() -> None:
    data = {"incremental_change_processing": True, "parameters": {"incremental": True}}
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "all"}) is False
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "incremental"}) is True
    assert resolve_effective_incremental_change_processing(data, {"query_scope_mode": "inherit"}) is True


def test_resolve_effective_incremental_change_processing_unknown_scope_mode_key() -> None:
    data = {"incremental_change_processing": False}
    assert resolve_effective_incremental_change_processing(data, {"scope_mode": "incremental"}) is False


def test_resolve_query_scope_mode() -> None:
    assert resolve_query_scope_mode({"query_scope_mode": "all"}) == "all"
    assert resolve_query_scope_mode({"scope_mode": "incremental"}) == "inherit"
    assert resolve_query_scope_mode({"query_scope_mode": "unknown"}) == "inherit"
    assert resolve_query_scope_mode(None) == "inherit"


def test_incremental_listing_narrowed() -> None:
    data = {"configuration": {"parameters": {"incremental_change_processing": True}}}
    assert incremental_listing_narrowed(data) is True
    assert incremental_listing_narrowed(
        {"configuration": {"parameters": {"incremental_change_processing": False}}}
    ) is False
    assert incremental_listing_narrowed(data, {"query_scope_mode": "all"}) is False
    assert incremental_listing_narrowed({"incremental_change_processing": False}) is False


def test_incremental_skip_unchanged() -> None:
    data = {"configuration": {"parameters": {"incremental_change_processing": True}}}
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
            {"configuration": {"parameters": {"incremental_skip_unchanged": False}}},
            {},
            listing_narrowed=True,
        )
        is False
    )
    assert (
        incremental_skip_unchanged(
            {"configuration": {"parameters": {"incremental_skip_unchanged": "false"}}},
            {},
            listing_narrowed=True,
        )
        is False
    )


def test_resolve_workflow_scope() -> None:
    assert resolve_workflow_scope({"configuration": {"parameters": {"workflow_scope": "site_a"}}}) == "site_a"
    assert resolve_workflow_scope({"configuration": {"id": "etl_default"}}) == "etl_default"
    assert resolve_workflow_scope({}) == ""
