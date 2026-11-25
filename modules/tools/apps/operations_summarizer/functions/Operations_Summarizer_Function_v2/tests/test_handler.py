"""Handler contract: any per-type failure fails the whole run and saves nothing."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import handler
import pdf_generation
import pytest
import summary_generation
from config import runtime

_TYPES = ["assets", "files"]


@pytest.fixture
def saved(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch every collaborator of ``_handle_summary``; returns the ``save_summary_to_cdf`` mock."""
    save = MagicMock(return_value={"external_id": "ops_summary_x", "space": "sp"})
    monkeypatch.setattr(handler, "save_summary_to_cdf", save)
    monkeypatch.setattr(handler, "get_config", lambda data, client: _config())
    monkeypatch.setattr(handler, "ensure_agent_exists", lambda *_args, **_kwargs: "agent")
    monkeypatch.setattr(handler, "CDFHelper", lambda _client: MagicMock())
    monkeypatch.setattr(handler, "DATA_TYPE_ORDER", list(_TYPES))
    monkeypatch.setattr(
        handler,
        "DATA_TYPE_CONFIGS",
        {name: SimpleNamespace(display_name=name.title(), is_special_case=False) for name in _TYPES},
    )
    monkeypatch.setattr(handler, "fetch_instances_for_data_type", lambda *_args, **_kwargs: ([object()], False, False))
    monkeypatch.setattr(handler, "process_dataset", lambda *_args, **_kwargs: ("", "### Section\ntext"))
    monkeypatch.setattr(summary_generation.time_module, "sleep", lambda _seconds: None)
    return save


def _config() -> dict[str, Any]:
    return {
        "summary_contents": dict.fromkeys(_TYPES, True),
        "prompt_templates": {},
        "agent_instructions": "",
        "chunk_size": 50,
        "save_to_cdf": True,
        "generate_pdf": False,
    }


def _use_real_process_dataset(monkeypatch: pytest.MonkeyPatch, helper: MagicMock) -> None:
    """Run only the ``assets`` type through the real ``process_dataset`` with the given agent helper."""
    cfg = _config()
    cfg["summary_contents"] = {"assets": True}
    monkeypatch.setattr(handler, "get_config", lambda data, client: cfg)
    monkeypatch.setattr(handler, "CDFHelper", lambda _client: helper)
    monkeypatch.setattr(handler, "process_dataset", summary_generation.process_dataset)
    monkeypatch.setattr(handler, "DATA_TYPE_ORDER", ["assets"])
    monkeypatch.setattr(summary_generation, "generate_table_for_instances", lambda *_args: "TABLE")
    monkeypatch.setattr(runtime, "prompt_header", "{num_instances} {data_type} {start_time} {end_time} {table_text}")


def _run(**data: Any) -> dict[str, Any]:
    return handler._handle_summary({"summary_period_start": "2026-11-01", "summary_period_end": "2026-11-24", **data})


def test_success_saves_once(saved: MagicMock) -> None:
    result = _run()
    assert result["success"] is True
    saved.assert_called_once()


def test_type_failure_fails_run_and_saves_nothing(saved: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    def fetch(_client: Any, data_type: str, *_args: Any, **_kwargs: Any) -> tuple[list[object], bool, bool]:
        if data_type == "files":
            raise RuntimeError("boom")
        return [object()], False, False

    monkeypatch.setattr(handler, "fetch_instances_for_data_type", fetch)

    result = _run()

    assert result["success"] is False
    assert result["failed_types"] == ["files"]
    assert result["markdown"] == ""
    saved.assert_not_called()


def test_agent_failure_in_chunk_fails_run(saved: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = MagicMock()
    helper.agent_chat.side_effect = RuntimeError("agent down")
    _use_real_process_dataset(monkeypatch, helper)

    result = _run()

    assert result["success"] is False
    assert result["failed_types"] == ["assets"]
    saved.assert_not_called()


def test_transient_agent_error_is_retried(saved: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = MagicMock()
    helper.agent_chat.side_effect = [TimeoutError("slow"), SimpleNamespace(text="summary")]
    _use_real_process_dataset(monkeypatch, helper)

    result = _run()

    assert result["success"] is True
    saved.assert_called_once()


def test_start_after_end_is_error(saved: MagicMock) -> None:
    result = _run(summary_period_start="2026-02-10", summary_period_end="2026-02-01")
    assert result["success"] is False
    assert "on or before" in result["error"]
    saved.assert_not_called()


def test_partial_summary_contents_disables_missing_types(saved: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    fetched: list[str] = []

    def fetch(_client: Any, data_type: str, *_args: Any, **_kwargs: Any) -> tuple[list[object], bool, bool]:
        fetched.append(data_type)
        return [object()], False, False

    monkeypatch.setattr(handler, "fetch_instances_for_data_type", fetch)

    result = _run(summary_contents={"assets": True})

    assert result["success"] is True
    assert fetched == ["assets"]


def test_empty_summary_contents_is_error(saved: MagicMock) -> None:
    result = _run(summary_contents={"assets": False})
    assert result["success"] is False
    assert "at least one data type" in result["error"]
    saved.assert_not_called()


def test_cdf_result_includes_space(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pdf_generation, "ensure_space_exists", lambda *_args: None)
    client = MagicMock()
    client.config.project = "p"
    client.config.base_url = "https://example.cognitedata.com"

    result = pdf_generation.save_summary_to_cdf(
        client,
        "# text",
        "2026-11-01",
        "2026-11-24",
        generate_pdf=False,
        fusion_domain="example.fusion.cognite.com",
        summary_space="my_space",
    )

    assert result["space"] == "my_space"
