"""Work-order buckets are chunked and prompts name the last day of the period."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import summary_generation
from config import runtime
from summary_generation import process_dataset, process_work_orders


@pytest.fixture(autouse=True)
def _fake_tables_and_templates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(summary_generation, "generate_table_for_instances", lambda *_args: "TABLE")
    monkeypatch.setattr(runtime, "prompt_header", "{num_instances} {data_type} {start_time}..{end_time}\n{table_text}")
    monkeypatch.setattr(
        runtime, "holistic_summary_template", "{num_chunks} {total_instances} {data_type}\n{combined_summaries}"
    )


def _helper() -> MagicMock:
    helper = MagicMock()
    helper.agent_chat.return_value = SimpleNamespace(text="agent text")
    return helper


def _completed_work_order() -> SimpleNamespace:
    return SimpleNamespace(
        properties={"view": {"startTime": "2020-01-01T00:00:00Z", "endTime": "2020-01-02T00:00:00Z"}}
    )


def test_work_order_bucket_chunked() -> None:
    helper = _helper()
    orders = [_completed_work_order() for _ in range(120)]
    now = datetime(2026, 11, 25, tzinfo=UTC)

    _, section = process_work_orders(
        orders, now, now, MagicMock(), cdf_helper=helper, selected_agent="agent", chunk_size=50
    )

    assert helper.agent_chat.call_count == 4  # 3 chunks + 1 holistic roll-up
    assert "Completed Work Orders (120)" in section


def test_prompt_end_date_is_inclusive() -> None:
    helper = _helper()
    start = datetime(2026, 11, 1, tzinfo=UTC)
    exclusive_end = datetime(2026, 11, 25, tzinfo=UTC)

    process_dataset([object()], "assets", start, exclusive_end, MagicMock(), helper, "agent", {}, {"chunk_size": 50})

    prompt = helper.agent_chat.call_args.kwargs["messages"][0]["content"]["text"]
    assert "2026-11-01..2026-11-24" in prompt


def test_agent_failure_propagates() -> None:
    helper = _helper()
    helper.agent_chat.side_effect = RuntimeError("agent down")
    now = datetime(2026, 11, 25, tzinfo=UTC)

    with pytest.raises(RuntimeError):
        process_dataset([object()], "assets", now, now, MagicMock(), helper, "agent", {}, {"chunk_size": 50})
