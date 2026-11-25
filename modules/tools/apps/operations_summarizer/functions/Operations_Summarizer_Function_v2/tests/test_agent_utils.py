"""Tests for agent lookup by external ID."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from agent_utils import ensure_agent_exists
from config import runtime

CONFIG_ID = "operation_summarizer_helper_agent"


def make_client(existing: list[SimpleNamespace]) -> MagicMock:
    client = MagicMock()
    client.agents.list.return_value = existing
    client.agents.upsert.side_effect = lambda agents: SimpleNamespace(external_id=agents.external_id)
    client.config.project = "proj"
    client.get.return_value.json.return_value = {"languageModels": [{"name": "cognite/balanced"}]}
    return client


@pytest.fixture(autouse=True)
def configured_runtime() -> None:
    runtime.helper_agent_name = "Operation Summarizer Agent"
    runtime.helper_agent_external_id = CONFIG_ID
    runtime.agent_instructions = "instructions"


def test_ignores_name_match() -> None:
    other = SimpleNamespace(external_id="someone_elses_agent", name="Operation Summarizer Agent")
    client = make_client([other])
    assert ensure_agent_exists(client) == CONFIG_ID
    assert client.agents.upsert.call_args.kwargs["agents"].external_id == CONFIG_ID


def test_unknown_agent_id_raises() -> None:
    client = make_client([SimpleNamespace(external_id=CONFIG_ID, name="x")])
    with pytest.raises(ValueError, match="Agent 'missing' not found"):
        ensure_agent_exists(client, agent_id="missing")
    client.agents.upsert.assert_not_called()


def test_creates_with_config_external_id() -> None:
    client = make_client([])
    assert ensure_agent_exists(client) == CONFIG_ID
    assert client.agents.upsert.call_args.kwargs["agents"].external_id == CONFIG_ID


def test_reuses_existing_config_agent() -> None:
    client = make_client([SimpleNamespace(external_id=CONFIG_ID, name="renamed")])
    assert ensure_agent_exists(client) == CONFIG_ID
    client.agents.upsert.assert_not_called()
