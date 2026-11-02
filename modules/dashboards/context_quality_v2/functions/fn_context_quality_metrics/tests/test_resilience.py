"""The retry policy for the data modeling reads."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client.exceptions import CogniteAPIError

from cq import resilience
from cq.resilience import harden


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    pauses: list[float] = []
    monkeypatch.setattr(resilience, "sleep", pauses.append)
    return pauses


def failing(codes: list[int]) -> Any:
    outcomes = iter(codes)

    def call(*_args: Any, **_kwargs: Any) -> str:
        code = next(outcomes)
        if code != 200:
            raise CogniteAPIError("failed", code=code)
        return "ok"

    return call


def test_a_busy_project_is_retried_with_a_growing_pause(no_sleep: list[float]) -> None:
    client = MagicMock()
    client.data_modeling.instances.aggregate = failing([429, 429, 429, 200])
    assert harden(client).data_modeling.instances.aggregate() == "ok"
    assert no_sleep == [1.0, 2.0, 4.0]


def test_a_timeout_is_tried_once_more_and_then_raised() -> None:
    client = MagicMock()
    client.data_modeling.instances.list = failing([408, 408])
    with pytest.raises(CogniteAPIError) as error:
        harden(client).data_modeling.instances.list()
    assert error.value.code == 408


def test_other_errors_are_not_retried(no_sleep: list[float]) -> None:
    client = MagicMock()
    client.data_modeling.instances.retrieve = failing([400, 200])
    with pytest.raises(CogniteAPIError):
        harden(client).data_modeling.instances.retrieve()
    assert no_sleep == []


def test_harden_is_idempotent_and_leaves_other_calls_alone() -> None:
    client = MagicMock()
    client.data_modeling.instances.apply = lambda: "applied"
    once = harden(client).data_modeling.instances
    assert harden(client).data_modeling.instances is once
    assert once.apply() == "applied"
