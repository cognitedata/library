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


def times_out_then(times: int) -> Any:
    calls: list[int] = []

    def work() -> str:
        calls.append(1)
        if len(calls) <= times:
            raise CogniteAPIError("Graph query timed out", code=408)
        return "done"

    return work


def test_a_piece_that_met_a_busy_project_is_read_again_after_a_pause(no_sleep: list[float]) -> None:
    assert resilience.patiently(times_out_then(2)) == "done"
    assert no_sleep == [30.0, 60.0]


def test_a_piece_is_given_up_after_its_pauses(no_sleep: list[float]) -> None:
    with pytest.raises(CogniteAPIError):
        resilience.patiently(times_out_then(3))
    assert no_sleep == [30.0, 60.0]


def test_an_error_that_is_not_about_load_is_not_waited_for(no_sleep: list[float]) -> None:
    def broken() -> str:
        raise CogniteAPIError("bad request", code=400)

    with pytest.raises(CogniteAPIError):
        resilience.patiently(broken)
    assert no_sleep == []


def test_there_is_no_pause_that_would_not_fit_in_the_call(
    monkeypatch: pytest.MonkeyPatch, no_sleep: list[float]
) -> None:
    ticks = iter([0.0, 290.0])
    monkeypatch.setattr(resilience, "clock", lambda: next(ticks))
    with pytest.raises(CogniteAPIError):
        resilience.patiently(times_out_then(1))
    assert no_sleep == []


@pytest.fixture
def call_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """The time of the call, set by the test: a call that began at 0 and is at `now[0]`."""
    now = [0.0]
    monkeypatch.setattr(resilience, "clock", lambda: now[0])
    monkeypatch.setattr(resilience, "_call_started", None)
    resilience.begin_call()
    return now


def test_a_call_has_unlimited_time_until_it_begins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resilience, "_call_started", None)

    assert resilience.time_left() == float("inf")


def test_the_time_left_of_a_call_counts_down_from_its_budget(call_clock: list[float]) -> None:
    assert resilience.time_left() == resilience.CALL_BUDGET
    call_clock[0] = 100.0
    assert resilience.time_left() == resilience.CALL_BUDGET - 100.0


def test_a_retry_whose_pause_does_not_fit_in_the_call_is_not_made(
    call_clock: list[float], no_sleep: list[float]
) -> None:
    call_clock[0] = resilience.CALL_BUDGET - 2.0  # a 408 is retried after 3 seconds
    client = MagicMock()
    client.data_modeling.instances.list = failing([408, 200])

    with pytest.raises(CogniteAPIError):
        harden(client).data_modeling.instances.list()

    assert no_sleep == []


def test_a_retry_that_fits_in_the_call_is_made(call_clock: list[float], no_sleep: list[float]) -> None:
    call_clock[0] = resilience.CALL_BUDGET - 4.0
    client = MagicMock()
    client.data_modeling.instances.list = failing([408, 200])

    assert harden(client).data_modeling.instances.list() == "ok"
    assert no_sleep == [3.0]


def test_every_call_of_the_handler_begins_its_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    import handler

    monkeypatch.setattr(resilience, "_call_started", None)
    monkeypatch.setattr(handler, "ACTIONS", {"validate": lambda _client, _cfg, _data: {}})

    handler.handle({"action": "validate"}, MagicMock())

    assert resilience.time_left() <= resilience.CALL_BUDGET
