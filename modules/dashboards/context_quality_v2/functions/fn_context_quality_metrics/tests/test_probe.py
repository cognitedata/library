"""`probe_slice` times the requests a batch makes for one slice, and writes nothing."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import handler
from cognite.client.exceptions import CogniteAPIError


def client_with(list_result: Any, count: int = 7) -> MagicMock:
    client = MagicMock()
    client.data_modeling.instances.aggregate.return_value = SimpleNamespace(value=count)
    client.data_modeling.instances.list.return_value = list_result
    return client


def test_a_probe_reports_each_request_and_changes_nothing() -> None:
    page = [SimpleNamespace(external_id=f"3{index}") for index in range(5)]
    client = client_with(page)

    out = handler.handle({"action": "probe_slice", "space": "sp", "kind": "assets", "prefix": "3"}, client)

    steps = out["steps"]
    assert steps["count of the kind with the prefix"]["count"] == 7
    assert steps["1000 ids of the space with the prefix, any kind"]["returned"] == 5
    assert steps["1000 ids of the space with the prefix, any kind"]["sample"] == ["30", "31", "32"]
    assert all("seconds" in step for step in steps.values())
    assert not client.post.called


def test_a_request_the_service_times_out_on_is_reported_not_raised() -> None:
    client = client_with([])
    client.data_modeling.instances.list.side_effect = CogniteAPIError("timed out", code=408)

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "3"}, client)

    assert out["steps"]["20 instances with their properties"]["error"] == 408


def test_a_probe_needs_a_space() -> None:
    import pytest

    with pytest.raises(ValueError, match="needs space"):
        handler.handle({"action": "probe_slice"}, MagicMock())
