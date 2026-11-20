"""`probe_slice` times the requests a batch makes for one slice, and writes nothing."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import handler
import pytest
from cognite.client.exceptions import CogniteAPIError
from cq import resilience


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resilience, "sleep", lambda _seconds: None)


def client_with(list_result: Any, count: int = 7) -> MagicMock:
    client = MagicMock()
    client.data_modeling.instances.aggregate.return_value = SimpleNamespace(value=count)
    client.data_modeling.instances.list.return_value = list_result
    client.config.project = "p"
    client.post.return_value.json.return_value = {"items": [{"space": "sp", "externalId": "30"}], "nextCursor": None}
    return client


def test_a_probe_reports_each_request_and_changes_nothing() -> None:
    page = [SimpleNamespace(external_id=f"3{index}") for index in range(5)]
    client = client_with(page)

    out = handler.handle({"action": "probe_slice", "space": "sp", "kind": "assets", "prefix": "3"}, client)

    steps = out["steps"]
    assert steps["count of the kind with the prefix"]["count"] == 7
    assert steps["first 1000 ids of the space with the prefix, any kind"]["returned"] == 5
    assert steps["first 1000 ids of the space with the prefix, any kind"]["sample"] == ["30", "31", "32"]
    assert steps["ids of the space with the prefix, any kind, at most 30 pages"]["ids"] == 1
    assert all("seconds" in step for step in steps.values())


def test_a_request_the_service_times_out_on_is_reported_not_raised() -> None:
    client = client_with([])
    client.data_modeling.instances.list.side_effect = CogniteAPIError("timed out", code=408)

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "3"}, client)

    assert out["steps"]["100 with properties, prefix, no sort"]["error"] == 408


def test_a_probe_needs_a_space() -> None:
    with pytest.raises(ValueError, match="needs space"):
        handler.handle({"action": "probe_slice"}, MagicMock())


def test_a_payload_with_nulls_uses_the_defaults() -> None:
    out = handler.handle({"action": "probe_slice", "space": "sp", "kind": None, "prefix": None}, client_with([]))

    assert out["steps"]["count of the kind with the prefix"]["count"] == 7


def test_a_kind_that_does_not_exist_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown kind 'pumps'"):
        handler.handle({"action": "probe_slice", "space": "sp", "kind": "pumps"}, MagicMock())


def test_a_busy_service_is_retried_when_listing_ids() -> None:
    client = client_with([])
    page = {"items": [{"space": "sp", "externalId": "30"}], "nextCursor": None}
    client.post.return_value.json.return_value = page
    client.post.side_effect = [CogniteAPIError("busy", code=429), client.post.return_value]

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "3"}, client)

    assert out["steps"]["ids of the space with the prefix, any kind, at most 30 pages"]["ids"] == 1


def test_a_prefix_that_fails_is_listed_beside_the_ones_that_answered() -> None:
    def list_ids(**options: Any) -> list[Any]:
        prefix = getattr(options.get("filter"), "_value", None)
        if prefix == "34":
            raise CogniteAPIError("timed out", code=408)
        return [SimpleNamespace(external_id="30")]

    client = client_with([])
    client.data_modeling.instances.list.side_effect = list_ids

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "3"}, client)

    sizes = out["steps"]["ids of the space per next character"]["ids per next character, capped at 1000"]
    assert sizes["4"] == "error 408"
    assert sizes["0"] == 1


def test_a_slice_with_no_instances_is_probed_without_retrieving_any() -> None:
    client = client_with([])
    client.post.return_value.json.return_value = {"items": [], "nextCursor": None}
    client.data_modeling.instances.retrieve.side_effect = ValueError("nothing to retrieve")

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "9"}, client)

    step = out["steps"]["properties of the first 500 of those ids, read by id"]
    assert (step["asked"], step["with data in the view"]) == (0, 0)
    client.data_modeling.instances.retrieve.assert_not_called()


def test_a_prefix_that_is_not_text_is_rejected() -> None:
    with pytest.raises(ValueError, match="prefix must be a string, not int"):
        handler.handle({"action": "probe_slice", "space": "sp", "prefix": 3}, MagicMock())


def test_an_instance_without_properties_is_not_counted_as_having_data() -> None:
    client = client_with([])
    client.data_modeling.instances.retrieve.return_value = SimpleNamespace(
        nodes=[SimpleNamespace(properties=None), SimpleNamespace(properties={})]
    )

    out = handler.handle({"action": "probe_slice", "space": "sp", "prefix": "3"}, client)

    step = out["steps"]["properties of the first 500 of those ids, read by id"]
    assert (step["asked"], step["with data in the view"]) == (1, 0)


def test_a_prefix_that_ends_in_the_last_code_point_is_probed_without_a_range() -> None:
    out = handler.handle(
        {"action": "probe_slice", "space": "sp", "prefix": "3\U0010ffff"},
        client_with([SimpleNamespace(external_id="30")]),
    )

    assert "100 with properties, range, sorted" not in out["steps"]
    assert "count of the kind with the prefix" in out["steps"]
