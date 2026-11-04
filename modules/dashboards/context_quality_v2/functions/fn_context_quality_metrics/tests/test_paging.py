"""Listing the ids of a slice in pages of our own size, when the service times out on the SDK's pages of 1000.

The pages are requested by hand, so what is checked is what the SDK cannot be trusted to check for us: that the request
is the one the SDK sends (apart from the limit), that every instance is listed exactly once whatever the page size and
however the service ends its pages, and that a timeout steps down the sizes and a different error does not.
"""

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import Token
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.exceptions import CogniteAPIError
from cq import resilience
from cq.batching import batch_filter, leaf_filter
from cq.reader import PAGE_SIZES, PAGE_STEP_SECONDS, _id_pairs, _id_pairs_in_pages, _list_instances, _read_slice

SLICE = leaf_filter(("prefix", "EQ-"))
VIEW = ViewId("cdf_cdm", "CogniteEquipment", "v1")


def timed_out() -> CogniteAPIError:
    return CogniteAPIError("Graph query timed out", code=408)


class Response:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body

    def json(self) -> dict[str, Any]:
        return self.body


class FakeService:
    """The instances list endpoint: pages by `limit` and an opaque cursor, and times out on a limit above `lightest`.

    `trailing_cursor` makes it end a full last page with a cursor, answered by an empty page, which a real service may
    do when the instances run out exactly at the end of a page.
    """

    def __init__(
        self,
        count: int,
        lightest: int = 1000,
        trailing_cursor: bool = False,
        fail_on_request: dict[int, int] | None = None,
    ) -> None:
        self.ids = [("sp", f"EQ-{n:05d}") for n in range(count)]
        self.lightest = lightest
        self.trailing_cursor = trailing_cursor
        self.fail_on_request = fail_on_request or {}  # the n-th request overall -> the code it fails with
        self.requests: list[dict[str, Any]] = []
        self.config = MagicMock(project="p")

    def post(self, url: str, json: dict[str, Any]) -> Response:
        assert url == "/api/v1/projects/p/models/instances/list"
        self.requests.append(json)
        assert len(self.requests) < 200, "the listing does not end"
        if (code := self.fail_on_request.get(len(self.requests))) is not None:
            raise CogniteAPIError("failed", code=code)
        if json["limit"] > self.lightest:
            raise timed_out()
        start = int(json.get("cursor") or 0)
        end = start + json["limit"]
        body: dict[str, Any] = {
            "items": [{"space": space, "externalId": xid} for space, xid in self.ids[start:end]],
        }
        if end < len(self.ids) or (self.trailing_cursor and end == len(self.ids) and self.ids):
            body["nextCursor"] = str(end)
        return Response(body)


def times_out_for_the_sdk(service: FakeService) -> Any:
    """A client whose SDK listing times out, and whose own requests go to `service`."""
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = timed_out()
    client.post = service.post
    client.config = service.config
    return client


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    pauses: list[float] = []
    monkeypatch.setattr(resilience, "sleep", pauses.append)
    return pauses


def test_the_pages_request_what_the_sdk_requests_apart_from_the_limit() -> None:
    """Captured from the installed SDK, so a change in how it builds the request shows up here."""
    global_config.disable_pypi_version_check = True
    sdk = CogniteClient(
        ClientConfig(client_name="t", project="p", credentials=Token("t"), base_url="https://example.cognitedata.com")
    )
    sent: list[dict[str, Any]] = []

    def capture(url_path: str, json: dict[str, Any], **_: Any) -> Response:
        sent.append({"url": url_path, **json})
        return Response({"items": []})

    sdk.data_modeling.instances._post = capture  # type: ignore[method-assign]
    ours = FakeService(0)
    for instance_type in ("node", "edge"):
        for spaces in (None, ["sp_a"], ["sp_a", "sp_b"]):
            for part in (SLICE, SLICE.dump(), batch_filter([("prefix", "A"), ("exact", "B")]).dump()["or"][1]):
                sent.clear()
                ours.requests.clear()
                sdk.data_modeling.instances.list(
                    instance_type=instance_type,
                    space=spaces,
                    filter=part,
                    sort=[InstanceSort([instance_type, "space"]), InstanceSort([instance_type, "externalId"])],
                    limit=-1,
                )
                _id_pairs_in_pages(ours, instance_type, spaces, part, 250)  # type: ignore[arg-type]

                asked, mine = sent[0], ours.requests[0]
                same = {key: value for key, value in asked.items() if key not in ("url", "limit", "cursor")}
                assert asked["url"].endswith("/models/instances/list")
                assert {key: value for key, value in mine.items() if key not in ("limit", "cursor")} == same
                assert (asked["limit"], mine["limit"]) == (1000, 250)


@pytest.mark.parametrize("page", PAGE_SIZES)
@pytest.mark.parametrize("count", [0, 1, 99, 100, 101, 250, 500, 1000, 1234])
@pytest.mark.parametrize("trailing_cursor", [False, True])
def test_every_instance_is_listed_once_in_order_whatever_the_page_size(
    page: int, count: int, trailing_cursor: bool
) -> None:
    service = FakeService(count, trailing_cursor=trailing_cursor)

    pairs = _id_pairs_in_pages(service, "node", ["sp"], SLICE, page)  # type: ignore[arg-type]

    assert pairs == service.ids
    assert all(request["limit"] == page for request in service.requests)


def test_the_first_request_has_no_cursor_and_each_next_one_has_the_cursor_it_was_given() -> None:
    service = FakeService(350)

    _id_pairs_in_pages(service, "node", ["sp"], SLICE, 100)  # type: ignore[arg-type]

    assert ["cursor" in request for request in service.requests] == [False, True, True, True]
    assert [request.get("cursor") for request in service.requests] == [None, "100", "200", "300"]


def test_the_request_is_not_changed_from_one_page_to_the_next() -> None:
    service = FakeService(350)

    _id_pairs_in_pages(service, "node", ["sp"], SLICE, 100)  # type: ignore[arg-type]

    first, *rest = ({key: value for key, value in request.items() if key != "cursor"} for request in service.requests)
    assert all(json.dumps(request, sort_keys=True) == json.dumps(first, sort_keys=True) for request in rest)


def test_a_busy_project_inside_the_pages_is_waited_for_like_any_other_read(no_sleep: list[float]) -> None:
    service = FakeService(250, fail_on_request={2: 429})  # the second page of 100 is refused once

    pairs = _id_pairs_in_pages(service, "node", ["sp"], SLICE, 100)  # type: ignore[arg-type]

    assert pairs == service.ids
    assert no_sleep == [1.0]


def test_a_listing_the_sdk_manages_is_not_requested_by_hand() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.return_value = [MagicMock(space="sp", external_id="EQ-1")]

    assert _id_pairs(client, "node", ["sp"], SLICE) == [("sp", "EQ-1")]
    client.post.assert_not_called()


def test_a_timeout_steps_down_the_page_size_until_the_service_manages() -> None:
    service = FakeService(1234, lightest=250)

    pairs = _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    assert pairs == service.ids  # nothing left out, nothing twice, although 500 got no further than its first page
    assert [request["limit"] for request in service.requests[:3]] == [500, 500, 250]  # 408 is tried twice, then 250
    assert {request["limit"] for request in service.requests[3:]} == {250}


def test_a_page_that_times_out_in_the_middle_starts_the_listing_over_at_the_next_size() -> None:
    class SlowerDeeper(FakeService):
        """A busy spell that only the later pages meet: the first page of any size answers."""

        def post(self, url: str, json: dict[str, Any]) -> Response:
            if json.get("cursor") and json["limit"] >= 250:
                self.requests.append(json)
                raise timed_out()
            return super().post(url, json)

    service = SlowerDeeper(1000)

    pairs = _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    assert pairs == service.ids
    assert {request["limit"] for request in service.requests} == {500, 250, 100}
    assert service.requests[-1]["limit"] == 100 and "cursor" in service.requests[-1]


def test_a_service_that_times_out_at_every_size_raises_the_timeout() -> None:
    service = FakeService(1000, lightest=50)

    with pytest.raises(CogniteAPIError) as error:
        _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    assert error.value.code == 408
    assert sorted({request["limit"] for request in service.requests}, reverse=True) == list(PAGE_SIZES)


@pytest.mark.parametrize("code", [400, 403, 404, 500])
def test_an_error_that_is_not_a_timeout_does_not_step_down(code: int) -> None:
    service = FakeService(1000, fail_on_request={1: code})

    with pytest.raises(CogniteAPIError) as error:
        _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    assert error.value.code == code
    assert len(service.requests) == 1


def test_an_error_that_is_not_a_timeout_on_the_sdk_listing_is_raised_before_any_page_is_requested() -> None:
    client = MagicMock()
    client.data_modeling.instances.list.side_effect = CogniteAPIError("no", code=403)

    with pytest.raises(CogniteAPIError):
        _id_pairs(client, "node", ["sp"], SLICE)

    client.post.assert_not_called()


def test_a_slice_whose_reads_all_time_out_at_1000_is_read_through_the_pages() -> None:
    """The case of a failed run: the read with the "has data" filter and the listing of ids both timed out."""
    service = FakeService(1234, lightest=250)
    client = times_out_for_the_sdk(service)
    client.data_modeling.instances.retrieve.side_effect = lambda nodes, sources: MagicMock(nodes=list(nodes))

    found = _read_slice(client, "node", VIEW, ["sp"], SLICE)

    assert found == service.ids
    assert client.data_modeling.instances.list.call_count == 2  # the read, then the listing of the ids, both at 1000


def test_each_slice_of_a_batch_is_listed_on_its_own_through_the_pages() -> None:
    """A batch is an OR of slices; each is read separately, so every slice gets the steps of its own."""
    service = FakeService(600, lightest=250)
    client = times_out_for_the_sdk(service)
    client.data_modeling.instances.retrieve.side_effect = lambda nodes, sources: MagicMock(nodes=list(nodes))
    batch = batch_filter([("prefix", "EQ-0"), ("prefix", "EQ-1")])

    found = _list_instances(client, "node", VIEW, ["sp"], batch)

    assert found == service.ids * 2  # the fake service answers each slice with all it has
    sizes = [request["limit"] for request in service.requests]
    assert sizes == [500, 500, 250, 250, 250] * 2  # per slice: 500 times out twice, then 250 takes three pages


def test_no_smaller_page_is_started_when_the_call_has_too_little_time_left(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resilience, "_call_started", None)
    now = [0.0]
    monkeypatch.setattr(resilience, "clock", lambda: now[0])
    resilience.begin_call()
    now[0] = resilience.CALL_BUDGET - PAGE_STEP_SECONDS + 1.0
    service = FakeService(1000, lightest=50)

    with pytest.raises(CogniteAPIError) as error:
        _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    assert error.value.code == 408
    assert service.requests == []


def test_the_steps_stop_where_the_time_of_the_call_runs_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every request that times out takes 130 seconds of the call; the call has 540, and a size needs 60 to start."""
    monkeypatch.setattr(resilience, "_call_started", None)
    now = [0.0]
    monkeypatch.setattr(resilience, "clock", lambda: now[0])
    resilience.begin_call()
    service = FakeService(1000, lightest=50)
    post = service.post

    def slow(url: str, json: dict[str, Any]) -> Response:
        now[0] += 130.0
        return post(url, json)

    service.post = slow  # type: ignore[method-assign]

    with pytest.raises(CogniteAPIError):
        _id_pairs(times_out_for_the_sdk(service), "node", ["sp"], SLICE)

    # 500 fails at 130 s and again (the retry) at 260 s, 250 at 390 s and 520 s: 20 s are left, and a size needs 60
    assert [request["limit"] for request in service.requests] == [500, 500, 250, 250]
