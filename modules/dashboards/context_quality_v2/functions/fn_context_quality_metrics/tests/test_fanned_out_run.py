"""A run split into plan, shards and finalize: the same numbers as one call, and a run that completes only when whole."""

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from cq import runner
from cq.availability import encode_warnings
from cq.config import build_config
from cq.discovery import ENTITY_KEYS
from cq.metrics import compute
from parity_dataset import FACILITIES, A, B, build_dataset
from sample_rules import SAMPLE_RESOURCES
from test_runner import RecordingClient, props, records

FUNCTION = "fn_context_quality_metrics"
CORE = "cdf_cdm:CogniteCore:v1"
RUN_ID, RUN_TS = "run-20261013T053425-c2326a", "2026-10-13T05:34:25.000Z"
CFG = build_config(SAMPLE_RESOURCES)


def data_of(spaces: dict[str, str]) -> dict[str, Any]:
    """The parity dataset restricted to some facility spaces, as `load_shard` would return it."""
    data, _latest, _now = build_dataset()
    for key in ENTITY_KEYS:
        data[key] = [entity for entity in data[key] if entity["space"] in spaces]
    data["facility_spaces"] = dict(spaces)
    return data


def shard_run(client: RecordingClient, spaces: dict[str, str], shard: int, **extra: Any) -> dict[str, Any]:
    payload = {
        "runId": RUN_ID,
        "runTimestamp": RUN_TS,
        "shard": shard,
        "shardCount": 2,
        "spaces": spaces,
        "extraSpaces": spaces,
        **extra,
    }
    return runner.compute_shard(client, CFG, payload)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def shard_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    _data, latest, _now = build_dataset()
    monkeypatch.setattr(runner, "load_shard", lambda _client, spaces: data_of(spaces))
    monkeypatch.setattr(runner, "latest_datapoints", lambda _client, _data: latest)


def test_counting_each_facility_space_alone_gives_the_numbers_of_counting_them_together() -> None:
    whole_data, latest, now = build_dataset()
    whole = compute(whole_data, CFG, latest, now=now)  # type: ignore[arg-type]
    parts = [compute(data_of({space: name}), CFG, latest, now=now) for space, name in FACILITIES.items()]  # type: ignore[arg-type]

    for key in ("agg", "exclusions", "annotation_buckets", "classifications"):
        together = {k: v for part in parts for k, v in part[key].items()}
        assert together == dict(whole[key]), key
    assert sorted(map(str, (item for part in parts for item in part["drilldown"]))) == sorted(
        map(str, whole["drilldown"])
    )


def test_a_shard_writes_the_rows_of_its_own_spaces_and_a_part_record_last() -> None:
    client = RecordingClient()

    summary = shard_run(client, {A: FACILITIES[A]}, 0)

    items = records(client)
    kinds = [props(item)["rowKind"] for item in items]
    assert kinds[-1] == "runPart"
    assert kinds.count("runPart") == 1
    units = {props(item).get("reportingUnitId") for item in items[:-1]} - {None}
    assert units == {A}
    part = props(items[-1])
    assert part["detail"] == "metrics"
    assert part["entityId"] == "shard-0"
    assert part["rowsWritten"] == len(items) - 1 == summary["rowsWritten"]
    assert items[-1]["externalId"] == f"{RUN_ID}:part:0000"
    assert {props(item)["runId"] for item in items} == {RUN_ID}


def test_two_shards_never_write_the_same_record() -> None:
    first, second = RecordingClient(), RecordingClient()

    shard_run(first, {A: FACILITIES[A]}, 0)
    shard_run(second, {B: FACILITIES[B]}, 1)

    ids = [item["externalId"] for item in records(first)], [item["externalId"] for item in records(second)]
    assert not set(ids[0]) & set(ids[1])


def test_a_shard_with_only_configured_links_counts_those_and_is_marked_so(monkeypatch: pytest.MonkeyPatch) -> None:
    ref = {"space": "r", "externalId": "t"}
    monkeypatch.setattr(
        runner,
        "load_extra_links",
        lambda _client, _links, spaces: {
            (CORE, "CogniteAsset"): [("sp_ucp", "a1", {"type": ref}), ("sp_ucp", "a2", {})]
        },
    )
    client = RecordingClient()

    shard_run(
        client,
        {},
        1,
        extraSpaces={"sp_ucp": "ucp"},
        extraLinks=[{"model": CORE, "links": ["CogniteAsset.type"]}],
    )

    items = records(client)
    kinds = [props(item)["rowKind"] for item in items]
    assert kinds == ["extraLink", "extraLinkGap", "runPart"]
    assert props(items[0])["issueCount"] == 1
    assert props(items[-1])["detail"] == "links"


def test_a_view_the_plan_found_empty_everywhere_is_marked_not_applicable_in_every_facility_of_the_shard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "load_extra_links", lambda _client, _links, spaces: {(CORE, "CogniteAsset"): []})
    client = RecordingClient()

    shard_run(
        client,
        {},
        0,
        extraSpaces={"sp_a": "a", "sp_b": "b"},
        extraLinks=[{"model": CORE, "links": ["CogniteAsset.type"]}],
        emptyLinkViews=[[CORE, "CogniteAsset"]],
    )

    rows = [props(item) for item in records(client) if props(item)["rowKind"] == "extraLink"]
    assert sorted(row["reportingUnitId"] for row in rows) == ["sp_a", "sp_b"]
    assert all(row["notApplicable"] is True for row in rows)


def test_a_view_that_is_empty_here_but_not_elsewhere_gets_no_row_in_this_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "load_extra_links", lambda _client, _links, spaces: {(CORE, "CogniteAsset"): []})
    client = RecordingClient()

    shard_run(client, {}, 0, extraSpaces={"sp_a": "a"}, extraLinks=[{"model": CORE, "links": ["CogniteAsset.type"]}])

    assert [props(item)["rowKind"] for item in records(client)] == ["runPart"]


# ---------------------------------------------------------------- plan


def plan_world(
    monkeypatch: pytest.MonkeyPatch,
    entity_counts: dict[str, int] | None = None,
    views: dict[tuple[str, str], Any] | None = None,
    link_counts: dict[tuple[str, str], dict[str, int]] | None = None,
) -> None:
    counts = entity_counts if entity_counts is not None else {A: 40, B: 30, "sp_ref": 3}
    monkeypatch.setattr(runner, "count_entities_by_space_remote", lambda _client: (counts, []))
    monkeypatch.setattr(runner, "validate_extra_links", lambda _client, _links: views or {})
    monkeypatch.setattr(runner, "count_extra_link_instances", lambda _client, _views: link_counts or {})


def plan(client: RecordingClient, payload: dict[str, Any] | None = None, **config: Any) -> dict[str, Any]:
    cfg = build_config({**SAMPLE_RESOURCES, **config})
    return runner.plan(client, cfg, {"functionExternalId": FUNCTION, **(payload or {})})  # type: ignore[arg-type]


def test_the_plan_starts_the_run_and_returns_one_task_per_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    client = RecordingClient()

    out = plan(client, shardTargetEntities=30)

    items = records(client)
    assert [props(item)["status"] for item in items] == ["started"]
    assert props(items[0])["runId"] == out["runId"]
    assert out["shardCount"] == len(out["tasks"]) == 2
    assert out["facilities"] == 2
    spaces = [sorted(task["parameters"]["function"]["data"]["spaces"]) for task in out["tasks"]]
    assert sorted(space for group in spaces for space in group) == [A, B]  # the reference space is no facility
    for index, task in enumerate(out["tasks"]):
        data = task["parameters"]["function"]["data"]
        assert task["parameters"]["function"]["externalId"] == FUNCTION
        assert (data["action"], data["shard"], data["shardCount"]) == ("compute_shard", index, 2)
        assert (data["runId"], data["runTimestamp"]) == (out["runId"], out["runTimestamp"])


def test_small_spaces_share_a_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)

    out = plan(RecordingClient())

    assert out["shardCount"] == 1
    assert sorted(out["tasks"][0]["parameters"]["function"]["data"]["spaces"]) == [A, B]


def test_every_shard_carries_the_configuration_and_the_extra_links(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    links = [{"model": CORE, "links": ["CogniteAsset.type"]}]

    out = plan(RecordingClient(), {"config": {"freshnessDays": 7}, "extraLinks": links}, shardTargetEntities=1)

    for task in out["tasks"]:
        data = task["parameters"]["function"]["data"]
        assert data["config"] == {"freshnessDays": 7}
        assert data["extraLinks"] == links


def test_a_space_that_only_holds_configured_views_becomes_a_facility_of_its_own_shard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_world(
        monkeypatch,
        views={("o:M:v1", "Ucp"): object()},
        link_counts={("o:M:v1", "Ucp"): {"sp_ucp": 12}},
    )
    links = [{"model": "o:M:v1", "links": ["Ucp.owner"]}]

    out = plan(RecordingClient(), {"extraLinks": links}, shardTargetEntities=30)

    by_space = {
        space: task["parameters"]["function"]["data"]
        for task in out["tasks"]
        for space in task["parameters"]["function"]["data"]["extraSpaces"]
    }
    assert set(by_space) == {A, B, "sp_ucp"}
    assert "sp_ucp" not in by_space["sp_ucp"]["spaces"]  # no built-in metrics there
    assert out["facilities"] == 3


def test_a_configured_view_with_no_instances_in_any_facility_is_passed_on_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_world(
        monkeypatch,
        views={("o:M:v1", "Ucp"): object(), ("o:M:v1", "Full"): object()},
        link_counts={("o:M:v1", "Ucp"): {}, ("o:M:v1", "Full"): {A: 20}},
    )
    links = [{"model": "o:M:v1", "links": ["Ucp.owner", "Full.owner"]}]

    out = plan(RecordingClient(), {"extraLinks": links})

    assert out["tasks"][0]["parameters"]["function"]["data"]["emptyLinkViews"] == [["o:M:v1", "Ucp"]]


def test_the_plan_needs_to_know_which_function_runs_the_shards(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    client = RecordingClient()

    with pytest.raises(ValueError, match="functionExternalId"):
        runner.plan(client, CFG, {})  # type: ignore[arg-type]

    assert client.calls == []


def test_no_facility_means_no_run_and_nothing_written(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, entity_counts={"sp_ref": 3})
    client = RecordingClient()

    with pytest.raises(runner.MissingInputError, match="No facility spaces"):
        plan(client)

    assert client.calls == []


def test_a_wrong_link_fails_the_plan_before_the_run_is_started(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)

    def reject(_client: Any, _links: Any) -> None:
        raise ValueError("The view Nope is not in o:M:v1")

    monkeypatch.setattr(runner, "validate_extra_links", reject)
    client = RecordingClient()

    with pytest.raises(ValueError, match="Nope"):
        plan(client, {"extraLinks": [{"model": "o:M:v1", "links": ["Nope.owner"]}]})

    assert client.calls == []


# ---------------------------------------------------------------- finalize


class ReadingClient(RecordingClient):
    """Also answers the Records filter the way the stream does: the `runPart` records written so far."""

    def __init__(self, parts: list[dict[str, Any]]) -> None:
        super().__init__()
        self.parts = parts
        self.filters: list[dict[str, Any]] = []

    def post(self, url: str, json: dict[str, Any]) -> Any:  # type: ignore[override]
        if url.endswith("/records/filter"):
            self.filters.append(json)
            items = [{"properties": {"sp_schema": {"ContextQualityRelationship": part}}} for part in self.parts]
            return type("Response", (), {"json": lambda _self: {"items": items}})()
        return super().post(url, json)


def part(
    shard: int, rows: int, warnings: list[dict[str, Any]] | None = None, detail: str = "metrics"
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "rowKind": "runPart",
        "runId": RUN_ID,
        "entityId": f"shard-{shard}",
        "detail": detail,
        "rowsWritten": rows,
    }
    if warnings:
        out["warnings"] = encode_warnings(warnings)
    return out


def finalize(client: RecordingClient, shards: int = 2) -> dict[str, Any]:
    cfg = build_config({"stream": "s", "schemaSpace": "sp_schema", "instanceSpace": "sp_data"})
    payload = {"runId": RUN_ID, "runTimestamp": RUN_TS, "shardCount": shards}
    return runner.finalize(client, cfg, payload)  # type: ignore[arg-type]


def test_the_run_is_completed_when_every_shard_has_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ReadingClient([part(1, 30), part(0, 12)])

    out = finalize(client)

    (item,) = records(client)
    marker = props(item)
    assert item["externalId"] == f"{RUN_ID}:run:complete"
    assert (marker["rowKind"], marker["status"], marker["runId"]) == ("run", "complete", RUN_ID)
    assert marker["rowsWritten"] == 42 == out["rowsWritten"]
    assert "warnings" not in marker


def test_the_warnings_of_the_shards_are_merged_into_the_marker() -> None:
    cut = {
        "code": "drilldown-truncated",
        "message": "m",
        "subject": "orphansByTier",
        "limit": 500,
        "dropped": 4,
        "units": 1,
    }
    client = ReadingClient([part(0, 5, [cut]), part(1, 5, [{**cut, "dropped": 6}])])

    finalize(client)

    (item,) = records(client)
    (warning,) = json.loads(props(item)["warnings"])
    assert (warning["code"], warning["dropped"], warning["units"], warning["limit"]) == (
        "drilldown-truncated",
        10,
        2,
        500,
    )


def test_the_part_records_of_this_run_are_looked_up_in_the_run_log() -> None:
    client = ReadingClient([part(0, 1)])

    finalize(client, shards=1)

    (query,) = client.filters
    equals = {tuple(f["equals"]["property"]): f["equals"]["value"] for f in query["filter"]["and"]}
    assert equals == {
        ("sp_schema", "ContextQualityRelationship", "rowKind"): "runPart",
        ("sp_schema", "ContextQualityRelationship", "runId"): RUN_ID,
    }
    assert query["sources"][0]["source"]["externalId"] == "ContextQualityRelationship"
    assert query["lastUpdatedTime"]["gt"] < RUN_TS < query["lastUpdatedTime"]["lt"]


def test_a_run_with_a_shard_missing_is_not_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "PART_WAIT_SECONDS", 0)
    client = ReadingClient([part(0, 12)])

    with pytest.raises(runner.MissingInputError, match=r"1 of 2 shards.*\[1\]"):
        finalize(client)

    assert records(client) == []


def test_finalize_waits_for_a_shard_whose_record_is_not_searchable_yet(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ReadingClient([part(0, 12)])
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        client.parts.append(part(1, 3))

    monkeypatch.setattr(runner.time, "sleep", sleep)

    out = finalize(client)

    assert slept == [runner.PART_POLL_SECONDS]
    assert out["rowsWritten"] == 15


def test_nothing_found_is_not_reported_by_a_run_where_a_shard_found_something() -> None:
    nothing = {"code": "no-source-entities", "message": "m", "subject": "work_orders"}
    client = ReadingClient([part(0, 5, [nothing]), part(1, 5)])

    finalize(client)

    (item,) = records(client)
    assert "warnings" not in props(item)


def test_the_time_of_the_run_is_the_time_of_the_plan() -> None:
    client = ReadingClient([part(0, 1)])

    finalize(client, shards=1)

    (item,) = records(client)
    assert props(item)["runTimestamp"] == RUN_TS
    assert datetime.fromisoformat(RUN_TS.replace("Z", "+00:00")).tzinfo == UTC
