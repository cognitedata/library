"""A run for some dimensions only: what the scope means, and that the plan, the pieces and the marker honour it."""

import json
from typing import Any

import pytest
from cq import runner
from cq.config import build_config
from cq.discovery import ENTITY_KEYS
from cq.metrics import KINDS, compute
from cq.scope import CONFIGURED, DIMENSION_KINDS, DIMENSIONS, Scope, filter_result, parse_scope
from parity_dataset import FACILITIES, A, build_dataset
from sample_rules import SAMPLE_RESOURCES
from test_fanned_out_run import CFG, RUN_ID, RUN_TS, data_of, plan, plan_world, shard_run, universe_tools
from test_runner import RecordingClient, props, records

ALL = frozenset(DIMENSIONS)


@pytest.fixture(autouse=True)
def datapoints(monkeypatch: pytest.MonkeyPatch) -> None:
    _data, latest, _now = build_dataset()
    monkeypatch.setattr(runner, "latest_datapoints", lambda _client, _data: latest)


def reading(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """`load_shard` as the reader does it: only the kinds asked for are read. -> the kinds each call asked for."""
    asked: list[Any] = []

    def load_shard(_client: Any, spaces: dict[str, str], kinds: Any = None) -> Any:
        asked.append(kinds)
        data = data_of(spaces)
        for key in ENTITY_KEYS:
            if kinds is not None and key not in kinds:
                data[key] = []
        return data

    monkeypatch.setattr(runner, "load_shard", load_shard)
    return asked


# ---------------------------------------------------------------- what a scope is


def test_without_a_scope_a_run_is_what_it_was_before_scopes() -> None:
    assert parse_scope(None, links_given=False) == Scope(ALL, False)
    assert parse_scope(None, links_given=True) == Scope(ALL, True)
    assert parse_scope("${workflow.input.scope}", links_given=False) == Scope(ALL, False)  # a reference not filled in


def test_all_is_every_dimension_without_the_configured_links() -> None:
    scope = parse_scope("all", links_given=True)

    assert scope == Scope(ALL, False)
    assert scope.kinds is None  # nothing is left out, so no kind is
    assert not scope.partial
    assert scope.label() == "all"


def test_a_scope_names_dimensions_and_whether_the_configured_links_are_measured() -> None:
    scope = parse_scope({"dimensions": ["files", "time-series"], "configured": True}, links_given=True)

    assert scope.partial
    assert scope.measured == {"files", "time-series", CONFIGURED}
    assert scope.label() == "time-series,files+configured"  # in the order of the dashboard
    assert scope.to_payload() == {"dimensions": ["time-series", "files"], "configured": True}
    assert parse_scope(scope.to_payload(), links_given=True) == scope


def test_only_the_configured_links_read_no_kind_of_data() -> None:
    scope = parse_scope("configured", links_given=True)

    assert scope.kinds == frozenset()
    assert scope.label() == "configured"
    assert scope.measured == {CONFIGURED}


def test_a_dimension_reads_the_kinds_it_needs_and_two_of_them_need_the_assets_too() -> None:
    assert parse_scope({"dimensions": ["equipment-asset"]}, None).kinds == {"equipment"}
    assert parse_scope({"dimensions": ["time-series"]}, None).kinds == {"timeseries", "assets"}
    assert parse_scope({"dimensions": ["model-3d"]}, None).kinds == {"objects3d", "assets"}
    assert parse_scope({"dimensions": ["maintenance"]}, None).kinds == {"work_orders", "notifications"}
    assert frozenset().union(*DIMENSION_KINDS.values()) == set(KINDS)  # every kind belongs to some dimension


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        ("everything", "scope must be"),
        ({"dimensions": ["nope"]}, "Unknown dimensions"),
        ({"dimensions": "files"}, "must be .all. or a list"),
        ({"dimensions": []}, "selects nothing"),
        ({"dimensions": ["files"], "configured": "yes"}, "true or false"),
        ({"dimension": ["files"]}, "Unknown scope keys"),
    ],
)
def test_a_scope_that_cannot_be_run_is_refused(raw: Any, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_scope(raw, links_given=True)


def test_the_configured_metrics_need_a_config_file() -> None:
    with pytest.raises(ValueError, match="config file"):
        parse_scope("configured", links_given=False)


def test_filtering_a_result_keeps_only_the_dimensions_of_the_scope() -> None:
    data, latest, now = build_dataset()
    result = compute(data, CFG, latest, now=now)  # type: ignore[arg-type]
    scope = parse_scope({"dimensions": ["equipment-asset", "files"]}, None)

    kept = filter_result(result, scope)

    assert {key[1] for key in kept["agg"]} == {"equipment-asset", "files"}
    assert {key[0] for key in kept["exclusions"]} <= {"equipment-asset", "files"}
    assert {key[3] for key in kept["classifications"]} <= {"equipment-asset", "files"}
    assert {item["dim"] for item in kept["drilldown"]} == {"equipment-asset", "files"}
    assert kept["agg"] == {key: value for key, value in result["agg"].items() if key[1] in scope.dimensions}
    assert filter_result(result, parse_scope("all", None)) is result


# ---------------------------------------------------------------- the pieces of a run


def dimensions_written(client: RecordingClient) -> set[str]:
    return {
        row["dimensionId"] for row in (props(item) for item in records(client)) if row.get("dimensionId") is not None
    }


def test_a_shard_for_one_dimension_reads_its_kinds_and_writes_only_that_dimension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = reading(monkeypatch)
    scoped, full = RecordingClient(), RecordingClient()

    shard_run(scoped, {A: FACILITIES[A]}, 0, scope={"dimensions": ["equipment-asset"], "configured": False})
    shard_run(full, {A: FACILITIES[A]}, 0)

    assert asked == [frozenset({"equipment"}), None]  # the full run reads every kind
    assert dimensions_written(scoped) == {"equipment-asset"}
    assert {"asset-hierarchy", "files", "equipment-asset"} <= dimensions_written(full)
    only_equipment = {
        item["externalId"]: props(item) for item in records(full) if props(item).get("dimensionId") == "equipment-asset"
    }
    assert {
        item["externalId"]: props(item)
        for item in records(scoped)
        if props(item).get("dimensionId") == "equipment-asset"
    } == only_equipment  # the same rows a full run writes for it


def test_a_shard_reads_the_assets_too_when_the_grain_of_an_entity_comes_from_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = reading(monkeypatch)
    cfg = build_config({**SAMPLE_RESOURCES, "divisionTagPrefix": "div:"})
    payload = {
        "runId": RUN_ID,
        "runTimestamp": RUN_TS,
        "shard": 0,
        "shardCount": 1,
        "spaces": {A: FACILITIES[A]},
        "scope": {"dimensions": ["equipment-asset"], "configured": False},
    }

    client = RecordingClient()

    runner.compute_shard(client, cfg, payload)  # type: ignore[arg-type]

    assert asked == [frozenset({"equipment", "assets"})]
    # but only the equipment is counted: the assets are there to name the division
    assert dimensions_written(client) == {"equipment-asset"}


def test_a_shard_for_only_the_configured_links_reads_no_entity(monkeypatch: pytest.MonkeyPatch) -> None:
    def load_shard(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no entity should be read")

    monkeypatch.setattr(runner, "load_shard", load_shard)
    client = RecordingClient()

    shard_run(
        client,
        {A: FACILITIES[A]},
        0,
        scope={"dimensions": [], "configured": True},
        extraLinks=[{"model": "cdf_cdm:CogniteCore:v1", "links": ["CogniteAsset.type"]}],
        extraSpaces={},
    )

    assert [props(item)["rowKind"] for item in records(client)] == ["runPart"]


def test_the_plan_marks_the_run_with_its_scope_and_hands_it_to_every_piece(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, by_kind={**{kind: {} for kind in KINDS}, "assets": {A: 40}, "equipment": {A: 20}})
    client = RecordingClient()

    out = plan(client, {"scope": {"dimensions": ["equipment-asset"], "configured": False}})

    assert out["scope"] == {"dimensions": ["equipment-asset"], "configured": False}
    started = [props(item) for item in records(client) if props(item)["rowKind"] == "run"]
    assert [(row["status"], row["scope"]) for row in started] == [("started", "equipment-asset")]
    for task in out["tasks"]:
        assert task["parameters"]["function"]["data"]["scope"] == out["scope"]


def test_the_plan_counts_only_the_kinds_of_the_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, by_kind={**{kind: {} for kind in KINDS}, "assets": {A: 40}, "equipment": {A: 20}})
    universe_tools(
        monkeypatch,
        {
            (A, "CogniteAsset"): [f"asset-{n:03d}" for n in range(40)],
            (A, "CogniteEquipment"): [f"eq-{n}" for n in range(20)],
        },
    )

    # the space holds 60 entities, over the target of 25: a full run cuts it by kind; a run for the equipment sees 20
    everything = plan(RecordingClient(), shardTargetEntities=25)
    scoped = plan(RecordingClient(), {"scope": {"dimensions": ["equipment-asset"]}}, shardTargetEntities=25)

    def actions(out: dict[str, Any]) -> list[str]:
        return [task["parameters"]["function"]["data"]["action"] for task in out["tasks"]]

    assert set(actions(everything)) == {"compute_batch"}
    assert actions(scoped) == ["compute_shard"]


def test_the_plan_does_not_warn_about_kinds_it_leaves_out(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, by_kind={**{kind: {} for kind in KINDS}, "assets": {A: 40}})
    client = RecordingClient()

    plan(client, {"scope": {"dimensions": ["equipment-asset"]}})

    (part_record,) = [props(i) for i in records(client) if props(i)["rowKind"] == "runPart"]
    subjects = {w["subject"] for w in json.loads(part_record["warnings"]) if w["code"] == "no-source-entities"}
    assert subjects == {"equipment"}  # the kind this run counts; files, time series... are not part of it


def test_a_run_without_the_configured_links_in_its_scope_carries_none(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    links = [{"model": "o:M:v1", "links": ["Asset.owner"]}]

    out = plan(RecordingClient(), {"scope": "all", "extraLinks": links})
    assert all(task["parameters"]["function"]["data"]["extraLinks"] == [] for task in out["tasks"])

    with_links = plan(RecordingClient(), {"scope": {"dimensions": "all", "configured": True}, "extraLinks": links})
    assert all(task["parameters"]["function"]["data"]["extraLinks"] == links for task in with_links["tasks"])


def test_a_scope_that_cannot_be_run_fails_the_plan_before_anything_is_written(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    client = RecordingClient()

    with pytest.raises(ValueError, match="config file"):
        plan(client, {"scope": "configured"})
    with pytest.raises(ValueError, match="Unknown dimensions"):
        plan(client, {"scope": {"dimensions": ["nope"]}})

    assert client.calls == []


def test_one_call_runs_everything_and_refuses_a_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    client = RecordingClient()

    with pytest.raises(ValueError, match="Use plan"):
        runner.compute_metrics(client, CFG, {"scope": {"dimensions": ["files"]}})  # type: ignore[arg-type]

    assert client.calls == []


def test_the_complete_marker_says_which_dimensions_the_run_measured() -> None:
    from test_fanned_out_run import ReadingClient, part

    client = ReadingClient([part(0, 5)])
    cfg = build_config({"stream": "s", "schemaSpace": "sp_schema", "instanceSpace": "sp_data"})
    scope = {"dimensions": ["files"], "configured": True}

    out = runner.finalize(
        client,  # type: ignore[arg-type]
        cfg,
        {"runId": RUN_ID, "runTimestamp": RUN_TS, "shardCount": 1, "scope": scope},
    )

    (marker,) = [props(item) for item in records(client) if props(item).get("status") == "complete"]
    assert marker["scope"] == "files+configured" == out["scope"]


def test_a_run_without_a_scope_is_marked_all() -> None:
    from test_fanned_out_run import ReadingClient, part

    client = ReadingClient([part(0, 5)])
    cfg = build_config({"stream": "s", "schemaSpace": "sp_schema", "instanceSpace": "sp_data"})

    runner.finalize(client, cfg, {"runId": RUN_ID, "runTimestamp": RUN_TS, "shardCount": 1})  # type: ignore[arg-type]

    (marker,) = [props(item) for item in records(client) if props(item).get("status") == "complete"]
    assert marker["scope"] == "all"


@pytest.mark.parametrize("dimension", DIMENSIONS)
def test_counting_only_the_kinds_of_a_dimension_gives_the_rows_a_full_count_gives_for_it(dimension: str) -> None:
    """The kinds each dimension is said to need are enough: nothing it measures depends on a kind that is left out."""
    data, latest, now = build_dataset()
    full = compute(data, CFG, latest, now=now)  # type: ignore[arg-type]
    scope = parse_scope({"dimensions": [dimension]}, None)

    scoped = filter_result(compute(data, CFG, latest, now=now, kinds=scope.kinds), scope)  # type: ignore[arg-type]

    for key in ("agg", "exclusions", "classifications"):
        position = {"agg": 1, "exclusions": 0, "classifications": 3}[key]
        assert scoped[key] == {k: v for k, v in full[key].items() if k[position] == dimension}, key
    assert sorted(map(str, scoped["drilldown"])) == sorted(
        str(item) for item in full["drilldown"] if item["dim"] == dimension
    )
