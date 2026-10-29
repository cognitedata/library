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
from cq.reader import KIND_VIEWS
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
    assert part["detail"] == "shard"
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
    assert props(items[-1])["detail"] == "shard"


def test_a_shard_never_fills_in_a_view_with_no_instances_because_the_plan_does(monkeypatch: pytest.MonkeyPatch) -> None:
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
    by_kind: dict[str, dict[str, int]] | None = None,
) -> None:
    counts = entity_counts if entity_counts is not None else {A: 40, B: 30, "sp_ref": 3}
    by_kind = by_kind or {**{kind: {} for kind in KIND_VIEWS}, "assets": counts}
    monkeypatch.setattr(runner, "count_kinds_by_space_remote", lambda _client: (by_kind, []))
    monkeypatch.setattr(runner, "validate_extra_links", lambda _client, _links: views or {})
    monkeypatch.setattr(runner, "count_extra_link_instances", lambda _client, _views: link_counts or {})


def plan(client: RecordingClient, payload: dict[str, Any] | None = None, **config: Any) -> dict[str, Any]:
    cfg = build_config({**SAMPLE_RESOURCES, **config})
    return runner.plan(client, cfg, {"functionExternalId": FUNCTION, **(payload or {})})  # type: ignore[arg-type]


def test_the_plan_starts_the_run_and_returns_one_task_per_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    client = RecordingClient()

    out = plan(client, shardTargetEntities=45)

    items = records(client)
    assert [props(item)["status"] for item in items if props(item)["rowKind"] == "run"] == ["started"]
    assert {props(item)["runId"] for item in items} == {out["runId"]}
    assert out["shardCount"] == len(out["tasks"]) == 2
    assert out["facilities"] == 2
    spaces = [sorted(task["parameters"]["function"]["data"]["spaces"]) for task in out["tasks"]]
    assert sorted(space for group in spaces for space in group) == [A, B]  # the reference space is no facility
    for index, task in enumerate(out["tasks"]):
        data = task["parameters"]["function"]["data"]
        assert task["parameters"]["function"]["externalId"] == FUNCTION
        assert (data["action"], data["shard"], data["shardCount"]) == ("compute_shard", index, 2)
        assert (data["runId"], data["runTimestamp"]) == (out["runId"], out["runTimestamp"])


def test_the_plan_writes_the_locations_that_cover_the_facilities(monkeypatch: pytest.MonkeyPatch) -> None:
    from cq import locations

    plan_world(monkeypatch)
    monkeypatch.setattr(
        locations,
        "list_locations",
        lambda _client: [{"id": 1, "externalId": "site", "name": "Site", "instanceSpaces": [A]}],
    )
    client = RecordingClient()

    out = plan(client)

    rows = [props(item) for item in records(client) if props(item)["rowKind"] == "location"]
    assert [row["entityId"] for row in rows] == ["site"]
    assert out["locations"] == 1


def test_small_spaces_share_a_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)

    out = plan(RecordingClient())

    assert out["shardCount"] == 1
    (task,) = out["tasks"]
    assert sorted(task["parameters"]["function"]["data"]["spaces"]) == [A, B]
    assert task["name"] == "Context Quality shard 1 of 1"  # not "of 2": there are two spaces but one shard
    assert task["parameters"]["function"]["data"]["shardCount"] == 1


def test_every_shard_carries_the_configuration_and_the_extra_links(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch)
    links = [{"model": CORE, "links": ["CogniteAsset.type"]}]

    out = plan(RecordingClient(), {"config": {"freshnessDays": 7}, "extraLinks": links}, shardTargetEntities=45)

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

    out = plan(RecordingClient(), {"extraLinks": links}, shardTargetEntities=45)

    by_space = {
        space: task["parameters"]["function"]["data"]
        for task in out["tasks"]
        for space in task["parameters"]["function"]["data"]["extraSpaces"]
    }
    assert set(by_space) == {A, B, "sp_ucp"}
    assert "sp_ucp" not in by_space["sp_ucp"]["spaces"]  # no built-in metrics there
    assert out["facilities"] == 3


def test_the_plan_writes_the_rows_of_a_configured_view_with_no_instances_in_any_facility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_world(
        monkeypatch,
        views={("o:M:v1", "Ucp"): object(), ("o:M:v1", "Full"): object()},
        link_counts={("o:M:v1", "Ucp"): {}, ("o:M:v1", "Full"): {A: 20}},
    )
    links = [{"model": "o:M:v1", "links": ["Ucp.owner", "Full.owner"]}]
    client = RecordingClient()

    plan(client, {"extraLinks": links})

    rows = [props(item) for item in records(client) if props(item)["rowKind"] == "extraLink"]
    assert sorted((row["dimensionId"], row["reportingUnitId"], row["notApplicable"]) for row in rows) == [
        ("o:M:v1/Ucp", A, True),
        ("o:M:v1/Ucp", B, True),
    ]


def test_the_plan_reports_a_kind_that_no_facility_holds_and_the_part_counts_its_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_world(monkeypatch)
    client = RecordingClient()

    plan(client)

    (part_record,) = [props(i) for i in records(client) if props(i)["rowKind"] == "runPart"]
    assert part_record["entityId"] == "plan"
    assert part_record["detail"] == "plan"
    names = {w["subject"] for w in json.loads(part_record["warnings"]) if w["code"] == "no-source-entities"}
    assert "equipment" in names
    assert "assets" not in names


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


# ---------------------------------------------------------------- batches of a big space


def universe_tools(monkeypatch: pytest.MonkeyPatch, ids_by_view: dict[tuple[str, str], list[str]]) -> None:
    """The plan looks at a big kind only by counting and sampling; here the instances are a list of ids."""
    from test_batching import Universe

    universes = {key: Universe(ids) for key, ids in ids_by_view.items()}

    def leaf_tools(_client: Any, view: Any, space: str, *, edge: bool = False) -> Any:
        universe = universes[(space, view.external_id)]
        return universe.count_leaves, universe.sample_id

    monkeypatch.setattr(runner, "leaf_tools", leaf_tools)


def test_a_space_over_the_target_is_cut_into_batches_that_cover_every_instance_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_batching import matches

    ids = [f"asset-{n:04d}" for n in range(600)]
    plan_world(
        monkeypatch,
        by_kind={**{kind: {} for kind in KIND_VIEWS}, "assets": {A: 600, B: 30}, "equipment": {A: 50}},
    )
    universe_tools(monkeypatch, {(A, "CogniteAsset"): ids})
    client = RecordingClient()

    out = plan(client, shardTargetEntities=200)

    tasks = [t["parameters"]["function"]["data"] for t in out["tasks"]]
    batches = [d for d in tasks if d["action"] == "compute_batch"]
    asset_slices = [d for d in batches if d["kinds"] == ["assets"]]
    assert len(asset_slices) >= 3
    assert out["batches"] == len(batches)
    assert all(leaf_set["space"] == A for leaf_set in batches)
    # every asset is in exactly one slice
    for xid in ids:
        assert sum(matches((kind, text), xid) for d in asset_slices for kind, text in d["leaves"]) == 1
    assert {d["groupBatches"] for d in asset_slices} == {len(asset_slices)}
    # the small kind of the big space is counted whole, and the small space stays a shard
    assert any(d["kinds"] == ["equipment"] and d["leaves"] is None for d in batches)
    (shard,) = [d for d in tasks if d["action"] == "compute_shard"]
    assert list(shard["spaces"]) == [B]
    assert {d["shardCount"] for d in tasks} == {len(tasks)}


def test_the_tasks_can_be_chained_so_that_the_batches_run_one_by_one(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, by_kind={**{kind: {} for kind in KIND_VIEWS}, "assets": {A: 600}})
    universe_tools(monkeypatch, {(A, "CogniteAsset"): [f"asset-{n:04d}" for n in range(600)]})

    sequential = plan(RecordingClient(), shardTargetEntities=200, maxConcurrentShards=1)
    two = plan(RecordingClient(), shardTargetEntities=200, maxConcurrentShards=2)
    free = plan(RecordingClient(), shardTargetEntities=200)

    chain = [[dep["externalId"] for dep in t["dependsOn"]] for t in sequential["tasks"]]
    assert chain[0] == []
    assert chain[1:] == [[t["externalId"]] for t in sequential["tasks"][:-1]]
    assert [t["dependsOn"] for t in two["tasks"][:2]] == [[], []]
    assert two["tasks"][2]["dependsOn"] == [{"externalId": two["tasks"][0]["externalId"]}]
    assert all(t["dependsOn"] == [] for t in free["tasks"])


def test_the_plan_cuts_a_big_space_only_when_it_has_to(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_world(monkeypatch, by_kind={**{kind: {} for kind in KIND_VIEWS}, "assets": {A: 600}})

    out = plan(RecordingClient(), shardTargetEntities=1000)

    assert out["batches"] == 0
    assert [t["parameters"]["function"]["data"]["action"] for t in out["tasks"]] == ["compute_shard"]


def batch_run(client: RecordingClient, cfg: Any = CFG, **payload: Any) -> dict[str, Any]:
    base = {"runId": RUN_ID, "runTimestamp": RUN_TS, "shard": 3, "shardCount": 5, "space": A, "facility": FACILITIES[A]}
    return runner.compute_batch(client, cfg, {**base, **payload})  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def batch_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    from cq.cross import full_cross

    def load_batch(_client: Any, space: str, facility: str, kinds: frozenset[str], flt: Any = None) -> dict[str, Any]:
        data = data_of({space: facility})
        for kind in ENTITY_KEYS:
            if kind not in kinds:
                data[kind] = []
        return data

    monkeypatch.setattr(runner, "load_batch", load_batch)
    monkeypatch.setattr(runner, "fetch_cross", lambda *_a, **_k: full_cross(build_dataset()[0]))


def test_a_batch_writes_the_rows_of_its_kinds_with_keys_of_its_own_and_a_part_record_last() -> None:
    client = RecordingClient()

    summary = batch_run(client, kinds=["equipment"])

    items = records(client)
    rows = [props(item) for item in items[:-1]]
    assert {row["reportingUnitId"] for row in rows} == {A}
    assert {row.get("dimensionId") for row in rows} - {None} == {"equipment-asset"}
    assert all(item["externalId"].endswith("|p3") for item in items[:-1])
    part = props(items[-1])
    assert (part["rowKind"], part["detail"], part["entityId"]) == ("runPart", "batch", "shard-3")
    assert part["rowsWritten"] == len(items) - 1 == summary["rowsWritten"]


def test_two_batches_of_one_kind_never_write_the_same_record() -> None:
    first, second = RecordingClient(), RecordingClient()

    batch_run(first, kinds=["equipment"], shard=1)
    batch_run(second, kinds=["equipment"], shard=2)

    assert not {i["externalId"] for i in records(first)} & {i["externalId"] for i in records(second)}


def test_the_drill_down_cap_of_a_space_is_shared_by_the_batches_of_a_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[int] = []
    build = runner.build_rows

    def spy(result: Any, cfg: Any) -> Any:
        seen.append(cfg["drilldownCap"])
        return build(result, cfg)

    monkeypatch.setattr(runner, "build_rows", spy)

    batch_run(RecordingClient(), kinds=["files"], groupBatches=4)
    batch_run(RecordingClient(), kinds=["files"])

    assert seen == [125, 500]


def test_a_batch_leaves_to_the_plan_what_it_cannot_know_about_the_whole_run() -> None:
    cfg = build_config({**SAMPLE_RESOURCES, "tieringEnabled": True, "tierByTypeCode": {}})
    client = RecordingClient()

    batch_run(client, cfg, kinds=["assets", "work_orders"])

    (part_record,) = [props(i) for i in records(client) if props(i)["rowKind"] == "runPart"]
    codes = {w["code"] for w in json.loads(part_record["warnings"])}
    assert "mapping-empty" in codes
    assert not codes & {"no-source-entities", "metric-not-applicable"}


def test_a_batch_of_a_big_configured_view_counts_the_slice_it_is_given(monkeypatch: pytest.MonkeyPatch) -> None:
    ref = {"space": "r", "externalId": "t"}
    asked: dict[str, Any] = {}

    def load_extra_links(_client: Any, links: Any, spaces: Any, flt: Any = None) -> Any:
        asked.update(links=links, spaces=spaces, flt=flt)
        return {(CORE, "CogniteAsset"): [(A, "a1", {"type": ref}), (A, "a2", {})]}

    monkeypatch.setattr(runner, "load_extra_links", load_extra_links)
    client = RecordingClient()

    batch_run(
        client,
        views=[[CORE, "CogniteAsset"]],
        leaves=[["prefix", "a"]],
        extraLinks=[{"model": CORE, "links": ["CogniteAsset.type", "CogniteEquipment.equipmentType"]}],
    )

    assert asked["spaces"] == [A]
    assert asked["flt"] is not None
    assert [(link["view"], link["property"]) for link in asked["links"]] == [("CogniteAsset", "type")]
    kinds = [props(item)["rowKind"] for item in records(client)]
    assert kinds == ["extraLink", "extraLinkGap", "runPart"]
    assert props(records(client)[0])["issueCount"] == 1


# ---------------------------------------------------------------- the export of unlinked items


def with_export(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int, list[tuple[str, ...]]]]:
    """Stands in for Cognite Files: records what each piece uploads."""
    uploaded: list[tuple[str, int, list[tuple[str, ...]]]] = []
    monkeypatch.setattr(runner.export, "data_set_id", lambda _client, cfg: 77 if cfg["exportDataSet"] else None)
    monkeypatch.setattr(
        runner.export,
        "write_part",
        lambda _client, _ds, run_id, piece, rows: uploaded.append((run_id, piece, rows)) or len(rows),
    )
    return uploaded


def test_a_shard_uploads_its_unlinked_items_and_summary_for_the_workbook(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded = with_export(monkeypatch)
    cfg = build_config({**SAMPLE_RESOURCES, "exportDataSet": "ds_cq"})
    payload = {
        "runId": RUN_ID,
        "runTimestamp": RUN_TS,
        "shard": 4,
        "shardCount": 6,
        "spaces": {A: "alpha"},
        "extraSpaces": {A: "alpha"},
    }

    runner.compute_shard(RecordingClient(), cfg, payload)  # type: ignore[arg-type]

    ((run_id, piece, rows),) = uploaded
    assert (run_id, piece) == (RUN_ID, 4)
    assert {row[0] for row in rows} == {"item", "summary"}
    assert {row[4] for row in rows if row[0] == "item"} == {A}  # only the items of this shard's space


def test_a_batch_uploads_its_items_too(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded = with_export(monkeypatch)
    cfg = build_config({**SAMPLE_RESOURCES, "exportDataSet": "ds_cq"})

    batch_run(RecordingClient(), cfg, kinds=["equipment"])

    ((_, piece, rows),) = uploaded
    assert piece == 3
    assert {row[1] for row in rows if row[0] == "item"} <= {
        "Equipment without asset",
        "Equipment no serial number",
        "Equipment no manufacturer",
    }


def test_nothing_is_uploaded_when_the_export_is_not_switched_on(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded = with_export(monkeypatch)

    shard_run(RecordingClient(), {A: FACILITIES[A]}, 0)

    assert uploaded == []


def test_a_failed_upload_is_a_warning_and_never_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    from cognite.client.exceptions import CogniteAPIError

    def refuse(*_a: Any, **_k: Any) -> Any:
        raise CogniteAPIError("Insufficient access rights", 403)

    monkeypatch.setattr(runner.export, "data_set_id", lambda _client, _cfg: 77)
    monkeypatch.setattr(runner.export, "write_part", refuse)
    client = RecordingClient()

    runner.compute_shard(
        client,
        build_config({**SAMPLE_RESOURCES, "exportDataSet": "ds_cq"}),
        {
            "runId": RUN_ID,
            "runTimestamp": RUN_TS,
            "shard": 0,
            "shardCount": 1,
            "spaces": {A: "alpha"},
            "extraSpaces": {A: "alpha"},
        },
    )  # type: ignore[arg-type]

    (part_record,) = [props(i) for i in records(client) if props(i)["rowKind"] == "runPart"]
    (warning_,) = [w for w in json.loads(part_record["warnings"]) if w["code"] == "export-failed"]
    assert warning_["code"] == "export-failed"
    assert "Insufficient access rights" in warning_["message"]


def test_the_export_step_builds_the_workbook_of_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, int]] = []
    monkeypatch.setattr(
        runner.export,
        "build_workbook",
        lambda _c, _cfg, run_id, pieces: seen.append((run_id, pieces)) or {"exported": True},
    )

    out = runner.export_workbook(RecordingClient(), CFG, {"runId": RUN_ID, "shardCount": "5"})  # type: ignore[arg-type]

    assert seen == [(RUN_ID, 5)]
    assert out == {"action": "export", "exported": True}


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

    with pytest.raises(runner.MissingInputError, match=r"1 of 2 calls.*\[1\]"):
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


def test_the_plans_own_warnings_are_part_of_the_runs() -> None:
    nothing = {"code": "no-source-entities", "message": "m", "subject": "work_orders"}
    plan_part = {"rowKind": "runPart", "runId": RUN_ID, "entityId": "plan", "detail": "plan", "rowsWritten": 3}
    plan_part["warnings"] = encode_warnings([nothing])
    client = ReadingClient([plan_part, part(0, 5), part(1, 5)])

    out = finalize(client)

    (item,) = records(client)
    assert [w["code"] for w in json.loads(props(item)["warnings"])] == ["no-source-entities"]
    assert props(item)["rowsWritten"] == 13 == out["rowsWritten"]


def test_the_time_of_the_run_is_the_time_of_the_plan() -> None:
    client = ReadingClient([part(0, 1)])

    finalize(client, shards=1)

    (item,) = records(client)
    assert props(item)["runTimestamp"] == RUN_TS
    assert datetime.fromisoformat(RUN_TS.replace("Z", "+00:00")).tzinfo == UTC
