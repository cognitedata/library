"""An incremental run reuses the stored result of a batch only when nothing the batch reads has changed."""

import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from cognite.client.exceptions import CogniteAPIError
from cq import incremental
from cq.config import build_config
from sample_rules import SAMPLE_RESOURCES
from test_runner import RecordingClient

CFG = build_config({**SAMPLE_RESOURCES, "stateDatabase": "db_cq", "stateTable": "pieces"})
RUN_TS = datetime(2026, 10, 13, 5, 34, 25, tzinfo=UTC)
PAYLOAD: dict[str, Any] = {
    "space": "sp_a",
    "facility": "fac_a",
    "kinds": ["equipment"],
    "leaves": [["prefix", "EQ-"]],
    "groupBatches": 2,
    "count": 1200,
    "mode": "incremental",
}
RESULT = incremental.PieceResult(
    rows=[("agg:associationRate", {"rowKind": "aggregate", "eligibleCount": 10})],
    annotation_rows=[],
    warnings=[{"code": "metric-not-applicable"}],
    listed=[("item", "Equipment without asset", "pump", "EQ-1", "sp_a", "fac_a", "no asset", "", "", "")],
    populated=["equipmentType"],
)


class FakeRaw:
    """The RAW table of the stored results, as far as they use it."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.deleted: list[str] = []
        self.age: dict[str, int] = {}  # the time a row was last written, ms

    def insert(self, db_name: str, table_name: str, row: dict[str, dict[str, Any]]) -> None:
        assert (db_name, table_name) == ("db_cq", "pieces")
        self.rows.update(row)

    def retrieve(self, db_name: str, table_name: str, key: str) -> Any:
        assert (db_name, table_name) == ("db_cq", "pieces")
        return SimpleNamespace(key=key, columns=self.rows[key]) if key in self.rows else None

    def list(
        self, db_name: str, table_name: str, max_last_updated_time: int | None = None, columns: Any = None, **_: Any
    ) -> list[Any]:
        assert columns == []  # only the keys
        return [
            SimpleNamespace(key=key)
            for key in self.rows
            if max_last_updated_time is None or self.age.get(key, 0) <= max_last_updated_time
        ]

    def delete(self, db_name: str, table_name: str, key: Sequence[str]) -> None:
        self.deleted.extend(key)


class FakeInstances:
    """The instances a query for `limit 1` finds: one when `changed(view, filter)` says something changed."""

    def __init__(self, changed: Callable[[str, dict[str, Any] | None], bool]) -> None:
        self.changed = changed
        self.asked: list[tuple[str, dict[str, Any] | None]] = []

    def list(self, sources: Any, filter: Any = None, **_: Any) -> list[object]:
        flt = filter.dump() if filter is not None else None
        self.asked.append((sources.external_id, flt))
        return [object()] if self.changed(sources.external_id, flt) else []


class IncrementalClient(RecordingClient):
    def __init__(self, changed: Callable[[str, dict[str, Any] | None], bool] = lambda _view, _flt: False) -> None:
        super().__init__()
        self.state = FakeRaw()
        self.instances = FakeInstances(changed)
        self.data_modeling = SimpleNamespace(instances=self.instances)
        self.raw = SimpleNamespace(rows=self.state)


def changed_in(*views: str) -> Callable[[str, dict[str, Any] | None], bool]:
    return lambda view, _flt: view in views


@pytest.fixture(autouse=True)
def classifications(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(incremental, "_populated_now", lambda _client, _payload: ["equipmentType"])


def stored(client: IncrementalClient, cfg: Any = CFG, payload: dict[str, Any] = PAYLOAD) -> None:
    incremental.remember(client, cfg, payload, RESULT, RUN_TS)  # type: ignore[arg-type]


def reuse(client: IncrementalClient, cfg: Any = CFG, payload: dict[str, Any] = PAYLOAD) -> tuple[Any, str]:
    return incremental.reuse(client, cfg, payload)  # type: ignore[arg-type]


def test_a_run_is_full_unless_it_asks_for_incremental() -> None:
    assert incremental.parse_mode(None) == incremental.parse_mode("${workflow.input.mode}") == "full"
    assert incremental.parse_mode("incremental") == "incremental"
    with pytest.raises(ValueError, match="mode must be"):
        incremental.parse_mode("sometimes")


def test_the_key_follows_what_decides_the_result_and_nothing_else() -> None:
    key = incremental.piece_key(CFG, PAYLOAD)

    assert key == incremental.piece_key(CFG, {**PAYLOAD, "count": 5, "mode": "full", "shard": 9, "runId": "other"})
    assert key == incremental.piece_key({**CFG, "shardTargetEntities": 5}, PAYLOAD)
    assert key != incremental.piece_key(CFG, {**PAYLOAD, "leaves": [["prefix", "PU-"]]})
    assert key != incremental.piece_key(CFG, {**PAYLOAD, "groupBatches": 3})
    # equipment feeds one dimension, so a run for every dimension and one for equipment-asset share the result
    assert key == incremental.piece_key(CFG, {**PAYLOAD, "scope": {"dimensions": ["equipment-asset", "files"]}})
    assets = {
        **PAYLOAD,
        "kinds": ["assets"],
    }  # assets feed three dimensions, and what is written depends on which are asked
    assert incremental.piece_key(CFG, assets) != incremental.piece_key(
        CFG, {**assets, "scope": {"dimensions": ["time-series"]}}
    )
    assert key != incremental.piece_key({**CFG, "freshnessDays": 3}, PAYLOAD)


def test_a_full_run_counts_everything_but_still_keeps_the_results() -> None:
    client = IncrementalClient()
    stored(client)

    result, note = reuse(client, payload={**PAYLOAD, "mode": "full"})

    assert (result, note) == (None, "full run")
    assert len(client.state.rows) == 1


@pytest.mark.parametrize(
    "payload",
    [{"kinds": ["timeseries"]}, {"views": [["m:x:v1", "View"]]}, {"kinds": []}],
    ids=["time series", "configured links", "nothing"],
)
def test_some_batches_are_always_counted(payload: dict[str, Any]) -> None:
    client = IncrementalClient()
    stored(client, payload={**PAYLOAD, **payload})

    result, _note = reuse(client, payload={**PAYLOAD, **payload})

    assert result is None
    assert client.state.rows == {}


def test_a_batch_with_nothing_changed_gets_its_earlier_result() -> None:
    client = IncrementalClient()
    stored(client)

    result, note = reuse(client)

    assert (result, note) == (RESULT, "reused")
    own = [flt for view, flt in client.instances.asked if view == "CogniteEquipment"]
    assert len(own) == 1
    assert "lastUpdatedTime" in json.dumps(own[0]) and "EQ-" in json.dumps(own[0])  # its slice, changed after its run


def test_a_change_in_the_slice_means_counting_it_again() -> None:
    client = IncrementalClient(changed_in("CogniteEquipment"))
    stored(client)

    assert reuse(client) == (None, "equipment changed")


def test_there_is_no_earlier_result_for_a_slice_never_counted() -> None:
    assert reuse(IncrementalClient()) == (None, "no earlier result")


def test_a_different_number_of_instances_means_counting_it_again() -> None:
    client = IncrementalClient()
    stored(client)

    assert reuse(client, payload={**PAYLOAD, "count": 1199}) == (None, "the number of instances changed")


def test_what_a_batch_looks_up_beyond_its_slice_counts_as_changed_too() -> None:
    payload = {**PAYLOAD, "kinds": ["assets"], "leaves": [["prefix", "A-"]]}
    series = IncrementalClient(changed_in("CogniteTimeSeries"))
    elsewhere = IncrementalClient(changed_in("CogniteAsset"))
    stored(series, payload=payload)
    stored(elsewhere, payload=payload)

    assert reuse(series, payload=payload) == (None, "timeseries of the space changed")
    assert reuse(elsewhere, payload=payload) == (None, "assets changed")  # its own slice is asked first


def test_the_assets_an_entity_links_to_matter_only_when_the_grain_comes_from_them() -> None:
    plain, tiered = IncrementalClient(changed_in("CogniteAsset")), IncrementalClient(changed_in("CogniteAsset"))
    with_tiers = {**CFG, "tieringEnabled": True}
    stored(plain)
    stored(tiered, with_tiers)

    assert reuse(plain)[1] == "reused"
    assert reuse(tiered, with_tiers) == (None, "assets of the space changed")


def test_a_change_to_the_nodes_that_label_a_classification_means_counting_again() -> None:
    client = IncrementalClient(changed_in("CogniteEquipmentType"))
    stored(client)

    assert reuse(client) == (None, "CogniteEquipmentType changed")


def test_a_classification_that_became_populated_means_counting_again(monkeypatch: pytest.MonkeyPatch) -> None:
    client = IncrementalClient()
    stored(client)
    monkeypatch.setattr(incremental, "_populated_now", lambda _client, _payload: [])

    assert reuse(client) == (None, "the classifications in use changed")


def test_a_check_that_fails_never_fails_the_run_it_counts_the_batch() -> None:
    def times_out(_view: str, _flt: dict[str, Any] | None) -> bool:
        raise CogniteAPIError("The query timed out", 408)

    client = IncrementalClient(times_out)
    stored(client)

    result, note = reuse(client)

    assert result is None
    assert note.startswith("could not reuse the earlier result (CogniteAPIError")


def test_a_result_that_cannot_be_read_is_counted_again() -> None:
    client = IncrementalClient()
    stored(client)
    (name,) = client.state.rows
    client.state.rows[name]["body"] = "not a stored result"

    assert reuse(client)[0] is None


def test_nothing_is_kept_or_reused_without_a_table() -> None:
    client = IncrementalClient()
    no_table = {**CFG, "stateTable": ""}

    incremental.remember(client, no_table, PAYLOAD, RESULT, RUN_TS)  # type: ignore[arg-type]

    assert client.state.rows == {}
    assert incremental.reuse(client, no_table, PAYLOAD)[0] is None  # type: ignore[arg-type]


def test_a_result_too_big_for_a_row_is_not_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    client = IncrementalClient()
    monkeypatch.setattr(incremental, "MAX_BODY_CHARS", 10)

    stored(client)

    assert client.state.rows == {}
    assert reuse(client) == (None, "no earlier result")


def test_a_result_is_kept_in_one_row_that_says_when_it_was_made_and_how_many_instances_it_counted() -> None:
    client = IncrementalClient()

    stored(client)

    ((key, row),) = client.state.rows.items()
    assert key == incremental.piece_key(CFG, PAYLOAD) and len(key) == 32
    assert row["computedAt"] == RUN_TS.isoformat() and row["count"] == PAYLOAD["count"]
    assert len(row["body"]) < incremental.MAX_BODY_CHARS and "agg:associationRate" not in row["body"]  # compressed


def test_a_table_that_cannot_be_written_never_fails_the_run() -> None:
    client = IncrementalClient()
    client.state.insert = lambda *_a, **_k: (_ for _ in ()).throw(CogniteAPIError("no access", 403))  # type: ignore[method-assign]

    stored(client)  # does not raise

    assert client.state.rows == {}


def test_a_check_is_made_after_the_run_that_made_the_result_with_a_margin() -> None:
    client = IncrementalClient()
    stored(client)

    reuse(client)

    ((_, flt),) = [ask for ask in client.instances.asked if ask[0] == "CogniteEquipment"]
    since = json.dumps(flt)
    assert str(int((RUN_TS - incremental.SAFETY).timestamp() * 1000)) in since


def test_results_nobody_wrote_for_the_retention_period_are_removed() -> None:
    client = IncrementalClient()
    stored(client)
    stored(client, payload={**PAYLOAD, "leaves": [["prefix", "PU-"]]})
    old, fresh = list(client.state.rows)
    now = datetime.now(UTC)
    client.state.age = {old: int((now - timedelta(days=40)).timestamp() * 1000), fresh: int(now.timestamp() * 1000)}

    removed = incremental.remove_old_results(client, CFG, now)  # type: ignore[arg-type]

    assert (removed, client.state.deleted) == (1, [old])


def test_there_is_nothing_to_remove_without_a_table() -> None:
    assert incremental.remove_old_results(IncrementalClient(), {**CFG, "stateDatabase": ""}, datetime.now(UTC)) == 0  # type: ignore[arg-type]
