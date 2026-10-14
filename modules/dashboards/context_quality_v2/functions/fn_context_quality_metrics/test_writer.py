"""The Records external ids the writer produces, and the payloads that go with them."""

import hashlib
import itertools
import json
from datetime import UTC, datetime
from typing import Any

from cq import writer
from cq.config import build_config
from cq.types import DrillItem, Grain


def grain(unit: str, tier: int = 1) -> Grain:
    return {"unit": unit, "facility": f"fac-{unit}", "div": None, "disc": None, "tier": tier}


def item(unit: str, entity_id: str, metric: str = "orphansByTier", label: str = "L") -> DrillItem:
    return {
        "metric": metric,
        "dim": "asset-hierarchy",
        "g": grain(unit),
        "id": entity_id,
        "label": label,
        "detail": "d",
        "owner": "o",
        "classes": None,
    }


def keys(rows: list[tuple[str, Any]]) -> list[str]:
    return [key for key, _ in rows]


class RecordingClient:
    def __init__(self) -> None:
        self.config = type("Config", (), {"project": "p"})()
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, json: dict[str, Any]) -> None:
        self.calls.append(json)

    def external_ids(self) -> list[str]:
        return [i["externalId"] for call in self.calls for i in call["items"]]


# ---------------------------------------------------------------- drilldown


def test_the_same_entity_id_in_two_spaces_gets_two_different_external_ids() -> None:
    rows = writer.drilldown_rows([item("sp_a", "asset-1"), item("sp_b", "asset-1")], cap=500)

    assert len(set(keys(rows))) == 2
    assert [props["reportingUnitId"] for _, props in rows] == ["sp_a", "sp_b"]


def test_the_same_entity_in_two_metrics_gets_two_different_external_ids() -> None:
    rows = writer.drilldown_rows([item("sp_a", "a-1", "orphansByTier"), item("sp_a", "a-1", "depthOutliers")], cap=500)

    assert len(set(keys(rows))) == 2


def test_a_drilldown_id_is_a_pure_function_of_its_logical_key() -> None:
    first = writer.drilldown_rows([item("sp_a", "a-1"), item("sp_b", "b-1")], cap=500)
    again = writer.drilldown_rows([item("sp_a", "a-1"), item("sp_b", "b-1")], cap=500)
    reordered = writer.drilldown_rows([item("sp_b", "b-1"), item("sp_a", "a-1")], cap=500)
    relabelled = writer.drilldown_rows([item("sp_a", "a-1", label="Renamed")], cap=500)

    assert keys(first) == keys(again)
    assert set(keys(first)) == set(keys(reordered))
    assert keys(relabelled)[0] == keys(first)[0]  # the id follows the logical key, not the label or other payload


def test_the_drilldown_id_scheme_is_pinned() -> None:
    """If this changes, retries of a run written by the previous version stop being idempotent. Change it knowingly."""
    rows = writer.drilldown_rows([item("sp_a", "a-1")], cap=500)

    assert keys(rows) == ["drill:orphansByTier:ea66eea231501a82699f4aa47cb0835e"]


def test_the_drilldown_id_follows_the_documented_encoding() -> None:
    """sha256 of the compact JSON of [rowKind, metric, dimension, space, entity id], first 32 hex characters."""
    digest = hashlib.sha256(
        json.dumps(["drilldown", "orphansByTier", "asset-hierarchy", "sp_a", "a-1"], separators=(",", ":")).encode()
    ).hexdigest()[:32]

    assert keys(writer.drilldown_rows([item("sp_a", "a-1")], cap=500)) == [f"drill:orphansByTier:{digest}"]


def test_the_classification_id_follows_the_documented_encoding() -> None:
    """sha256 of the compact JSON of the complete row key (kind, id, name, dimension, space, facility, division, discipline, owner, tier)."""
    logical_key = [
        "classification",
        "assetType",
        "pump",
        "Pump",
        "asset-hierarchy",
        "sp_a",
        "fac-sp_a",
        None,
        None,
        "o",
        1,
    ]
    digest = hashlib.sha256(json.dumps(logical_key, separators=(",", ":")).encode()).hexdigest()[:32]

    assert keys(writer.classification_rows(classification())) == [f"cls:assetType:{digest}"]


def test_separator_characters_in_entity_ids_cannot_make_two_entities_look_alike() -> None:
    rows = writer.drilldown_rows([item("u", "a|b"), item("u|a", "b"), item("u", "a:b"), item("u:a", "b")], cap=500)

    assert len(set(keys(rows))) == 4


def test_a_very_long_entity_id_still_gives_a_short_external_id() -> None:
    rows = writer.drilldown_rows([item("sp_a", "x" * 400)], cap=500)

    assert len(keys(rows)[0]) < 80
    assert props_of(rows)[0]["entityId"] == "x" * 400  # the payload keeps the full entity id


def props_of(rows: list[tuple[str, Any]]) -> list[dict[str, Any]]:
    return [props for _, props in rows]


def test_the_drilldown_payload_is_unchanged() -> None:
    entry = item("sp_a", "a-1")
    entry["classes"] = {"assetType": "Pump"}

    ((_, props),) = writer.drilldown_rows([entry], cap=500)

    assert props == {
        "rowKind": "drilldown",
        "metricId": "orphansByTier",
        "dimensionId": "asset-hierarchy",
        "reportingUnitId": "sp_a",
        "facilityId": "fac-sp_a",
        "ownerId": "o",
        "tier": 1,
        "entityId": "a-1",
        "entityLabel": "L",
        "detail": "d",
        "matched": False,
        "cellsJson": json.dumps({"assetType": "Pump"}, sort_keys=True),
    }


def test_the_per_metric_and_unit_cap_still_applies() -> None:
    rows = writer.drilldown_rows([item("sp_a", f"a-{i}") for i in range(5)] + [item("sp_b", "b-1")], cap=2)

    assert [props["entityId"] for props in props_of(rows)] == ["a-0", "a-1", "b-1"]


# ---------------------------------------------------------------- classification


def classification(
    kind: str = "assetType", cid: str = "pump", name: str = "Pump", unit: str = "sp_a", **over: Any
) -> dict[tuple, list[int]]:
    key = {
        "dim": "asset-hierarchy",
        "facility": f"fac-{unit}",
        "div": None,
        "disc": None,
        "owner": "o",
        "tier": 1,
        **over,
    }
    return {
        (kind, cid, name, key["dim"], unit, key["facility"], key["div"], key["disc"], key["owner"], key["tier"]): [4, 3]
    }


def test_classification_nodes_with_the_same_external_id_but_different_labels_do_not_collide() -> None:
    both = {**classification(name="Pump (space A)"), **classification(name="Pump (space B)")}

    rows = writer.classification_rows(both)

    assert len(rows) == 2
    assert len(set(keys(rows))) == 2


def test_the_same_classification_in_two_spaces_does_not_collide() -> None:
    both = {**classification(unit="sp_a"), **classification(unit="sp_b")}

    assert len(set(keys(writer.classification_rows(both)))) == 2


def test_every_part_of_the_classification_key_is_part_of_its_id() -> None:
    base = classification()
    variants = [
        classification(kind="assetClass"),
        classification(cid="tank"),
        classification(name="Other"),
        classification(dim="files"),
        classification(unit="sp_b"),
        classification(div="north"),
        classification(disc="mechanical"),
        classification(owner="p"),
        classification(tier=2),
    ]
    ids = keys(writer.classification_rows(base))
    for variant in variants:
        ids += keys(writer.classification_rows(variant))

    assert len(set(ids)) == len(variants) + 1


def test_a_classification_id_is_deterministic() -> None:
    assert keys(writer.classification_rows(classification())) == keys(writer.classification_rows(classification()))


def test_the_classification_id_scheme_is_pinned() -> None:
    assert keys(writer.classification_rows(classification())) == ["cls:assetType:67203390f5a6c81acee03d418e74b9cd"]


def test_the_classification_payload_is_unchanged() -> None:
    ((_, props),) = writer.classification_rows(classification())

    assert props == {
        "rowKind": "classification",
        "classificationKind": "assetType",
        "classificationId": "pump",
        "classificationName": "Pump",
        "dimensionId": "asset-hierarchy",
        "reportingUnitId": "sp_a",
        "facilityId": "fac-sp_a",
        "ownerId": "o",
        "tier": 1,
        "eligibleCount": 4,
        "passingCount": 3,
        "issueCount": 1,
    }


# ---------------------------------------------------------------- every kind, and repeated writes


def test_the_id_prefixes_are_the_ones_the_contract_lists() -> None:
    contract = json.loads(
        (__import__("pathlib").Path(__file__).resolve().parents[2] / "contract" / "contract.json").read_text()
    )
    rows = writer.drilldown_rows([item("sp_a", "a-1")], cap=5) + writer.classification_rows(classification())

    assert all(key.startswith(tuple(contract["recordKeyPrefixes"])) for key in keys(rows))


def test_the_other_row_kinds_keep_their_existing_ids() -> None:
    agg = writer.aggregate_rows(
        {("orphanRate", "asset-hierarchy", "sp_a", "fac", "div", "disc", "own", 1, None): [3, 2, 1, 0.0]}
    )
    excl = writer.exclusion_rows({("files", "user:file-extension", "sp_a", "fac", "div", "disc"): 2})
    anno = writer.annotation_bucket_rows({("sp_a", "fac", "div", 1, "Approved", "high"): [2, 1.5]})

    assert keys(agg) == ["agg:orphanRate|sp_a|div|disc|own|1|None"]
    assert keys(excl) == ["excl:files|user:file-extension|sp_a|div|disc"]
    assert keys(anno) == ["anno:sp_a|div|1|Approved|high"]


def test_writing_the_same_run_twice_uses_the_same_external_ids() -> None:
    rows = writer.drilldown_rows([item("sp_a", "a-1"), item("sp_b", "a-1")], cap=500) + writer.classification_rows(
        classification()
    )
    cfg = build_config(None)
    ts = datetime(2026, 10, 13, 5, 34, 25, tzinfo=UTC)
    client = RecordingClient()

    writer.ingest(client, cfg, "relationshipContainer", "run-x", ts, rows)  # type: ignore[arg-type]
    first = client.external_ids()
    client.calls.clear()
    writer.ingest(client, cfg, "relationshipContainer", "run-x", ts, rows)  # type: ignore[arg-type]

    assert client.external_ids() == first
    assert len(set(first)) == len(first)
    assert all(i.startswith("run-x:") for i in first)


def test_a_new_run_gets_new_ids_for_the_same_logical_records() -> None:
    rows = writer.drilldown_rows([item("sp_a", "a-1")], cap=500)
    cfg = build_config(None)
    ts = datetime(2026, 10, 13, 5, 34, 25, tzinfo=UTC)
    first, second = RecordingClient(), RecordingClient()

    writer.ingest(first, cfg, "relationshipContainer", "run-1", ts, rows)  # type: ignore[arg-type]
    writer.ingest(second, cfg, "relationshipContainer", "run-2", ts, rows)  # type: ignore[arg-type]

    assert {i.split(":", 1)[1] for i in first.external_ids()} == {i.split(":", 1)[1] for i in second.external_ids()}
    assert not set(first.external_ids()) & set(second.external_ids())


def test_no_two_logical_records_share_an_id_whatever_order_they_arrive_in() -> None:
    entries = [item(space, entity) for space, entity in itertools.product(["sp_a", "sp_b", "sp_c"], ["x", "y", "a|b"])]

    for permutation in itertools.islice(itertools.permutations(entries), 20):
        ids = keys(writer.drilldown_rows(list(permutation), cap=500))
        assert len(ids) == len(set(ids)) == len(entries)
