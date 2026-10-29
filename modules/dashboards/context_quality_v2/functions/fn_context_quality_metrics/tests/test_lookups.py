"""The queries that answer, for a batch, what its entities link to outside it, against a stand-in data modeling service.

The stand-in holds the parity dataset as the service would (direct relations as {space, externalId}) and understands the
filters the lookups use, so what is checked is that the right questions are asked and the answers are used right: counting
a space in batches with `fetch_cross` gives the numbers of counting the space whole.
"""

from collections import Counter
from collections.abc import Callable
from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import ViewId
from cq import lookups
from cq.config import build_config
from cq.cross import Cross
from cq.metrics import KINDS, compute
from cq.types import Ref
from parity_dataset import A, B, build_dataset
from test_batches import CONFIGS, summed

CDM, IDM = "cdf_cdm", "cdf_idm"
VIEWS = {
    "assets": ViewId(CDM, "CogniteAsset", "v1"),
    "equipment": ViewId(CDM, "CogniteEquipment", "v1"),
    "timeseries": ViewId(CDM, "CogniteTimeSeries", "v1"),
    "files": ViewId(CDM, "CogniteFile", "v1"),
    "notifications": ViewId(IDM, "CogniteNotification", "v1"),
}


def rel(ref: Ref | None) -> dict[str, str] | None:
    return None if ref is None else {"space": ref[0] or "", "externalId": ref[1]}


class Node:
    def __init__(self, space: str, external_id: str, view: ViewId, props: dict[str, Any]) -> None:
        self.space, self.external_id = space, external_id
        self.properties = {view: props}


def nodes_of(data: dict[str, Any]) -> dict[str, list[Node]]:
    """The parity dataset as the service holds it."""
    out: dict[str, list[Node]] = {kind: [] for kind in VIEWS}
    for a in data["assets"]:
        props = {
            "name": a["name"],
            "tags": a["tags"],
            "parent": rel(a["parent"]),
            "type": rel(a["type"]),
            "assetClass": rel(a["aclass"]),
            "object3D": rel(a["object3d"]),
        }
        out["assets"].append(Node(a["space"], a["id"], VIEWS["assets"], props))
    for e in data["equipment"]:
        props = {"asset": rel(e["asset"]), "equipmentType": rel(e["etype"])}
        out["equipment"].append(Node(e["space"], e["id"], VIEWS["equipment"], props))
    for t in data["timeseries"]:
        out["timeseries"].append(
            Node(t["space"], t["id"], VIEWS["timeseries"], {"assets": [rel(r) for r in t["assets"]]})
        )
    for f in data["files"]:
        props = {"category": rel(f["category"]), "assets": [rel(r) for r in f["assets"]]}
        out["files"].append(Node(f["space"], f["id"], VIEWS["files"], props))
    for n in data["notifications"]:
        out["notifications"].append(
            Node(n["space"], n["id"], VIEWS["notifications"], {"maintenanceOrder": rel(n["work_order"])})
        )
    return out


def matches(flt: dict[str, Any] | None, props: dict[str, Any]) -> bool:
    """The filters the lookups use, as the service evaluates them."""
    if flt is None:
        return True
    (name, body), *_ = flt.items()
    if name == "and":
        return all(matches(part, props) for part in body)
    if name == "or":
        return any(matches(part, props) for part in body)
    if name == "not":
        return not matches(body, props)
    value = props.get(body["property"][-1])
    if name == "in":
        return value in body["values"]
    if name == "containsAny":
        return any(item in body["values"] for item in value or [])
    raise AssertionError(f"the lookups asked with a filter this stand-in does not know: {name}")


class Instances:
    def __init__(self, nodes: dict[str, list[Node]]) -> None:
        self.nodes = nodes
        self.questions: list[tuple[str, str]] = []

    def _select(self, view: ViewId, flt: Any, space: list[str] | None) -> list[Node]:
        kind = next(kind for kind, v in VIEWS.items() if v == view)
        dumped = flt.dump() if flt is not None else None
        return [
            n for n in self.nodes[kind] if (space is None or n.space in space) and matches(dumped, n.properties[view])
        ]

    def aggregate(
        self,
        view: ViewId,
        aggregates: Any,
        group_by: str | None = None,
        filter: Any = None,
        space: Any = None,
        limit: int = 25,
        **_: Any,
    ) -> list[Any]:
        self.questions.append(("aggregate", f"{view.external_id}.{group_by}"))
        counts: Counter[tuple[str, str]] = Counter()
        for node in self._select(view, filter, space):
            value = node.properties[view].get(group_by)
            for item in value if isinstance(value, list) else [value]:
                if item:
                    counts[(item["space"], item["externalId"])] += 1
        groups = [
            type("Group", (), {"group": {group_by: {"space": s, "externalId": x}}, "aggregates": [n]})
            for (s, x), n in counts.items()
        ]
        return groups[:limit]

    def retrieve_nodes(self, nodes: list[Any], sources: ViewId) -> list[Node]:
        self.questions.append(("retrieve", sources.external_id))
        wanted = {(n.space, n.external_id) for n in nodes}
        kind = next(kind for kind, v in VIEWS.items() if v == sources)
        return [n for n in self.nodes[kind] if (n.space, n.external_id) in wanted]

    def list(
        self, instance_type: str, sources: ViewId, space: Any = None, filter: Any = None, limit: int = 25
    ) -> list[Node]:
        self.questions.append(("list", sources.external_id))
        return self._select(sources, filter, space)


def client_for(data: dict[str, Any]) -> Any:
    instances = Instances(nodes_of(data))
    return type("Client", (), {"data_modeling": type("DM", (), {"instances": instances})()})()


def data_of_kind(kind: str, space: str, take: Callable[[dict[str, Any]], bool] = lambda _e: True) -> dict[str, Any]:
    data, _latest, _now = build_dataset()
    for other in KINDS:
        data[other] = [e for e in data[other] if other == kind and e["space"] == space and take(e)]
    data["facility_spaces"] = {space: space}
    return data


@pytest.mark.parametrize("name", CONFIGS)
@pytest.mark.parametrize("space", [A, B])
def test_batches_with_looked_up_answers_give_the_numbers_of_counting_the_space_whole(name: str, space: str) -> None:
    cfg = build_config(CONFIGS[name])
    whole_data, latest, now = build_dataset()
    for kind in KINDS:
        whole_data[kind] = [e for e in whole_data[kind] if e["space"] == space]
    whole_data["facility_spaces"] = {space: space}
    whole = compute(whole_data, cfg, latest, now=now)  # type: ignore[arg-type]
    client = client_for(build_dataset()[0])
    parts = []
    for kind in KINDS:
        batch = data_of_kind(kind, space)
        if not batch[kind]:
            continue
        cross = lookups.fetch_cross(client, batch, cfg, frozenset({kind}), space)  # type: ignore[arg-type]
        parts.append(compute(batch, cfg, latest, now=now, kinds=frozenset({kind}), cross=cross))  # type: ignore[arg-type]

    assert summed(parts, "agg", 4) == summed([whole], "agg", 4)
    assert summed(parts, "exclusions", 1) == summed([whole], "exclusions", 1)
    assert summed(parts, "classifications", 2) == summed([whole], "classifications", 2)
    assert summed(parts, "annotation_buckets", 2) == summed([whole], "annotation_buckets", 2)
    listed = sorted(map(str, (item for part in parts for item in part["drilldown"])))
    assert listed == sorted(map(str, whole["drilldown"]))


def test_the_answers_are_looked_up_in_chunks_and_only_for_the_batchs_own_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lookups, "CHUNK", 2)
    monkeypatch.setattr(lookups, "TIME_SERIES_CHUNK", 2)
    client = client_for(build_dataset()[0])
    batch = data_of_kind("assets", A)

    cross = lookups.fetch_cross(client, batch, build_config({}), frozenset({"assets"}), A)  # type: ignore[arg-type]

    assert len(batch["assets"]) > 2
    questions = client.data_modeling.instances.questions
    # parentless assets are asked about in chunks of two, and so are the assets a time series may link to
    assert questions.count(("aggregate", "CogniteTimeSeries.assets")) >= len(batch["assets"]) // 2
    assert isinstance(cross, Cross)


def test_the_assets_up_a_parent_chain_are_fetched_level_by_level_as_far_as_the_depth_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = client_for(build_dataset()[0])
    deep = data_of_kind("assets", B, take=lambda e: e["id"] == "deep-9")
    cfg = build_config({"maxDepth": 3})

    cross = lookups.fetch_cross(client, deep, cfg, frozenset({"assets"}), B)  # type: ignore[arg-type]

    fetched = {xid for _space, xid in cross.assets}
    assert fetched == {"deep-8", "deep-7", "deep-6", "deep-5", "deep-4"}  # maxDepth + 2 levels up, no further
    assert client.data_modeling.instances.questions.count(("retrieve", "CogniteAsset")) == 5


def test_an_asset_in_another_space_is_not_counted_as_loaded() -> None:
    client = client_for(build_dataset()[0])
    own = data_of_kind("assets", A, take=lambda e: e["id"] == "site-a")

    cross = lookups.fetch_cross(client, own, build_config({}), frozenset({"assets"}), A)  # type: ignore[arg-type]

    assert all(space == A for space, _ in cross.assets)  # the root above the site lies outside the facility


def test_the_populated_classification_kinds_are_judged_for_the_whole_space() -> None:
    client = client_for(build_dataset()[0])
    one_asset = data_of_kind("assets", A, take=lambda e: e["id"] == "untyped")  # references no type or class itself

    cross = lookups.fetch_cross(client, one_asset, build_config({}), frozenset({"assets"}), A)  # type: ignore[arg-type]

    assert cross.populated == {"assetType", "assetClass"}


def test_a_missing_notification_view_means_no_work_order_has_one() -> None:
    from cognite.client.exceptions import CogniteAPIError

    client = client_for(build_dataset()[0])

    def missing(*_a: Any, **_k: Any) -> Any:
        raise CogniteAPIError("Views do not exist", 400)

    client.data_modeling.instances.aggregate = missing
    assert lookups._linked_to_notification(client, [(A, "w-1")], A) == set()  # type: ignore[arg-type]


def test_other_errors_are_not_hidden() -> None:
    from cognite.client.exceptions import CogniteAPIError

    client = client_for(build_dataset()[0])

    def denied(*_a: Any, **_k: Any) -> Any:
        raise CogniteAPIError("Forbidden", 403)

    client.data_modeling.instances.aggregate = denied
    with pytest.raises(CogniteAPIError):
        lookups._linked_to_notification(client, [(A, "w-1")], A)  # type: ignore[arg-type]
