"""Reading the views of the extra links, against the Cognite SDK's own data classes (the client is a fake)."""

from typing import Any

import pytest
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DataModel,
    DirectRelation,
    MappedProperty,
    Node,
    Text,
    View,
    ViewId,
)
from cognite.client.data_classes.data_modeling.instances import Properties
from cq.extra_links import ExtraLink
from cq.reader import load_extra_links

CORE = "cdf_cdm:CogniteCore:v1"
ASSET = ViewId("cdf_cdm", "CogniteAsset", "v1")
BOX = ContainerId("cdf_cdm", "CogniteAsset")


def prop(kind: Any) -> MappedProperty:
    return MappedProperty(BOX, "p", kind, True, False, False)


def asset_view() -> View:
    properties = {
        "type": prop(DirectRelation()),
        "tags": prop(Text(is_list=True)),
        "assets": prop(DirectRelation(is_list=True)),
        "name": prop(Text()),
    }
    return View("cdf_cdm", "CogniteAsset", "v1", properties, 0, 0, None, None, None, None, True, "node", True)  # type: ignore[arg-type]


class FakeModels:
    def __init__(self, models: list[DataModel[View]]) -> None:
        self.models, self.calls = models, []  # type: ignore[var-annotated]

    def retrieve(self, ids: Any, inline_views: bool = False) -> list[DataModel[View]]:
        self.calls.append((ids, inline_views))
        return self.models


class FakeInstances:
    def __init__(self, nodes: list[Node]) -> None:
        self.nodes, self.calls = nodes, []  # type: ignore[var-annotated]

    def list(self, instance_type: str, sources: ViewId, space: Any = None, limit: int = 25) -> list[Node]:
        self.calls.append({"instance_type": instance_type, "sources": sources, "space": space, "limit": limit})
        return [n for n in self.nodes if sources in (n.properties or {})]


class FakeClient:
    def __init__(self, models: list[DataModel[View]], nodes: list[Node] | None = None) -> None:
        self.data_modeling = type("DM", (), {})()
        self.data_modeling.data_models = FakeModels(models)
        self.data_modeling.instances = FakeInstances(nodes or [])


def core(views: list[View] | None = None) -> DataModel[View]:
    return DataModel(
        "cdf_cdm", "CogniteCore", "v1", True, 0, 0, None, None, views if views is not None else [asset_view()]
    )


def asset(space: str, xid: str, **props: Any) -> Node:
    return Node(space, xid, 1, 0, 0, None, Properties({ASSET: props}), None)


def links(*names: str) -> list[ExtraLink]:
    return [{"model": CORE, "view": "CogniteAsset", "property": name} for name in names]


def test_the_nodes_of_each_view_are_read_once_from_every_space() -> None:
    client = FakeClient([core()], [asset("plant", "a1", type={"space": "r", "externalId": "t"}), asset("plant", "a2")])

    nodes = load_extra_links(client, links("type", "assets"))  # type: ignore[arg-type]

    assert nodes == {(CORE, "CogniteAsset"): [("plant", {"type": {"space": "r", "externalId": "t"}}), ("plant", {})]}
    assert client.data_modeling.instances.calls == [
        {"instance_type": "node", "sources": ASSET, "space": None, "limit": -1}
    ]


def test_the_model_is_looked_up_with_its_views_inlined() -> None:
    client = FakeClient([core()])

    load_extra_links(client, links("type"))  # type: ignore[arg-type]

    ids, inline = client.data_modeling.data_models.calls[0]
    assert inline is True
    assert ids == ("cdf_cdm", "CogniteCore", "v1")


def test_a_model_that_does_not_exist_fails_the_run_before_any_instance_is_read() -> None:
    client = FakeClient([])

    with pytest.raises(ValueError, match=r"cdf_cdm:CogniteCore:v1.*not found"):
        load_extra_links(client, links("type"))  # type: ignore[arg-type]
    assert client.data_modeling.instances.calls == []


def test_a_view_that_is_not_in_the_model_is_rejected_and_the_views_that_are_listed() -> None:
    client = FakeClient([core()])
    wrong: list[ExtraLink] = [{"model": CORE, "view": "CogniteWidget", "property": "type"}]

    with pytest.raises(ValueError, match=r"CogniteWidget.*CogniteAsset"):
        load_extra_links(client, wrong)  # type: ignore[arg-type]


def test_a_property_that_does_not_exist_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"CogniteAsset\.nope"):
        load_extra_links(FakeClient([core()]), links("nope"))  # type: ignore[arg-type]


def test_a_property_that_is_not_a_link_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"CogniteAsset\.name.*not a direct relation"):
        load_extra_links(FakeClient([core()]), links("name"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"CogniteAsset\.tags.*not a direct relation"):
        load_extra_links(FakeClient([core()]), links("tags"))  # type: ignore[arg-type]


def test_a_list_of_direct_relations_is_a_link() -> None:
    client = FakeClient([core()], [asset("plant", "a1", assets=[{"space": "r", "externalId": "x"}])])

    nodes = load_extra_links(client, links("assets"))  # type: ignore[arg-type]

    assert nodes[(CORE, "CogniteAsset")] == [("plant", {"assets": [{"space": "r", "externalId": "x"}]})]


def test_no_links_reads_nothing() -> None:
    client = FakeClient([core()])

    assert load_extra_links(client, []) == {}  # type: ignore[arg-type]
    assert client.data_modeling.data_models.calls == []
