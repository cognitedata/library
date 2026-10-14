"""The reader and the latest-datapoints lookup, run against real Cognite SDK data classes.

The client is a fake, but every value it returns is built from the SDK's own classes (Node, Edge, Properties,
Datapoints, ViewId, NodeId), so a change in how the installed SDK 7.x shapes them shows up here.
"""

import inspect
from datetime import UTC, datetime
from typing import Any

import pytest
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import Token
from cognite.client.data_classes import Datapoints
from cognite.client.data_classes.data_modeling import Edge, Node, NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import Properties
from cognite.client.exceptions import CogniteAPIError
from cq.config import build_config
from cq.reader import load_all
from cq.runner import latest_datapoints

CDM, IDM = "cdf_cdm", "cdf_idm"
REF, PLANT = "ref_space", "plant_space"
TS_MS = 1_760_000_000_000


def view(space: str, xid: str) -> ViewId:
    return ViewId(space, xid, "v1")


def node(space: str, xid: str, view_id: ViewId, **props: Any) -> Node:
    return Node(space, xid, 1, 0, 0, None, Properties({view_id: props}), None)


def edge(xid: str, view_id: ViewId, start: tuple[str, str], end: tuple[str, str], **props: Any) -> Edge:
    return Edge(PLANT, xid, 1, (CDM, "diagrams.AssetLink"), 0, 0, start, end, None, Properties({view_id: props}))


class FakeInstances:
    def __init__(self, instances: list[Node | Edge], errors: dict[str, CogniteAPIError] | None = None) -> None:
        self.instances = instances
        self.errors = errors or {}
        self.calls: list[dict[str, Any]] = []

    def list(self, instance_type: str, sources: ViewId, space: Any = None, limit: int = 25) -> list[Node | Edge]:
        self.calls.append({"instance_type": instance_type, "sources": sources, "space": space, "limit": limit})
        if sources.external_id in self.errors:
            raise self.errors[sources.external_id]
        kind = Edge if instance_type == "edge" else Node
        return [i for i in self.instances if isinstance(i, kind) and sources in (i.properties or {})]


class FakeDatapoints:
    def __init__(self, latest: dict[NodeId, list[int]]) -> None:
        self.latest = latest
        self.requested: list[list[NodeId]] = []

    def retrieve_latest(self, instance_id: list[NodeId], ignore_unknown_ids: bool = False) -> list[Datapoints]:
        self.requested.append(list(instance_id))
        return [
            Datapoints(instance_id=i, timestamp=self.latest[i], value=[1.0] * len(self.latest[i]))
            for i in instance_id
            if i in self.latest
        ]


class FakeClient:
    def __init__(
        self,
        instances: list[Node | Edge],
        latest: dict[NodeId, list[int]] | None = None,
        errors: dict[str, CogniteAPIError] | None = None,
    ) -> None:
        self.data_modeling = type("DataModeling", (), {"instances": FakeInstances(instances, errors)})()
        self.time_series = type("TimeSeries", (), {"data": FakeDatapoints(latest or {})})()


def source_instances() -> list[Node | Edge]:
    asset_type, pump = view(CDM, "CogniteAssetType"), view(CDM, "CogniteAsset")
    return [
        node(REF, "type-pump", asset_type, code="pump", name="Pump"),
        node(REF, "type-no-name", asset_type, code="valve", name=None),
        node(REF, "class-1", view(CDM, "CogniteAssetClass"), name="Rotating", code="rot"),
        node(REF, "et-1", view(CDM, "CogniteEquipmentType"), code="centrifugal_pump", name="Centrifugal Pump"),
        node(REF, "cat-1", view(CDM, "CogniteFileCategory"), code="datasheet", name=None),
        node(
            PLANT,
            "a-1",
            pump,
            name="A-1",
            tags=["division:x"],
            parent={"space": REF, "externalId": "root"},
            type={"space": REF, "externalId": "type-pump"},
            assetClass={"space": REF, "externalId": "class-1"},
            object3D={"space": PLANT, "externalId": "o-1"},
        ),
        node(
            PLANT,
            "e-1",
            view(CDM, "CogniteEquipment"),
            name="E-1",
            asset={"space": PLANT, "externalId": "a-1"},
            equipmentType={"space": REF, "externalId": "et-1"},
            serialNumber="SN",
            manufacturer="Acme",
        ),
        node(
            PLANT,
            "ts-1",
            view(CDM, "CogniteTimeSeries"),
            name="TS-1",
            assets=[{"space": PLANT, "externalId": "a-1"}],
            sourceUnit="bar",
        ),
        node(
            PLANT,
            "f-1",
            view(CDM, "CogniteFile"),
            name="F-1",
            category={"space": REF, "externalId": "cat-1"},
            assets=[{"space": PLANT, "externalId": "a-1"}],
            sourceId="src",
        ),
        node(PLANT, "o-1", view(CDM, "Cognite3DObject"), name="O-1", xMin=0, xMax=1, yMin=0, yMax=1, zMin=0, zMax=1),
        node(PLANT, "o-2", view(CDM, "Cognite3DObject"), name="O-2", xMin=0),
        node(
            PLANT,
            "w-1",
            view(IDM, "CogniteMaintenanceOrder"),
            name="W-1",
            mainAsset={"space": PLANT, "externalId": "a-1"},
            endTime="2026-01-01T00:00:00Z",
        ),
        node(
            PLANT,
            "n-1",
            view(IDM, "CogniteNotification"),
            name="N-1",
            asset={"space": PLANT, "externalId": "a-1"},
            maintenanceOrder={"space": PLANT, "externalId": "w-1"},
        ),
        edge(
            "an-1",
            view(CDM, "CogniteDiagramAnnotation"),
            (PLANT, "f-1"),
            (PLANT, "a-1"),
            confidence=0.9,
            status="Approved",
            sourceCreatedTime="2026-10-01T10:00:00Z",
        ),
        # an instance in a space with too few entities is read but dropped from scoring
        node("shared", "stray", pump, name="Stray"),
    ]


def test_load_all_reads_every_view_across_every_space_and_maps_sdk_nodes_to_dicts() -> None:
    client = FakeClient(source_instances())
    cfg = build_config({"minFacilityEntities": 1, "excludeSpaces": [REF, "shared", "cdf_cdm"]})

    data = load_all(client, cfg)  # type: ignore[arg-type]

    assert all(call["space"] is None for call in client.data_modeling.instances.calls)
    assert all(call["limit"] == -1 for call in client.data_modeling.instances.calls)
    assert data["facility_spaces"] == {PLANT: PLANT}
    assert data["asset_types"] == {(REF, "type-pump"): "pump", (REF, "type-no-name"): "valve"}
    assert data["class_labels"]["assetType"] == {(REF, "type-pump"): "Pump", (REF, "type-no-name"): "valve"}
    assert data["class_labels"]["fileCategory"] == {(REF, "cat-1"): "datasheet"}
    assert data["assets"] == [
        {
            "space": PLANT,
            "id": "a-1",
            "name": "A-1",
            "tags": ["division:x"],
            "parent": (REF, "root"),
            "type": (REF, "type-pump"),
            "aclass": (REF, "class-1"),
            "object3d": (PLANT, "o-1"),
        }
    ]
    assert data["equipment"][0]["asset"] == (PLANT, "a-1")
    assert data["timeseries"][0]["assets"] == [(PLANT, "a-1")]
    assert [o["bbox"] for o in data["objects3d"]] == [True, False]
    assert data["work_orders"][0]["completed"] is True
    assert data["notifications"][0]["work_order"] == (PLANT, "w-1")
    assert data["annotations"] == [
        {
            "space": PLANT,
            "id": "an-1",
            "file": (PLANT, "f-1"),
            "asset": (PLANT, "a-1"),
            "confidence": 0.9,
            "status": "Approved",
            "created": datetime(2026, 10, 1, 10, 0, tzinfo=UTC),
        }
    ]
    assert data["entity_counts_by_space"][PLANT] == 9


def test_entities_outside_the_facility_spaces_are_dropped() -> None:
    cfg = build_config({"facilitySpaces": {PLANT: "plant"}})

    data = load_all(FakeClient(source_instances()), cfg)  # type: ignore[arg-type]

    assert {a["space"] for a in data["assets"]} == {PLANT}


def test_latest_datapoints_turns_sdk_timestamps_into_aware_utc_datetimes() -> None:
    known, unknown, empty = NodeId(PLANT, "ts-1"), NodeId(PLANT, "ts-2"), NodeId(PLANT, "ts-3")
    client = FakeClient([], {known: [TS_MS], empty: []})
    data: Any = {
        "timeseries": [{"space": PLANT, "id": "ts-1"}, {"space": PLANT, "id": "ts-2"}, {"space": PLANT, "id": "ts-3"}]
    }

    latest = latest_datapoints(client, data)  # type: ignore[arg-type]

    assert latest == {
        (PLANT, "ts-1"): datetime.fromtimestamp(TS_MS / 1000, tz=UTC),
        (PLANT, "ts-2"): None,
        (PLANT, "ts-3"): None,
    }
    assert unknown not in client.time_series.data.latest


def test_latest_datapoints_asks_in_batches_of_500() -> None:
    client = FakeClient([])
    data: Any = {"timeseries": [{"space": PLANT, "id": f"ts-{i}"} for i in range(1201)]}

    latest_datapoints(client, data)  # type: ignore[arg-type]

    assert [len(batch) for batch in client.time_series.data.requested] == [500, 500, 201]


def test_the_installed_sdk_offers_the_calls_the_function_makes() -> None:
    """The fakes above stand in for these real signatures, so check the real ones as well."""
    global_config.disable_pypi_version_check = True
    client = CogniteClient(
        ClientConfig(
            client_name="test", project="p", credentials=Token("t"), base_url="https://example.cognitedata.com"
        )
    )

    assert "json" in inspect.signature(client.post).parameters
    assert {"instance_type", "sources", "space", "limit"} <= set(
        inspect.signature(client.data_modeling.instances.list).parameters
    )
    assert {"instance_id", "ignore_unknown_ids"} <= set(
        inspect.signature(client.time_series.data.retrieve_latest).parameters
    )
    assert client.config.project == "p"


def missing_view(view: str) -> CogniteAPIError:
    return CogniteAPIError(f"One or more views do not exist: '{view}'.", code=400)


def test_the_maintenance_views_are_optional_and_their_absence_is_recorded() -> None:
    errors = {
        "CogniteMaintenanceOrder": missing_view("cdf_idm:CogniteMaintenanceOrder/v1"),
        "CogniteNotification": missing_view("cdf_idm:CogniteNotification/v1"),
    }
    cfg = build_config({"minFacilityEntities": 1, "excludeSpaces": [REF, "shared", "cdf_cdm"]})

    data = load_all(FakeClient(source_instances(), errors=errors), cfg)  # type: ignore[arg-type]

    assert data["work_orders"] == [] and data["notifications"] == []
    assert data.get("unavailable_sources") == ["cdf_idm.CogniteMaintenanceOrder", "cdf_idm.CogniteNotification"]
    assert [a["id"] for a in data["assets"]] == ["a-1"]  # everything else was still read


def test_nothing_is_recorded_as_unavailable_when_every_view_exists() -> None:
    data = load_all(
        FakeClient(source_instances()),
        build_config({"minFacilityEntities": 1, "excludeSpaces": [REF, "shared", "cdf_cdm"]}),
    )  # type: ignore[arg-type]

    assert data.get("unavailable_sources") == []


@pytest.mark.parametrize(
    "view",
    [
        "CogniteAsset",
        "CogniteTimeSeries",
        "CogniteFile",
        "Cognite3DObject",
        "CogniteEquipment",
        "CogniteDiagramAnnotation",
    ],
)
def test_a_missing_required_view_fails_the_read_instead_of_being_skipped(view: str) -> None:
    errors = {view: missing_view(f"cdf_cdm:{view}/v1")}

    with pytest.raises(CogniteAPIError):
        load_all(FakeClient(source_instances(), errors=errors), build_config(None))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "error",
    [
        CogniteAPIError("Forbidden", code=403),
        CogniteAPIError("Internal error", code=500),
        CogniteAPIError("Some other bad request", code=400),
    ],
)
def test_other_errors_on_an_optional_view_still_fail_the_read(error: CogniteAPIError) -> None:
    with pytest.raises(CogniteAPIError):
        load_all(FakeClient(source_instances(), errors={"CogniteMaintenanceOrder": error}), build_config(None))  # type: ignore[arg-type]


def test_a_run_with_no_facility_spaces_fails_before_anything_is_written() -> None:
    from cq.runner import MissingInputError, compute_run

    cfg = build_config({"facilitySpaces": {}, "minFacilityEntities": 10_000})

    with pytest.raises(MissingInputError, match="No facility spaces"):
        compute_run(FakeClient(source_instances()), cfg)  # type: ignore[arg-type]


def test_an_unavailable_optional_source_ends_up_in_the_run_warnings() -> None:
    from cq.runner import compute_run

    errors = {
        "CogniteMaintenanceOrder": missing_view("cdf_idm:CogniteMaintenanceOrder/v1"),
        "CogniteNotification": missing_view("cdf_idm:CogniteNotification/v1"),
    }
    cfg = build_config({"minFacilityEntities": 1, "excludeSpaces": [REF, "shared", "cdf_cdm"]})

    _, result = compute_run(FakeClient(source_instances(), errors=errors), cfg)  # type: ignore[arg-type]

    codes = {(w["code"], w.get("subject")) for w in result["warnings"]}
    assert ("optional-source-unavailable", "cdf_idm.CogniteMaintenanceOrder") in codes
    assert ("optional-source-unavailable", "cdf_idm.CogniteNotification") in codes
    assert ("no-source-entities", "work_orders") not in codes  # explained by the unavailable view, not repeated
    assert not [k for k in result["agg"] if k[1] == "maintenance"]  # nothing is reported for maintenance
