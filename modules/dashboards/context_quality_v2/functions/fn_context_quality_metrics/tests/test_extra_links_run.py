"""compute_metrics with the payload's extraLinks: parsed first, counted per facility, written before the complete marker."""

from typing import Any

import pytest
from cq import runner
from cq.config import build_config
from cq.extra_links import ExtraLink, ViewNodes
from parity_dataset import build_dataset
from sample_rules import SAMPLE_RESOURCES
from test_runner import RecordingClient, canned_run, props, records

CORE = "cdf_cdm:CogniteCore:v1"
PAYLOAD = {"extraLinks": [{"model": CORE, "links": ["CogniteAsset.type", "CogniteEquipment.equipmentType"]}]}


def fake_loader(monkeypatch: pytest.MonkeyPatch) -> list[list[ExtraLink]]:
    """Replaces the SDK read; returns the links each call was asked for."""
    asked: list[list[ExtraLink]] = []
    facility = next(iter(build_dataset()[0]["facility_spaces"]))

    def load(_client: Any, links: list[ExtraLink]) -> ViewNodes:
        asked.append(links)
        ref = {"space": "r", "externalId": "t"}
        return {
            (CORE, "CogniteAsset"): [(facility, {"type": ref}), (facility, {})],
            (CORE, "CogniteEquipment"): [(facility, {})],
            ("o:Other:v1", "Ucp"): [("sp_ucp", {}) for _ in range(10)],
        }

    monkeypatch.setattr(runner, "load_extra_links", load)
    return asked


def extra_rows(client: RecordingClient) -> list[dict[str, Any]]:
    return [props(item) for item in records(client) if props(item)["rowKind"] == "extraLink"]


def test_the_extra_links_are_counted_and_written_before_the_complete_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    asked = fake_loader(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), PAYLOAD)  # type: ignore[arg-type]

    assert [(link["view"], link["property"]) for link in asked[0]] == [
        ("CogniteAsset", "type"),
        ("CogniteEquipment", "equipmentType"),
    ]
    facility = next(iter(build_dataset()[0]["facility_spaces"]))
    rows = {(row["dimensionId"], row["metricId"], row["reportingUnitId"]): row for row in extra_rows(client)}
    asset = rows[(f"{CORE}/CogniteAsset", "type", facility)]
    assert (asset["eligibleCount"], asset["passingCount"], asset["issueCount"]) == (2, 1, 1)
    items = records(client)
    assert props(items[-1])["rowKind"] == "run"
    assert summary["extraLinkRows"] == len(extra_rows(client))
    assert props(items[-1])["rowsWritten"] == len(items) - 2


def test_a_run_without_extra_links_writes_none_and_reads_no_model(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    asked = fake_loader(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    assert extra_rows(client) == []
    assert summary["extraLinkRows"] == 0
    assert asked == []


def test_a_malformed_file_fails_before_anything_is_read_or_written(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(runner, "compute_run", lambda *a, **k: called.append("compute"))
    client = RecordingClient()

    with pytest.raises(ValueError, match=r"View\.property"):
        runner.compute_metrics(
            client,  # type: ignore[arg-type]
            build_config(SAMPLE_RESOURCES),
            {"extraLinks": [{"model": CORE, "links": ["CogniteAsset"]}]},
        )

    assert called == []
    assert client.calls == []


def test_a_link_the_model_does_not_have_fails_before_anything_is_written(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)

    def load(*_a: Any, **_k: Any) -> ViewNodes:
        raise ValueError("The view CogniteWidget is not in the model")

    monkeypatch.setattr(runner, "load_extra_links", load)
    client = RecordingClient()

    with pytest.raises(ValueError, match="CogniteWidget"):
        runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), PAYLOAD)  # type: ignore[arg-type]

    assert client.calls == []


def test_a_dry_run_returns_the_extra_link_rows_and_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    fake_loader(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(
        client,  # type: ignore[arg-type]
        build_config(SAMPLE_RESOURCES),
        {**PAYLOAD, "dry_run": True, "return_rows": True},
    )

    assert client.calls == []
    assert {row["rowKind"] for row in summary["rows"]} >= {"extraLink"}


def test_a_space_that_holds_only_the_configured_views_instances_becomes_a_facility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canned_run(monkeypatch)
    fake_loader(monkeypatch)
    client = RecordingClient()
    payload = {
        "extraLinks": [{"model": CORE, "links": ["CogniteAsset.type"]}, {"model": "o:Other:v1", "links": ["Ucp.link"]}]
    }

    runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), payload)  # type: ignore[arg-type]

    ucp = [row for row in extra_rows(client) if row["reportingUnitId"] == "sp_ucp"]
    assert [(row["metricId"], row["eligibleCount"]) for row in ucp] == [("link", 10)]
    assert ucp[0]["facilityId"] == "sp_ucp"
