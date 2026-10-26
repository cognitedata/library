"""What the Context Quality metrics function writes must fit the module's containers and the contract.

The function's own tests check that it behaves like the baseline implementation. These check the other side: the
records it produces are valid for the containers this module deploys, in the shape contract/contract.json describes.
"""

import sys
from pathlib import Path
from typing import Any

import pytest

from tests.test_context_quality_contract import (
    CONTAINERS,
    CONTRACT,
    DEFAULTS,
    LEGACY_VARIABLES,
    properties_fit_container,
)

FUNCTION_DIR = (
    Path(__file__).resolve().parents[1]
    / "modules"
    / "dashboards"
    / "context_quality_v2"
    / "functions"
    / "fn_context_quality_metrics"
)
sys.path.insert(0, str(FUNCTION_DIR))

from cq import runner
from cq.config import DEFAULT_CONFIG, build_config, require_resources
from cq.metrics import compute
from parity_dataset import build_dataset

CONTAINER_KEY_BY_NAME = {name: key for key, name in CONTAINERS.items()}


class RecordingClient:
    def __init__(self) -> None:
        self.config = type("Config", (), {"project": "test-project"})()
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def post(self, url: str, json: dict[str, Any]) -> None:
        self.calls.append((url, json))


def write_a_run(
    monkeypatch: pytest.MonkeyPatch, overrides: dict[str, Any] | None = None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    def compute_run(_client: Any, cfg: Any) -> tuple[Any, dict[str, Any]]:
        data, latest, now = build_dataset()
        return data, compute(data, cfg, latest, now=now)

    def load_extra_links(_client: Any, _links: Any) -> dict[tuple[str, str], list[Any]]:
        return {
            ("cdf_cdm:CogniteCore:v1", "CogniteAsset"): [
                (space, "a1", {}) for space in build_dataset()[0]["facility_spaces"]
            ]
        }

    monkeypatch.setattr(runner, "compute_run", compute_run)
    monkeypatch.setattr(runner, "load_extra_links", load_extra_links)
    client = RecordingClient()
    cfg = build_config(
        {
            "stream": DEFAULTS["metricsStreamExternalId"],
            "schemaSpace": DEFAULTS["metricsSchemaSpace"],
            "instanceSpace": DEFAULTS["metricsInstanceSpace"],
            **(overrides or {}),
        }
    )
    runner.compute_metrics(
        client,  # type: ignore[arg-type]
        cfg,
        {"extraLinks": [{"model": "cdf_cdm:CogniteCore:v1", "links": ["CogniteAsset.type"]}]},
    )
    return [item for _, body in client.calls for item in body["items"]], {"urls": {url for url, _ in client.calls}}


class ReadingClient(RecordingClient):
    """Answers the Records filter with the `runPart` records written so far, as the stream does."""

    def post(self, url: str, json: dict[str, Any]) -> Any:  # type: ignore[override]
        if not url.endswith("/records/filter"):
            return super().post(url, json)
        schema, container = DEFAULTS["metricsSchemaSpace"], CONTAINERS["relationship"]
        items = [
            {"properties": {schema: {container: item["sources"][0]["properties"]}}}
            for _, body in self.calls
            for item in body["items"]
            if item["sources"][0]["properties"]["rowKind"] == "runPart"
        ]
        return type("Response", (), {"json": lambda _self: {"items": items}})()


def write_a_fanned_out_run(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """One shard per facility space of the parity dataset, then finalize, as the workflow's dynamic task does."""
    facilities = dict(build_dataset()[0]["facility_spaces"])

    def load_shard(_client: Any, spaces: dict[str, str]) -> dict[str, Any]:
        data = build_dataset()[0]
        for key in (
            "assets",
            "equipment",
            "timeseries",
            "files",
            "objects3d",
            "work_orders",
            "notifications",
            "annotations",
        ):
            data[key] = [entity for entity in data[key] if entity["space"] in spaces]
        data["facility_spaces"] = dict(spaces)
        return data

    def load_extra_links(_client: Any, _links: Any, spaces: list[str]) -> dict[tuple[str, str], list[Any]]:
        return {("cdf_cdm:CogniteCore:v1", "CogniteAsset"): [(space, "a1", {}) for space in spaces]}

    monkeypatch.setattr(runner, "load_shard", load_shard)
    monkeypatch.setattr(runner, "load_extra_links", load_extra_links)
    monkeypatch.setattr(runner, "latest_datapoints", lambda _client, _data: build_dataset()[1])
    client = ReadingClient()
    cfg = build_config(
        {
            "stream": DEFAULTS["metricsStreamExternalId"],
            "schemaSpace": DEFAULTS["metricsSchemaSpace"],
            "instanceSpace": DEFAULTS["metricsInstanceSpace"],
        }
    )
    run = {"runId": "run-20261013T053425-c2326a", "runTimestamp": "2026-10-13T05:34:25.000Z"}
    runner.writer.ingest(
        client,
        cfg,
        "relationshipContainer",
        run["runId"],
        runner._parse_iso(run["runTimestamp"]),
        [("run:started", {"rowKind": "run", "status": "started"})],
    )  # type: ignore[arg-type]
    for shard, (space, name) in enumerate(facilities.items()):
        payload = {
            **run,
            "shard": shard,
            "shardCount": len(facilities),
            "spaces": {space: name},
            "extraSpaces": {space: name},
            "extraLinks": [{"model": "cdf_cdm:CogniteCore:v1", "links": ["CogniteAsset.type"]}],
        }
        runner.compute_shard(client, cfg, payload)  # type: ignore[arg-type]
    runner.finalize(client, cfg, {**run, "shardCount": len(facilities)})  # type: ignore[arg-type]
    return [item for _, body in client.calls for item in body["items"]]


def test_a_fanned_out_run_writes_records_that_fit_the_containers_and_the_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items = write_a_fanned_out_run(monkeypatch)

    for item in items:
        source = item["sources"][0]
        properties = source["properties"]
        spec = CONTRACT.rowKinds[properties["rowKind"]]
        assert properties_fit_container(properties, CONTAINER_KEY_BY_NAME[source["source"]["externalId"]]) == []
        assert set(spec.required) <= set(properties) or properties["rowKind"] == "run", item["externalId"]
        assert set(properties) <= set(spec.required + spec.optional), item["externalId"]
        assert item["externalId"].split(":", 1)[1].startswith(tuple(CONTRACT.recordKeyPrefixes))


def test_a_fanned_out_run_writes_every_row_kind_and_completes_last(monkeypatch: pytest.MonkeyPatch) -> None:
    items = write_a_fanned_out_run(monkeypatch)
    kinds = [item["sources"][0]["properties"]["rowKind"] for item in items]

    assert set(kinds) == set(CONTRACT.rowKinds)
    assert kinds.count("runPart") == 2
    assert items[-1]["externalId"].endswith(":run:complete")
    assert items[-1]["sources"][0]["properties"]["rowsWritten"] == len(items) - 4  # started, complete and two parts


def test_the_function_writes_every_row_kind_in_the_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    items, _ = write_a_run(monkeypatch)

    # `runPart` records exist only in a fanned-out run (see below)
    assert {item["sources"][0]["properties"]["rowKind"] for item in items} == set(CONTRACT.rowKinds) - {"runPart"}


def test_the_function_has_no_default_resource_and_the_legacy_identifiers_work_when_passed_explicitly() -> None:
    """A deployment names its own stream and spaces in the task's config; the legacy ones are just one choice."""
    assert [DEFAULT_CONFIG[key] for key in ("stream", "schemaSpace", "instanceSpace")] == ["", "", ""]  # type: ignore[literal-required]
    legacy = build_config(
        {
            "stream": LEGACY_VARIABLES["metricsStreamExternalId"],
            "schemaSpace": LEGACY_VARIABLES["metricsSchemaSpace"],
            "instanceSpace": LEGACY_VARIABLES["metricsInstanceSpace"],
        }
    )
    require_resources(legacy, writes=True)


def test_the_function_writes_to_the_containers_the_contract_names() -> None:
    assert DEFAULT_CONFIG["relationshipContainer"] == CONTAINERS["relationship"]
    assert DEFAULT_CONFIG["fileAnnotationContainer"] == CONTAINERS["fileAnnotation"]


def test_every_record_fits_the_container_it_is_written_to(monkeypatch: pytest.MonkeyPatch) -> None:
    items, _ = write_a_run(monkeypatch)

    assert items
    for item in items:
        source = item["sources"][0]["source"]
        assert source["space"] == DEFAULTS["metricsSchemaSpace"]
        problems = properties_fit_container(
            item["sources"][0]["properties"], CONTAINER_KEY_BY_NAME[source["externalId"]]
        )
        assert problems == [], item["externalId"]
        assert item["space"] == DEFAULTS["metricsInstanceSpace"]


def test_every_record_has_the_required_properties_of_its_row_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    items, _ = write_a_run(monkeypatch)

    for item in items:
        properties = item["sources"][0]["properties"]
        spec = CONTRACT.rowKinds[properties["rowKind"]]
        assert set(spec.required) <= set(properties), item["externalId"]
        assert set(properties) <= set(spec.required + spec.optional), item["externalId"]
        assert CONTAINERS[spec.container] == item["sources"][0]["source"]["externalId"]


def test_the_run_is_only_readable_once_the_complete_marker_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    items, _ = write_a_run(monkeypatch)
    statuses = [
        (index, item["sources"][0]["properties"]["status"])
        for index, item in enumerate(items)
        if item["sources"][0]["properties"]["rowKind"] == "run"
    ]

    assert [status for _, status in statuses] == ["started", "complete"]
    assert statuses[-1][0] == len(items) - 1
    assert set(CONTRACT.runStatuses) >= {status for _, status in statuses}


def test_the_function_only_writes_metrics_dimensions_and_categories_of_the_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items, _ = write_a_run(monkeypatch)
    metrics = {metric.id for metric in CONTRACT.metrics}
    dimensions = {dimension.id for dimension in CONTRACT.dimensions}
    categories = {category.id for category in CONTRACT.exclusionCategories}

    for item in items:
        properties = item["sources"][0]["properties"]
        if properties["rowKind"] in (
            "extraLink",
            "extraLinkGap",
        ):  # named by the user's config file, not by the contract
            continue
        assert properties.get("metricId", next(iter(metrics))) in metrics
        assert properties.get("dimensionId", next(iter(dimensions))) in dimensions
        assert properties.get("exclusionCategory", next(iter(categories))) in categories
        assert properties.get("classificationKind", CONTRACT.classificationKinds[0]) in CONTRACT.classificationKinds


def test_record_keys_start_with_the_prefixes_the_contract_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    items, _ = write_a_run(monkeypatch)

    for item in items:
        row_key = item["externalId"].split(":", 1)[1]
        assert row_key.startswith(tuple(CONTRACT.recordKeyPrefixes)), row_key


def test_not_applicable_rows_and_run_warnings_fit_the_existing_container_without_a_schema_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items, _ = write_a_run(monkeypatch, {"tieringEnabled": True, "tierByTypeCode": {}})

    not_applicable = [i for i in items if i["sources"][0]["properties"].get("notApplicable") is True]
    marker = items[-1]["sources"][0]["properties"]
    assert not_applicable
    for item in items:
        source = item["sources"][0]
        assert (
            properties_fit_container(source["properties"], CONTAINER_KEY_BY_NAME[source["source"]["externalId"]]) == []
        )
    for item in not_applicable:
        properties = item["sources"][0]["properties"]
        assert properties["rowKind"] == "aggregate"
        assert properties["eligibleCount"] == 0
        assert set(properties) <= set(CONTRACT.rowKinds["aggregate"].required + CONTRACT.rowKinds["aggregate"].optional)
    assert isinstance(marker["warnings"], str) and marker["warnings"].startswith("[")
    assert marker["rowsWritten"] == len([i for i in items if i["sources"][0]["properties"]["rowKind"] != "run"])
