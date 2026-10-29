"""compute_metrics, validate and the handler: what is written, in which order, and what is never written."""

import json
from typing import Any

import handler
import pytest
from cq import runner, writer
from cq.config import DEFAULT_CONFIG, build_config
from parity_dataset import build_dataset
from sample_rules import SAMPLE_RESOURCES


class RecordingClient:
    """Stands in for CogniteClient: records every Records API write."""

    def __init__(self) -> None:
        self.config = type("Config", (), {"project": "test-project"})()
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def post(self, url: str, json: dict[str, Any]) -> None:
        self.calls.append((url, json))


def canned_run(monkeypatch: pytest.MonkeyPatch) -> None:
    from cq.metrics import compute

    def compute_run(_client: Any, cfg: Any) -> tuple[Any, dict[str, Any]]:
        data, latest, now = build_dataset()
        return data, compute(data, cfg, latest, now=now)

    monkeypatch.setattr(runner, "compute_run", compute_run)


def records(client: RecordingClient) -> list[dict[str, Any]]:
    return [item for _, body in client.calls for item in body["items"]]


def props(item: dict[str, Any]) -> dict[str, Any]:
    return item["sources"][0]["properties"]


def test_the_complete_marker_is_written_last_and_counts_the_other_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    items = records(client)
    run_rows = [(index, props(item)) for index, item in enumerate(items) if props(item)["rowKind"] == "run"]
    assert [row["status"] for _, row in run_rows] == ["started", "complete"]
    assert run_rows[0][0] == 0
    assert run_rows[-1][0] == len(items) - 1
    assert run_rows[-1][1]["rowsWritten"] == len(items) - 2 == summary["rowsWritten"]
    assert items[-1]["externalId"].endswith(":run:complete")


def test_every_record_goes_to_the_configured_stream_space_and_container(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()
    cfg = build_config({"stream": "s", "schemaSpace": "schema", "instanceSpace": "inst"})

    runner.compute_metrics(client, cfg, {})  # type: ignore[arg-type]

    assert {url for url, _ in client.calls} == {"/api/v1/projects/test-project/streams/s/records"}
    for item in records(client):
        assert item["space"] == "inst"
        source = item["sources"][0]["source"]
        expected = (
            "ContextQualityFileAnnotation"
            if props(item)["rowKind"] == "annotationBucket"
            else "ContextQualityRelationship"
        )
        assert source == {"type": "container", "space": "schema", "externalId": expected}


def test_all_rows_of_one_run_share_the_run_id_and_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    assert {props(item)["runId"] for item in records(client)} == {summary["runId"]}
    assert len({props(item)["runTimestamp"] for item in records(client)}) == 1


def test_a_dry_run_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()

    summary = runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {"dry_run": True})  # type: ignore[arg-type]

    assert client.calls == []
    assert summary["dryRun"] is True
    assert "runId" not in summary


def test_a_dry_run_can_return_the_aggregate_exclusion_and_classification_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)

    summary = runner.compute_metrics(
        RecordingClient(), build_config(SAMPLE_RESOURCES), {"dry_run": True, "return_rows": True}
    )  # type: ignore[arg-type]

    assert {row["rowKind"] for row in summary["rows"]} == {"aggregate", "exclusion", "classification"}
    json.dumps(summary)  # the summary is returned as the function result, so it must be JSON-serialisable


def test_validate_reports_totals_per_metric_without_writing(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()

    result = runner.validate(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    assert client.calls == []
    assert result["action"] == "validate"
    assert result["metrics"]["orphanRate"] == {"eligible": 21, "passing": 19, "issue": 2}
    assert "approvalMix/Approved" in result["metrics"]


def test_records_are_written_in_batches_of_the_configured_size(monkeypatch: pytest.MonkeyPatch) -> None:
    canned_run(monkeypatch)
    client = RecordingClient()

    runner.compute_metrics(client, build_config({**SAMPLE_RESOURCES, "batchSize": 10}), {})  # type: ignore[arg-type]

    assert all(len(body["items"]) <= 10 for _, body in client.calls)
    assert len(client.calls) > 3


def test_the_handler_runs_only_the_production_actions() -> None:
    assert sorted(handler.ACTIONS) == [
        "compute_batch",
        "compute_metrics",
        "compute_shard",
        "export",
        "finalize",
        "plan",
        "validate",
    ]


@pytest.mark.parametrize("action", ["seed_datapoints", "seed_history", "nope", None])
def test_the_handler_rejects_any_other_action(action: str | None) -> None:
    with pytest.raises(ValueError, match="Unknown action"):
        handler.handle({"action": action}, RecordingClient())  # type: ignore[arg-type]


def test_the_handler_applies_the_payload_config_over_the_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_compute(_client: Any, cfg: Any, payload: dict[str, Any]) -> dict[str, Any]:
        seen["cfg"], seen["payload"] = cfg, payload
        return {"ok": True}

    monkeypatch.setitem(handler.ACTIONS, "compute_metrics", fake_compute)

    result = handler.handle(
        {"action": "compute_metrics", "config": {"stream": "other"}, "dry_run": True}, RecordingClient()
    )  # type: ignore[arg-type]

    assert result == {"ok": True}
    assert seen["cfg"]["stream"] == "other"
    assert seen["cfg"]["freshnessDays"] == DEFAULT_CONFIG["freshnessDays"]
    assert seen["payload"]["dry_run"] is True


def test_dict_overrides_replace_the_defaults_instead_of_merging_into_them() -> None:
    """Covered in detail by test_config.py; kept here because the handler's config handling is part of the runner flow."""
    cfg = build_config({"tierByTypeCode": {"widget": 1}, "excludeSpaces": ["only_this"]})

    assert cfg["tierByTypeCode"] == {"widget": 1}  # the default entries are gone
    assert cfg["excludeSpaces"] == ["only_this"]


def test_the_writer_helper_formats_timestamps_the_way_the_records_need() -> None:
    from datetime import UTC, datetime

    assert writer.now_iso(datetime(2026, 10, 13, 5, 34, 25, tzinfo=UTC)) == "2026-10-13T05:34:25.000Z"
