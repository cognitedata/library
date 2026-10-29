"""Metric availability: which metrics are not applicable, what the run warns about, and how that is written.

Row semantics: a *measured* metric has eligibleCount > 0 (a measured zero is passingCount 0 with
eligible entities); a metric that could not be measured for a reporting unit has eligibleCount 0 and notApplicable true.
"""

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from cq import availability, runner, writer
from cq.config import build_config
from cq.metrics import compute
from parity_dataset import A, B, build_dataset
from sample_rules import SAMPLE_RESOURCES, SAMPLE_RULES


def run(overrides: dict[str, Any] | None = None, mutate: Any = None) -> tuple[dict[str, Any], dict[str, Any]]:
    data, latest, now = build_dataset()
    if mutate:
        mutate(data)
    result = compute(data, build_config({**SAMPLE_RULES, **(overrides or {})}), latest, now=now)  # type: ignore[arg-type]
    return data, result


def agg_rows(result: dict[str, Any]) -> dict[tuple[str, str | None, str], list[Any]]:
    """(metric, subKey, unit) -> [eligible, passing, issue, valueSum], summed over every other slice."""
    out: dict[tuple[str, str | None, str], list[Any]] = {}
    for key, value in result["agg"].items():
        total = out.setdefault((key[0], key[8], key[2]), [0, 0, 0, 0.0])
        for i in range(4):
            total[i] += value[i]
    return out


def not_applicable_keys(result: dict[str, Any]) -> set[tuple[str, str | None, str]]:
    return {(k[0], k[8], k[2]) for k, v in result["agg"].items() if v[0] == 0}


def warning(result: dict[str, Any], code: str, subject: str | None = None) -> list[dict[str, Any]]:
    return [w for w in result["warnings"] if w["code"] == code and (subject is None or w.get("subject") == subject)]


# ---------------------------------------------------------------- zero eligible vs measured zero vs all excluded


def test_a_metric_with_no_eligible_entities_is_written_as_not_applicable() -> None:
    def only_synthetic(data: dict[str, Any]) -> None:
        for t in data["timeseries"]:
            if t["space"] == B:
                t["name"] = "CALC-" + (t["name"] or "x")

    _, result = run(mutate=only_synthetic)

    key = next(k for k in result["agg"] if k[0] == "tsToAssetRate" and k[2] == B)
    assert result["agg"][key][:3] == [0, 0, 0]
    assert agg_rows(result)[("tsToAssetRate", None, B)][0] == 0
    row = next(
        p
        for _, p in writer.aggregate_rows(result["agg"])
        if p["metricId"] == "tsToAssetRate" and p["reportingUnitId"] == B
    )
    assert row["notApplicable"] is True
    assert row["eligibleCount"] == 0
    assert "valueSum" not in row


def test_a_measured_zero_is_not_marked_not_applicable() -> None:
    def nothing_linked(data: dict[str, Any]) -> None:
        for f in data["files"]:
            f["assets"] = []

    _, result = run(mutate=nothing_linked)

    rows = [
        p
        for _, p in writer.aggregate_rows(result["agg"])
        if p["metricId"] == "fileToAssetRate" and p["reportingUnitId"] == B
    ]
    assert rows
    assert all(r["eligibleCount"] > 0 and r["passingCount"] == 0 for r in rows)
    assert all("notApplicable" not in r for r in rows)  # measured: 0 of N pass


def test_not_applicable_and_measured_zero_are_told_apart_by_the_row_not_by_the_count_alone() -> None:
    def shape(data: dict[str, Any]) -> None:
        for t in data["timeseries"]:
            if t["space"] == B:
                t["name"] = "CALC-" + (t["name"] or "x")  # sp_b: every time series excluded
        for f in data["files"]:
            f["assets"] = []  # files: measured, none linked

    _, result = run(mutate=shape)
    rows = [p for _, p in writer.aggregate_rows(result["agg"]) if p["reportingUnitId"] == B]
    na = next(r for r in rows if r["metricId"] == "tsToAssetRate")
    measured = next(r for r in rows if r["metricId"] == "fileToAssetRate")

    assert (na["eligibleCount"], na.get("notApplicable")) == (0, True)
    assert (measured["eligibleCount"] > 0, measured["passingCount"], measured.get("notApplicable")) == (True, 0, None)


def test_an_all_excluded_population_is_not_applicable_and_still_reports_its_exclusions() -> None:
    def all_excluded(data: dict[str, Any]) -> None:
        for f in data["files"]:
            if f["space"] == B:
                f["name"] = (f["name"] or "x") + ".png"

    _, result = run(mutate=all_excluded)

    assert ("fileToAssetRate", None, B) in not_applicable_keys(result)
    assert ("metadataCompleteness", "category", B) in not_applicable_keys(result)
    assert any(k[0] == "files" and k[1] == "user:file-extension" and k[2] == B for k in result["exclusions"])
    reasons = {w["reason"] for w in warning(result, "metric-not-applicable", "fileToAssetRate")}
    assert reasons == {"all-excluded"}


def test_no_not_applicable_row_is_written_for_a_metric_that_has_measured_rows_in_the_unit() -> None:
    _, result = run()

    measured = {(k[0], k[8], k[2]) for k, v in result["agg"].items() if v[0] > 0}
    assert not (measured & not_applicable_keys(result))


def test_a_unit_without_any_entities_of_a_kind_gets_no_rows_for_that_dimension() -> None:
    def no_maintenance_in_b(data: dict[str, Any]) -> None:
        data["work_orders"] = [w for w in data["work_orders"] if w["space"] != B]
        data["notifications"] = [n for n in data["notifications"] if n["space"] != B]

    _, result = run(mutate=no_maintenance_in_b)

    assert not [
        k for k in result["agg"] if k[1] == "maintenance" and k[2] == B
    ]  # not measured and not "not applicable"
    assert not warning(result, "no-source-entities", "work_orders")  # still present in sp_a


def test_a_population_missing_from_every_unit_is_reported_once() -> None:
    def no_annotations(data: dict[str, Any]) -> None:
        data["annotations"] = []

    _, result = run(mutate=no_annotations)

    assert not [k for k in result["agg"] if k[1] == "file-annotation"]
    assert [w["subject"] for w in warning(result, "no-source-entities")] == ["annotations"]


def test_the_registry_covers_every_aggregate_metric_the_sample_dataset_writes_except_the_issue_only_one() -> None:
    _, result = run()
    written = {k[0] for k in result["agg"]}

    assert written - {spec.metric for spec in availability.METRIC_SPECS} == {"activeChildrenUnderRetiredParents"}


# ---------------------------------------------------------------- mappings and unmapped types


def test_an_empty_tier_mapping_makes_the_tier_coverage_metrics_not_applicable_and_warns() -> None:
    _, result = run({"tierByTypeCode": {}})

    for metric, sub in (("monitoringCoverage", None), ("asset3dCoverage", None)):
        assert (metric, sub, A) in not_applicable_keys(result)
        assert {w["reason"] for w in warning(result, "metric-not-applicable", metric)} == {
            "mapping-empty:tierByTypeCode"
        }
    assert [w["subject"] for w in warning(result, "mapping-empty")] == ["tierByTypeCode"]


def test_mappings_that_are_present_raise_no_mapping_warning() -> None:
    _, result = run()

    assert not warning(result, "mapping-empty")


def test_asset_types_that_cannot_be_mapped_are_counted_with_a_bounded_list_of_codes() -> None:
    _, result = run()

    (w,) = warning(result, "unmapped-asset-types")
    assert w["unmapped"] == 2  # the two assets typed "widget" (area-a and stray)
    assert w["untyped"] == 3  # the untyped asset and the two sites, which have a parent and so are not roots
    assert w["codes"] == ["widget"]


def test_the_unmapped_code_list_is_bounded_and_sorted() -> None:
    def many_codes(data: dict[str, Any]) -> None:
        refs = {}
        for i in range(25):
            ref = ("sp_ref", f"type-extra-{i:02d}")
            data["asset_types"][ref] = f"code_{24 - i:02d}"
            refs[i] = ref
        for i, asset in enumerate(a for a in data["assets"] if a["parent"] and a["parent"][0] in (A, B)):
            asset["type"] = refs[i % 25]

    _, result = run(mutate=many_codes)

    (w,) = warning(result, "unmapped-asset-types")
    assert len(w["codes"]) == availability.MAX_LISTED_CODES
    assert w["codes"] == sorted(w["codes"])


# ---------------------------------------------------------------- warnings: shape, determinism, truncation


def test_drilldown_results_cut_at_the_cap_are_reported_with_what_was_dropped() -> None:
    def many_orphans(data: dict[str, Any]) -> None:
        for i in range(12):
            data["assets"].append(
                {
                    "space": A,
                    "id": f"orphan-{i}",
                    "name": f"O{i}",
                    "tags": [],
                    "parent": None,
                    "type": None,
                    "aclass": None,
                    "object3d": None,
                }
            )

    _, result = run({"drilldownCap": 5}, many_orphans)

    (w,) = warning(result, "drilldown-truncated", "orphansByTier")
    assert w["limit"] == 5
    assert w["dropped"] == 7
    assert w["units"] == 1


def test_nothing_is_reported_as_truncated_when_the_cap_is_not_reached() -> None:
    _, result = run()

    assert not warning(result, "drilldown-truncated")


def test_warnings_are_plain_serialisable_values_sorted_by_code_and_subject() -> None:
    _, result = run({"tierByTypeCode": {}})

    encoded = json.dumps(result["warnings"])
    assert json.loads(encoded) == result["warnings"]
    order = [(w["code"], w.get("subject") or "", w.get("reason") or "") for w in result["warnings"]]
    assert order == sorted(order)
    assert all(set(w) >= {"code", "message"} for w in result["warnings"])


def test_warnings_are_deterministic_whatever_the_input_order() -> None:
    def reverse(data: dict[str, Any]) -> None:
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
            data[key].reverse()

    _, first = run({"tierByTypeCode": {}})
    _, second = run({"tierByTypeCode": {}}, reverse)

    assert availability.encode_warnings(first["warnings"]) == availability.encode_warnings(second["warnings"])


def test_warnings_carry_no_entity_names_or_ids() -> None:
    _, result = run({"tierByTypeCode": {}, "drilldownCap": 1})
    text = availability.encode_warnings(result["warnings"]) or ""
    data, _, _ = build_dataset()
    leaked = [
        e["id"] for key in ("assets", "equipment", "timeseries", "files") for e in data[key] if f'"{e["id"]}"' in text
    ]

    assert leaked == []


def test_no_warnings_encode_to_nothing() -> None:
    assert availability.encode_warnings([]) is None


def test_the_encoded_warnings_are_compact_json_and_round_trip() -> None:
    warnings = [{"code": "b", "message": "m"}, {"code": "a", "subject": "x", "message": "é"}]

    encoded = availability.encode_warnings(warnings)

    assert encoded is not None and " " not in encoded.replace("m", "")
    assert json.loads(encoded) == sorted(warnings, key=lambda w: (w["code"], w.get("subject") or ""))


def test_too_many_warnings_are_cut_deterministically_and_say_so() -> None:
    warnings = [{"code": "metric-not-applicable", "subject": f"metric{i:03d}", "message": "x" * 60} for i in range(200)]

    encoded = availability.encode_warnings(warnings)
    assert encoded is not None
    decoded = json.loads(encoded)

    assert len(encoded) <= availability.MAX_WARNINGS_LENGTH
    assert decoded[-1]["code"] == "warnings-truncated"
    assert decoded[-1]["dropped"] == 200 - (len(decoded) - 1)
    assert availability.encode_warnings(warnings) == encoded


# ---------------------------------------------------------------- the run


class RecordingClient:
    def __init__(self, fail_after: int | None = None) -> None:
        self.config = type("Config", (), {"project": "p"})()
        self.calls: list[dict[str, Any]] = []
        self.fail_after = fail_after

    def post(self, url: str, json: dict[str, Any]) -> None:
        if self.fail_after is not None and len(self.calls) >= self.fail_after:
            raise RuntimeError("the API failed")
        self.calls.append(json)

    def records(self) -> list[dict[str, Any]]:
        return [item for call in self.calls for item in call["items"]]


def props(item: dict[str, Any]) -> dict[str, Any]:
    return item["sources"][0]["properties"]


def canned(monkeypatch: pytest.MonkeyPatch, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = build_config({**SAMPLE_RULES, **SAMPLE_RESOURCES, **(overrides or {})})

    def compute_run(_client: Any, run_cfg: Any) -> tuple[Any, dict[str, Any]]:
        data, latest, now = build_dataset()
        return data, compute(data, run_cfg, latest, now=now)

    monkeypatch.setattr(runner, "compute_run", compute_run)
    return cfg  # type: ignore[return-value]


def test_the_complete_marker_carries_the_warnings_and_the_true_row_count(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = canned(monkeypatch, {"tierByTypeCode": {}})
    client = RecordingClient()

    summary = runner.compute_metrics(client, cfg, {})  # type: ignore[arg-type]

    records = client.records()
    marker = props(records[-1])
    started = props(records[0])
    others = [r for r in records if props(r)["rowKind"] != "run"]
    assert marker["status"] == "complete" and started["status"] == "started"
    assert marker["rowsWritten"] == len(others) == summary["rowsWritten"]
    assert "warnings" not in started
    decoded = json.loads(marker["warnings"])
    assert {w["code"] for w in decoded} >= {"mapping-empty", "metric-not-applicable"}
    assert decoded == json.loads(availability.encode_warnings(summary["warnings"]) or "[]")


def test_rows_written_counts_the_not_applicable_rows_too(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = canned(monkeypatch, {"tierByTypeCode": {}})
    client = RecordingClient()

    runner.compute_metrics(client, cfg, {})  # type: ignore[arg-type]

    na = [r for r in client.records() if props(r).get("notApplicable") is True]
    assert na and all(props(r)["eligibleCount"] == 0 for r in na)
    assert props(client.records()[-1])["rowsWritten"] == len(
        [r for r in client.records() if props(r)["rowKind"] != "run"]
    )


def test_a_run_without_warnings_writes_a_marker_without_the_property(monkeypatch: pytest.MonkeyPatch) -> None:
    def clean(data: dict[str, Any]) -> None:
        for a in data["assets"]:
            if a["type"] is None or data["asset_types"].get(a["type"]) == "widget":
                a["type"] = ("sp_ref", "type-pump")

    def compute_run(_client: Any, run_cfg: Any) -> tuple[Any, dict[str, Any]]:
        data, latest, now = build_dataset()
        clean(data)
        return data, compute(data, run_cfg, latest, now=now)

    monkeypatch.setattr(runner, "compute_run", compute_run)
    client = RecordingClient()

    runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    marker = props(client.records()[-1])
    codes = {w["code"] for w in json.loads(marker.get("warnings", "[]"))}
    assert "unmapped-asset-types" not in codes


def test_a_dry_run_writes_nothing_and_reports_the_warnings_and_not_applicable_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = canned(monkeypatch, {"tierByTypeCode": {}})
    client = RecordingClient()

    summary = runner.compute_metrics(client, cfg, {"dry_run": True, "return_rows": True})  # type: ignore[arg-type]

    assert client.calls == []
    assert summary["dryRun"] is True
    assert summary["notApplicableRows"] == len([r for r in summary["rows"] if r.get("notApplicable") is True]) > 0
    assert {w["code"] for w in summary["warnings"]} >= {"mapping-empty"}
    json.dumps(summary)


def test_validate_reports_the_warnings_without_writing(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = canned(monkeypatch, {"tierByTypeCode": {}})
    client = RecordingClient()

    result = runner.validate(client, cfg, {})  # type: ignore[arg-type]

    assert client.calls == []
    assert {w["code"] for w in result["warnings"]} >= {"mapping-empty"}


def test_a_failure_while_writing_never_produces_a_complete_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = canned(monkeypatch)
    for fail_after in (1, 2, 3):
        client = RecordingClient(fail_after=fail_after)

        with pytest.raises(RuntimeError):
            runner.compute_metrics(client, cfg, {"batchSize": 50})  # type: ignore[arg-type]

        assert all(props(r).get("status") != "complete" for r in client.records())


def test_a_failure_while_computing_writes_nothing_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(_client: Any, _cfg: Any) -> tuple[Any, dict[str, Any]]:
        raise RuntimeError("required view unreadable")

    monkeypatch.setattr(runner, "compute_run", broken)
    client = RecordingClient()

    with pytest.raises(RuntimeError):
        runner.compute_metrics(client, build_config(SAMPLE_RESOURCES), {})  # type: ignore[arg-type]

    assert client.calls == []


def test_the_run_timestamp_is_shared_and_the_marker_is_last(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = canned(monkeypatch)
    client = RecordingClient()

    runner.compute_metrics(client, cfg, {})  # type: ignore[arg-type]

    records = client.records()
    assert props(records[-1])["status"] == "complete"
    assert [props(r)["status"] for r in records if props(r)["rowKind"] == "run"] == ["started", "complete"]
    assert len({props(r)["runTimestamp"] for r in records}) == 1
    datetime.strptime(props(records[0])["runTimestamp"], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=UTC)
