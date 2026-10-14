"""compute_metrics and validate actions."""

from datetime import UTC, datetime
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeId

from cq import writer
from cq.availability import encode_warnings
from cq.config import require_resources
from cq.metrics import compute
from cq.reader import load_all
from cq.types import EntityKey, LoadedData, Row, RunConfig


def latest_datapoints(client: CogniteClient, data: LoadedData) -> dict[EntityKey, datetime | None]:
    ids = [NodeId(t["space"], t["id"]) for t in data["timeseries"]]
    out = {(i.space, i.external_id): None for i in ids}
    for i in range(0, len(ids), 500):
        res = client.time_series.data.retrieve_latest(instance_id=ids[i : i + 500], ignore_unknown_ids=True)
        for r in res:
            ts = r.timestamp
            if isinstance(ts, (list, tuple)) or hasattr(ts, "__len__"):
                ts = ts[0] if len(ts) else None
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts / 1000, tz=UTC)
            iid = r.instance_id
            if iid is not None and isinstance(ts, datetime):
                out[(iid.space, iid.external_id)] = ts.astimezone(UTC)
    return out


class MissingInputError(RuntimeError):
    """The inputs a run needs are not there. Raised before anything is written, so no run is started or completed."""


def compute_run(client: CogniteClient, cfg: RunConfig) -> tuple[LoadedData, dict[str, Any]]:
    data = load_all(client, cfg)
    if not data["facility_spaces"]:
        raise MissingInputError(
            "No facility spaces were found: no instance space holds enough entities, or facilitySpaces is empty. "
            "Nothing was written, so the previous run stays current."
        )
    result = compute(data, cfg, latest_datapoints(client, data))
    return data, result


def build_rows(result: dict[str, Any], cfg: RunConfig) -> tuple[list[Row], list[Row]]:
    return (
        writer.aggregate_rows(result["agg"])
        + writer.exclusion_rows(result["exclusions"])
        + writer.drilldown_rows(result["drilldown"], cfg["drilldownCap"])
        + writer.classification_rows(result["classifications"]),
        writer.annotation_bucket_rows(result["annotation_buckets"]),
    )


def compute_metrics(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    require_resources(cfg, writes=not payload.get("dry_run"))
    data, result = compute_run(client, cfg)
    main_rows, anno_rows = build_rows(result, cfg)
    summary = {
        "action": "compute_metrics",
        "facilities": data["facility_spaces"],
        "aggregateRows": sum(1 for k, _ in main_rows if k.startswith("agg")),
        "exclusionRows": sum(1 for k, _ in main_rows if k.startswith("excl")),
        "drilldownRows": sum(1 for k, _ in main_rows if k.startswith("drill")),
        "classificationRows": sum(1 for k, _ in main_rows if k.startswith("cls")),
        "annotationBucketRows": len(anno_rows),
        "notApplicableRows": sum(1 for k, p in main_rows if k.startswith("agg") and p.get("notApplicable") is True),
        "warnings": result["warnings"],
    }
    if payload.get("dry_run"):
        summary["dryRun"] = True
        if payload.get("return_rows"):
            summary["rows"] = [dict(key=k, **p) for k, p in main_rows if k.startswith(("agg", "excl", "cls"))]
        return summary
    run_id, run_ts = writer.new_run_id(), datetime.now(UTC)
    writer.ingest(
        client,
        cfg,
        "relationshipContainer",
        run_id,
        run_ts,
        [("run:started", writer.run_row(run_id, run_ts, "started")[2])],
    )
    n = writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, main_rows)
    n += writer.ingest(client, cfg, "fileAnnotationContainer", run_id, run_ts, anno_rows)
    # the 'complete' marker is written last: a run is only readable once it exists
    writer.ingest(
        client,
        cfg,
        "relationshipContainer",
        run_id,
        run_ts,
        [
            (
                "run:complete",
                writer.run_row(
                    run_id, run_ts, "complete", warnings=encode_warnings(result["warnings"]), rows_written=n
                )[2],
            )
        ],
    )
    summary.update(runId=run_id, rowsWritten=n)
    return summary


def validate(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Dry check of what compute_metrics would write: totals per metric, no writes."""
    require_resources(cfg, writes=False)
    data, result = compute_run(client, cfg)
    totals: dict[str, dict[str, int]] = {}
    for (metric, *_, sub), (e, p, i, _value_sum) in result["agg"].items():
        t = totals.setdefault(f"{metric}{'/' + sub if sub else ''}", {"eligible": 0, "passing": 0, "issue": 0})
        t["eligible"] += e
        t["passing"] += p
        t["issue"] += i
    return {
        "action": "validate",
        "facilities": data["facility_spaces"],
        "metrics": totals,
        "warnings": result["warnings"],
    }
