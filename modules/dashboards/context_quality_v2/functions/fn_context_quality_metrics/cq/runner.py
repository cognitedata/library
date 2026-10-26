"""compute_metrics and validate actions."""

import json
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeId

from cq import writer
from cq.availability import encode_warnings, extra_link_truncation_warnings
from cq.config import require_resources
from cq.discovery import discover_facility_spaces
from cq.extra_links import (
    ExtraLink,
    count_links,
    extra_facility_spaces,
    extra_facility_spaces_from_counts,
    find_gaps,
    parse_extra_links,
)
from cq.fanout import build_tasks, merge_warnings, pack_spaces
from cq.metrics import compute
from cq.reader import (
    count_entities_by_space_remote,
    count_extra_link_instances,
    load_all,
    load_extra_links,
    load_shard,
    validate_extra_links,
)
from cq.types import EntityKey, LoadedData, Row, RowProps, RunConfig


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


def extra_link_rows(
    client: CogniteClient, links: list[ExtraLink], data: LoadedData, cfg: RunConfig
) -> tuple[list[Row], list[dict[str, Any]]]:
    """The rows for the payload's extra links: the counts per facility, then the instances without the link.

    Also returns the warnings for lists cut at the cap. A run without links reads no model.
    """
    if not links:
        return [], []
    nodes = load_extra_links(client, links)
    facilities = extra_facility_spaces(nodes, data["facility_spaces"], cfg)
    gaps = find_gaps(links, nodes, facilities)
    rows = writer.extra_link_rows(count_links(links, nodes, facilities), facilities)
    rows += writer.extra_link_gap_rows(gaps, facilities, cfg["drilldownCap"])
    return rows, extra_link_truncation_warnings(gaps, cfg["drilldownCap"])


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
    links = parse_extra_links(payload.get("extraLinks"))
    data, result = compute_run(client, cfg)
    main_rows, anno_rows = build_rows(result, cfg)
    link_rows, link_warnings = extra_link_rows(client, links, data, cfg)
    main_rows += link_rows
    warnings = [*result["warnings"], *link_warnings]
    summary = {
        "action": "compute_metrics",
        "facilities": data["facility_spaces"],
        "aggregateRows": sum(1 for k, _ in main_rows if k.startswith("agg")),
        "exclusionRows": sum(1 for k, _ in main_rows if k.startswith("excl")),
        "drilldownRows": sum(1 for k, _ in main_rows if k.startswith("drill")),
        "classificationRows": sum(1 for k, _ in main_rows if k.startswith("cls")),
        "extraLinkRows": sum(1 for k, _ in main_rows if k.startswith("xlink")),
        "extraLinkGapRows": sum(1 for k, _ in main_rows if k.startswith("xgap")),
        "annotationBucketRows": len(anno_rows),
        "notApplicableRows": sum(1 for k, p in main_rows if k.startswith("agg") and p.get("notApplicable") is True),
        "warnings": warnings,
    }
    if payload.get("dry_run"):
        summary["dryRun"] = True
        if payload.get("return_rows"):
            summary["rows"] = [
                dict(key=k, **p) for k, p in main_rows if k.startswith(("agg", "excl", "cls", "xlink", "xgap"))
            ]
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
                writer.run_row(run_id, run_ts, "complete", warnings=encode_warnings(warnings), rows_written=n)[2],
            )
        ],
    )
    summary.update(runId=run_id, rowsWritten=n)
    return summary


def _parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def plan(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step one of a fanned-out run: finds the facilities without loading anything, starts the run, and returns the shards.

    The output is read by the workflow's dynamic task, which runs `compute_shard` once per returned task, and by
    `finalize`. Facilities are found with aggregate counts per space, the same rule `load_all` applies to what it reads.
    A shard holds whole spaces (see cq.fanout), so nothing a shard writes is written by another.

    Payload: `functionExternalId` (the function the shards call), and the same `config` and `extraLinks` as
    `compute_metrics`, which every shard receives.
    """
    require_resources(cfg, writes=True)
    function_external_id = payload.get("functionExternalId")
    if not isinstance(function_external_id, str) or not function_external_id:
        raise ValueError("plan needs functionExternalId: the external id of this function, which runs the shards")
    links = parse_extra_links(payload.get("extraLinks"))
    counts, _unavailable = count_entities_by_space_remote(client)
    core = discover_facility_spaces(counts, cfg)
    if not core:
        raise MissingInputError(
            "No facility spaces were found: no instance space holds enough entities, or facilitySpaces is empty. "
            "Nothing was written, so the previous run stays current."
        )
    view_ids = validate_extra_links(client, links) if links else {}
    link_counts = count_extra_link_instances(client, view_ids)
    per_space: dict[str, int] = defaultdict(int)
    for by_space in link_counts.values():
        for space, count in by_space.items():
            per_space[space] += count
    facilities = extra_facility_spaces_from_counts(dict(per_space), core, cfg) if links else dict(core)
    empty_views = [
        [model, view]
        for (model, view), by_space in link_counts.items()
        if not any(space in facilities for space in by_space)
    ]
    weights = {
        space: (counts.get(space, 0) if space in core else 0) + per_space.get(space, 0) + 1 for space in facilities
    }
    run_id, run_ts = writer.new_run_id(), datetime.now(UTC)

    def make_data(index: int, count: int, spaces: list[str]) -> dict[str, Any]:
        return {
            "action": "compute_shard",
            "runId": run_id,
            "runTimestamp": writer.now_iso(run_ts),
            "shard": index,
            "shardCount": count,
            "spaces": {space: core[space] for space in spaces if space in core},
            "extraSpaces": {space: facilities[space] for space in spaces},
            "emptyLinkViews": empty_views,
            "extraLinks": payload.get("extraLinks"),
            "config": payload.get("config"),
        }

    packings = {
        count: pack_spaces(weights, count, cfg["shardTargetEntities"])
        for count in range(min(cfg["maxShards"], len(weights)), 0, -1)
    }
    tasks = build_tasks(packings, function_external_id, make_data)
    writer.ingest(
        client,
        cfg,
        "relationshipContainer",
        run_id,
        run_ts,
        [("run:started", writer.run_row(run_id, run_ts, "started")[2])],
    )
    return {
        "action": "plan",
        "runId": run_id,
        "runTimestamp": writer.now_iso(run_ts),
        "shardCount": len(tasks),
        "facilities": len(facilities),
        "tasks": tasks,
    }


def compute_shard(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step two, once per shard and in parallel: counts the shard's facility spaces and appends their rows to the run.

    Writes the rows first and a `runPart` record last, which is how `finalize` knows the shard finished.
    """
    require_resources(cfg, writes=True)
    run_id, run_ts = payload["runId"], _parse_iso(payload["runTimestamp"])
    shard = int(payload["shard"])
    spaces: dict[str, str] = payload.get("spaces") or {}
    extra_spaces: dict[str, str] = payload.get("extraSpaces") or {}
    main_rows: list[Row] = []
    anno_rows: list[Row] = []
    warnings: list[dict[str, Any]] = []
    if spaces:
        data = load_shard(client, spaces)
        result = compute(data, cfg, latest_datapoints(client, data))
        main_rows, anno_rows = build_rows(result, cfg)
        warnings = result["warnings"]
    links = parse_extra_links(payload.get("extraLinks"))
    if links and extra_spaces:
        nodes = load_extra_links(client, links, list(extra_spaces))
        empty_views = {(model, view) for model, view in payload.get("emptyLinkViews") or []}
        gaps = find_gaps(links, nodes, extra_spaces)
        main_rows += writer.extra_link_rows(count_links(links, nodes, extra_spaces, empty_views), extra_spaces)
        main_rows += writer.extra_link_gap_rows(gaps, extra_spaces, cfg["drilldownCap"])
        warnings = [*warnings, *extra_link_truncation_warnings(gaps, cfg["drilldownCap"])]
    written = writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, main_rows)
    written += writer.ingest(client, cfg, "fileAnnotationContainer", run_id, run_ts, anno_rows)
    part = writer.run_part_row(shard, bool(spaces), encode_warnings(warnings), written)
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, [part])
    return {"action": "compute_shard", "shard": shard, "rowsWritten": written, "spaces": sorted(extra_spaces)}


PART_WAIT_SECONDS = 120
PART_POLL_SECONDS = 5


def read_run_parts(
    client: CogniteClient, cfg: RunConfig, run_id: str, run_ts: datetime, expected: int
) -> list[RowProps]:
    """The `runPart` records of a run. Waits briefly for them, since a record is searchable a moment after it is written.

    Raises when a shard has not reported by then: the run is then not completed, so it is never shown.
    """
    container, schema = cfg["relationshipContainer"], cfg["schemaSpace"]
    url = f"/api/v1/projects/{client.config.project}/streams/{cfg['stream']}/records/filter"
    body = {
        "lastUpdatedTime": {
            "gt": writer.now_iso(run_ts - timedelta(minutes=5)),
            "lt": writer.now_iso(datetime.now(UTC) + timedelta(minutes=5)),
        },
        "sources": [{"source": {"type": "container", "space": schema, "externalId": container}}],
        "filter": {
            "and": [
                {"equals": {"property": [schema, container, "rowKind"], "value": "runPart"}},
                {"equals": {"property": [schema, container, "runId"], "value": run_id}},
            ]
        },
        "limit": 1000,
    }
    deadline = time.monotonic() + PART_WAIT_SECONDS
    while True:
        items = client.post(url, json=body).json().get("items", [])
        parts = {
            props["entityId"]: props
            for item in items
            if (props := item.get("properties", {}).get(schema, {}).get(container)) is not None
        }
        if len(parts) >= expected:
            return [parts[f"shard-{index}"] for index in range(expected)]
        if time.monotonic() >= deadline:
            missing = [index for index in range(expected) if f"shard-{index}" not in parts]
            raise MissingInputError(
                f"{len(missing)} of {expected} shards have not reported (shards {missing}). "
                "The run is not completed, so it is never shown."
            )
        time.sleep(PART_POLL_SECONDS)


def finalize(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step three: when every shard has reported, writes the complete marker with the run's warnings and row count."""
    require_resources(cfg, writes=True)
    run_id, run_ts = payload["runId"], _parse_iso(payload["runTimestamp"])
    expected = int(payload["shardCount"])
    parts = read_run_parts(client, cfg, run_id, run_ts, expected)
    warnings = merge_warnings(
        [
            (part.get("detail") == "metrics", json.loads(part["warnings"]) if part.get("warnings") else [])
            for part in parts
        ]
    )
    rows_written = sum(int(part.get("rowsWritten") or 0) for part in parts)
    marker = writer.run_row(run_id, run_ts, "complete", warnings=encode_warnings(warnings), rows_written=rows_written)
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, [("run:complete", marker[2])])
    return {
        "action": "finalize",
        "runId": run_id,
        "shards": expected,
        "rowsWritten": rows_written,
        "warnings": warnings,
    }


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
