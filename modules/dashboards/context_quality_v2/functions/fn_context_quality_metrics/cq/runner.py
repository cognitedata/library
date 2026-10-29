"""compute_metrics and validate actions."""

import json
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.exceptions import CogniteAPIError

from cq import export, locations, writer
from cq.availability import encode_warnings, extra_link_truncation_warnings, no_source_warnings
from cq.batching import Leaf, batch_filter, split_leaves
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
from cq.fanout import finish_tasks, merge_warnings, tasks_fit
from cq.lookups import fetch_cross
from cq.metrics import compute
from cq.planning import plan_work
from cq.reader import (
    KIND_VIEWS,
    count_extra_link_instances,
    count_kinds_by_space_remote,
    leaf_tools,
    load_all,
    load_batch,
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
    found, location_warnings = locations.read(client, data["facility_spaces"])
    main_rows += writer.location_rows(found)
    warnings = [*result["warnings"], *link_warnings, *location_warnings]
    summary = {
        "action": "compute_metrics",
        "facilities": data["facility_spaces"],
        "aggregateRows": sum(1 for k, _ in main_rows if k.startswith("agg")),
        "exclusionRows": sum(1 for k, _ in main_rows if k.startswith("excl")),
        "drilldownRows": sum(1 for k, _ in main_rows if k.startswith("drill")),
        "classificationRows": sum(1 for k, _ in main_rows if k.startswith("cls")),
        "extraLinkRows": sum(1 for k, _ in main_rows if k.startswith("xlink")),
        "extraLinkGapRows": sum(1 for k, _ in main_rows if k.startswith("xgap")),
        "locationRows": len(found),
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


def _rekey(rows: list[Row], piece: int) -> list[Row]:
    """Row keys that no other piece of the run can also produce: pieces count slices of the same space, and rows of the
    same grain would otherwise get the same key. A consumer sums rows, so two rows of one grain are fine."""
    return [(f"{key}|p{piece}", props) for key, props in rows]


def _facility_totals(by_kind: dict[str, dict[str, int]], spaces: dict[str, str]) -> dict[str, int]:
    return {kind: sum(counts.get(space, 0) for space in spaces) for kind, counts in by_kind.items()}


def plan(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step one of a run in parallel pieces: finds the facilities without loading anything, starts the run, and returns
    the tasks that count it.

    The output is read by the workflow's dynamic task, which runs the returned tasks, and by `finalize`. Facilities are
    found with aggregate counts per space, the same rule `load_all` applies to what it reads. A space with no more than
    `shardTargetEntities` entities is counted whole (`compute_shard`; small spaces share a call). A bigger space is cut
    by kind and, for a kind that is still too big, by the start of the entities' ids (`compute_batch`, cq.planning).
    With `maxConcurrentShards`, only that many calls run at once.

    Payload: `functionExternalId` (the function the tasks call), and the same `config` and `extraLinks` as
    `compute_metrics`, which every task receives.
    """
    require_resources(cfg, writes=True)
    function_external_id = payload.get("functionExternalId")
    if not isinstance(function_external_id, str) or not function_external_id:
        raise ValueError("plan needs functionExternalId: the external id of this function, which runs the tasks")
    links = parse_extra_links(payload.get("extraLinks"))
    by_kind, unavailable = count_kinds_by_space_remote(client)
    totals: dict[str, int] = defaultdict(int)
    for counts in by_kind.values():
        for space, count in counts.items():
            totals[space] += count
    core = discover_facility_spaces(dict(totals), cfg)
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
    link_counts = {key: {s: n for s, n in by_space.items() if s in facilities} for key, by_space in link_counts.items()}
    run_id, run_ts = writer.new_run_id(), datetime.now(UTC)
    common = {
        "runId": run_id,
        "runTimestamp": writer.now_iso(run_ts),
        "extraLinks": payload.get("extraLinks"),
        "config": payload.get("config"),
    }

    def leaves_of(space: str, key: str) -> list[tuple[Leaf, int]]:
        """The slices of a big kind, or of a configured view, of one space."""
        if "|" in key:
            model, view = key.split("|", 1)
            view_id, edge, total = view_ids[(model, view)], False, link_counts[(model, view)][space]
        else:
            space_, xid, edge = KIND_VIEWS[key]
            view_id, total = ViewId(space_, xid, "v1"), by_kind[key][space]
        count_leaves, sample_id = leaf_tools(client, view_id, space, edge=edge)
        return split_leaves(total, cfg["shardTargetEntities"], count_leaves, sample_id)

    def tasks_for(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return finish_tasks(items, function_external_id, common, cfg["maxConcurrentShards"])

    items = plan_work(
        core=core,
        facilities=facilities,
        kind_counts=by_kind,
        link_counts=link_counts,
        target=cfg["shardTargetEntities"],
        max_tasks=cfg["maxShards"],
        leaves_of=leaves_of,
        fits=lambda candidate: tasks_fit(tasks_for(candidate)),
    )
    tasks = tasks_for(items)
    # a configured view that holds nothing in any facility still shows up, as not applicable, in every facility
    empty = {key for key, by_space in link_counts.items() if not any(by_space.values())}
    nothing = writer.extra_link_rows(count_links(links, {}, facilities, empty_views=empty) if empty else {}, facilities)
    found, location_warnings = locations.read(client, facilities)
    nothing += writer.location_rows(found)
    warnings = [*no_source_warnings(_facility_totals(by_kind, core), unavailable), *location_warnings]
    start = [("run:started", writer.run_row(run_id, run_ts, "started")[2])]
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, start)
    written = writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, nothing)
    part = writer.run_part_row(None, "plan", encode_warnings(warnings), written)
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, [part])
    return {
        "action": "plan",
        "runId": run_id,
        "runTimestamp": common["runTimestamp"],
        "shardCount": len(tasks),
        "facilities": len(facilities),
        "locations": len(found),
        "batches": sum(1 for task in tasks if task["parameters"]["function"]["data"]["action"] == "compute_batch"),
        "tasks": tasks,
    }


def _piece(payload: dict[str, Any]) -> tuple[str, datetime, int]:
    return payload["runId"], _parse_iso(payload["runTimestamp"]), int(payload["shard"])


def _export_part(
    client: CogniteClient, cfg: RunConfig, payload: dict[str, Any], rows: list[tuple[str, ...]]
) -> list[dict[str, Any]]:
    """Uploads the piece's items for the workbook (cq.export). -> a warning when that failed: it never fails the run."""
    try:
        dataset = export.data_set_id(client, cfg)
        if dataset is not None:
            export.write_part(client, dataset, payload["runId"], int(payload["shard"]), rows)
    except CogniteAPIError as error:
        message = f"The unlinked items of one part of the run could not be exported: {error.message}"
        return [{"code": "export-failed", "message": message, "subject": "unlinked-items"}]
    return []


def _finish_piece(
    client: CogniteClient,
    cfg: RunConfig,
    payload: dict[str, Any],
    detail: str,
    rows: list[Row],
    annotation_rows: list[Row],
    warnings: list[dict[str, Any]],
    export_rows: list[tuple[str, ...]] | None = None,
) -> int:
    """Writes the rows of a piece, then its `runPart` record last: the record is how `finalize` knows it finished."""
    run_id, run_ts, shard = _piece(payload)
    if export_rows is not None:
        warnings = [*warnings, *_export_part(client, cfg, payload, export_rows)]
    written = writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, rows)
    written += writer.ingest(client, cfg, "fileAnnotationContainer", run_id, run_ts, annotation_rows)
    part = writer.run_part_row(shard, detail, encode_warnings(warnings), written)
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, [part])
    return written


def compute_shard(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step two, once per shard and in parallel: counts the shard's whole facility spaces and appends their rows."""
    require_resources(cfg, writes=True)
    spaces: dict[str, str] = payload.get("spaces") or {}
    extra_spaces: dict[str, str] = payload.get("extraSpaces") or {}
    main_rows: list[Row] = []
    anno_rows: list[Row] = []
    warnings: list[dict[str, Any]] = []
    listed: list[tuple[str, ...]] = []
    if spaces:
        data = load_shard(client, spaces)
        result = compute(data, cfg, latest_datapoints(client, data))
        main_rows, anno_rows = build_rows(result, cfg)
        listed = [*export.item_rows(result["drilldown"]), *export.summary_rows(result["agg"])]
        warnings = [
            w for w in result["warnings"] if w["code"] != "no-source-entities"
        ]  # the plan says that, for the run
    links = parse_extra_links(payload.get("extraLinks"))
    if links and extra_spaces:
        nodes = load_extra_links(client, links, list(extra_spaces))
        gaps = find_gaps(links, nodes, extra_spaces)
        # the plan writes the rows of a view that is empty everywhere, so nothing is filled in here
        main_rows += writer.extra_link_rows(count_links(links, nodes, extra_spaces, set()), extra_spaces)
        main_rows += writer.extra_link_gap_rows(gaps, extra_spaces, cfg["drilldownCap"])
        warnings = [*warnings, *extra_link_truncation_warnings(gaps, cfg["drilldownCap"])]
        listed += export.item_rows([], gaps)
    written = _finish_piece(client, cfg, payload, "shard", main_rows, anno_rows, warnings, listed)
    return {
        "action": "compute_shard",
        "shard": int(payload["shard"]),
        "rowsWritten": written,
        "spaces": sorted(extra_spaces),
    }


# What a batch leaves to the plan or cannot say: whether a kind exists anywhere, and why a metric has nothing to measure.
_NOT_FROM_A_BATCH = ("no-source-entities", "metric-not-applicable")


def compute_batch(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step two for a big space, once per batch and in parallel: counts some kinds, or one slice of one kind, of a space.

    What the batch's entities link to outside it is looked up (cq.lookups), so the numbers add up to those of counting
    the whole space. The rows of a kind are written by the batches of that kind only; their keys carry the batch number.
    """
    require_resources(cfg, writes=True)
    space, facility = payload["space"], payload["facility"]
    kinds = frozenset(payload.get("kinds") or [])
    views = payload.get("views") or []
    leaves: list[Leaf] = [(kind, text) for kind, text in payload.get("leaves") or []]
    flt = batch_filter(leaves, edge=kinds == frozenset({"annotations"})) if leaves else None
    # the cap on a drill-down list is per space; the batches of one kind share it
    cfg = {**cfg, "drilldownCap": max(1, cfg["drilldownCap"] // max(1, int(payload.get("groupBatches") or 1)))}  # type: ignore[assignment]
    shard = int(payload["shard"])
    main_rows: list[Row] = []
    anno_rows: list[Row] = []
    warnings: list[dict[str, Any]] = []
    listed: list[tuple[str, ...]] = []
    if kinds:
        data = load_batch(client, space, facility, kinds, flt)
        cross = fetch_cross(client, data, cfg, kinds, space)
        result = compute(data, cfg, latest_datapoints(client, data), kinds=kinds, cross=cross)
        main_rows, anno_rows = build_rows(result, cfg)
        listed = [*export.item_rows(result["drilldown"]), *export.summary_rows(result["agg"])]
        warnings = [w for w in result["warnings"] if w["code"] not in _NOT_FROM_A_BATCH]
    links = parse_extra_links(payload.get("extraLinks"))
    for model, view in views:
        of_view = [link for link in links if link["model"] == model and link["view"] == view]
        nodes = load_extra_links(client, of_view, [space], flt)
        gaps = find_gaps(of_view, nodes, {space: facility})
        main_rows += writer.extra_link_rows(count_links(of_view, nodes, {space: facility}, set()), {space: facility})
        main_rows += writer.extra_link_gap_rows(gaps, {space: facility}, cfg["drilldownCap"])
        warnings = [*warnings, *extra_link_truncation_warnings(gaps, cfg["drilldownCap"])]
        listed += export.item_rows([], gaps)
    written = _finish_piece(
        client, cfg, payload, "batch", _rekey(main_rows, shard), _rekey(anno_rows, shard), warnings, listed
    )
    return {"action": "compute_batch", "shard": shard, "rowsWritten": written, "space": space}


PART_WAIT_SECONDS = 120
PART_POLL_SECONDS = 5


def read_run_parts(
    client: CogniteClient, cfg: RunConfig, run_id: str, run_ts: datetime, expected: int
) -> list[RowProps]:
    """The `runPart` records of a run: the plan's, if there is one, and one for each of the `expected` pieces.

    Waits briefly for them, since a record is searchable a moment after it is written. Raises when a piece has not
    reported by then: the run is then not completed, so it is never shown.
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
        if all(f"shard-{index}" in parts for index in range(expected)):
            return [
                *([parts["plan"]] if "plan" in parts else []),
                *(parts[f"shard-{index}"] for index in range(expected)),
            ]
        if time.monotonic() >= deadline:
            missing = [index for index in range(expected) if f"shard-{index}" not in parts]
            raise MissingInputError(
                f"{len(missing)} of {expected} calls have not reported (numbers {missing}). "
                "The run is not completed, so it is never shown."
            )
        time.sleep(PART_POLL_SECONDS)


def finalize(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step three: when every piece has reported, writes the complete marker with the run's warnings and row count."""
    require_resources(cfg, writes=True)
    run_id, run_ts = payload["runId"], _parse_iso(payload["runTimestamp"])
    expected = int(payload["shardCount"])
    parts = read_run_parts(client, cfg, run_id, run_ts, expected)
    warnings = merge_warnings([json.loads(part["warnings"]) if part.get("warnings") else [] for part in parts])
    rows_written = sum(int(part.get("rowsWritten") or 0) for part in parts)
    marker = writer.run_row(run_id, run_ts, "complete", warnings=encode_warnings(warnings), rows_written=rows_written)
    writer.ingest(client, cfg, "relationshipContainer", run_id, run_ts, [("run:complete", marker[2])])
    return {
        "action": "finalize",
        "runId": run_id,
        "pieces": expected,
        "rowsWritten": rows_written,
        "warnings": warnings,
    }


def export_workbook(client: CogniteClient, cfg: RunConfig, payload: dict[str, Any]) -> dict[str, Any]:
    """Step four, after the run is complete: the workbook of every unlinked item of the run (cq.export).

    It is separate from `finalize` so that a slow or failed export never keeps a run from completing.
    """
    return {"action": "export", **export.build_workbook(client, cfg, payload["runId"], int(payload["shardCount"]))}


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
