"""Builds record rows and appends them to the immutable stream (Records API)."""

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from cognite.client import CogniteClient

from cq.extra_links import LinkGap
from cq.types import DrillItem, Row, RowProps, RunConfig

NO_DIM = "unknown"


def now_iso(ts: datetime | None = None) -> str:
    return (ts or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def new_run_id(ts: datetime | None = None) -> str:
    return (ts or datetime.now(UTC)).strftime("run-%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:6]


def _digest(*logical_key: Any) -> str:
    """A stable 128-bit identifier of a complete logical key.

    The key is encoded as JSON, so no value can be mistaken for a separator, and hashed so that the record's external
    id stays short however long the entity id is. The same logical key always gives the same digest.
    """
    encoded = json.dumps(logical_key, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(encoded.encode("ascii")).hexdigest()[:32]


def _clean(props: RowProps) -> RowProps:
    return {k: v for k, v in props.items() if v is not None}


def run_row(
    run_id: str,
    run_ts: datetime,
    status: str,
    scope: str = "all",
    warnings: str | None = None,
    rows_written: int | None = None,
) -> tuple[str, str, RowProps]:
    return (
        "run",
        status,
        _clean({"rowKind": "run", "status": status, "scope": scope, "warnings": warnings, "rowsWritten": rows_written}),
    )


def run_part_row(shard: int | None, detail: str, warnings: str | None, rows_written: int) -> tuple[str, RowProps]:
    """What one piece of a run reports to `finalize`: `shard` is its number, or None for the plan's own record.

    `detail` is `shard` for whole spaces, `batch` for a slice of one space and `plan` for the plan; `rowsWritten`
    counts the piece's rows, not this record."""
    return (
        "part:plan" if shard is None else f"part:{shard:04d}",
        _clean(
            {
                "rowKind": "runPart",
                "entityId": "plan" if shard is None else f"shard-{shard}",
                "detail": detail,
                "warnings": warnings,
                "rowsWritten": rows_written,
            }
        ),
    )


def location_rows(locations: list[dict[str, Any]]) -> list[Row]:
    """One row per location that can be used as a scope: `entityId` is its external id, `entityLabel` its name, `detail`
    the external id of its parent, and `cellsJson` the instance spaces and facility ids it covers."""
    return [
        (
            f"loc:{_digest('location', location['id'])}",
            _clean(
                {
                    "rowKind": "location",
                    "entityId": location["id"],
                    "entityLabel": location["name"],
                    "detail": location["parent"],
                    "cellsJson": json.dumps(
                        {"spaces": location["spaces"], "facilityIds": location["facilityIds"]}, sort_keys=True
                    ),
                }
            ),
        )
        for location in locations
    ]


def aggregate_rows(agg: dict[tuple, list[Any]]) -> list[Row]:
    out: list[Row] = []
    for (metric, dim, unit, facility, div, disc, owner, tier, sub), (elig, passing, issue, vsum) in agg.items():
        n = "|".join(str(x) for x in (metric, unit, div, disc, owner, tier, sub))
        out.append(
            (
                f"agg:{n}",
                _clean(
                    {
                        "rowKind": "aggregate",
                        "metricId": metric,
                        "dimensionId": dim,
                        "reportingUnitId": unit,
                        "facilityId": facility,
                        "divisionId": div,
                        "disciplineId": disc,
                        "ownerId": owner,
                        "tier": tier,
                        "subKey": sub,
                        "eligibleCount": elig,
                        "passingCount": passing,
                        "issueCount": issue,
                        "valueSum": round(vsum, 4) if vsum else None,
                        "notApplicable": True if elig == 0 else None,
                    }
                ),
            )
        )
    return out


def exclusion_rows(excl: dict[tuple, int]) -> list[Row]:
    out: list[Row] = []
    for (dim, cat, unit, facility, div, disc), count in excl.items():
        n = "|".join(str(x) for x in (dim, cat, unit, div, disc))
        out.append(
            (
                f"excl:{n}",
                _clean(
                    {
                        "rowKind": "exclusion",
                        "dimensionId": dim,
                        "exclusionCategory": cat,
                        "reportingUnitId": unit,
                        "facilityId": facility,
                        "divisionId": div,
                        "disciplineId": disc,
                        "excludedCount": count,
                    }
                ),
            )
        )
    return out


def classification_rows(classifications: dict[tuple, list[int]]) -> list[Row]:
    out: list[Row] = []
    for (kind, cid, name, dim, unit, facility, div, disc, owner, tier), (elig, passing) in classifications.items():
        logical_key = ("classification", kind, cid, name, dim, unit, facility, div, disc, owner, tier)
        out.append(
            (
                f"cls:{kind}:{_digest(*logical_key)}",
                _clean(
                    {
                        "rowKind": "classification",
                        "classificationKind": kind,
                        "classificationId": cid,
                        "classificationName": name,
                        "dimensionId": dim,
                        "reportingUnitId": unit,
                        "facilityId": facility,
                        "divisionId": div,
                        "disciplineId": disc,
                        "ownerId": owner,
                        "tier": tier,
                        "eligibleCount": elig,
                        "passingCount": passing,
                        "issueCount": elig - passing,
                    }
                ),
            )
        )
    return out


def drilldown_rows(drill: list[DrillItem], cap: int) -> list[Row]:
    out: list[Row] = []
    per: dict[tuple, int] = {}
    for d in drill:
        g = d["g"]
        k = (d["metric"], g["unit"])
        per[k] = per.get(k, 0) + 1
        if per[k] > cap:
            continue
        out.append(
            (
                f"drill:{d['metric']}:{_digest('drilldown', d['metric'], d['dim'], g['unit'], d['id'])}",
                _clean(
                    {
                        "rowKind": "drilldown",
                        "metricId": d["metric"],
                        "dimensionId": d["dim"],
                        "reportingUnitId": g["unit"],
                        "facilityId": g["facility"],
                        "divisionId": g["div"],
                        "disciplineId": g["disc"],
                        "ownerId": d["owner"],
                        "tier": g["tier"],
                        "entityId": d["id"],
                        "entityLabel": d["label"],
                        "detail": d["detail"],
                        "matched": False,
                        "cellsJson": json.dumps(d["classes"], sort_keys=True) if d.get("classes") else None,
                    }
                ),
            )
        )
    return out


def extra_link_rows(counts: dict[tuple[str, str, str, str], list[int]], facility_spaces: dict[str, str]) -> list[Row]:
    """One row per (model, view, property, facility). dimensionId names the model and view, metricId the property."""
    out: list[Row] = []
    for (model, view, prop, space), (elig, passing, issue) in counts.items():
        out.append(
            (
                f"xlink:{_digest('extraLink', model, view, prop, space)}",
                _clean(
                    {
                        "rowKind": "extraLink",
                        "metricId": prop,
                        "dimensionId": f"{model}/{view}",
                        "reportingUnitId": space,
                        "facilityId": facility_spaces.get(space),
                        "eligibleCount": elig,
                        "passingCount": passing,
                        "issueCount": issue,
                        "notApplicable": True if elig == 0 else None,
                    }
                ),
            )
        )
    return out


def extra_link_gap_rows(gaps: list[LinkGap], facility_spaces: dict[str, str], cap: int) -> list[Row]:
    """One row per instance whose link is not set, cut at `cap` per (model, view, property, facility)."""
    out: list[Row] = []
    per: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for gap in gaps:
        link = (gap["model"], gap["view"], gap["property"], gap["space"])
        per[link] += 1
        if per[link] > cap:
            continue
        out.append(
            (
                f"xgap:{_digest('extraLinkGap', *link, gap['external_id'])}",
                _clean(
                    {
                        "rowKind": "extraLinkGap",
                        "metricId": gap["property"],
                        "dimensionId": f"{gap['model']}/{gap['view']}",
                        "reportingUnitId": gap["space"],
                        "facilityId": facility_spaces.get(gap["space"]),
                        "entityId": gap["external_id"],
                        "entityLabel": gap["label"],
                        "detail": "link not set",
                    }
                ),
            )
        )
    return out


def annotation_bucket_rows(buckets: dict[tuple, list[Any]]) -> list[Row]:
    out: list[Row] = []
    for (unit, facility, div, tier, status, bucket), (count, csum) in buckets.items():
        n = "|".join(str(x) for x in (unit, div, tier, status, bucket))
        out.append(
            (
                f"anno:{n}",
                _clean(
                    {
                        "rowKind": "annotationBucket",
                        "reportingUnitId": unit,
                        "facilityId": facility,
                        "divisionId": div,
                        "tier": tier,
                        "annotationType": "asset-tag",
                        "status": status,
                        "confidenceBucket": bucket,
                        "count": count,
                        "confidenceSum": round(csum, 4),
                    }
                ),
            )
        )
    return out


def ingest(
    client: CogniteClient, cfg: RunConfig, container_key: str, run_id: str, run_ts: datetime, rows: list[Row]
) -> int:
    """rows: [(rowKey, props)]. Adds runId/runTimestamp, writes in batches. Returns rows written."""
    project = client.config.project
    url = f"/api/v1/projects/{project}/streams/{cfg['stream']}/records"
    container = cfg[container_key]
    ts = now_iso(run_ts)
    written = 0
    for i in range(0, len(rows), cfg["batchSize"]):
        items = []
        for key, props in rows[i : i + cfg["batchSize"]]:
            p = dict(props, runId=run_id, runTimestamp=ts)
            items.append(
                {
                    "space": cfg["instanceSpace"],
                    "externalId": f"{run_id}:{key}",
                    "sources": [
                        {
                            "source": {"type": "container", "space": cfg["schemaSpace"], "externalId": container},
                            "properties": p,
                        }
                    ],
                }
            )
        client.post(url, json={"items": items})
        written += len(items)
    return written
