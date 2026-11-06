"""One Excel workbook of every unlinked (or incomplete) item of a run, a sheet for each list.

The drill-down rows in the stream are capped, so the dashboard can only show a preview. The workbook holds the full
lists:

1. Each piece of a run (a shard or a batch, cq.runner) writes its items as a small CSV part to Cognite Files while it
   runs. The part names the sheets its rows belong to, and how many.
2. After the run is complete, the `export` step reads the parts one at a time, writes the rows into the sheets of one
   workbook in streaming mode, uploads it as `cq-unlinked-{runId}.xlsx`, removes the parts, and removes the workbooks of
   runs older than `exportRetentionDays`.
3. A run can be for some dimensions only (cq.scope). Its workbook still holds every dimension: the rows of the
   dimensions it measured come from its own parts, and the others from the *dimension files* of earlier runs. Each
   export keeps what it measured as one `cq-unlinked-dim-{dimension}-{runId}.csv` per dimension (an empty one for a
   dimension with nothing to list, so that it replaces the older rows) and removes the dimension files it replaces.

The workbook is a side product: a failure here never fails the run, and the run's numbers do not depend on it.
"""

import csv
import io
import json
import os
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import xlsxwriter
from cognite.client import CogniteClient

from cq.scope import CONFIGURED
from cq.types import DrillItem, RunConfig

WORKBOOK_PREFIX = "cq-unlinked-"
PART_PREFIX = "cq-unlinked-part-"
DIMENSION_PREFIX = "cq-unlinked-dim-"
# An Excel sheet holds at most this many rows, the header included.
MAX_SHEET_ROWS = 1_048_576
ITEM, SUMMARY = "item", "summary"

COLUMNS = ("Name", "External ID", "Space", "Facility", "Details", "Owner", "Tier", "Classification")
# a part is a CSV of (kind, sheet, then these): the same columns for an item and for a summary line
PART_HEADER = ("kind", "sheet", "a", "b", "c", "d", "e", "f", "g", "h")

# The lists, in the order of the dimensions: drill-down metric -> (sheet title, what a row is). Titles are at most 31
# characters, the limit of an Excel sheet name.
SHEETS: dict[str, tuple[str, str]] = {
    "orphansByTier": ("Orphan assets", "asset with no parent and no children"),
    "depthOutliers": ("Assets too deep", "asset deeper than the depth limit"),
    "typeCoverageGaps": ("Assets without type", "asset with no asset type"),
    "activeChildrenUnderRetiredParents": ("Active under retired", "active asset under a retired parent"),
    "unmatchedWithCandidates": ("Equipment without asset", "equipment not linked to an asset"),
    "equipmentMissingSerial": ("Equipment no serial number", "equipment without a serial number"),
    "equipmentMissingManufacturer": ("Equipment no manufacturer", "equipment without a manufacturer"),
    "orphansByPrefix": ("Time series without asset", "time series not linked to an asset"),
    "staleTimeSeries": ("Stale time series", "time series with no recent datapoint"),
    "tier1CoverageGaps": ("Assets without time series", "asset no time series is linked to"),
    "unlinkedMaintenance": ("Maintenance without asset", "work order or notification without an asset"),
    "incompleteWorkOrders": ("Work orders not completed", "work order that is not completed"),
    "woWithoutNotification": ("Work orders no notification", "work order no notification is linked to"),
    "notificationsWithoutWorkOrder": ("Notifications no work order", "notification without a work order"),
    "unlinkedByCategory": ("Files without asset", "file not linked to an asset"),
    "filesMissingCategory": ("Files no category", "file without a category"),
    "filesMissingDescription": ("Files no description", "file without a description"),
    "filesMissingSourceId": ("Files no source ID", "file without a source ID"),
    "unlinked3dObjects": ("3D objects without asset", "3D object no asset is linked to"),
    "bboxGaps": ("3D no bounding box", "3D object without a full bounding box"),
    "unlinkedTier1": ("Assets without 3D object", "asset not linked to a 3D object"),
    "lowConfidenceQueue": ("Low confidence annotations", "annotation below the confidence threshold"),
    "pendingReview": ("Annotations pending review", "annotation still waiting for review"),
    "unapprovedAnnotations": ("Unapproved annotations", "annotation that is not approved"),
}

# the numbers of the metric cards, for the summary sheet: (dimension, metric, field) -> label
METRIC_LABELS: dict[str, str] = {
    "hierarchyCompletionRate": "Assets with a parent",
    "orphanRate": "Assets that are not orphans",
    "associationRate": "Equipment linked to an asset",
    "serialMfrCompleteness": "Equipment completeness",
    "tsToAssetRate": "Time series linked to an asset",
    "freshness": "Time series with recent data",
    "monitoringCoverage": "Assets with a time series",
    "woToAssetRate": "Work orders linked to an asset",
    "woToNotificationRate": "Work orders with a notification",
    "woCompletionRate": "Work orders completed",
    "notificationToAssetRate": "Notifications linked to an asset",
    "notificationToWoRate": "Notifications with a work order",
    "fileToAssetRate": "Files linked to an asset",
    "metadataCompleteness": "File metadata completeness",
    "threeDToAssetRate": "3D objects linked to an asset",
    "bboxCompleteness": "3D objects with a bounding box",
    "asset3dCoverage": "Assets with a 3D object",
    "annotationCount": "Annotations",
    "confidenceAverage": "Annotations with a confidence",
    "approvalMix": "Annotations by status",
    "lowConfidenceQueue": "Annotations not low confidence",
    "pendingReview": "Annotations reviewed",
}
DIMENSION_ORDER = (
    "asset-hierarchy",
    "equipment-asset",
    "time-series",
    "maintenance",
    "files",
    "model-3d",
    "file-annotation",
)
DIMENSION_NAMES = {
    "asset-hierarchy": "Asset hierarchy",
    "equipment-asset": "Equipment-Asset",
    "time-series": "Time series",
    "maintenance": "Maintenance workflow",
    "files": "Files",
    "model-3d": "3D model",
    "file-annotation": "File annotation",
}
_BAD_SHEET_CHARS = str.maketrans(dict.fromkeys("[]:*?/\\", "_"))

# The dimension each list belongs to (the sheets of configured links belong to CONFIGURED).
_LIST_DIMENSION = {
    "orphansByTier": "asset-hierarchy",
    "depthOutliers": "asset-hierarchy",
    "typeCoverageGaps": "asset-hierarchy",
    "activeChildrenUnderRetiredParents": "asset-hierarchy",
    "unmatchedWithCandidates": "equipment-asset",
    "equipmentMissingSerial": "equipment-asset",
    "equipmentMissingManufacturer": "equipment-asset",
    "orphansByPrefix": "time-series",
    "staleTimeSeries": "time-series",
    "tier1CoverageGaps": "time-series",
    "unlinkedMaintenance": "maintenance",
    "incompleteWorkOrders": "maintenance",
    "woWithoutNotification": "maintenance",
    "notificationsWithoutWorkOrder": "maintenance",
    "unlinkedByCategory": "files",
    "filesMissingCategory": "files",
    "filesMissingDescription": "files",
    "filesMissingSourceId": "files",
    "unlinked3dObjects": "model-3d",
    "bboxGaps": "model-3d",
    "unlinkedTier1": "model-3d",
    "lowConfidenceQueue": "file-annotation",
    "pendingReview": "file-annotation",
    "unapprovedAnnotations": "file-annotation",
}
SHEET_DIMENSION = {SHEETS[metric][0]: dimension for metric, dimension in _LIST_DIMENSION.items()}


def row_dimension(row: Iterable[str]) -> str:
    """The dimension a row of a part belongs to: a summary line names it, a list item has it by its sheet."""
    kind, second, *_ = row
    return second if kind == SUMMARY else SHEET_DIMENSION.get(second, CONFIGURED)


def part_external_id(run_id: str, piece: int) -> str:
    return f"{PART_PREFIX}{run_id}-{piece:04d}.csv"


def workbook_external_id(run_id: str) -> str:
    return f"{WORKBOOK_PREFIX}{run_id}.xlsx"


def dimension_external_id(dimension: str, run_id: str) -> str:
    return f"{DIMENSION_PREFIX}{dimension}-{run_id}.csv"


def link_sheet(view: str, prop: str) -> str:
    """The sheet of a configured link: `View.property`, as a valid Excel sheet name."""
    return f"{view}.{prop}".translate(_BAD_SHEET_CHARS)[:31]


def _tier(value: Any) -> str:
    return "" if value is None else str(value)


def _classification(classes: dict[str, str] | None) -> str:
    return "; ".join(f"{kind}: {label}" for kind, label in sorted((classes or {}).items()))


def item_rows(drill: Iterable[DrillItem], links: Iterable[dict[str, Any]] = ()) -> list[tuple[str, ...]]:
    """The part's rows for the drill-down items and the configured-link gaps: (kind, sheet, the eight columns)."""
    rows: list[tuple[str, ...]] = []
    for item in drill:
        sheet = SHEETS.get(item["metric"])
        if sheet is None:
            continue
        g = item["g"]
        rows.append(
            (
                ITEM,
                sheet[0],
                item["label"] or "",
                item["id"],
                g["unit"],
                g["facility"] or "",
                item["detail"],
                item["owner"] or "",
                _tier(g["tier"]),
                _classification(item.get("classes")),
            )
        )
    for gap in links:
        rows.append(
            (
                ITEM,
                link_sheet(gap["view"], gap["property"]),
                gap["label"],
                gap["external_id"],
                gap["space"],
                "",
                "link not set",
                "",
                "",
                "",
            )
        )
    return rows


def summary_rows(agg: dict[tuple, list[Any]]) -> list[tuple[str, ...]]:
    """One line per (dimension, metric, field): how many were eligible, passed and did not, summed over the piece."""
    totals: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    for key, values in agg.items():
        metric, dim, sub = key[0], key[1], key[8]
        entry = totals[(dim, metric, sub or "")]
        entry[0] += values[0]
        entry[1] += values[1]
        entry[2] += values[2]
    return [
        (SUMMARY, dim, metric, sub, str(e), str(p), str(i), "", "", "")
        for (dim, metric, sub), (e, p, i) in totals.items()
    ]


def _sheet_counts(rows: list[tuple[str, ...]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if row[0] == ITEM:
            counts[row[1]] += 1
    return dict(counts)


def write_part(client: CogniteClient, data_set_id: int, run_id: str, piece: int, rows: list[tuple[str, ...]]) -> int:
    """Uploads the rows of one piece of the run as a CSV. -> rows uploaded. The sheet counts go in the file's
    metadata."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(PART_HEADER)
    writer.writerows(rows)
    client.files.upload_bytes(
        content=buffer.getvalue().encode("utf-8"),
        name=part_external_id(run_id, piece),
        external_id=part_external_id(run_id, piece),
        mime_type="text/csv",
        data_set_id=data_set_id,
        overwrite=True,
        metadata={"runId": run_id, "piece": str(piece), "sheets": json.dumps(_sheet_counts(rows))},
    )
    return len(rows)


def data_set_id(client: CogniteClient, cfg: RunConfig) -> int | None:
    """The id of the data set the files go to, or None when exporting is switched off (no `exportDataSet`)."""
    if not cfg["exportDataSet"]:
        return None
    found = client.data_sets.retrieve(external_id=cfg["exportDataSet"])
    return None if found is None else found.id


def _sheet_order(counts: dict[str, int]) -> list[str]:
    known = [title for title, _ in SHEETS.values() if title in counts]
    return known + sorted(sheet for sheet in counts if sheet not in known)


def _read_part(client: CogniteClient, external_id: str) -> Iterator[list[str]]:
    """The rows of a part, one at a time, without the header."""
    content = client.files.download_bytes(external_id=external_id).decode("utf-8")
    reader = csv.reader(io.StringIO(content))
    next(reader, None)
    yield from reader


class _DimensionFile:
    """A dimension file of an earlier run: the lists and summary lines of one dimension, as that run measured them."""

    def __init__(self, file: Any) -> None:
        meta = file.metadata or {}
        self.external_id: str = file.external_id
        self.dimension: str = meta.get("dimension", "")
        self.run_id: str = meta.get("runId", "")
        self.sheets: dict[str, int] = json.loads(meta.get("sheets", "{}"))


def dimension_files(client: CogniteClient, dataset: int) -> list[_DimensionFile]:
    found = client.files.list(external_id_prefix=DIMENSION_PREFIX, data_set_ids=[dataset], limit=-1)
    return [_DimensionFile(file) for file in found if file.external_id and (file.metadata or {}).get("dimension")]


def newest_by_dimension(files: Iterable[_DimensionFile]) -> dict[str, _DimensionFile]:
    """The dimension file of the latest run (run ids sort by time) for each dimension."""
    newest: dict[str, _DimensionFile] = {}
    for file in files:
        if file.dimension not in newest or file.run_id > newest[file.dimension].run_id:
            newest[file.dimension] = file
    return newest


def _read_dimension_file(client: CogniteClient, external_id: str) -> Iterator[list[str]]:
    """The rows of a dimension file, from disk so that a big one is never held in memory."""
    folder = tempfile.mkdtemp()
    path = os.path.join(folder, "dimension.csv")
    client.files.download_to_path(path, external_id=external_id)
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            yield from reader
    finally:
        os.remove(path)
        os.rmdir(folder)


class _DimensionOut:
    """What a run measured of one dimension, written as it goes into the workbook: the dimension file it leaves."""

    def __init__(self, folder: str, dimension: str, run_id: str) -> None:
        self.dimension, self.run_id = dimension, run_id
        self.path = os.path.join(folder, dimension_external_id(dimension, run_id))
        self._handle = open(self.path, "w", newline="", encoding="utf-8")  # noqa: SIM115 - held open until close()
        self._writer = csv.writer(self._handle)
        self._writer.writerow(PART_HEADER)
        self.sheets: dict[str, int] = defaultdict(int)

    def add(self, row: list[str]) -> None:
        self._writer.writerow(row)
        if row[0] == ITEM:
            self.sheets[row[1]] += 1

    def close(self) -> None:
        self._handle.close()


def build_workbook(
    client: CogniteClient,
    cfg: RunConfig,
    run_id: str,
    pieces: int,
    now: datetime | None = None,
    measured: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Assembles the workbook of a run from its parts, uploads it, and tidies up. -> what was done.

    `measured` names the dimensions the run measured (CONFIGURED for the configured links); every dimension when None.
    The workbook holds all dimensions: those the run did not measure come from the dimension files of earlier runs.
    """
    dataset = data_set_id(client, cfg)
    if dataset is None:
        return {"exported": False, "reason": "exportDataSet is not set"}
    everything = (*DIMENSION_ORDER, CONFIGURED)
    this_run = frozenset(everything if measured is None else measured)
    wanted = [part_external_id(run_id, piece) for piece in range(pieces)]
    found = client.files.retrieve_multiple(external_ids=wanted, ignore_unknown_ids=True)
    present = {file.external_id: file for file in found}
    missing = [piece for piece, name in enumerate(wanted) if name not in present]
    every_dimension_file = dimension_files(client, dataset)
    earlier = {
        dimension: file
        for dimension, file in newest_by_dimension(every_dimension_file).items()
        if dimension not in this_run and file.run_id != run_id
    }
    counts: dict[str, int] = defaultdict(int)
    for file in present.values():
        for sheet, count in json.loads((file.metadata or {}).get("sheets", "{}")).items():
            if SHEET_DIMENSION.get(sheet, CONFIGURED) in this_run:  # a list of a dimension the run left out has no rows
                counts[sheet] += count
    for old in earlier.values():
        for sheet, count in old.sheets.items():
            counts[sheet] += count
    order = _sheet_order(counts)
    folder = tempfile.mkdtemp()
    path = os.path.join(folder, workbook_external_id(run_id))
    book = xlsxwriter.Workbook(path, {"constant_memory": True})
    header = book.add_format({"bold": True, "bg_color": "#E4E6E8", "bottom": 1})
    summary = book.add_worksheet("Summary")
    sheets = {title: book.add_worksheet(title) for title in order}
    next_row = dict.fromkeys(order, 1)
    for title, sheet in sheets.items():
        sheet.write_row(0, 0, COLUMNS, header)
        sheet.freeze_panes(1, 0)
        sheet.autofilter(0, 0, min(counts[title], MAX_SHEET_ROWS - 1), len(COLUMNS) - 1)
        sheet.set_column(0, 1, 34)
        sheet.set_column(2, 3, 26)
        sheet.set_column(4, 5, 36)
        sheet.set_column(6, 7, 16)
    totals: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    outs = {dimension: _DimensionOut(folder, dimension, run_id) for dimension in everything if dimension in this_run}

    def take(row: list[str], out: _DimensionOut | None) -> None:
        if row[0] == ITEM:
            title = row[1]
            if title in sheets and next_row[title] < MAX_SHEET_ROWS:
                sheets[title].write_row(next_row[title], 0, row[2:10])
                next_row[title] += 1
                if out is not None:
                    out.add(row)
        elif row[0] == SUMMARY:
            entry = totals[(row[1], row[2], row[3])]
            entry[0] += int(row[4])
            entry[1] += int(row[5])
            entry[2] += int(row[6])
            if out is not None:
                out.add(row)

    for name in wanted:
        if name not in present:
            continue
        for row in _read_part(client, name):
            if row[0] in (ITEM, SUMMARY) and row_dimension(row) in this_run:
                take(row, outs[row_dimension(row)])
    for old in earlier.values():
        for row in _read_dimension_file(client, old.external_id):
            take(row, None)
    sources = {dimension: run_id for dimension in everything if dimension in this_run}
    sources.update({dimension: old.run_id for dimension, old in earlier.items()})
    _write_summary(summary, header, totals, counts, order, missing, run_id, sources)
    book.close()
    for out in outs.values():
        out.close()
        name = dimension_external_id(out.dimension, run_id)
        client.files.upload(
            path=out.path,
            name=name,
            external_id=name,
            mime_type="text/csv",
            data_set_id=dataset,
            overwrite=True,
            metadata={"runId": run_id, "dimension": out.dimension, "sheets": json.dumps(dict(out.sheets))},
        )
        os.remove(out.path)
    external_id = workbook_external_id(run_id)
    client.files.upload(
        path=path,
        name=external_id,
        external_id=external_id,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        data_set_id=dataset,
        overwrite=True,
        metadata={"runId": run_id, "missingPieces": json.dumps(missing), "measured": json.dumps(sorted(this_run))},
    )
    os.remove(path)
    os.rmdir(folder)
    client.files.delete(external_id=list(present), ignore_unknown_ids=True)
    replaced = [
        file.external_id
        for file in every_dimension_file
        if file.dimension in sources and file.run_id != sources[file.dimension]
    ]
    if replaced:
        client.files.delete(external_id=replaced, ignore_unknown_ids=True)
    removed = remove_old_workbooks(client, cfg, dataset, external_id, now or datetime.now(UTC))
    return {
        "exported": True,
        "externalId": external_id,
        "sheets": len(order),
        "rows": sum(counts.values()),
        "missingPieces": missing,
        "measured": sorted(this_run),
        "keptFrom": {dimension: old.run_id for dimension, old in earlier.items()},
        "removed": removed,
    }


def _write_summary(
    sheet: Any,
    header: Any,
    totals: dict[tuple[str, str, str], list[int]],
    counts: dict[str, int],
    order: list[str],
    missing: list[int],
    run_id: str,
    sources: dict[str, str],
) -> None:
    sheet.write_row(0, 0, ("Dimension", "Metric", "Field", "Eligible", "Passing", "Remaining", "Share passing"), header)
    sheet.set_column(0, 2, 30)
    sheet.set_column(3, 6, 14)
    ordered = sorted(
        totals.items(),
        key=lambda item: (
            DIMENSION_ORDER.index(item[0][0]) if item[0][0] in DIMENSION_ORDER else len(DIMENSION_ORDER),
            item[0][1:],
        ),
    )
    row = 1
    for (dim, metric, sub), (eligible, passing, issue) in ordered:
        share = f"{round(100 * passing / eligible)}%" if eligible else ""
        sheet.write_row(
            row,
            0,
            (DIMENSION_NAMES.get(dim, dim), METRIC_LABELS.get(metric, metric), sub, eligible, passing, issue, share),
        )
        row += 1
    row += 1
    sheet.write_row(row, 0, ("List (sheet)", "Rows"), header)
    for title in order:
        row += 1
        sheet.write_row(row, 0, (title, counts[title]))
    row += 2
    sheet.write_row(row, 0, ("Dimension", "Measured in run"), header)
    for dimension in (*DIMENSION_ORDER, CONFIGURED):
        if dimension in sources:
            row += 1
            sheet.write_row(row, 0, (DIMENSION_NAMES.get(dimension, "Configured metrics"), sources[dimension]))
    row += 2
    sheet.write(row, 0, f"Run {run_id}")
    if missing:
        sheet.write(
            row + 1, 0, f"Incomplete: the lists of {len(missing)} parts of the run could not be exported ({missing})."
        )


def remove_old_workbooks(client: CogniteClient, cfg: RunConfig, dataset: int, keep: str, now: datetime) -> int:
    """Deletes the workbooks of runs older than `exportRetentionDays`. -> how many."""
    cutoff = int((now - timedelta(days=cfg["exportRetentionDays"])).timestamp() * 1000)
    old = [
        file.external_id
        for file in client.files.list(external_id_prefix=WORKBOOK_PREFIX, data_set_ids=[dataset], limit=-1)
        if file.external_id
        and file.external_id != keep
        and not file.external_id.startswith((PART_PREFIX, DIMENSION_PREFIX))
        and (file.created_time or 0) < cutoff
    ]
    if old:
        client.files.delete(external_id=old, ignore_unknown_ids=True)
    return len(old)
