"""The workbook of every unlinked item of a run: the parts the pieces upload, and the workbook built from them."""

import csv
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from typing import Any
from xml.etree import ElementTree

import pytest
from cq import export
from cq.config import build_config
from cq.types import DrillItem

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
RUN = "run-20261013T053425-c2326a"


class File:
    def __init__(self, external_id: str, metadata: dict[str, str] | None = None, created: int = 0) -> None:
        self.external_id, self.metadata, self.created_time = external_id, metadata, created


class FakeFiles:
    """Cognite Files as far as the export uses it."""

    def __init__(self) -> None:
        self.content: dict[str, bytes] = {}
        self.meta: dict[str, dict[str, str]] = {}
        self.created: dict[str, int] = {}
        self.uploads: list[dict[str, Any]] = []

    def upload_bytes(self, content: bytes, external_id: str, metadata: dict[str, str], **kwargs: Any) -> None:
        self.content[external_id], self.meta[external_id] = content, metadata
        self.created.setdefault(external_id, int(datetime.now(UTC).timestamp() * 1000))
        self.uploads.append({"external_id": external_id, **kwargs})

    def upload(self, path: str, external_id: str, metadata: dict[str, str], **kwargs: Any) -> None:
        with open(path, "rb") as handle:
            self.content[external_id] = handle.read()
        self.meta[external_id] = metadata
        self.created.setdefault(external_id, int(datetime.now(UTC).timestamp() * 1000))
        self.uploads.append({"external_id": external_id, **kwargs})

    def retrieve_multiple(self, external_ids: list[str], ignore_unknown_ids: bool = False) -> list[File]:
        return [File(x, self.meta[x]) for x in external_ids if x in self.content]

    def download_bytes(self, external_id: str) -> bytes:
        return self.content[external_id]

    def delete(self, external_id: list[str], ignore_unknown_ids: bool = False) -> None:
        for x in external_id:
            self.content.pop(x, None)
            self.meta.pop(x, None)

    def list(self, external_id_prefix: str, data_set_ids: list[int], limit: int = 25) -> list[File]:
        return [File(x, self.meta[x], self.created[x]) for x in self.content if x.startswith(external_id_prefix)]


class FakeClient:
    def __init__(self, with_data_set: bool = True) -> None:
        self.files = FakeFiles()
        found = type("DataSet", (), {"id": 77})() if with_data_set else None
        self.data_sets = type("DataSets", (), {"retrieve": lambda _self, external_id: found})()


CFG = build_config({"exportDataSet": "ds_cq"})


def item(metric: str, xid: str, space: str = "sp_a", label: str | None = None, **extra: Any) -> DrillItem:
    base: dict[str, Any] = {
        "metric": metric,
        "dim": "equipment-asset",
        "g": {"unit": space, "facility": "alpha", "div": None, "disc": None, "tier": None},
        "id": xid,
        "label": label,
        "detail": "no linked asset",
        "owner": None,
        "classes": None,
    }
    return {**base, **extra}  # type: ignore[return-value]


def read_workbook(content: bytes) -> dict[str, list[list[str]]]:
    """The sheets of a workbook as lists of rows of text."""
    book = zipfile.ZipFile(io.BytesIO(content))
    workbook = ElementTree.fromstring(  # noqa: S314 - the workbook this test just built
        book.read("xl/workbook.xml")
    )
    names = [sheet.attrib["name"] for sheet in workbook.find("m:sheets", NS)]  # type: ignore[union-attr]
    shared: list[str] = []
    if "xl/sharedStrings.xml" in book.namelist():
        shared = [
            "".join(t.text or "" for t in si.iter(f"{{{NS['m']}}}t"))
            for si in ElementTree.fromstring(  # noqa: S314 - the workbook this test just built
                book.read("xl/sharedStrings.xml")
            )
        ]
    out: dict[str, list[list[str]]] = {}
    for index, name in enumerate(names, start=1):
        sheet = ElementTree.fromstring(  # noqa: S314 - the workbook this test just built
            book.read(f"xl/worksheets/sheet{index}.xml")
        )
        rows = []
        for row in sheet.iter(f"{{{NS['m']}}}row"):
            cells: dict[int, str] = {}  # blank cells are not written, so a cell is placed by its reference
            for cell in row:
                column = ord(cell.attrib["r"].rstrip("0123456789")) - ord("A")
                if cell.attrib.get("t") == "inlineStr":
                    cells[column] = "".join(t.text or "" for t in cell.iter(f"{{{NS['m']}}}t"))
                elif cell.attrib.get("t") == "s":
                    cells[column] = shared[int(cell.find("m:v", NS).text)]  # type: ignore[union-attr, arg-type]
                else:
                    cells[column] = cell.find("m:v", NS).text or ""  # type: ignore[union-attr]
            rows.append([cells.get(i, "") for i in range(max(cells) + 1)] if cells else [])
        out[name] = rows
    return out


def test_every_list_the_function_writes_has_a_sheet() -> None:
    from cq.config import build_config
    from cq.metrics import compute
    from parity_dataset import build_dataset
    from sample_rules import SAMPLE_RESOURCES

    data, latest, now = build_dataset()
    lists = {d["metric"] for d in compute(data, build_config(SAMPLE_RESOURCES), latest, now=now)["drilldown"]}

    assert lists <= set(export.SHEETS)
    assert len({title for title, _ in export.SHEETS.values()}) == len(export.SHEETS)
    assert all(len(title) <= 31 for title, _ in export.SHEETS.values())


def test_an_item_becomes_a_row_of_the_sheet_of_its_list_and_what_has_no_sheet_is_left_out() -> None:
    rows = export.item_rows(
        [
            item("unmatchedWithCandidates", "e1", label="Pump E-1"),
            item("noSheetForThis", "x"),
            item("unlinkedByCategory", "f1", classes={"fileCategory": "Datasheet"}, owner="steward"),
        ]
    )

    assert rows == [
        ("item", "Equipment without asset", "Pump E-1", "e1", "sp_a", "alpha", "no linked asset", "", "", ""),
        (
            "item",
            "Files without asset",
            "",
            "f1",
            "sp_a",
            "alpha",
            "no linked asset",
            "steward",
            "",
            "fileCategory: Datasheet",
        ),
    ]


def test_a_configured_link_gets_a_sheet_named_after_it_that_excel_accepts() -> None:
    gap = {
        "model": "m:M:v1",
        "view": "CogniteAsset",
        "property": "type",
        "space": "sp_a",
        "external_id": "a1",
        "label": "Pump",
    }

    (row,) = export.item_rows([], [gap])

    assert row[1] == "CogniteAsset.type"
    assert export.link_sheet("A" * 40, "p") == ("A" * 40 + ".p")[:31]
    assert export.link_sheet("V:iew", "pr/op") == "V_iew.pr_op"


def test_a_pieces_summary_adds_up_its_rows_per_metric() -> None:
    agg = {
        ("associationRate", "equipment-asset", "sp_a", "alpha", None, None, None, None, None): [10, 8, 2, 0.0],
        ("associationRate", "equipment-asset", "sp_b", "beta", None, None, None, 1, None): [5, 5, 0, 0.0],
        ("serialMfrCompleteness", "equipment-asset", "sp_a", "alpha", None, None, None, None, "serialNumber"): [
            10,
            7,
            3,
            0.0,
        ],
    }

    rows = export.summary_rows(agg)

    assert sorted(rows) == sorted(
        [
            ("summary", "equipment-asset", "associationRate", "", "15", "13", "2", "", "", ""),
            ("summary", "equipment-asset", "serialMfrCompleteness", "serialNumber", "10", "7", "3", "", "", ""),
        ]
    )


def test_a_part_is_a_csv_in_the_data_set_whose_metadata_counts_the_rows_of_each_sheet() -> None:
    client = FakeClient()
    rows = export.item_rows([item("unmatchedWithCandidates", "e1"), item("unmatchedWithCandidates", "e2")])

    export.write_part(client, 77, RUN, 3, rows)  # type: ignore[arg-type]

    name = export.part_external_id(RUN, 3)
    (upload,) = client.files.uploads
    assert (upload["external_id"], upload["data_set_id"], upload["overwrite"]) == (name, 77, True)
    assert json.loads(client.files.meta[name]["sheets"]) == {"Equipment without asset": 2}
    assert len(list(csv.reader(io.StringIO(client.files.content[name].decode())))) == 3  # header and two rows


def parts(client: FakeClient, *pieces: list[tuple[str, ...]]) -> None:
    for index, rows in enumerate(pieces):
        export.write_part(client, 77, RUN, index, rows)  # type: ignore[arg-type]


def test_the_workbook_has_a_summary_and_a_sheet_for_each_list_with_the_rows_of_all_the_parts() -> None:
    client = FakeClient()
    agg = {("associationRate", "equipment-asset", "sp_a", "alpha", None, None, None, None, None): [10, 8, 2, 0.0]}
    parts(
        client,
        [
            *export.item_rows([item("unmatchedWithCandidates", "e1", label="E-1"), item("unlinkedByCategory", "f1")]),
            *export.summary_rows(agg),
        ],
        export.item_rows([item("unmatchedWithCandidates", "e2", space="sp_b")]),
    )

    out = export.build_workbook(client, CFG, RUN, 2)  # type: ignore[arg-type]

    sheets = read_workbook(client.files.content[export.workbook_external_id(RUN)])
    assert list(sheets) == ["Summary", "Equipment without asset", "Files without asset"]
    assert [row[1] for row in sheets["Equipment without asset"]] == ["External ID", "e1", "e2"]
    assert sheets["Equipment without asset"][0] == list(export.COLUMNS)
    assert sheets["Files without asset"][1][1] == "f1"
    summary = sheets["Summary"]
    assert summary[0][:4] == ["Dimension", "Metric", "Field", "Eligible"]
    assert summary[1][:6] == ["Equipment-Asset", "Equipment linked to an asset", "", "10", "8", "2"]
    assert ["Equipment without asset", "2"] in summary
    assert out == {
        "exported": True,
        "externalId": export.workbook_external_id(RUN),
        "sheets": 2,
        "rows": 3,
        "missingPieces": [],
        "removed": 0,
    }


def test_the_parts_are_removed_once_the_workbook_is_uploaded() -> None:
    client = FakeClient()
    parts(client, export.item_rows([item("unmatchedWithCandidates", "e1")]))

    export.build_workbook(client, CFG, RUN, 1)  # type: ignore[arg-type]

    assert list(client.files.content) == [export.workbook_external_id(RUN)]


def test_a_part_that_never_arrived_is_named_in_the_workbook_and_does_not_stop_it() -> None:
    client = FakeClient()
    parts(client, export.item_rows([item("unmatchedWithCandidates", "e1")]))

    out = export.build_workbook(client, CFG, RUN, 3)  # type: ignore[arg-type]

    sheets = read_workbook(client.files.content[export.workbook_external_id(RUN)])
    assert out["missingPieces"] == [1, 2]
    assert any("Incomplete" in cell for row in sheets["Summary"] for cell in row)
    assert client.files.meta[export.workbook_external_id(RUN)]["missingPieces"] == "[1, 2]"


def test_a_run_with_no_unlinked_items_still_gets_a_workbook_with_a_summary() -> None:
    client = FakeClient()
    parts(client, [])

    out = export.build_workbook(client, CFG, RUN, 1)  # type: ignore[arg-type]

    assert list(read_workbook(client.files.content[export.workbook_external_id(RUN)])) == ["Summary"]
    assert out["rows"] == 0


def test_workbooks_older_than_the_retention_are_deleted_and_the_new_one_is_kept() -> None:
    client = FakeClient()
    now = datetime.now(UTC)
    for name, age in (("cq-unlinked-run-old.xlsx", 40), ("cq-unlinked-run-recent.xlsx", 5)):
        client.files.content[name], client.files.meta[name] = b"x", {}
        client.files.created[name] = int((now - timedelta(days=age)).timestamp() * 1000)
    parts(client, export.item_rows([item("unmatchedWithCandidates", "e1")]))

    out = export.build_workbook(client, CFG, RUN, 1, now)  # type: ignore[arg-type]

    assert out["removed"] == 1
    assert sorted(client.files.content) == sorted(["cq-unlinked-run-recent.xlsx", export.workbook_external_id(RUN)])


def test_nothing_is_exported_without_a_data_set() -> None:
    client = FakeClient()
    cfg = build_config({})

    assert export.data_set_id(client, cfg) is None  # type: ignore[arg-type]
    assert export.build_workbook(client, cfg, RUN, 1)["exported"] is False  # type: ignore[arg-type]
    assert client.files.uploads == []


def test_the_data_set_must_exist() -> None:
    assert export.data_set_id(FakeClient(with_data_set=False), CFG) is None  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [3, None, ["x"]])
def test_the_data_set_setting_must_be_text(bad: Any) -> None:
    with pytest.raises(ValueError, match="exportDataSet"):
        build_config({"exportDataSet": bad})
