"""Unit tests for file_content_query."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from ui.server import file_content_query


def test_validate_select_only_allows_select():
    file_content_query.validate_select_only_query("SELECT * FROM data")
    file_content_query.validate_select_only_query("WITH x AS (SELECT 1) SELECT * FROM x")


def test_validate_select_only_rejects_dml():
    with pytest.raises(ValueError, match="read-only"):
        file_content_query.validate_select_only_query("DELETE FROM data")
    with pytest.raises(ValueError, match="read-only"):
        file_content_query.validate_select_only_query("INSERT INTO t SELECT 1")


def test_validate_select_only_rejects_multiple_statements():
    with pytest.raises(ValueError, match="single SELECT"):
        file_content_query.validate_select_only_query("SELECT 1; SELECT 2")


def test_detect_format_from_metadata():
    parquet = MagicMock(mime_type="application/x-parquet", name="data.parquet")
    csv = MagicMock(mime_type="text/csv", name="export.csv")
    json_file = MagicMock(mime_type="application/json", name="rows.jsonl")
    pdf = MagicMock(mime_type="application/pdf", name="doc.pdf")

    assert file_content_query.detect_format_from_metadata(parquet) == "parquet"
    assert file_content_query.detect_format_from_metadata(csv) == "csv"
    assert file_content_query.detect_format_from_metadata(json_file) == "json"
    assert file_content_query.detect_format_from_metadata(pdf) is None


def test_run_file_content_sql_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name\n1,alpha\n2,beta\n", encoding="utf-8")

    file_obj = MagicMock(
        id=42,
        mime_type="text/csv",
        name="sample.csv",
        uploaded=True,
        uploaded_time=None,
    )

    client = MagicMock()
    client.files.retrieve.return_value = file_obj
    client.files.download_bytes.return_value = csv_path.read_bytes()

    monkeypatch.setattr(file_content_query, "_module_cache_dir", lambda: tmp_path)

    out = file_content_query.run_file_content_sql(
        client,
        query="SELECT * FROM data WHERE id = 1",
        limit=10,
        file_id=42,
        fmt="csv",
    )

    assert out["columns"] == ["id", "name"]
    assert out["row_count"] == 1
    assert out["items"][0]["name"] == "alpha"


def test_run_file_content_sql_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    parquet_path = tmp_path / "sample.parquet"
    con = duckdb.connect()
    try:
        con.execute("COPY (SELECT 10 AS id, 'x' AS label) TO ? (FORMAT PARQUET)", [str(parquet_path)])
    finally:
        con.close()

    file_obj = MagicMock(
        id=99,
        mime_type="application/x-parquet",
        name="sample.parquet",
        uploaded=True,
        uploaded_time=None,
    )

    client = MagicMock()
    client.files.retrieve.return_value = file_obj
    client.files.download_bytes.return_value = parquet_path.read_bytes()

    monkeypatch.setattr(file_content_query, "_module_cache_dir", lambda: tmp_path)

    out = file_content_query.run_file_content_sql(
        client,
        query="SELECT label FROM data",
        limit=5,
        file_id=99,
        fmt="parquet",
    )

    assert out["row_count"] == 1
    assert out["items"][0]["label"] == "x"


def test_run_file_content_sql_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    json_path = tmp_path / "sample.ndjson"
    json_path.write_text('{"id": 1, "name": "alpha"}\n{"id": 2, "name": "beta"}\n', encoding="utf-8")

    file_obj = MagicMock(
        id=7,
        mime_type="application/x-ndjson",
        name="sample.ndjson",
        uploaded=True,
        uploaded_time=None,
    )

    client = MagicMock()
    client.files.retrieve.return_value = file_obj
    client.files.download_bytes.return_value = json_path.read_bytes()

    monkeypatch.setattr(file_content_query, "_module_cache_dir", lambda: tmp_path)

    out = file_content_query.run_file_content_sql(
        client,
        query="SELECT name FROM data WHERE id = 2",
        limit=10,
        file_id=7,
        fmt="json",
    )

    assert out["columns"] == ["name"]
    assert out["row_count"] == 1
    assert out["items"][0]["name"] == "beta"
