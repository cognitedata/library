"""Unit tests for file_download."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ui.server import file_download


def test_probe_download_size_from_head():
    file_obj = MagicMock()
    file_obj.id = 7
    file_obj.name = "data.csv"
    file_obj.mime_type = "text/csv"
    file_obj.uploaded = True
    client = MagicMock()
    client.config.file_transfer_timeout = 30
    client.files._get_download_link.return_value = "https://storage.example/file"
    head_resp = MagicMock()
    head_resp.headers = {"Content-Length": "12345"}
    client.files._http_client_with_retry.request.return_value = head_resp

    size, name, mime = file_download.probe_download_size_bytes(client, file_obj)
    assert size == 12345
    assert name == "data.csv"
    assert mime == "text/csv"
    client.files._http_client_with_retry.request.assert_called_once()
    assert client.files._http_client_with_retry.request.call_args.args[0] == "HEAD"


def test_probe_download_size_from_content_range_fallback():
    file_obj = MagicMock()
    file_obj.id = 8
    file_obj.name = "big.parquet"
    file_obj.mime_type = "application/octet-stream"
    file_obj.uploaded = True
    client = MagicMock()
    client.config.file_transfer_timeout = 30
    client.files._get_download_link.return_value = "https://storage.example/file"

    head_resp = MagicMock(headers={})
    range_resp = MagicMock(headers={"Content-Range": "bytes 0-0/9876543"})
    client.files._http_client_with_retry.request.side_effect = [head_resp, range_resp]

    size, _, _ = file_download.probe_download_size_bytes(client, file_obj)
    assert size == 9876543
    assert client.files._http_client_with_retry.request.call_count == 2


def test_download_file_bytes(monkeypatch: pytest.MonkeyPatch):
    file_obj = MagicMock()
    file_obj.id = 9
    file_obj.name = "doc.pdf"
    file_obj.mime_type = "application/pdf"
    file_obj.uploaded = True
    client = MagicMock()
    client.files.download_bytes.return_value = b"%PDF-1.4"
    monkeypatch.setattr(file_download, "resolve_uploaded_file", lambda _c, **_: file_obj)

    data, name, mime = file_download.download_file_bytes(client, file_id=9)
    assert data == b"%PDF-1.4"
    assert name == "doc.pdf"
    assert mime == "application/pdf"


def test_resolve_uploaded_file_rejects_missing_identity():
    client = MagicMock()
    with pytest.raises(ValueError, match="file_id or file_external_id"):
        file_download.resolve_uploaded_file(client)


def test_download_file_bytes_by_instance(monkeypatch: pytest.MonkeyPatch):
    from cognite.client.data_classes.data_modeling import NodeId

    file_obj = MagicMock()
    file_obj.id = None
    file_obj.instance_id = NodeId("cdf_cdm", "my-drawing")
    file_obj.name = "drawing.pdf"
    file_obj.mime_type = "application/pdf"
    file_obj.uploaded = True
    client = MagicMock()
    client.files.download_bytes.return_value = b"%PDF-1.4"
    monkeypatch.setattr(
        file_download,
        "resolve_uploaded_file",
        lambda _c, **_: file_obj,
    )

    data, name, mime = file_download.download_file_bytes(
        client, file_instance_space="cdf_cdm", file_external_id="my-drawing"
    )
    assert data == b"%PDF-1.4"
    assert name == "drawing.pdf"
    assert mime == "application/pdf"
    client.files.download_bytes.assert_called_once()
    call_kwargs = client.files.download_bytes.call_args.kwargs
    assert call_kwargs["id"] is None
    assert call_kwargs["external_id"] is None
    assert call_kwargs["instance_id"] == NodeId("cdf_cdm", "my-drawing")
